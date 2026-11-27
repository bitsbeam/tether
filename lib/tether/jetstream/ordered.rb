# frozen_string_literal: true

module Tether
  class JetStream
    # Strictly ordered, gap-free delivery from a stream, without acknowledgments.
    #
    # An ordered consumer is a client-side construct: an ephemeral,
    # memory-backed server consumer with `ack_policy: :none`, checked on every
    # delivery. When a consumer sequence is not the previous plus one, when
    # heartbeats stop, when the server consumer disappears or the connection is
    # re-established, a new server consumer is created right after the last
    # message handed out, so the reader sees one uninterrupted stream.
    #
    # Nothing exists on the server until {#consume}, {#messages} or {#fetch} is called.
    class Ordered
      INACTIVE_THRESHOLD = 300
      RECOVERABLE = [ConsumerDeletedError, ConsumerNotFoundError, ServiceUnavailableError, NoHeartbeatError].freeze

      # Where a reader is: the last stream sequence handed out and the consumer sequence it came with.
      Position = Struct.new(:stream_sequence, :consumer_sequence)

      # @return [String] the name of the stream read from
      attr_reader :stream

      # @return [JetStream] the context this belongs to
      attr_reader :jetstream

      # @param jetstream [JetStream] the context
      # @param stream [String] the stream's name
      # @param filter_subjects [Array<String>, String, nil] only messages on subjects matching these filters
      # @param deliver_policy [Symbol, nil] where to start: :all, :last, :new, :last_per_subject,
      #   :by_start_sequence or :by_start_time; derived from the start options when omitted
      # @param start_sequence [Integer, nil] the stream sequence to start at
      # @param start_time [Time, nil] start at the first message stored at or after this time
      # @param headers_only [Boolean] deliver headers and the payload size, without payloads
      # @param replay_policy [Symbol, nil] :instant or :original
      # @param inactive_threshold [Numeric] seconds the server keeps an unused server consumer
      # @param name_prefix [String, nil] the prefix of the server consumers' names; generated when omitted
      # @param metadata [Hash{String => String}, nil] metadata for the server consumers
      # @raise [ArgumentError] if the start options contradict each other
      def initialize(jetstream, stream, filter_subjects: nil, deliver_policy: nil, start_sequence: nil,
                     start_time: nil, headers_only: false, replay_policy: nil,
                     inactive_threshold: INACTIVE_THRESHOLD, name_prefix: nil, metadata: nil)
        raise ArgumentError, "start_sequence: and start_time: cannot be combined" if start_sequence && start_time

        @jetstream = jetstream
        @stream = stream
        @prefix = jetstream.check_name(name_prefix || Connection::NUID.next)
        @start = { deliver_policy: deliver_policy || start_policy(start_sequence, start_time),
                   start_sequence: start_sequence, start_time: start_time }.compact
        @base = { ack_policy: :none, memory_storage: true, replicas: 1, inactive_threshold: inactive_threshold,
                  headers_only: headers_only || nil, replay_policy: replay_policy, metadata: metadata }
                .merge(filters(Array(filter_subjects))).compact
        @serial = 0
        @lock = Mutex.new
        @fetching = Mutex.new
        @reader = nil
        @position = Position.new(0, 0)
      end

      # Delivers messages to a block, in stream order, on a worker thread.
      #
      # @param on_caught_up [#call, nil] called once, on the worker, when everything that existed when
      #   the consumer was created has been handled
      # @param options [Hash{Symbol => Object}] see {JetStream::Pull#initialize}; `concurrency:` must be 1
      # @yieldparam message [Message] each message, on the worker thread
      # @return [Ordered::Pull] a handle to stop, drain or wait for the loop
      # @raise [StreamNotFoundError] if the stream does not exist
      def consume(on_caught_up: nil, **options, &block)
        raise ArgumentError, "consume needs a block; use messages to iterate" unless block

        Pull.new(self, block, on_caught_up: on_caught_up, **options)
      end

      # Starts ordered delivery driven by the caller: take messages with
      # {JetStream::Pull#next} or {JetStream::Pull#each}.
      #
      # @param on_caught_up [#call, nil] called once, on the calling thread, before {JetStream::Pull#next}
      #   returns the last message that existed when the consumer was created
      # @param options [Hash{Symbol => Object}] see {JetStream::Pull#initialize}, except `concurrency:`
      # @return [Ordered::Pull] the iterator
      # @raise [StreamNotFoundError] if the stream does not exist
      def messages(on_caught_up: nil, **options)
        Pull.new(self, nil, on_caught_up: on_caught_up, **options)
      end

      # Pulls one bounded batch, continuing after the last message an earlier
      # fetch on this object returned.
      #
      # @param batch [Integer, nil] the most messages to return; may be left out when `max_bytes:` bounds the batch
      # @param timeout [Numeric, Deadline, nil] how long to wait for the batch to fill; defaults to 30 seconds
      # @param max_bytes [Integer, nil] end the batch before it exceeds this many bytes
      # @param wait [Boolean] false returns what is available now
      # @yieldparam message [Message] each message, in stream order
      # @return [Batch] the messages, possibly fewer than asked, possibly none
      # @raise [ArgumentError] if neither a batch nor `max_bytes:` is given
      def fetch(batch = nil, timeout: nil, max_bytes: nil, wait: true, &block)
        batch ||= max_bytes ? Pull::Buffer::BYTES_BATCH : raise(ArgumentError, "fetch needs a batch or max_bytes:")
        timeout ||= Fetch::DEFAULT_TIMEOUT if wait
        deadline = timeout && Deadline.coerce(timeout)
        @fetching.synchronize do
          result = in_order(fetch_once(batch, deadline, max_bytes, wait))
          result.each(&block) if block
          result
        end
      end

      # Deletes the server consumer kept between fetches. Loops started with
      # {#consume} or {#messages} remove theirs when they stop.
      #
      # @return [self] this ordered consumer
      def close
        @fetching.synchronize { abandon_reader }
        self
      end

      # @return [String] the stream and name prefix
      def inspect
        "#<Tether::JetStream::Ordered stream=#{@stream.inspect} prefix=#{@prefix.inspect}>"
      end

      # Creates the next server consumer.
      #
      # @api private
      # @param after [Integer] the last stream sequence handed out; 0 starts where the options say
      # @param deadline [Deadline, nil] how long to wait
      # @return [Consumer] a handle to the new server consumer
      def create(after:, deadline: nil)
        serial = @lock.synchronize { @serial += 1 }
        start = after.positive? ? { deliver_policy: :by_start_sequence, start_sequence: after + 1 } : @start
        @jetstream.create_consumer(@stream, name: "#{@prefix}_#{serial}", timeout: deadline, **@base, **start)
      end

      # Deletes a superseded server consumer without waiting for the answer.
      #
      # @api private
      # @param consumer [Consumer, nil] the server consumer that is no longer read from
      # @return [void] nothing
      def discard(consumer)
        return unless consumer

        api = @jetstream.api
        api.client.publish("#{api.prefix}.CONSUMER.DELETE.#{@stream}.#{consumer.name}")
        nil
      rescue Tether::Error
        nil
      end

      private

      def start_policy(start_sequence, start_time)
        return :by_start_sequence if start_sequence
        return :by_start_time if start_time

        :all
      end

      def filters(subjects)
        return { filter_subject: subjects.first } if subjects.size == 1
        return { filter_subjects: subjects } if subjects.size > 1

        @start[:deliver_policy] == :last_per_subject ? { filter_subject: ">" } : {}
      end

      def fetch_once(batch, deadline, max_bytes, wait)
        retried = false
        begin
          @reader ||= create(after: @position.stream_sequence, deadline: deadline)
          @reader.fetch(batch, timeout: deadline, max_bytes: max_bytes, wait: wait)
        rescue *RECOVERABLE
          abandon_reader
          raise if retried

          retried = true
          retry
        end
      end

      def in_order(result)
        accepted = result.take_while do |message|
          meta = message.metadata
          next false unless meta.consumer_sequence == @position.consumer_sequence + 1

          @position.consumer_sequence = meta.consumer_sequence
          @position.stream_sequence = meta.stream_sequence
        end
        gap = accepted.size < result.size || RECOVERABLE.any? { |error| result.error.is_a?(error) }
        abandon_reader if gap
        gap || result.error.nil? ? Batch.new(accepted) : Batch.new(accepted, result.error)
      end

      def abandon_reader
        discard(@reader)
        @reader = nil
        @position.consumer_sequence = 0
      end
    end
  end
end

require "tether/jetstream/ordered/pull"
