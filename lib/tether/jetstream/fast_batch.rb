# frozen_string_literal: true

require "json"

module Tether
  class JetStream
    # A fast-ingest batch: messages are stored as they arrive, and the server
    # acknowledges them a window at a time instead of one by one.
    #
    # All control information travels in the reply subject of each publish,
    # on an inbox of the batch's own. The batch waits for the server's first
    # flow acknowledgment before sending more, then publishes freely while it
    # is less than a few acknowledgment windows ahead of the server.
    class FastBatch
      # A message the server refused.
      Failure = Data.define(:sequence, :error)

      SUFFIX = "$FI"
      START = 0
      APPEND = 1
      END_OF_BATCH = 3
      PING = 4
      GAPS = { fail: "fail", ok: "ok" }.freeze
      MAX_FLOW = 65_535
      WINDOWS = (1..3)
      PING_INTERVAL = 1
      API_LEVEL = 4

      # @return [String] the generated batch id
      attr_reader :id

      # @return [Integer] messages published into the batch so far
      attr_reader :size

      # @return [Integer] the highest batch sequence the server has acknowledged
      attr_reader :acknowledged

      # @return [Array<Failure>] messages the server refused, each with its batch sequence and error
      attr_reader :errors

      # @return [Array<Range>] batch sequences the server did not receive
      attr_reader :gaps

      # @return [PubAck, nil] the final acknowledgment, once the batch has ended
      attr_reader :ack

      # Runs a block with a new batch, then commits it.
      #
      # @param jetstream [JetStream] the context
      # @param options [Hash{Symbol => Object}] see {#initialize}
      # @yieldparam batch [FastBatch] the open batch
      # @return [PubAck, nil] the final acknowledgment; nil when nothing was published
      def self.open(jetstream, **options)
        batch = new(jetstream, **options)
        begin
          yield batch
          batch.commit
        ensure
          batch.close
        end
      end

      # @param jetstream [JetStream] the context
      # @param gap [Symbol] :fail ends the batch on the first lost or refused message, :ok collects them
      # @param flow [Integer] the largest number of messages per acknowledgment the server may choose
      # @param windows [Integer] acknowledgment windows that may be outstanding, 1 to 3
      # @param timeout [Numeric, Deadline] how long each wait for the server may take
      # @raise [ArgumentError] if an option is out of range
      def initialize(jetstream, timeout:, gap: :fail, flow: 100, windows: 2)
        @gap = GAPS.fetch(gap) { raise ArgumentError, "gap: must be :fail or :ok" }
        raise ArgumentError, "flow: must be between 1 and #{MAX_FLOW}" unless (1..MAX_FLOW).cover?(flow)
        raise ArgumentError, "windows: must be 1, 2 or 3" unless WINDOWS.cover?(windows)

        @client = jetstream.client
        @timeout = timeout
        @flow = flow
        @windows = windows
        @id = Connection::NUID.next
        @lock = Mutex.new
        @replies = Thread::Queue.new
        prepare
      end

      # Publishes a message into the batch, waiting first when the batch is
      # too far ahead of the server's acknowledgments.
      #
      # @param subject [String] a subject of the stream the batch goes to
      # @param payload [String, nil] the message body
      # @param headers [Protocol::Headers, Hash, nil] headers to send with the message
      # @param options [Hash{Symbol => Object}] the options of {JetStream#publish}: `msg_id:`, `expect:`,
      #   `ttl:`, `rollup:`, `schedule:`, `cancel_schedule:`
      # @return [self] this batch
      # @raise [APIError] the mapped error when the server refused the batch, or a message with `gap: :fail`
      # @raise [MessageGapError] if the server missed a message with `gap: :fail`
      # @raise [NoStreamResponseError] if no stream captures the first subject
      # @raise [TimeoutError] if the server stopped acknowledging
      # @raise [DisconnectedError, ConnectionClosedError] if the connection went away
      def publish(subject, payload = nil, headers: nil, **options)
        headers = PublishHeaders.build(headers, **options)
        @lock.synchronize do
          raise Error, "the batch is closed" if @closed

          deadline = Deadline.coerce(@timeout)
          @size.zero? ? start(subject, payload, headers, deadline) : append(subject, payload, headers, deadline)
        end
        self
      end

      # Ends the batch and waits for the final acknowledgment.
      #
      # @api private
      # @return [PubAck, nil] the final acknowledgment; nil when nothing was published
      # @raise [APIError, MessageGapError] what ended the batch early
      # @raise [TimeoutError] if the server did not answer, which leaves the outcome unknown
      def commit
        @lock.synchronize do
          raise Error, "the batch is closed" if @closed

          @closed = true
          next nil if @size.zero?

          deadline = Deadline.coerce(@timeout)
          drain
          send_message(@subject, nil, nil, @size + 1, END_OF_BATCH) unless @ended || @failure
          conclude(deadline)
          @ack
        end
      end

      # Stops listening for the server. A batch that was not committed is
      # ended without waiting; what it already stored stays in the stream.
      #
      # @api private
      # @return [void] nothing
      def close
        @lock.synchronize do
          send_message(@subject, nil, nil, @size + 1, END_OF_BATCH) unless @closed || @ended || @failure || @size.zero?
        rescue Tether::Error
          nil
        ensure
          @closed = true
          @listener&.cancel
          @resolver&.unsubscribe
        end
        nil
      end

      # @return [String] the id, size and what the server acknowledged
      def inspect
        "#<Tether::JetStream::FastBatch id=#{@id.inspect} size=#{@size} acknowledged=#{@acknowledged}>"
      end

      private

      def prepare
        @size = 0
        @acknowledged = 0
        @cadence = nil
        @errors = []
        @gaps = []
        @ack = nil
        @failure = nil
        @ended = false
        @closed = false
        @subject = nil
        @prefix = nil
        @resolver = nil
        @listener = nil
      end

      def start(subject, payload, headers, deadline)
        listen
        headers ||= Protocol::Headers.new
        headers[API::LEVEL_HEADER] = API_LEVEL.to_s
        send_message(subject, payload, headers, 1, START)
        @subject = subject
        @size = 1
        receive(deadline) until @cadence || @failure || @ended
        check(deadline)
      end

      def append(subject, payload, headers, deadline)
        drain
        check(deadline)
        sequence = @size + 1
        until @acknowledged + (@cadence * @windows) >= sequence
          receive(deadline)
          check(deadline)
        end
        send_message(subject, payload, headers, sequence, APPEND, flush: false)
        @subject = subject
        @size = sequence
      end

      def listen
        replies = @replies
        @prefix = "#{@client.new_inbox}.#{@id}"
        @resolver = @client.resolver("#{@prefix}.>") { |msg| replies.push(msg) }
        @listener = @client.listen { |event, _| interrupted(replies, event) }
      end

      def interrupted(replies, event)
        case event
        when :disconnect then replies.push(DisconnectedError.new("connection lost during a fast-ingest batch"))
        when :close then replies.push(ConnectionClosedError.new("connection closed during a fast-ingest batch"))
        end
      end

      def send_message(subject, payload, headers, sequence, operation, flush: true)
        reply_to = "#{@prefix}.#{@flow}.#{@gap}.#{sequence}.#{operation}.#{SUFFIX}"
        @client.publish(subject, payload, reply_to: reply_to, headers: headers, flush: flush)
      end

      def drain
        handle(@replies.pop) until @replies.empty?
      end

      def receive(deadline)
        reply = @replies.pop(timeout: deadline.with_at_most(PING_INTERVAL).remaining)
        return handle(reply) if reply

        deadline.check!("the server stopped acknowledging fast-ingest batch #{@id}")
        send_message(@subject, nil, nil, @size, PING)
      end

      def check(deadline)
        return unless @failure || @ended

        conclude(deadline)
        raise Error, "the server ended fast-ingest batch #{@id}"
      end

      def conclude(deadline)
        begin
          receive(deadline) until @ended
        rescue TimeoutError
          raise unless @failure
        end
        raise @failure if @failure
      end

      def handle(reply)
        return if @ended
        raise reply if reply.is_a?(Exception)
        return unanswered(reply) if reply.no_responders?

        body = JSON.parse(reply.data)
        case body["type"]
        when "ack" then acknowledge(body)
        when "gap" then missed(body["last_seq"].to_i, body["seq"].to_i)
        when "err" then refused(body["seq"].to_i, APIError.for(body["error"] || {}))
        else finish(body)
        end
      rescue JSON::ParserError
        raise Error, "the server answered fast-ingest batch #{@id} with something that is not JSON"
      end

      def unanswered(reply)
        return if reply.subject.split(".")[-2] == APPEND.to_s

        @ended = true
        failed(NoStreamResponseError.new("no stream answered a batch publish to #{@subject}"))
      end

      def acknowledge(body)
        @acknowledged = [@acknowledged, body["seq"].to_i].max
        @cadence = body["msgs"].to_i.clamp(1, @flow)
      end

      def missed(expected, received)
        if received > expected
          @gaps << (expected..(received - 1))
          return unless @gap == GAPS[:fail]

          failed(MessageGapError.new("the server did not receive messages #{expected} to #{received - 1}"))
        else
          failed(MessageGapError.new("the server received message #{received} when it expected #{expected}"))
        end
      end

      def failed(error)
        @failure ||= error
        nil
      end

      def refused(sequence, error)
        @errors << Failure.new(sequence: sequence, error: error)
        failed(error) if @gap == GAPS[:fail]
      end

      def finish(body)
        @ended = true
        if body["error"]
          failed(APIError.for(body["error"]))
        elsif body["batch"] == @id
          @ack = PubAck.from_h(body)
        else
          failed(Error.new("the server answered with a plain acknowledgment: it does not support " \
                           "fast-ingest batches"))
        end
      end
    end
  end
end
