# frozen_string_literal: true

require "json"

module Tether
  class JetStream
    # A handle to a consumer on the server, carrying the last info read for it.
    #
    # Messages are read by pulling: {#fetch} and {#next} for a bounded batch,
    # {#consume} and {#messages} for continuous delivery. A push consumer made
    # by another client can be inspected and deleted, and reading from it
    # raises {PushConsumerError}.
    class Consumer
      GROUP = %r{\A[a-zA-Z0-9/_=-]{1,16}\z}
      PULL_FIELDS = %i[group min_pending min_ack_pending priority].freeze
      NANOSECONDS = 1_000_000_000

      # @return [String] the consumer's name
      attr_reader :name

      # @return [String] the name of the stream the consumer reads from
      attr_reader :stream

      # @return [JetStream] the context this handle belongs to
      attr_reader :jetstream

      # @return [ConsumerInfo] the info last read from the server
      attr_reader :cached_info

      # @param jetstream [JetStream] the context
      # @param info [ConsumerInfo] what the server returned for the consumer
      def initialize(jetstream, info)
        @jetstream = jetstream
        @api = jetstream.api
        @cached_info = info
        @name = info.name
        @stream = info.stream
        @subject = "#{@stream}.#{@name}"
        @pin_id = nil
        @lock = Mutex.new
      end

      # Reads the consumer's info from the server.
      #
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @return [ConsumerInfo] fresh info, also kept as {#cached_info}
      # @raise [ConsumerNotFoundError] if the consumer no longer exists
      def info(timeout: nil)
        @cached_info = ConsumerInfo.from_h(@api.request("CONSUMER.INFO.#{@subject}", timeout: timeout))
      end

      # @return [ConsumerConfig] the configuration from the cached info
      def config
        @cached_info.config
      end

      # @return [Boolean] whether this is a push consumer, which tether does not read from
      def push?
        config.push?
      end

      # Suspends delivery until a time, after which the consumer resumes by itself.
      #
      # @param time [Time] when delivery resumes
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @return [ConsumerPause] whether the consumer is paused, and until when
      def pause_until(time, timeout: nil)
        request = { "pause_until" => time.utc.iso8601(9) }
        ConsumerPause.from_h(@api.request("CONSUMER.PAUSE.#{@subject}", request, timeout: timeout, level: 1))
      end

      # Resumes delivery of a paused consumer.
      #
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @return [ConsumerPause] the pause state, no longer paused
      def resume(timeout: nil)
        ConsumerPause.from_h(@api.request("CONSUMER.PAUSE.#{@subject}", timeout: timeout, level: 1))
      end

      # Moves the consumer's position and forgets what is awaiting acknowledgment.
      #
      # @param sequence [Integer, nil] the stream sequence to deliver next; nil rewinds to the acknowledgment floor
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @return [self] this consumer, with fresh cached info
      def reset(sequence: nil, timeout: nil)
        request = sequence ? { "seq" => sequence } : nil
        @cached_info = ConsumerInfo.from_h(@api.request("CONSUMER.RESET.#{@subject}", request, timeout: timeout))
        self
      end

      # Makes the server choose a new pinned client for a priority group.
      #
      # @param group [String] the priority group
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @return [self] this consumer
      def unpin(group, timeout: nil)
        @api.request("CONSUMER.UNPIN.#{@subject}", { "group" => group.to_s }, timeout: timeout, level: 1)
        self
      end

      # Asks the consumer's leader to step down, on a clustered server.
      #
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @return [Boolean] whether the server confirmed it
      def step_down(timeout: nil)
        @api.request("CONSUMER.LEADER.STEPDOWN.#{@subject}", timeout: timeout)["success"] == true
      end

      # Deletes the consumer.
      #
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @return [Boolean] whether the server confirmed the deletion
      def delete(timeout: nil)
        @api.request("CONSUMER.DELETE.#{@subject}", timeout: timeout)["success"] == true
      end

      # Pulls one bounded batch. Finding nothing is not an error.
      #
      # @param batch [Integer] the most messages to return
      # @param timeout [Numeric, Deadline, nil] how long to wait for the batch to fill; defaults to 30 seconds,
      #   or to not waiting at all with `wait: false`
      # @param max_bytes [Integer, nil] end the batch before it exceeds this many bytes
      # @param wait [Boolean] false returns what is available now; with a timeout it waits only while empty
      # @param heartbeat [Numeric, nil] seconds between the server's idle heartbeats; 5 for a timeout of 10 or more
      # @param options [Hash{Symbol => Object}] priority group fields: `group:`, `min_pending:`,
      #   `min_ack_pending:`, `priority:`
      # @yieldparam message [Message] each message as it arrives
      # @return [Batch] the messages, possibly fewer than asked, possibly none
      # @raise [PushConsumerError] if this is a push consumer
      # @raise [ConsumerDeletedError, PullError, NoHeartbeatError] if the pull failed and delivered nothing
      def fetch(batch, timeout: nil, max_bytes: nil, wait: true, heartbeat: nil, **options, &block)
        timeout ||= Fetch::DEFAULT_TIMEOUT if wait
        Fetch.new(self, batch, deadline: timeout && Deadline.coerce(timeout), wait: wait, max_bytes: max_bytes,
                               heartbeat: heartbeat, **options).call(&block)
      end

      # Pulls one message.
      #
      # @param timeout [Numeric, Deadline, nil] how long to wait; defaults to 30 seconds
      # @return [Message] the next message
      # @raise [TimeoutError] if none arrives in time
      def next(timeout: nil)
        fetch(1, timeout: timeout).first or raise TimeoutError, "no message from #{@name} within the deadline"
      end

      # Delivers messages to a block, continuously, on worker threads.
      #
      # @param options [Hash{Symbol => Object}] see {Pull#initialize}: `max_messages:`, `max_bytes:`, `expires:`,
      #   `heartbeat:`, `threshold:`, `concurrency:`, `on_error:` and the priority group fields
      # @yieldparam message [Message] each message, on a worker thread
      # @return [Pull] a handle to stop, drain or wait for the loop
      # @raise [PushConsumerError] if this is a push consumer
      def consume(**options, &block)
        raise ArgumentError, "consume needs a block; use messages to iterate" unless block

        Pull.new(self, block, **options)
      end

      # Starts continuous delivery driven by the caller: take messages with
      # {Pull#next} or {Pull#each}.
      #
      # @param options [Hash{Symbol => Object}] see {Pull#initialize}, except `concurrency:`
      # @return [Pull] the iterator
      # @raise [PushConsumerError] if this is a push consumer
      def messages(**options)
        Pull.new(self, nil, **options)
      end

      # @return [String] the stream and name without the cached info
      def inspect
        "#<Tether::JetStream::Consumer stream=#{@stream.inspect} name=#{@name.inspect}>"
      end

      # @api private
      # @return [String, nil] the pin id of a pinned-client priority group, shared by every pull of this handle
      def pin_id
        @lock.synchronize { @pin_id }
      end

      # @api private
      # @param id [String, nil] the pin id to send with pulls from now on
      # @return [void] nothing
      def pin_id=(id)
        @lock.synchronize { @pin_id = id }
      end

      # Wraps a delivered message, remembering the pin id it carries.
      #
      # @api private
      # @param msg [Protocol::Msg] a user message from a pull
      # @param pull [Pull, nil] the loop that received it
      # @return [Message] the message
      def wrap(msg, pull: nil)
        if config.priority_policy == :pinned_client && (id = msg.headers&.[](PullStatus::PIN_ID))
          self.pin_id = id
        end
        Message.new(msg, @api.client, acks: config.ack_policy != :none, pull: pull)
      end

      # Refuses a pull that cannot work before anything is sent.
      #
      # @api private
      # @param options [Hash{Symbol => Object}] the priority group fields of the pull
      # @return [void] nothing
      # @raise [PushConsumerError] if this is a push consumer
      # @raise [ArgumentError] if a group is missing, unexpected or malformed
      def check_pull(options)
        raise PushConsumerError, "#{@name} is a push consumer; tether reads only by pulling" if push?

        unknown = options.keys - PULL_FIELDS
        raise ArgumentError, "unknown option: #{unknown.join(", ")}" unless unknown.empty?

        check_group(options[:group])
        nil
      end

      # Builds the JSON of a pull request.
      #
      # @api private
      # @param batch [Integer] the most messages to deliver
      # @param expires [Numeric, nil] seconds the server keeps the request
      # @param heartbeat [Numeric, nil] seconds between idle heartbeats
      # @param max_bytes [Integer, nil] a byte budget
      # @param no_wait [Boolean] end the request when nothing is available
      # @param options [Hash{Symbol => Object}] priority group fields
      # @return [String] the request body
      def pull_request(batch:, expires: nil, heartbeat: nil, max_bytes: nil, no_wait: false, **options)
        request = { "batch" => batch }
        request["expires"] = nanoseconds(expires) if expires
        request["max_bytes"] = max_bytes if max_bytes
        request["no_wait"] = true if no_wait
        request["idle_heartbeat"] = nanoseconds(heartbeat) if heartbeat
        PULL_FIELDS.each { |field| request[field.to_s] = options[field] if options[field] }
        id = pin_id
        request["id"] = id if id
        JSON.generate(request)
      end

      # Publishes a pull request.
      #
      # @api private
      # @param reply_to [String] the inbox messages and statuses are delivered to
      # @param flush [Boolean] write now instead of waiting for the flusher
      # @param fields [Hash{Symbol => Object}] see {#pull_request}
      # @return [void] nothing
      def request_next(reply_to, flush: false, **fields)
        headers = fields[:group] ? { API::LEVEL_HEADER => "1" } : nil
        @api.client.publish("#{@api.prefix}.CONSUMER.MSG.NEXT.#{@subject}", pull_request(**fields),
                            reply_to: reply_to, headers: headers, flush: flush)
        nil
      end

      private

      def check_group(group)
        groups = config.priority_groups || []
        if groups.empty?
          raise ArgumentError, "group: given, but #{@name} has no priority groups" if group
        elsif group.nil?
          raise ArgumentError, "group: is required, #{@name} has priority groups #{groups.join(", ")}"
        elsif !group.to_s.match?(GROUP)
          raise ArgumentError, "invalid priority group #{group.inspect}"
        end
      end

      def nanoseconds(seconds)
        (seconds * NANOSECONDS).round
      end
    end

    # The answer to a pause or resume request.
    class ConsumerPause < Schema
      field :paused, :boolean
      field :pause_until, :time
      field :pause_remaining, :duration
    end
  end
end
