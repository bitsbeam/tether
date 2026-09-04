# frozen_string_literal: true

module Tether
  class JetStream
    # One pull request and the bounded batch it yields, on an inbox of its own
    # so that concurrent fetches never see each other's messages or statuses.
    #
    # @api private
    class Fetch
      DEFAULT_TIMEOUT = 30
      DEFAULT_HEARTBEAT = 5
      HEARTBEAT_FROM = 10
      MARGIN = 0.1
      MAX_MARGIN = 1
      MIN_EXPIRES = 0.001

      # @param consumer [Consumer] the consumer to pull from
      # @param batch [Integer] the most messages to return
      # @param deadline [Deadline, nil] when the caller stops waiting; nil only for a pull that does not wait
      # @param wait [Boolean] false asks only for what is available now
      # @param max_bytes [Integer, nil] a byte budget for the pull
      # @param heartbeat [Numeric, nil] seconds between the server's idle heartbeats
      # @param options [Hash{Symbol => Object}] priority group fields of the pull request
      # @raise [ArgumentError] if the batch, the heartbeat or the group cannot be used
      def initialize(consumer, batch, deadline:, wait: true, max_bytes: nil, heartbeat: nil, **options)
        @consumer = consumer
        @client = consumer.jetstream.client
        @batch = Integer(batch)
        raise ArgumentError, "batch must be at least 1" unless @batch.positive?

        @deadline = deadline || Deadline.infinite
        @max_bytes = max_bytes
        @no_wait = !wait
        @expires = expiry(deadline)
        @heartbeat = heartbeat || (DEFAULT_HEARTBEAT if @expires && @expires >= HEARTBEAT_FROM)
        @idle = @heartbeat ? @heartbeat * 2 : (consumer.jetstream.timeout if @no_wait && @expires.nil?)
        @options = options
        validate
      end

      # Publishes the pull and collects what it delivers.
      #
      # @yieldparam message [Message] each message as it arrives
      # @return [Batch] what was delivered, possibly nothing
      # @raise [Error] the reason the pull failed, when nothing was delivered
      def call(&block)
        return Batch.new([]) if @deadline.expired?

        frames = Thread::Queue.new
        resolver = @client.resolver(inbox = @client.new_inbox) { |msg| frames.push(msg) }
        @consumer.request_next(inbox, batch: @batch, expires: @expires, max_bytes: @max_bytes, no_wait: @no_wait,
                                      heartbeat: @heartbeat, flush: true, **@options)
        messages = []
        error = collect(frames, messages, &block)
        raise error if error && messages.empty?

        Batch.new(messages, error)
      ensure
        resolver&.unsubscribe
      end

      private

      def expiry(deadline)
        remaining = deadline&.remaining
        return nil unless remaining

        [remaining - [remaining * MARGIN, MAX_MARGIN].min, MIN_EXPIRES].max
      end

      def validate
        @consumer.check_pull(@options)
        return unless @heartbeat
        raise ArgumentError, "heartbeat: needs a timeout" unless @expires
        raise ArgumentError, "heartbeat: may be at most half of the timeout" if @heartbeat * 2 > @expires
      end

      def collect(frames, messages)
        received = 0
        loop do
          msg = frames.pop(timeout: @deadline.with_at_most(@idle).remaining)
          return silence unless msg

          status = PullStatus.of(msg)
          next if status&.heartbeat?
          return ended(status) if status

          messages << (message = @consumer.wrap(msg))
          yield message if block_given?
          received += msg.subject.bytesize + msg.reply_to.to_s.bytesize + msg.size
          return nil if messages.size >= @batch || (@max_bytes && received >= @max_bytes)
        end
      end

      def silence
        return nil if @deadline.expired? || @heartbeat.nil?

        NoHeartbeatError.new("no heartbeat from #{@consumer.name} for #{@idle}s")
      end

      def ended(status)
        return nil if status.normal_end?

        @consumer.pin_id = nil if status.kind == :pin
        status.error
      end
    end
  end
end
