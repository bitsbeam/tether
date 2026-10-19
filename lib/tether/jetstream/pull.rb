# frozen_string_literal: true

module Tether
  class JetStream
    # Continuous delivery from a pull consumer: what {Consumer#consume} and
    # {Consumer#messages} return.
    #
    # The loop keeps a buffer topped up with overlapping pull requests on one
    # inbox. The reader thread classifies every arriving frame, stamps the time
    # of last activity and corrects the outstanding count; the workers, or the
    # thread calling {#next}, hand messages out, publish the next pull when the
    # outstanding count falls below the threshold, and act on what the reader,
    # the heartbeat monitor and the connection report.
    class Pull
      DEFAULT_MAX_MESSAGES = 500
      DEFAULT_EXPIRES = 30
      MIN_EXPIRES = 1
      HEARTBEAT_RANGE = (0.5..30)

      # @return [Consumer] the consumer being read from
      attr_reader :consumer

      # @param consumer [Consumer] the consumer to read from
      # @param handler [Proc, nil] called with each message on a worker thread; without one, use {#next}
      # @param max_messages [Integer, nil] messages asked for at once and held in memory; 500 by default
      # @param max_bytes [Integer, nil] a byte budget, which replaces the message count as the limit
      # @param expires [Numeric] seconds a pull request lives on the server
      # @param heartbeat [Numeric, nil] seconds between the server's idle heartbeats; half of `expires` by default
      # @param threshold [Integer, nil] pull again when fewer messages, or bytes, than this are outstanding
      # @param concurrency [Integer] worker threads calling the handler
      # @param on_error [#call, nil] receives warnings and the error that ends the loop
      # @param options [Hash{Symbol => Object}] priority group fields: `group:`, `min_pending:`,
      #   `min_ack_pending:`, `priority:`
      # @raise [PushConsumerError] if the consumer is a push consumer
      # @raise [ArgumentError] if an option cannot be used
      def initialize(consumer, handler = nil, max_messages: nil, max_bytes: nil, expires: DEFAULT_EXPIRES,
                     heartbeat: nil, threshold: nil, concurrency: 1, on_error: nil, **options)
        @consumer = consumer
        @client = consumer.jetstream.client
        @handler = handler
        @expires = expires
        @heartbeat = heartbeat || (expires / 2.0).clamp(HEARTBEAT_RANGE.begin, HEARTBEAT_RANGE.end)
        @options = options
        validate(max_messages: max_messages, max_bytes: max_bytes, threshold: threshold, concurrency: concurrency)
        @buffer = Buffer.new(max_messages: max_messages || (max_bytes ? Buffer::BYTES_BATCH : DEFAULT_MAX_MESSAGES),
                             max_bytes: max_bytes, threshold: threshold)
        @error_handlers = [on_error].compact
        @lock = Mutex.new
        @ended = ConditionVariable.new
        @finished = false
        @error = nil
        @generation = Process.pid
        @epoch = 0
        @quiet = false
        @workers = []
        @running = handler ? concurrency : 0
        start
      end

      # Registers a handler for this loop's warnings and for the error that ends it.
      #
      # @yieldparam error [Exception] the warning or error
      # @return [self] this loop
      def on_error(&handler)
        @lock.synchronize { @error_handlers << handler }
        self
      end

      # Takes the next message, for a loop without a handler.
      #
      # @param timeout [Numeric, Deadline, nil] how long to wait; nil waits indefinitely
      # @return [Message] the next message
      # @raise [TimeoutError] if none arrives in time
      # @raise [StopIteration] if the loop was stopped or has drained
      # @raise [Error] the error that ended the loop
      def next(timeout: nil)
        raise ArgumentError, "this loop delivers to a block" if @handler
        raise ForkedError, "the loop belongs to the parent process" if forked?

        message = take(Deadline.coerce(timeout))
        return message if message
        raise @error if @error

        raise StopIteration, "the loop on #{@consumer.name} has stopped"
      end

      # Yields messages until the loop is stopped.
      #
      # @yieldparam message [Message] each message, on the calling thread
      # @return [self, Enumerator] an enumerator when no block is given
      # @raise [Error] the error that ended the loop
      def each
        return enum_for(:each) unless block_given?

        loop { yield self.next }
        self
      end

      # Stops now. Buffered messages are discarded; with acknowledgments, the
      # server delivers them again later.
      #
      # @return [self] this loop
      def stop
        finish unless forked?
        self
      end

      # Stops pulling and lets buffered messages be handled. With a handler it
      # waits for them; called from the handler itself, or on a loop without
      # one, it returns at once and the loop ends when the buffer is empty.
      #
      # @param timeout [Numeric, Deadline, nil] how long to wait; defaults to the client's `drain_timeout`
      # @return [self] this loop
      # @raise [TimeoutError] if the buffer is not emptied in time; the loop is stopped anyway
      def drain(timeout: nil)
        deadline = Deadline.coerce(timeout || @client.options.drain_timeout)
        return self if forked?

        @buffer.drain
        return self if @handler.nil? || own_worker? || await_end(deadline)

        stop
        raise TimeoutError, "the loop on #{@consumer.name} did not drain within the deadline"
      end

      # Blocks until the loop has ended and its handlers have returned.
      #
      # @param timeout [Numeric, Deadline, nil] how long to wait; nil waits indefinitely
      # @return [self] this loop
      # @raise [Error] the error that ended the loop
      # @raise [TimeoutError] if it has not ended in time
      def wait(timeout: nil)
        raise ForkedError, "the loop belongs to the parent process" if forked?
        return self if own_worker?

        unless await_end(Deadline.coerce(timeout))
          raise TimeoutError, "the loop on #{@consumer.name} did not end within the deadline"
        end
        raise @error if @error

        self
      end

      # @return [Boolean] whether the loop has ended
      def stopped?
        forked? || @finished
      end

      # @return [Exception, nil] the error that ended the loop
      attr_reader :error

      # @return [String] the consumer and state without the buffer
      def inspect
        "#<#{self.class.name} consumer=#{@consumer.name.inspect} stopped=#{stopped?}>"
      end

      # Counts messages asked for with an acknowledgment and builds their request.
      #
      # @api private
      # @param batch [Integer] how many more messages to ask for
      # @return [Array(String, String)] the inbox to reply to and the pull request body
      def next_request(batch)
        @buffer.add(batch)
        [@inbox, @consumer.pull_request(batch: batch, expires: @expires, heartbeat: @heartbeat, **@options)]
      end

      private

      def validate(**counts)
        @consumer.check_pull(@options)
        raise ArgumentError, "expires: must be at least #{MIN_EXPIRES} second" if @expires < MIN_EXPIRES
        raise ArgumentError, "heartbeat: may be at most half of expires:" if @heartbeat * 2 > @expires

        counts.each { |name, value| raise ArgumentError, "#{name}: must be at least 1" if value && value < 1 }
      end

      def start
        open_session
        @listener = @client.listen { |event, _| transition(event) }
        @monitor = @client.timer.every(@heartbeat) { check_heartbeat }
        pull_more
        started
        @generation = @client.generation
        @running.times { Thread.new { work } }
      rescue StandardError
        finish
        raise
      end

      def started; end

      def open_session
        epoch = (@epoch += 1)
        @last_activity = Deadline.now
        @inbox = @client.new_inbox
        @resolver = @client.resolver(@inbox) { |msg| receive(msg, epoch) }
      end

      def close_session
        @resolver&.unsubscribe
      rescue Tether::Error
        nil
      end

      def receive(msg, epoch)
        @last_activity = Deadline.now
        status = PullStatus.of(msg)
        return @buffer.push(msg) unless status
        return idle(epoch) if status.heartbeat?

        case @buffer.correct(status.pending_messages, status.pending_bytes, too_large: status.kind == :too_large)
        when :refill then @buffer.post([:refill, nil, epoch], once: true)
        when :stalled then @buffer.post([:stalled, PullError.new(status.code, msg.description), epoch])
        end
        @buffer.post([status.kind, status.error, epoch]) if status.error
      end

      def transition(event)
        case event
        when :reconnect
          @last_activity = Deadline.now
          @buffer.post([:reconnected, nil, @epoch], once: true)
        when :close
          @buffer.post([:terminal, ConnectionClosedError.new("connection closed while consuming"), nil])
        end
      end

      def check_heartbeat
        now = Deadline.now
        return if now - @last_activity < @heartbeat * 2 || !@client.connected?

        @last_activity = now
        error = NoHeartbeatError.new("no heartbeat from #{@consumer.name} for #{@heartbeat * 2}s")
        @buffer.post([:silence, (error if @buffer.owed? && !@quiet), @epoch], once: true)
      end

      def work
        Thread.current.name = "tether:consume:#{@consumer.name}"
        @lock.synchronize { @workers << Thread.current }
        before_work
        while (message = take(Deadline.infinite))
          deliver(message)
        end
      rescue StandardError => e
        finish(e)
      ensure
        finish
        @lock.synchronize do
          @running -= 1
          @ended.broadcast
        end
      end

      def before_work; end

      def deliver(message)
        @handler.call(message)
      rescue StandardError => e
        failed(e, message)
      ensure
        delivered(message)
      end

      def delivered(_message); end

      def idle(_epoch); end

      def take(deadline)
        loop do
          prepare(deadline)
          case (item = @buffer.take(deadline))
          when Protocol::Msg
            pull_more
            message = admit(item) and return message
          when Array then handle(*item, deadline)
          when :drained, :stopped then return finish && nil
          else raise TimeoutError, "no message from #{@consumer.name} within the deadline"
          end
        end
      end

      def prepare(_deadline); end

      def admit(msg)
        @consumer.wrap(msg, pull: self)
      end

      def handle(kind, error, epoch, deadline)
        return if epoch && epoch != @epoch

        return fail_with(gone) if kind == :unavailable && deleted?(deadline)

        report(error)
        case kind
        when :refill then pull_more
        when :reset, :silence, :reconnected then pull_again
        when :pin then unpin
        when :unavailable, :stalled, :warning then @quiet = true
        when :terminal then finish(error)
        end
      end

      def fail_with(error)
        report(error)
        finish(error)
      end

      def deleted?(deadline)
        @consumer.info(timeout: deadline.with_at_most(@consumer.jetstream.timeout))
        false
      rescue ConsumerNotFoundError, StreamNotFoundError
        true
      rescue Tether::Error
        false
      end

      def gone
        ConsumerDeletedError.new("consumer #{@consumer.name} no longer exists")
      end

      def unpin
        @consumer.pin_id = nil
        pull_again
      end

      def pull_again
        @buffer.reset
        pull_more
      end

      def pull_more
        return unless @client.connected?

        request = @buffer.refill or return
        @quiet = false
        @consumer.request_next(@inbox, expires: @expires, heartbeat: @heartbeat, **request, **@options)
      rescue ConnectionClosedError => e
        finish(e)
      rescue ConnectionError
        nil
      end

      def failed(error, message)
        report(error) unless @lock.synchronize { @error_handlers.empty? }
        @client.report_handler_error(error, message)
      end

      def report(error)
        return unless error

        handlers = @lock.synchronize { @error_handlers.dup }
        return @client.report_error(error) if handlers.empty?

        handlers.each do |handler|
          handler.call(error)
        rescue StandardError => e
          @client.report_handler_error(e)
        end
      end

      def finish(error = nil)
        @lock.synchronize do
          return nil if @finished

          @finished = true
          @error = error
        end
        @buffer.stop
        @listener&.cancel
        @monitor&.cancel
        close_session
        @lock.synchronize { @ended.broadcast }
        nil
      end

      def await_end(deadline)
        @lock.synchronize do
          until @finished && @running.zero?
            remaining = deadline.remaining
            return false if remaining&.zero?

            @ended.wait(@lock, remaining)
          end
          true
        end
      end

      def own_worker?
        @workers.include?(Thread.current)
      end

      def forked?
        @generation != Process.pid
      end
    end
  end
end

require "tether/jetstream/pull/buffer"
