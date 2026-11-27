# frozen_string_literal: true

module Tether
  module Connection
    # One subscription, with its own bounded queue. A block runs on the
    # client's {WorkerPool}, or with `dedicated: true` on a pool of its own.
    #
    # The reader thread only ever enqueues here and schedules the
    # subscription on its pool when it is not already scheduled.
    class Subscription
      # Messages a worker handles before letting other subscriptions have a turn.
      BATCH = 64

      # @return [String] the token sent to the server and echoed back on every message
      attr_reader :sid

      # @return [String] the subject or pattern subscribed to
      attr_reader :subject

      # @return [String, nil] the queue group this belongs to
      attr_reader :queue

      # @return [Client] the connection this subscription lives on
      attr_reader :client

      # @param client [Client] the connection, used to unsubscribe, flush and report
      # @param sid [String] the token the server echoes back
      # @param subject [String] the subject or pattern
      # @param queue [String, nil] the queue group to join
      # @param max_pending [Integer] messages that may wait before newer ones are dropped
      # @param max_pending_bytes [Integer] bytes that may wait before newer messages are dropped
      # @param max [Integer, nil] end the subscription after this many messages
      # @param concurrency [Integer] workers that may call the block at once
      # @param dedicated [Boolean] run the block on threads of its own instead of the client's pool
      # @yieldparam msg [Protocol::Msg] each message, on a worker thread
      def initialize(client:, sid:, subject:, max_pending:, max_pending_bytes:, queue: nil, max: nil,
                     concurrency: 1, dedicated: false, &block)
        @client = client
        @sid = sid
        @subject = subject
        @queue = queue
        @max_pending = max_pending
        @max_pending_bytes = max_pending_bytes
        @max = max
        @block = block
        @lock = Mutex.new
        @messages = Thread::Queue.new
        @finished = Thread::Queue.new
        @close_handlers = []
        @pending = @pending_bytes = @received = @delivered = @dropped = 0
        @state = :active
        @slow = false
        @error = nil
        @concurrency = concurrency
        @dedicated = dedicated
        @jobs = 0
        @pool = (dedicated ? own_pool : client.worker_pool) if block
      end

      # Enqueues a message. Called on the reader thread, so it never blocks.
      #
      # @param msg [Protocol::Msg] the message
      # @return [Boolean] false when the message was dropped
      def deliver(msg)
        start = false
        outcome = @lock.synchronize do
          admitted = admit(msg)
          start = claim_job if @jobs < @concurrency
          admitted
        end
        job_done unless !start || @pool.schedule(self)
        @client.report_slow_consumer(self) if outcome == :slow
        finish_intake if outcome == :last
        outcome != :slow && outcome != :closed
      end

      # Takes the next message, for a subscription without a block.
      #
      # @param timeout [Numeric, Deadline, nil] how long to wait; nil waits indefinitely
      # @return [Protocol::Msg] the next message
      # @raise [TimeoutError] if nothing arrives in time
      # @raise [SubscriptionClosedError] if the subscription has ended and nothing is left
      def next(timeout: nil)
        msg = @messages.pop(timeout: Deadline.coerce(timeout).remaining)
        return account(msg) if msg

        raise @error if @error
        raise SubscriptionClosedError, "subscription on #{@subject} is closed" if @messages.closed?

        raise TimeoutError, "no message on #{@subject} within the deadline"
      end

      # Yields messages until the subscription ends.
      #
      # @param idle_timeout [Numeric, nil] stop when nothing has arrived for this many seconds
      # @yieldparam msg [Protocol::Msg] each message, on the calling thread
      # @return [self, Enumerator] an enumerator when no block is given
      def each(idle_timeout: nil)
        return enum_for(:each, idle_timeout: idle_timeout) unless block_given?

        while (msg = @messages.pop(timeout: idle_timeout))
          yield account(msg)
        end
        self
      end

      # Stops delivery. Messages still queued are discarded.
      #
      # @param after [Integer, nil] end the subscription once it has received this many messages in total
      # @return [self] this subscription
      def unsubscribe(after: nil)
        return self if closed?

        if after && limit(after)
          @client.unsubscribed(self, after)
        else
          @client.unsubscribed(self)
          close
        end
        self
      end

      # Stops intake, lets everything already received be handled, then ends
      # the subscription. Called from this subscription's own block it returns
      # at once and the subscription ends when the block does.
      #
      # @param timeout [Numeric, Deadline, nil] how long to wait; defaults to the client's `drain_timeout`
      # @return [self] this subscription
      # @raise [TimeoutError] if the queue is not emptied in time; the subscription is closed anyway
      def drain(timeout: nil)
        deadline = Deadline.coerce(timeout || @client.options.drain_timeout)
        return self unless start_drain

        @client.flush(timeout: deadline)
        finish_drain(deadline)
        self
      end

      # Sends UNSUB and marks the subscription as draining.
      #
      # @api private
      # @return [Boolean] false when it was not active
      def start_drain
        @lock.synchronize do
          return false unless @state == :active

          @state = :draining
        end
        @client.unsubscribed(self)
        true
      end

      # Ends intake after the server has confirmed the UNSUB and waits for the
      # workers to empty the queue.
      #
      # @api private
      # @param deadline [Deadline] how long to wait
      # @return [void] nothing
      # @raise [TimeoutError] if the queue is not emptied in time
      def finish_drain(deadline)
        finish_intake
        return if @pool.nil? || Thread.current[:tether_subscription].equal?(self)
        return if @finished.pop(timeout: deadline.remaining) || @finished.closed?

        @lock.synchronize { @state = :closed }
        @messages.clear
        raise TimeoutError, "subscription on #{@subject} did not drain within the deadline"
      end

      # Ends the subscription without telling the server, discarding what is queued.
      #
      # @api private
      # @param error [Exception, nil] why, when it was not requested
      # @return [void] nothing
      def close(error = nil)
        @lock.synchronize do
          return if @state == :closed

          @state = :closed
          @error ||= error
        end
        @client.forget(self)
        @messages.clear
        @messages.close
        finished if idle?
        nil
      end

      # Handles a batch of messages on a worker of the pool.
      #
      # @api private
      # @return [Boolean] whether messages are still waiting, so the pool queues this again
      def run
        Thread.current[:tether_subscription] = self
        handled = 0
        while handled < BATCH && !@messages.empty?
          invoke(account(@messages.pop(true)))
          handled += 1
        end
        more?
      rescue ThreadError
        more?
      ensure
        Thread.current[:tether_subscription] = nil
      end

      # Ends the subscription in a forked child without running any callback.
      #
      # @api private
      # @return [void] nothing
      def abandon
        @lock.synchronize { @state = :closed }
        @close_handlers.clear
        @messages.clear
        @messages.close
        nil
      end

      # Records an error the server reported for this subscription, raised by
      # the next call to {#next}.
      #
      # @api private
      # @param error [Exception] the error
      # @return [void] nothing
      def fail(error)
        @lock.synchronize { @error = error }
        nil
      end

      # @api private
      # @return [String] the frames that restore this subscription on a new connection
      def replay_frames
        @lock.synchronize do
          next "" unless @state == :active

          frame = Protocol::Encoder.sub(@subject, @sid, queue: @queue)
          @max ? frame + Protocol::Encoder.unsub(@sid, [@max - @received, 1].max) : frame
        end
      end

      # Registers a block to call once the subscription has ended, on the
      # thread that ends it. Called at once when it already has.
      #
      # @yield with no arguments
      # @return [self] this subscription
      def on_close(&handler)
        run_now = @lock.synchronize do
          @close_handlers << handler unless @finished.closed?
          @finished.closed?
        end
        handler.call if run_now
        self
      end

      # @return [Integer] messages waiting to be handled
      def pending = @lock.synchronize { @pending }

      # @return [Integer] bytes waiting to be handled
      def pending_bytes = @lock.synchronize { @pending_bytes }

      # @return [Integer] messages handed to the block or returned by {#next}
      def delivered = @lock.synchronize { @delivered }

      # @return [Integer] messages dropped because the queue was full
      def dropped = @lock.synchronize { @dropped }

      # @return [Integer] messages that may wait before newer ones are dropped
      def max_pending = @lock.synchronize { @max_pending }

      # Changes how many messages may wait. Messages already waiting stay, even above the new limit.
      #
      # @param limit [Integer] a positive count
      # @raise [ArgumentError] if the limit is not a positive integer
      def max_pending=(limit)
        limit = positive_limit(limit, :max_pending)
        @lock.synchronize { @max_pending = limit }
      end

      # @return [Integer] bytes that may wait before newer messages are dropped
      def max_pending_bytes = @lock.synchronize { @max_pending_bytes }

      # Changes how many bytes may wait. Messages already waiting stay, even above the new limit.
      #
      # @param limit [Integer] a positive byte count
      # @raise [ArgumentError] if the limit is not a positive integer
      def max_pending_bytes=(limit)
        limit = positive_limit(limit, :max_pending_bytes)
        @lock.synchronize { @max_pending_bytes = limit }
      end

      # @return [Boolean] whether the subscription has ended for any reason
      def closed?
        @state == :closed || @messages.closed?
      end

      # @return [Boolean] whether {#drain} is in progress
      def draining?
        @state == :draining
      end

      # @return [String] the subject and state without the queued messages
      def inspect
        "#<Tether::Subscription subject=#{@subject.inspect} sid=#{@sid} state=#{@state}>"
      end

      private

      def positive_limit(limit, name)
        return limit if limit.is_a?(Integer) && limit.positive?

        raise ArgumentError, "#{name}: must be a positive integer, got #{limit.inspect}"
      end

      def admit(msg)
        return :closed if @state == :closed || @messages.closed?
        if @pending >= @max_pending || (@pending.positive? && @pending_bytes + msg.size > @max_pending_bytes)
          return overflow
        end

        @pending += 1
        @pending_bytes += msg.size
        @received += 1
        @messages.push(msg)
        @max && @received >= @max ? :last : :queued
      rescue ClosedQueueError
        :closed
      end

      def overflow
        @dropped += 1
        return :dropped if @slow

        @slow = true
        :slow
      end

      def limit(count)
        @lock.synchronize do
          next false if @received >= count

          @max = count
          true
        end
      end

      def account(msg)
        @lock.synchronize do
          @pending -= 1
          @pending_bytes -= msg.size
          @delivered += 1
          @slow = false if @slow && @pending < @max_pending / 2
        end
        msg
      end

      def finish_intake
        @client.forget(self)
        @lock.synchronize { @state = :closed unless @state == :draining }
        @messages.close
        finished if idle?
      end

      def own_pool
        WorkerPool.new(max_threads: @concurrency, name: "tether:sub:#{@subject}")
      end

      def claim_job
        return false unless @pool && @jobs < @concurrency && @jobs < @pending

        @jobs += 1
        true
      end

      def more?
        last = @lock.synchronize do
          return true unless @messages.empty?

          @jobs -= 1
          @jobs.zero? && @messages.closed?
        end
        finished if last
        false
      end

      def job_done
        finished if @lock.synchronize { (@jobs -= 1).zero? && @messages.closed? }
      end

      def idle?
        @lock.synchronize { @jobs.zero? }
      end

      def invoke(msg)
        @block.call(msg)
      rescue StandardError => e
        @client.report_handler_error(e, msg)
      end

      def finished
        handlers = @lock.synchronize do
          next [] if @finished.closed?

          @state = :closed
          @finished.close
          @close_handlers.dup
        end
        @pool.close if @dedicated
        handlers.each do |handler|
          handler.call
        rescue StandardError => e
          @client.report_handler_error(e)
        end
      end
    end

    # A registry entry with no queue and no worker: its block runs on the
    # reader thread, so it must only do what cannot block. Library components
    # use it so that user handlers can never starve them.
    class Resolver
      # @return [String] the token the server echoes back
      attr_reader :sid

      # @return [String] the subject subscribed to
      attr_reader :subject

      # @return [Client] the connection this entry lives on
      attr_reader :client

      # @param client [Client] the connection
      # @param sid [String] the token the server echoes back
      # @param subject [String] the subject or pattern
      # @yieldparam msg [Protocol::Msg] each message, on the reader thread
      def initialize(client:, sid:, subject:, &block)
        @client = client
        @sid = sid
        @subject = subject
        @block = block
        @closed = false
      end

      # @param msg [Protocol::Msg] the message
      # @return [void] nothing
      def deliver(msg)
        @block.call(msg) unless @closed
        nil
      rescue StandardError => e
        @client.report_handler_error(e, msg)
      end

      # Stops delivery and tells the server.
      #
      # @return [void] nothing
      def unsubscribe
        return if @closed

        @closed = true
        @client.unsubscribed(self)
        nil
      end

      # @api private
      # @return [void] nothing
      def close(_error = nil)
        @closed = true
        nil
      end
      alias abandon close

      # @return [Boolean] whether delivery has stopped
      def closed?
        @closed
      end

      # @api private
      # @return [String] the frame that restores this entry on a new connection
      def replay_frames
        @closed ? "" : Protocol::Encoder.sub(@subject, @sid)
      end
    end
  end
end
