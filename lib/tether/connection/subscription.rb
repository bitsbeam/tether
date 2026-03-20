# frozen_string_literal: true

module Tether
  module Connection
    # One subscription, with its own bounded queue and worker thread.
    #
    # The reader thread only ever enqueues here, never runs the callback: a slow
    # handler must not delay delivery to other subscriptions, and must not delay
    # the PONG that keeps the connection from being dropped as stale.
    class Subscription
      DEFAULT_MAX_PENDING = 65_536

      # @return [String] the token sent to the server and echoed back on every message
      attr_reader :sid

      # @return [String] the token sent at subscribe time and echoed back on every message
      attr_reader :subject

      # @return [String, nil] the queue group this belongs to, if any
      attr_reader :queue_group

      # @return [Integer] messages delivered to this subscription
      attr_reader :delivered

      # @return [Integer] messages dropped because the queue was full
      attr_reader :dropped

      # @return [Client, nil] used by {Protocol::Msg#respond}
      attr_reader :responder

      # Starts a worker thread when a callback is given. Without one, messages
      # queue up until {#next_message} takes them on the caller's thread.
      #
      # @param sid [String] the token the server will echo back on every message
      # @param subject [String] the subject or pattern subscribed to
      # @param queue_group [String, nil] the queue group to join, so the work is shared
      # @param max_pending [Integer] queue depth before this is a slow consumer
      # @param on_slow_consumer [Proc, nil] called with self when a message is dropped
      # @param responder [Client, nil] lets {Protocol::Msg#respond} publish a reply
      # @param callback [Proc, nil] when given, messages are delivered to it on a worker thread
      def initialize(sid:, subject:, queue_group: nil, max_pending: DEFAULT_MAX_PENDING,
                     on_slow_consumer: nil, responder: nil, &callback)
        @responder = responder
        @sid = sid
        @subject = subject
        @queue_group = queue_group
        @queue = Thread::SizedQueue.new(max_pending)
        @on_slow_consumer = on_slow_consumer
        @callback = callback
        @delivered = 0
        @dropped = 0
        @lock = Mutex.new
        @worker = start_worker if callback
      end

      # Enqueues a message. Called on the reader thread, so it never blocks.
      #
      # @param msg [Protocol::Msg] the message to hand to the worker
      # @return [Boolean] false when the message was dropped
      def deliver(msg)
        @queue.push(msg, true)
        @lock.synchronize { @delivered += 1 }
        true
      rescue ThreadError
        @lock.synchronize { @dropped += 1 }
        @on_slow_consumer&.call(self)
        false
      rescue ClosedQueueError
        false
      end

      # Takes the next message, for a subscription with no callback.
      #
      # @param deadline [Deadline] how long to wait for a message
      # @return [Protocol::Msg] the next message
      # @raise [TimeoutError] if the deadline passes first
      def next_message(deadline = Deadline.infinite)
        msg = @queue.pop(timeout: deadline.remaining)
        raise TimeoutError, "no message on #{@subject} within the deadline" if msg.nil? && !@queue.closed?
        raise ConnectionClosedError, "subscription on #{@subject} was closed" if msg.nil?

        msg
      end

      # Depth of the queue between the reader thread and this subscription's
      # worker. Growth here means the handler is not keeping up.
      #
      # @return [Integer] messages waiting to be handled
      def pending
        @queue.size
      end

      # @return [Boolean] whether delivery has stopped; a closed subscription never resumes
      def closed?
        @queue.closed?
      end

      # Stops delivery and lets the worker finish what it already took.
      #
      # @return [void] nothing
      def close
        @queue.close
        @worker&.join(1)
        nil
      end

      private

      def start_worker
        Thread.new do
          Thread.current.name = "tether:sub:#{@subject}"
          while (msg = @queue.pop)
            invoke(msg)
          end
        end
      end

      def invoke(msg)
        @callback.call(msg)
      rescue StandardError => e
        @on_slow_consumer.nil? ? warn("tether: subscription callback raised: #{e.class}: #{e.message}") : nil
      end
    end
  end
end
