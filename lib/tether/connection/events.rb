# frozen_string_literal: true

module Tether
  module Connection
    # The handlers a user registered for connection events, and the thread
    # that calls them.
    #
    # Events are delivered one at a time, in order, on a dedicated thread that
    # holds no internal lock, so a handler may call back into the client.
    class Events
      NAMES = %i[error connect disconnect reconnect reconnect_error close lame_duck servers_discovered status].freeze
      QUEUE_LIMIT = 10_000

      # One registered handler. {#cancel} removes it.
      class Registration
        # @param list [Array<Registration>] the list this registration lives in
        # @param lock [Mutex] guards the list
        # @param handler [Proc] the block to call
        def initialize(list, lock, handler)
          @list = list
          @lock = lock
          @handler = handler
        end

        # Removes the handler. Events already queued may still reach it.
        #
        # @return [void] nothing
        def cancel
          @lock.synchronize { @list.delete(self) }
          nil
        end

        # @param args [Array<Object>] the event's arguments
        # @return [void] nothing
        def call(*args)
          @handler.call(*args)
        end
      end

      # @param logger [Logger, nil] receives exceptions raised by handlers
      def initialize(logger: nil)
        @logger = logger
        @handlers = Hash.new { |hash, name| hash[name] = [] }
        @listeners = []
        @lock = Mutex.new
        @queue = Thread::Queue.new
        @last_error = nil
        @thread = nil
      end

      # @return [Exception, nil] the most recent asynchronous error
      def last_error
        @lock.synchronize { @last_error }
      end

      # Registers a handler for an event.
      #
      # @param name [Symbol] one of {NAMES}
      # @yield the event's arguments, on the event thread
      # @return [Registration] a handle whose `cancel` removes the handler
      def on(name, &handler)
        raise ArgumentError, "unknown event: #{name.inspect}" unless NAMES.include?(name)
        raise ArgumentError, "a block is required" unless handler

        @lock.synchronize do
          registration = Registration.new(@handlers[name], @lock, handler)
          @handlers[name] << registration
          start_thread
          registration
        end
      end

      # Registers an internal listener, called synchronously on the thread that
      # makes a state transition. A listener must not block.
      #
      # @yieldparam event [Symbol] :connect, :disconnect, :reconnect or :close
      # @yieldparam detail [Object, nil] the error or server the event concerns
      # @return [Registration] a handle whose `cancel` removes the listener
      def listen(&listener)
        @lock.synchronize do
          Registration.new(@listeners, @lock, listener).tap { |registration| @listeners << registration }
        end
      end

      # Calls every internal listener on the calling thread.
      #
      # @param event [Symbol] the transition that happened
      # @param detail [Object, nil] the error or server it concerns
      # @return [void] nothing
      def notify(event, detail = nil)
        @lock.synchronize { @listeners.dup }.each do |listener|
          listener.call(event, detail)
        rescue StandardError => e
          log(e)
        end
        nil
      end

      # Queues an event for the handlers registered for it.
      #
      # @param name [Symbol] one of {NAMES}
      # @param args [Array<Object>] what the handlers receive
      # @return [void] nothing
      def emit(name, *args)
        waiting = @lock.synchronize do
          @last_error = args.first if name == :error
          !@handlers[name].empty? && !@queue.closed?
        end
        log(args.first) if name == :error && !waiting
        @queue.push([name, args]) if waiting && @queue.size < QUEUE_LIMIT
        nil
      rescue ClosedQueueError
        nil
      end

      # Reports an asynchronous error.
      #
      # @param error [Exception] the error
      # @return [void] nothing
      def error(error)
        emit(:error, error)
      end

      # Stops the event thread once everything queued has been delivered.
      #
      # @param deadline [Deadline] how long to wait for the handlers to finish
      # @return [void] nothing
      def shutdown(deadline = Deadline.after(2))
        thread = @lock.synchronize do
          @queue.close
          @thread
        end
        thread.join(deadline.remaining) if thread && thread != Thread.current
        nil
      end

      # Forgets the event thread of the parent process and starts clean,
      # keeping the handlers.
      #
      # @return [void] nothing
      def reset
        @lock.synchronize do
          @queue = Thread::Queue.new
          @thread = nil
          start_thread unless @handlers.each_value.all?(&:empty?)
        end
        nil
      end

      private

      def start_thread
        return if @thread&.alive? || @queue.closed?

        queue = @queue
        @thread = Thread.new do
          Thread.current.name = "tether:events"
          while (name, args = queue.pop)
            deliver(name, args)
          end
        end
      end

      def deliver(name, args)
        @lock.synchronize { @handlers[name].dup }.each do |registration|
          registration.call(*args)
        rescue StandardError => e
          log(e)
        end
      end

      def log(error)
        return unless @logger && error

        @logger.error("tether: #{error.class}: #{error.message}")
      end
    end
  end
end
