# frozen_string_literal: true

module Tether
  class JetStream
    class Pull
      # What a pull loop has received and how much the server still owes it.
      #
      # The reader thread pushes messages, posts control events and corrects
      # the outstanding count from status headers; the worker takes from it and
      # asks whether another pull is due. Control events are served before
      # messages, so a slow handler never delays the protocol.
      #
      # Counts are in messages, or in bytes when the loop has a byte budget.
      # The outstanding count covers what the server still owes plus what is
      # buffered here, so the buffer never holds more than the limit.
      class Buffer
        BYTES_BATCH = 1_000_000

        # @return [Symbol] :running, :draining or :stopped
        attr_reader :state

        # @param max_messages [Integer] the batch of a pull and, without a byte budget, the limit
        # @param max_bytes [Integer, nil] a byte budget, which switches the accounting to bytes
        # @param threshold [Integer, nil] pull again when the outstanding count falls below this
        def initialize(max_messages:, max_bytes: nil, threshold: nil)
          @bytes = !max_bytes.nil?
          @batch = max_messages
          @limit = max_bytes || max_messages
          @threshold = threshold || (@limit / 2.0).ceil
          @pending = @buffered = 0
          @full = false
          @messages = []
          @control = []
          @posted = {}
          @state = :running
          @lock = Mutex.new
          @changed = ConditionVariable.new
        end

        # Adds a message. Reader thread.
        #
        # @param msg [Protocol::Msg] a user message
        # @return [void] nothing
        def push(msg)
          @lock.synchronize do
            next if @state == :stopped

            @messages << msg
            @buffered += cost(msg)
            @pending = @buffered if @buffered > @pending
            @changed.signal
          end
          nil
        end

        # Adds a control event. Any thread.
        #
        # @param event [Array] the event, whose first element is its kind
        # @param once [Boolean] drop the event when one of its kind is already waiting
        # @return [void] nothing
        def post(event, once: false)
          @lock.synchronize do
            next if @state == :stopped || (once && @posted[event.first])

            @posted[event.first] = true if once
            @control << event
            @changed.broadcast
          end
          nil
        end

        # Subtracts what an ended pull will never deliver. Reader thread.
        #
        # @param messages [Integer] the pull's undelivered message count
        # @param bytes [Integer] the pull's undelivered byte budget
        # @param too_large [Boolean] whether the pull ended because a message exceeds its byte budget
        # @return [Symbol, nil] :refill when another pull is due, :stalled when a full budget cannot
        #   hold the next message
        def correct(messages, bytes, too_large: false)
          @lock.synchronize do
            owed = @bytes ? bytes : messages
            @pending = [@pending - owed, @buffered].max if owed.positive?
            next nil unless @state == :running && @pending < @threshold
            next :stalled if too_large && @bytes && @full && @pending.zero?

            :refill
          end
        end

        # Takes the next thing to act on, control events first.
        #
        # @param deadline [Deadline] how long to wait
        # @return [Array, Protocol::Msg, Symbol, nil] a control event, a message, :drained once a draining
        #   buffer is empty, :stopped, or nil when the deadline passes
        def take(deadline)
          @lock.synchronize do
            loop do
              return :stopped if @state == :stopped
              return take_control unless @control.empty?
              return take_message unless @messages.empty?
              return :drained if @state == :draining

              remaining = deadline.remaining
              return nil if remaining&.zero?

              @changed.wait(@lock, remaining)
            end
          end
        end

        # Decides whether another pull is due and, if so, counts it as issued.
        #
        # @return [Hash{Symbol => Integer}, nil] the `batch` and `max_bytes` of the pull to publish
        def refill
          @lock.synchronize do
            next nil unless @state == :running && @pending < @threshold

            owed = @limit - @pending
            @full = @pending.zero?
            @pending = @limit
            @bytes ? { batch: @batch, max_bytes: owed } : { batch: owed }
          end
        end

        # Forgets what the server owed, because every outstanding pull is void.
        #
        # @return [void] nothing
        def reset
          @lock.synchronize { @pending = @buffered }
          nil
        end

        # Counts messages requested outside the refill cycle.
        #
        # @param messages [Integer] how many more the server was asked for
        # @return [void] nothing
        def add(messages)
          @lock.synchronize { @pending += messages unless @bytes }
          nil
        end

        # Discards buffered messages and what the server owed. Control events stay.
        #
        # @return [void] nothing
        def clear
          @lock.synchronize do
            @messages.clear
            @pending = @buffered = 0
          end
          nil
        end

        # Stops asking for more; what is buffered is still handed out.
        #
        # @return [Boolean] false when it was not running
        def drain
          @lock.synchronize do
            next false unless @state == :running

            @state = :draining
            @changed.broadcast
            true
          end
        end

        # Ends the buffer and discards everything in it.
        #
        # @return [void] nothing
        def stop
          @lock.synchronize do
            @state = :stopped
            @messages.clear
            @control.clear
            @changed.broadcast
          end
          nil
        end

        # Waits, and returns early when the buffer stops running.
        #
        # @param deadline [Deadline] how long to wait
        # @return [Boolean] whether the buffer is still running
        def pause(deadline)
          @lock.synchronize do
            while @state == :running
              remaining = deadline.remaining
              break if remaining&.zero?

              @changed.wait(@lock, remaining)
            end
            @state == :running
          end
        end

        # @return [Boolean] whether the server is believed to owe more than what is buffered
        def owed?
          @lock.synchronize { @pending > @buffered }
        end

        # @return [Integer] messages waiting to be taken
        def size
          @lock.synchronize { @messages.size }
        end

        private

        def take_control
          event = @control.shift
          @posted.delete(event.first)
          event
        end

        def take_message
          msg = @messages.shift
          spent = cost(msg)
          @buffered -= spent
          @pending = [@pending - spent, @buffered].max
          msg
        end

        def cost(msg)
          @bytes ? msg.subject.bytesize + msg.reply_to.to_s.bytesize + msg.size : 1
        end
      end
    end
  end
end
