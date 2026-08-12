# frozen_string_literal: true

module Tether
  module Connection
    # One thread that runs every "call this in N seconds" of a connection.
    #
    # A callback runs on the timer thread, so it may only do what cannot block:
    # append a frame to the write buffer or post to a queue.
    class Timer
      # One scheduled callback.
      class Entry
        # @return [Float] the monotonic instant the callback is due
        attr_accessor :at

        # @return [Numeric, nil] the period of a repeating entry
        attr_reader :interval

        # @param timer [Timer] the timer that owns this entry
        # @param at [Float] the monotonic instant the callback is due
        # @param interval [Numeric, nil] the period of a repeating entry
        # @param callback [Proc] what to call
        def initialize(timer, at, interval, callback)
          @timer = timer
          @at = at
          @interval = interval
          @delay = interval || (at - Deadline.now)
          @callback = callback
          @canceled = false
        end

        # Prevents any further call. Safe from any thread, including the callback.
        #
        # @return [void] nothing
        def cancel
          @canceled = true
          @timer.remove(self)
          nil
        end

        # @return [Boolean] whether {#cancel} has been called
        def canceled?
          @canceled
        end

        # Moves a one-shot or repeating entry so it next fires a full period,
        # or the given number of seconds, from now.
        #
        # @param seconds [Numeric] the delay; defaults to the entry's original delay
        # @return [void] nothing
        def reset(seconds = @delay)
          @timer.reschedule(self, seconds)
          nil
        end

        # @return [void] nothing
        def call
          @callback.call unless @canceled
        end
      end

      # @param on_error [Proc, nil] receives an exception raised by a callback
      def initialize(on_error: nil)
        @on_error = on_error
        @entries = []
        @lock = Mutex.new
        @wakeup = ConditionVariable.new
        @stopped = false
        @thread = nil
      end

      # Runs a block once, later.
      #
      # @param seconds [Numeric] the delay
      # @yield on the timer thread; must not block
      # @return [Entry] a handle whose `cancel` prevents the call
      def after(seconds, &callback)
        schedule(seconds, nil, callback)
      end

      # Runs a block repeatedly.
      #
      # @param seconds [Numeric] the period
      # @yield on the timer thread; must not block
      # @return [Entry] a handle whose `cancel` ends the repetition
      def every(seconds, &callback)
        schedule(seconds, seconds, callback)
      end

      # @param entry [Entry] the entry to drop
      # @return [void] nothing
      def remove(entry)
        @lock.synchronize { @entries.delete(entry) }
        nil
      end

      # @param entry [Entry] the entry to move
      # @param seconds [Numeric] the new delay from now
      # @return [void] nothing
      def reschedule(entry, seconds)
        @lock.synchronize do
          next if entry.canceled? || @stopped

          entry.at = Deadline.now + seconds
          @entries << entry unless @entries.include?(entry)
          @wakeup.signal
        end
        nil
      end

      # Ends the timer thread and drops every entry.
      #
      # @return [void] nothing
      def stop
        thread = @lock.synchronize do
          @stopped = true
          @entries.clear
          @wakeup.signal
          @thread
        end
        thread.join(1) if thread && thread != Thread.current
        nil
      end

      private

      def schedule(seconds, interval, callback)
        entry = Entry.new(self, Deadline.now + seconds, interval, callback)
        @lock.synchronize do
          next if @stopped

          @entries << entry
          @thread = Thread.new { run } unless @thread&.alive?
          @wakeup.signal
        end
        entry
      end

      def run
        Thread.current.name = "tether:timer"
        while (entry = next_due)
          begin
            entry.call
          rescue StandardError => e
            @on_error&.call(e)
          end
        end
      end

      def next_due
        @lock.synchronize do
          loop do
            return nil if @stopped

            entry = @entries.min_by(&:at)
            wait = entry && (entry.at - Deadline.now)
            return take(entry) if wait && wait <= 0

            @wakeup.wait(@lock, wait)
          end
        end
      end

      def take(entry)
        if entry.interval
          entry.at = Deadline.now + entry.interval
        else
          @entries.delete(entry)
        end
        entry
      end
    end
  end
end
