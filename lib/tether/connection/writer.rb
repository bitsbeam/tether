# frozen_string_literal: true

module Tether
  module Connection
    # The outbound buffer of a connection and the only code that writes to
    # its socket.
    #
    # Two locks keep a slow socket from stalling the client. The buffer lock
    # guards the buffer, the mode and the PONG FIFO, and is never held across
    # I/O. The I/O lock serializes socket writes: its holder swaps the buffer
    # out under the buffer lock and writes the detached bytes.
    class Writer
      # Frames accumulate until this much is waiting, at which point the thread
      # that filled it writes instead of waiting for the flusher.
      #
      # The size is measured. Publishing 100k messages: 32 KB gave 244 syscalls
      # at 710k msg/s, 128 KB gave 62 at 770k, 512 KB gave 16 at 772k, and 2 MB
      # gave 4 at 753k, so this sits where the curve flattens.
      BUFFER_LIMIT = 128 * 1024

      # Past this much unwritten data a publisher waits for the write in
      # progress, which is the only back-pressure on a connected client.
      HARD_CAP = 8 * 1024 * 1024

      # @param write_timeout [Numeric] seconds one buffer may take to reach the socket
      # @param reconnect_buffer [Integer] bytes that may accumulate during an outage; 0 refuses them
      # @param counters [Counters] the connection's traffic counters
      # @param on_failure [Proc] called with the transport and the error when a write fails
      def initialize(write_timeout:, reconnect_buffer:, counters:, on_failure:)
        @write_timeout = write_timeout
        @reconnect_buffer = reconnect_buffer
        @counters = counters
        @on_failure = on_failure
        @buffer_lock = Mutex.new
        @io_lock = Mutex.new
        @buffer = empty_buffer
        @spare = empty_buffer
        @pongs = []
        @buffered_pings = 0
        @mode = :buffering
        @transport = nil
        @kick = Thread::SizedQueue.new(1)
        @flusher = nil
      end

      # Starts writing to a transport. The block runs under the buffer lock and
      # returns frames to put in front of whatever accumulated, so a
      # subscription made at any moment is either in those frames or written
      # after them.
      #
      # @param transport [Transport] the freshly connected socket
      # @yieldreturn [String] frames to send first
      # @return [void] nothing
      def attach(transport)
        @io_lock.synchronize do
          @buffer_lock.synchronize do
            return if @mode == :closed

            pending = @buffer
            @buffer = empty_buffer << yield << pending
            @transport = transport
            @mode = :open
          end
        end
        start_flusher
        nil
      end

      # Stops writing and starts accumulating for the next connection. Callers
      # waiting for a PONG are released with the error.
      #
      # @param error [Exception] what waiting callers are told
      # @return [void] nothing
      def detach(error)
        waiters = @buffer_lock.synchronize do
          next [] if @mode == :closed

          @mode = :buffering
          @transport = nil
          take_waiters
        end
        waiters.each { |waiter| waiter&.push(error) }
        nil
      end

      # Appends one complete binary frame. Only the frame that lands in an empty
      # buffer wakes the flusher; frames after it ride along with that wake-up,
      # since the flusher swaps out the whole buffer.
      #
      # @param frame [String] a whole frame
      # @param flush [Boolean] write before returning when the socket is free
      # @param control [Boolean] whether this is a SUB or UNSUB, which an outage drops because reconnect replays them
      # @param messages [Integer] how many published messages the frame holds
      # @return [void] nothing
      # @raise [ReconnectBufferFullError] if an outage has filled the reconnect buffer
      # @raise [DisconnectedError] if buffering during an outage is disabled
      # @raise [ConnectionClosedError] if the connection is closed
      def write(frame, flush: false, control: false, messages: 0)
        size = @buffer_lock.synchronize do
          next 0 unless accept?(frame, control)

          @counters.messages_out += messages
          @buffer << frame
          @mode == :open ? @buffer.bytesize : 0
        end
        return if size.zero?

        if size >= HARD_CAP then drain(wait: true)
        elsif flush || size >= BUFFER_LIMIT then drain(wait: false) || kick
        elsif size == frame.bytesize then kick
        end
        nil
      end

      # Sends a PING whose PONG releases the waiter, registered and appended in
      # one critical section so the answer cannot arrive unowned.
      #
      # @param waiter [Thread::Queue] receives true, or an exception
      # @return [void] nothing
      # @raise [DisconnectedError] if there is no live connection
      # @raise [ConnectionClosedError] if the connection is closed
      def ping(waiter)
        @buffer_lock.synchronize do
          raise ConnectionClosedError, "connection is closed" if @mode == :closed
          raise DisconnectedError, "not connected" unless @mode == :open

          append_ping(waiter)
        end
        drain(wait: false) || kick
        nil
      end

      # Sends a PING nobody waits for.
      #
      # @return [void] nothing
      def keepalive_ping
        @buffer_lock.synchronize { append_ping(nil) if @mode == :open }
        kick
        nil
      end

      # Answers a server PING without touching the socket.
      #
      # @return [void] nothing
      def pong
        @buffer_lock.synchronize { @buffer << Protocol::PONG if @mode == :open }
        kick
        nil
      end

      # Pairs a PONG from the server with the oldest PING.
      #
      # @return [void] nothing
      def resolve_pong
        @buffer_lock.synchronize { @pongs.shift }&.push(true)
        nil
      end

      # Wakes the flusher. Dropped when a kick is already pending, so a million
      # publishes do not become a million wakeups.
      #
      # @return [void] nothing
      def kick
        return unless @kick.empty?

        @kick.push(true, true)
        nil
      rescue ThreadError, ClosedQueueError
        nil
      end

      # @return [Integer] bytes waiting to be written
      def buffered
        @buffer_lock.synchronize { @buffer.bytesize }
      end

      # Stops for good. Waiting callers are released with a closed error.
      #
      # @param flush [Boolean] make one last attempt to write what is buffered
      # @return [void] nothing
      def close(flush: false)
        final_write if flush
        waiters = @buffer_lock.synchronize do
          @mode = :closed
          @transport = nil
          @buffer = empty_buffer
          take_waiters
        end
        error = ConnectionClosedError.new("connection is closed")
        waiters.each { |waiter| waiter&.push(error) }
        @kick.close
        @flusher.join(1) if @flusher && @flusher != Thread.current
        nil
      end

      private

      def empty_buffer
        String.new(capacity: BUFFER_LIMIT, encoding: Encoding::BINARY)
      end

      def accept?(frame, control)
        case @mode
        when :open then true
        when :buffering
          return false if control
          if @reconnect_buffer.zero?
            raise DisconnectedError,
                  "not connected, and buffering during an outage is disabled"
          end
          if @buffer.bytesize + frame.bytesize > @reconnect_buffer
            raise ReconnectBufferFullError, "the reconnect buffer of #{@reconnect_buffer} bytes is full"
          end

          true
        else raise ConnectionClosedError, "connection is closed"
        end
      end

      def append_ping(waiter)
        @buffer << Protocol::PING
        @pongs << waiter
        @buffered_pings += 1
      end

      def take_waiters
        waiters = @pongs
        @pongs = Array.new(@buffered_pings)
        waiters
      end

      def drain(wait:)
        if wait
          @io_lock.lock
        else
          return false unless @io_lock.try_lock
        end
        failure = begin
          write_pending(Deadline.after(@write_timeout))
        ensure
          @io_lock.unlock
        end
        @on_failure.call(*failure) if failure
        true
      end

      def write_pending(deadline)
        transport, data = swap
        return unless data

        transport.write(data, deadline)
        @counters.bytes_out += data.bytesize
        data.clear
        @spare = data
        nil
      rescue ConnectionError, TimeoutError => e
        @spare = empty_buffer
        [transport, e]
      end

      def swap
        @buffer_lock.synchronize do
          next if @mode != :open || @buffer.empty?

          data = @buffer
          @buffer = @spare
          @buffered_pings = 0
          [@transport, data]
        end
      end

      def final_write
        return unless @io_lock.try_lock

        begin
          write_pending(Deadline.after(1))
        ensure
          @io_lock.unlock
        end
      end

      def start_flusher
        return if @flusher&.alive?

        @flusher = Thread.new do
          Thread.current.name = "tether:flusher"
          drain(wait: true) while @kick.pop
        end
      end
    end
  end
end
