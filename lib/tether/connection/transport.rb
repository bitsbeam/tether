# frozen_string_literal: true

require "socket"
require "uri"

module Tether
  module Connection
    # The socket a connection reads and writes.
    #
    # Deadlines are applied with `IO#timeout=`, which raises `IO::TimeoutError` at
    # the point the operating system reports the socket is not ready, rather than
    # at an arbitrary bytecode boundary the way `Timeout.timeout` would.
    class Transport
      DEFAULT_PORT = 4222
      READ_SIZE = 64 * 1024

      # Frames accumulate until this much is waiting, at which point the thread
      # that filled it writes rather than waiting for the flusher.
      #
      # Writing each frame as it arrives cost a syscall per message and made
      # publishing four times slower with eight threads than with one, since a
      # lock held across a syscall turns concurrent publishers into a queue.
      #
      # The size is measured rather than inherited. Publishing 100k messages:
      # 32 KB gave 244 syscalls at 710k msg/s, 128 KB gave 62 at 770k, 512 KB
      # gave 16 at 772k, and 2 MB gave 4 at 753k. Fewer syscalls stop paying
      # almost immediately, and the largest buffer is slower, so this sits at
      # the point where the curve flattens rather than at its extreme.
      BUFFER_LIMIT = 128 * 1024

      # @return [URI::Generic] the server this transport is connected to
      attr_reader :uri

      # Opens a TCP connection.
      #
      # @param uri [String, URI::Generic] the server to reach; a bare host:port is read as nats://
      # @param deadline [Deadline] bounds the connect and every later read
      # @return [Transport] an open transport
      # @raise [ConnectionError] if the server cannot be reached
      def self.connect(uri, deadline: Deadline.infinite)
        new(uri).tap { _1.open(deadline) }
      end

      # Prepares a transport without opening it.
      #
      # @param uri [String, URI::Generic] a bare host:port is treated as nats://
      def initialize(uri)
        @uri = normalize(uri)
        @lock = Mutex.new
        @buffer = String.new(capacity: BUFFER_LIMIT, encoding: Encoding::BINARY)
      end

      # Opens the socket with TCP_NODELAY set, since the protocol is latency
      # sensitive and frames are already written whole.
      #
      # @param deadline [Deadline] how long to allow for the connection
      # @return [self] this transport, now open
      # @raise [ConnectionError] if the server cannot be reached
      def open(deadline = Deadline.infinite)
        @socket = Socket.tcp(@uri.host, @uri.port, connect_timeout: deadline.remaining)
        @socket.sync = true
        @socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, true)
        self
      rescue SystemCallError, IO::TimeoutError => e
        raise ConnectionError, "could not connect to #{@uri}: #{e.message}"
      end

      # Reads whatever is available.
      #
      # @param deadline [Deadline] how long to wait for bytes to arrive
      # @return [String, nil] bytes, or nil at end of stream
      # @raise [TimeoutError] if the deadline passes with nothing readable
      def read(deadline = Deadline.infinite)
        socket = @socket or raise ConnectionClosedError, "transport is closed"
        socket.timeout = deadline.remaining
        socket.readpartial(READ_SIZE)
      rescue EOFError
        nil
      rescue IO::TimeoutError
        raise TimeoutError, "timed out reading from #{@uri}"
      rescue IOError, SystemCallError => e
        raise ConnectionClosedError, "read failed on #{@uri}: #{e.message}"
      end

      # Buffers one complete frame, and writes when the buffer is full.
      #
      # The lock spans the whole frame rather than each append, so frames from
      # different threads cannot interleave, in the buffer or on the wire.
      #
      # Buffered means a successful return says the frame is queued, not that
      # the server has it. Pass flush: true for anything that cannot wait
      # behind a publish backlog, which is how PING and PONG stay prompt.
      #
      # @param frame [String] a complete, already encoded frame
      # @param flush [Boolean] write immediately rather than waiting for the flusher
      # @return [Integer] bytes buffered
      def write(frame, flush: false)
        @lock.synchronize do
          raise ConnectionClosedError, "transport is closed" unless @socket

          @buffer << frame
          flush_buffer if flush || @buffer.bytesize >= BUFFER_LIMIT
          frame.bytesize
        end
      end

      # Writes whatever is buffered. Called by the flusher, and by whoever
      # filled the buffer past its limit.
      #
      # @return [Integer] bytes written
      def flush
        @lock.synchronize { flush_buffer }
      end

      # @return [Integer] bytes waiting to be written
      def buffered
        @lock.synchronize { @buffer.bytesize }
      end

      # Closes the socket, which also unblocks a read waiting on it. Anything
      # still buffered is discarded: the connection is going away, and a
      # partial write would be worse than none.
      #
      # @return [void] nothing
      def close
        @lock.synchronize do
          @buffer.clear
          @socket&.close
          @socket = nil
        end
      rescue IOError, SystemCallError
        @socket = nil
      end

      # @return [Boolean] whether the socket is gone; every read and write raises after this
      def closed?
        @socket.nil? || @socket.closed?
      end

      private

      # Runs with the lock already held.
      def flush_buffer
        return 0 if @buffer.empty?

        socket = @socket or raise ConnectionClosedError, "transport is closed"
        written = socket.write(@buffer)
        @buffer.clear
        written
      rescue IOError, SystemCallError => e
        @buffer.clear
        raise ConnectionClosedError, "write failed on #{@uri}: #{e.message}"
      end

      def normalize(uri)
        parsed = uri.is_a?(URI::Generic) ? uri : URI.parse(coerce_scheme(uri.to_s))
        parsed.port ||= DEFAULT_PORT
        parsed
      end

      def coerce_scheme(value)
        value.include?("://") ? value : "nats://#{value}"
      end
    end
  end
end
