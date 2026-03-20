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

      # Writes one complete frame.
      #
      # The lock spans the whole frame rather than each write, so frames from
      # different threads cannot interleave on the wire.
      #
      # @param frame [String] a complete, already encoded frame
      # @return [Integer] bytes written
      def write(frame)
        @lock.synchronize do
          socket = @socket or raise ConnectionClosedError, "transport is closed"
          socket.write(frame)
        end
      rescue IOError, SystemCallError => e
        raise ConnectionClosedError, "write failed on #{@uri}: #{e.message}"
      end

      # Closes the socket, which also unblocks a read waiting on it.
      #
      # @return [void] nothing
      def close
        @lock.synchronize do
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
