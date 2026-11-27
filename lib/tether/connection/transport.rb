# frozen_string_literal: true

require "socket"
require "uri"
require "openssl"
require "io/wait"

module Tether
  module Connection
    # The socket a connection reads and writes: TCP, optionally upgraded to TLS.
    #
    # A transport only moves bytes. Buffering belongs to {Writer}, and every
    # wait here is bounded by a {Deadline} through socket readiness, never by
    # `Timeout.timeout`.
    class Transport
      READ_SIZE = 64 * 1024

      # @return [URI::Generic] the server this transport connects to
      attr_reader :uri

      # Picks the transport class for a server URL.
      #
      # @param uri [URI::Generic] the server
      # @param options [Options] the client's options
      # @return [Transport] a transport that is not open yet
      def self.for(uri, options)
        %w[ws wss].include?(uri.scheme) ? WebSocket.new(uri, options) : new(uri)
      end

      # @param uri [URI::Generic] the server to reach
      def initialize(uri)
        @uri = uri
        @socket = nil
        @io = nil
        @tls = false
      end

      # Opens the socket with TCP_NODELAY set, since frames are written whole.
      #
      # @param deadline [Deadline] how long to allow for DNS and the TCP connect
      # @return [self] this transport, open
      # @raise [ConnectionError] if the server cannot be reached
      def open(deadline)
        @socket = Socket.tcp(@uri.host, @uri.port, connect_timeout: deadline.remaining)
        @socket.sync = true
        @socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, true)
        @io = @socket
        self
      rescue SystemCallError, IO::TimeoutError, SocketError => e
        raise ConnectionError, "could not connect to #{label}: #{e.message}"
      end

      # Upgrades the open socket to TLS.
      #
      # @param context [OpenSSL::SSL::SSLContext] the TLS settings
      # @param hostname [String] the name to send as SNI and to verify the certificate against
      # @param deadline [Deadline] how long the handshake may take
      # @return [self] this transport, encrypted
      # @raise [TLSError] if the handshake or the certificate check fails
      def upgrade_to_tls(context, hostname, deadline)
        tls = OpenSSL::SSL::SSLSocket.new(@socket, context)
        tls.hostname = hostname
        tls.sync_close = true
        negotiate(tls, deadline)
        tls.post_connection_check(hostname) if context.verify_mode == OpenSSL::SSL::VERIFY_PEER
        @io = tls
        @tls = true
        self
      rescue OpenSSL::SSL::SSLError, SystemCallError, IOError => e
        raise TLSError, "TLS handshake with #{label} failed: #{e.message}"
      end

      # @return [Boolean] whether the socket is encrypted
      def tls?
        @tls
      end

      # @return [Addrinfo, nil] this side of the socket, nil once it is closed
      def local_address
        @socket&.local_address
      rescue IOError, SystemCallError
        nil
      end

      # @return [Addrinfo, nil] the server's side of the socket, nil once it is closed
      def remote_address
        @socket&.remote_address
      rescue IOError, SystemCallError
        nil
      end

      # @return [TLS::State, nil] what the TLS handshake settled on, nil when the socket is not encrypted
      def tls_state
        io = @io
        TLS::State.of(io) if @tls && io.is_a?(OpenSSL::SSL::SSLSocket)
      end

      # Reads whatever is available, waiting for it when nothing is.
      #
      # @param deadline [Deadline] how long to wait for bytes
      # @return [String, nil] bytes, or nil at end of stream
      # @raise [TimeoutError] if the deadline passes with nothing readable
      # @raise [ConnectionClosedError] if the socket fails or was closed
      def read(deadline = Deadline.infinite)
        io = @io or raise ConnectionClosedError, "transport is closed"
        loop do
          case (result = io.read_nonblock(READ_SIZE, exception: false))
          when :wait_readable then wait(:readable, deadline, "reading from")
          when :wait_writable then wait(:writable, deadline, "reading from")
          else return result
          end
        end
      rescue IOError, SystemCallError, OpenSSL::SSL::SSLError => e
        raise ConnectionClosedError, "read failed on #{label}: #{e.message}"
      end

      # Writes every byte or raises.
      #
      # @param data [String] the bytes to send
      # @param deadline [Deadline] how long the whole write may take
      # @return [Integer] bytes written
      # @raise [TimeoutError] if the socket does not accept the data in time
      # @raise [ConnectionClosedError] if the socket fails or was closed
      def write(data, deadline = Deadline.infinite)
        io = @io or raise ConnectionClosedError, "transport is closed"
        size = data.bytesize
        offset = 0
        offset += write_some(io, offset.zero? ? data : data.byteslice(offset, size - offset),
                             deadline) while offset < size
        size
      rescue IOError, SystemCallError, OpenSSL::SSL::SSLError => e
        raise ConnectionClosedError, "write failed on #{label}: #{e.message}"
      end

      # Ends the connection in both directions, which also unblocks a read
      # waiting on it, even when another process holds a copy of the descriptor.
      #
      # @return [void] nothing
      def close
        socket = @socket
        @io = @socket = nil
        return unless socket

        socket.shutdown
        socket.close
        nil
      rescue IOError, SystemCallError
        nil
      end

      # Closes this process's copy of the descriptor and nothing else, so the
      # process that owns the connection is not disturbed.
      #
      # @return [void] nothing
      def abandon
        socket = @socket
        @io = @socket = nil
        socket&.close
        nil
      rescue IOError, SystemCallError
        nil
      end

      # @return [Boolean] whether the socket is gone
      def closed?
        @socket.nil? || @socket.closed?
      end

      private

      def label
        "#{@uri.host}:#{@uri.port}"
      end

      def negotiate(tls, deadline)
        loop do
          case tls.connect_nonblock(exception: false)
          when :wait_readable then wait(:readable, deadline, "TLS handshake with")
          when :wait_writable then wait(:writable, deadline, "TLS handshake with")
          else return
          end
        end
      end

      def write_some(io, chunk, deadline)
        case (result = io.write_nonblock(chunk, exception: false))
        when :wait_writable then wait(:writable, deadline, "writing to")
        when :wait_readable then wait(:readable, deadline, "writing to")
        else return result
        end
        0
      end

      def wait(direction, deadline, action)
        socket = @socket or raise ConnectionClosedError, "transport is closed"
        remaining = deadline.remaining
        ready = direction == :readable ? socket.wait_readable(remaining) : socket.wait_writable(remaining)
        raise TimeoutError, "timed out #{action} #{label}" unless ready
      end
    end
  end
end
