# frozen_string_literal: true

module Tether
  module Connection
    # The state machine of one client: connecting, reconnecting, draining and
    # closing, and the only code that assigns the status.
    #
    # It owns the parts of a live connection (writer, registry, timer, pool)
    # and replaces them all at once in a forked child.
    class Lifecycle
      CONNECT_ERRORS = [Error, SystemCallError, IOError, OpenSSL::OpenSSLError].freeze
      AUTH_ERRORS = [AuthorizationError, AuthenticationExpiredError, AuthenticationRevokedError].freeze

      # @return [Symbol] :disconnected, :connecting, :connected, :reconnecting, :draining or :closed
      attr_reader :status

      # @return [ServerInfo, nil] the latest INFO from the connected server
      attr_reader :server_info

      # @return [Server, nil] the server currently or most recently connected to
      attr_reader :server

      # @return [Writer] the outbound buffer
      attr_reader :writer

      # @return [Protocol::Registry] the live subscriptions
      attr_reader :registry

      # @return [Events] event handlers and listeners
      attr_reader :events

      # @return [Timer] the connection's timer
      attr_reader :timer

      # @return [ServerPool] the servers to try
      attr_reader :pool

      # @return [Counters] traffic counters
      attr_reader :counters

      # @return [Integer] the process this connection belongs to
      attr_reader :pid

      # @param urls [String, Array<String>] the seed servers
      # @param options [Options] the client's options
      def initialize(urls, options)
        @options = options
        settings = options.reconnect || Options::RECONNECT_DEFAULTS
        @pool = ServerPool.new(urls, randomize: options.randomize, max_attempts: settings[:max_attempts])
        @handshake = Handshake.new(options, Auth.new(options))
        @events = Events.new(logger: options.logger)
        @fork_lock = Mutex.new
        @status = :disconnected
        @server_info = nil
        @server = nil
        build
      end

      # Connects to the first server that completes the handshake.
      #
      # @return [void] nothing
      # @raise [ConnectionError, ServerError, TimeoutError] the last failure when no server could be reached
      def connect
        begin_connecting
        error = try_each_server
        return unless error
        return start_recovery(error) if @options.retry_on_failed_connect && @options.reconnect?

        @lock.synchronize { change_status(:disconnected) unless @status == :closed }
        raise error
      end

      # Round trips a PING.
      #
      # @param deadline [Deadline] when to give up
      # @return [void] nothing
      # @raise [TimeoutError] if the server does not answer in time
      # @raise [DisconnectedError] if there is no live connection, or it is lost while waiting
      # @raise [ConnectionClosedError] if the connection is closed
      def flush(deadline)
        waiter = Thread::Queue.new
        @writer.ping(waiter)
        answer = waiter.pop(timeout: deadline.remaining)
        raise TimeoutError, "the server did not answer a PING within the deadline" if answer.nil?
        raise answer if answer.is_a?(Exception)
      end

      # Reports that a socket is no longer usable. It only records the
      # transition; the work happens on a thread of its own, so this is safe
      # from the reader, the timer and any publisher.
      #
      # @param transport [Transport, nil] the socket that failed
      # @param error [Exception, nil] why, when it is known
      # @return [void] nothing
      def connection_lost(transport, error = nil)
        @lock.synchronize do
          return if transport.nil? || !transport.equal?(@transport)

          reader = release_transport
          if @status == :connected && @options.reconnect?
            change_status(:reconnecting)
            @writer.detach(DisconnectedError.new("connection lost"))
            spawn("reconnect") { recover(transport, reader, error) }
          else
            spawn("close") do
              transport.close
              close(error)
            end
          end
        end
        nil
      end

      # Drops the current socket and reconnects, or cuts short the wait of a
      # reconnect in progress.
      #
      # @return [void] nothing
      # @raise [ConnectionClosedError] if the connection is closed
      # @raise [ConnectionDrainingError] if the connection is draining
      def force_reconnect
        raise ConfigurationError, "reconnect is disabled" unless @options.reconnect?

        @pool.reset_attempts
        case @status
        when :closed then raise ConnectionClosedError, "connection is closed"
        when :draining then raise ConnectionDrainingError, "connection is draining"
        when :connected then connection_lost(@transport)
        else @wakeup.push(true)
        end
        nil
      end

      # Unsubscribes everything, lets handlers finish what was received,
      # flushes and closes.
      #
      # @param deadline [Deadline] when to give up and close anyway
      # @return [void] nothing
      # @raise [DisconnectedError] if there is no live connection to drain; the client is closed
      # @raise [TimeoutError] if the deadline passes; the client is closed
      def drain(deadline)
        return if @status == :closed
        return await_close(deadline) if @status == :draining

        unless begin_draining
          close
          raise DisconnectedError, "cannot drain without a live connection; the client was closed"
        end
        drain_subscriptions(deadline)
      ensure
        close unless @status == :draining && !@drainer.equal?(Thread.current)
      end

      # Closes the connection for good. Idempotent.
      #
      # @param error [Exception, nil] why, when it was not requested
      # @return [void] nothing
      def close(error = nil)
        previous, transport, reader = begin_closing
        return unless previous

        @wakeup.push(true)
        @writer.close(flush: %i[connected draining].include?(previous))
        transport&.close
        reader.join(2) if reader && reader != Thread.current
        @timer.stop
        @registry.close(error)
        announce_close(previous, error)
        nil
      end

      # Detects a fork and, in the child, replaces the inherited connection
      # with a fresh one.
      #
      # @return [void] nothing
      def check_fork
        return if @pid == Process.pid

        @fork_lock.synchronize { reset_after_fork unless @pid == Process.pid }
        nil
      end

      # @api private
      # @param info [Hash] an INFO document received after the handshake
      # @return [void] nothing
      def info_received(info)
        info = ServerInfo.new(info)
        @server_info = info
        discover(info)
        return unless info.lame_duck?

        @events.emit(:lame_duck, @server)
        connection_lost(@transport) if @options.reconnect && @options.reconnect[:on_lame_duck]
      end

      # @api private
      # @param error [Exception] an `-ERR` or a protocol violation
      # @return [void] nothing
      def error_received(error)
        if error.is_a?(PermissionsViolationError) && (match = error.message.match(/subscription to "([^"]+)"/i))
          @registry.to_a.each { |entry| entry.fail(error) if entry.subject == match[1] && entry.respond_to?(:fail) }
        end
        @events.error(error)
        nil
      end

      # @api private
      # @return [void] nothing
      def pong_received
        @keepalive.pong
      end

      private

      def build
        @pid = Process.pid
        @lock = Mutex.new
        @wakeup = Thread::Queue.new
        @counters = Counters.new
        @registry = Protocol::Registry.new
        @timer = Timer.new(on_error: ->(error) { @events.error(error) })
        settings = @options.reconnect || Options::RECONNECT_DEFAULTS
        @writer = Writer.new(write_timeout: @options.write_timeout, reconnect_buffer: settings[:buffer_size],
                             counters: @counters, on_failure: method(:connection_lost))
        @dispatcher = Protocol::Dispatcher.new(connection: self, registry: @registry, writer: @writer,
                                               counters: @counters)
        @keepalive = Keepalive.new(timer: @timer, writer: @writer, interval: @options.ping_interval,
                                   max_out: @options.max_pings_out, on_stale: method(:stale))
        @transport = nil
        @reader = nil
        @drainer = nil
        @closed = Thread::Queue.new
      end

      def spawn(role, &block)
        Thread.new do
          Thread.current.name = "tether:#{role}"
          block.call
        end
      end

      def change_status(status)
        @status = status
        @events.emit(:status, status)
      end

      def begin_connecting
        @lock.synchronize do
          raise ConnectionClosedError, "connection is closed" if @status == :closed
          raise Error, "already #{@status}" unless @status == :disconnected

          change_status(:connecting)
        end
      end

      def try_each_server
        error = NoServersError.new("no servers to connect to")
        @pool.to_a.each do |server|
          return nil if establish(server, :connect)
        rescue *CONNECT_ERRORS => e
          @pool.failed(server, e)
          error = e
        end
        error
      end

      def establish(server, event)
        result = @handshake.run(server, Deadline.after(@options.connect_timeout))
        install(server, result, event)
      end

      def install(server, result, event)
        @lock.synchronize do
          unless %i[connecting reconnecting].include?(@status)
            result.transport.close
            return true
          end
          adopt(server, result)
          change_status(:connected)
        end
        @pool.connected(server)
        discover(result.info)
        @counters.reconnects += 1 if event == :reconnect
        @writer.kick
        @events.notify(event, server)
        @events.emit(event, server)
        true
      end

      def adopt(server, result)
        @server = server
        @server_info = result.info
        @transport = result.transport
        result.parser.dispatcher = @dispatcher
        @writer.attach(result.transport) { @registry.replay_frames }
        @keepalive.start
        @reader = start_reader(result.transport, result.parser)
      end

      def start_reader(transport, parser)
        spawn("reader") do
          error = read_loop(transport, parser)
          connection_lost(transport, error)
        end
      end

      def read_loop(transport, parser)
        until parser.broken?
          data = transport.read or return DisconnectedError.new("the server closed the connection")
          @counters.bytes_in += data.bytesize
          parser.feed(data)
        end
        ProtocolError.new("the server sent bytes that are not NATS protocol")
      rescue ConnectionError => e
        e
      rescue StandardError => e
        @events.error(e)
        e
      end

      def release_transport
        @transport = nil
        @keepalive.stop
        @reader
      end

      def stale
        connection_lost(@transport, StaleConnectionError.new("the server stopped answering PINGs"))
      end

      def discover(info)
        return if @options.ignore_discovered_servers || @server.nil?

        added = @pool.discover(info.connect_urls, @server)
        @events.emit(:servers_discovered, added) unless added.empty?
      end

      def start_recovery(error)
        @lock.synchronize do
          return if @status == :closed

          change_status(:reconnecting)
          spawn("reconnect") { reconnect_loop }
        end
        @events.emit(:reconnect_error, error)
      end

      def recover(transport, reader, error)
        transport.close
        reader&.join(2) unless reader == Thread.current
        @events.notify(:disconnect, error)
        @events.emit(:disconnect, error)
        reconnect_loop
      end

      def reconnect_loop
        attempt = 0
        until @status == :closed
          pause(attempt) if attempt.positive? && (attempt % [@pool.size, 1].max).zero?
          server = next_server or return close(NoServersError.new("no server could be reached"))
          attempt += 1
          return if try_reconnect(server)
        end
      rescue StandardError => e
        close(e)
      end

      def next_server
        chooser = @options.reconnect[:server]
        return @pool.next_server unless chooser

        @pool.next_server && chooser.call(@pool.to_a, @server_info)
      end

      def try_reconnect(server)
        return true if @status == :closed

        establish(server, :reconnect)
      rescue *CONNECT_ERRORS => e
        @pool.failed(server, e)
        @events.notify(:reconnect_error, e)
        @events.emit(:reconnect_error, e)
        close(e) if AUTH_ERRORS.any? { |type| e.is_a?(type) } && server.auth_failures >= 2 &&
                    @options.reconnect[:abort_on_auth_error]
        false
      end

      def pause(attempt)
        settings = @options.reconnect
        delay = if settings[:delay]
                  settings[:delay].call(attempt)
                else
                  settings[:wait] + (rand * (@server&.uri&.scheme == "tls" ? settings[:tls_jitter] : settings[:jitter]))
                end
        @wakeup.pop(timeout: delay)
      end

      def await_close(deadline)
        @closed.pop(timeout: deadline.remaining)
        nil
      end

      def begin_draining
        @lock.synchronize do
          next false unless @status == :connected

          @drainer = Thread.current
          change_status(:draining)
          true
        end
      end

      def drain_subscriptions(deadline)
        subscriptions = @registry.to_a.grep(Subscription).select(&:start_drain)
        flush(deadline)
        subscriptions.each { |subscription| subscription.finish_drain(deadline) }
        flush(deadline)
      end

      def begin_closing
        @lock.synchronize do
          return nil if @status == :closed

          previous = @status
          transport = @transport
          reader = release_transport
          change_status(:closed)
          [previous, transport, reader]
        end
      end

      def announce_close(previous, error)
        @events.error(error) if error
        @events.notify(:close, error)
        @events.emit(:disconnect, error) if %i[connected draining].include?(previous)
        @events.emit(:close)
        @closed.close
        @events.shutdown
      end

      def reset_after_fork
        previous = @status
        @transport&.abandon
        @registry.abandon
        @events.reset
        build
        return if previous == :closed

        @status = :disconnected
        return connect if @options.reconnect?

        @status = :closed
        @writer.close
      end
    end
  end
end
