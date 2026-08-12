# frozen_string_literal: true

require "json"

module Tether
  module Connection
    # The exchange that turns an open socket into an authenticated connection:
    # INFO, an optional TLS upgrade, CONNECT, PING, PONG.
    #
    # It runs on the connecting thread under one deadline that covers DNS, TCP,
    # TLS and the protocol exchange.
    class Handshake
      # Collects what the server says before the reader thread exists.
      class Probe
        # @return [Hash, nil] the latest INFO document
        attr_reader :info

        # @return [Exception, nil] an error the server or the parser reported
        attr_reader :error

        def initialize
          @info = nil
          @error = nil
          @pong = false
        end

        # @return [Boolean] whether the server answered the PING
        def pong?
          @pong
        end

        # @param json [String] the INFO document
        # @return [void] nothing
        def on_info(json)
          @info = JSON.parse(json)
        rescue JSON::ParserError => e
          @error ||= ProtocolError.new("malformed INFO: #{e.message}")
        end

        # @return [void] nothing
        def on_pong
          @pong = true
        end

        # @param description [String] the text of an `-ERR` line
        # @return [void] nothing
        def on_error(description)
          @error ||= ServerError.for(description)
          nil
        end

        # @param message [String] what the parser could not accept
        # @return [void] nothing
        def on_protocol_error(message)
          @error ||= ProtocolError.new(message)
          nil
        end

        # @return [void] nothing
        def on_ping = nil

        # @return [void] nothing
        def on_ok = nil

        # @return [void] nothing
        def on_msg(*) = nil
      end

      # What a completed handshake hands to the connection.
      Result = Data.define(:transport, :parser, :info)

      # @param options [Options] the client's options
      # @param auth [Auth] the source of CONNECT's authentication fields
      def initialize(options, auth)
        @options = options
        @auth = auth
      end

      # Connects to one server and authenticates.
      #
      # @param server [Server] the server to connect to
      # @param deadline [Deadline] covers everything up to the server's PONG
      # @return [Result] the open transport, its parser and the server's INFO
      # @raise [ConnectionError, ServerError, ProtocolError, TimeoutError] if any step fails
      def run(server, deadline)
        transport = Transport.for(server.uri, @options).open(deadline)
        probe = Probe.new
        parser = Protocol::Parser.new(probe)
        negotiate(server, transport, parser, probe, deadline)
        Result.new(transport: transport, parser: parser, info: ServerInfo.new(probe.info))
      rescue StandardError
        transport&.close
        raise
      end

      private

      def negotiate(server, transport, parser, probe, deadline)
        hostname = @options.tls&.fetch(:hostname) || server.tls_hostname || server.uri.host
        secure(transport, hostname, deadline) if tls_first?(transport)
        read_until(transport, parser, probe, deadline) { probe.info }
        upgrade(server, transport, probe, hostname, deadline)
        transport.write(greeting(server, transport, probe.info), deadline)
        read_until(transport, parser, probe, deadline) { probe.pong? }
      end

      def tls_first?(transport)
        !transport.tls? && @options.tls && @options.tls[:first]
      end

      def secure(transport, hostname, deadline)
        transport.upgrade_to_tls(TLS.context(@options.tls || Options::TLS_DEFAULTS), hostname, deadline)
      end

      def upgrade(server, transport, probe, hostname, deadline)
        return if transport.tls? || %w[ws wss].include?(server.uri.scheme)

        info = ServerInfo.new(probe.info)
        wanted = !@options.tls.nil? || server.uri.scheme == "tls"
        return unless wanted || info.tls_required?
        raise TLSError, "TLS is required but #{server.url} does not offer it" unless info.tls_available?

        secure(transport, hostname, deadline)
      end

      def greeting(server, transport, info)
        fields = { name: @options.name || "tether", echo: @options.echo, tls_required: transport.tls?,
                   headers: info["headers"] != false, no_responders: info["headers"] != false }
        Protocol::Encoder.connect(fields.merge(@auth.fields(server.uri, info["nonce"]))) + Protocol::PING
      end

      def read_until(transport, parser, probe, deadline)
        until yield
          raise probe.error if probe.error

          data = transport.read(deadline) or raise ConnectionError,
                                                   "#{transport.uri.host} closed the connection during the handshake"
          parser.feed(data)
        end
        raise probe.error if probe.error && !probe.pong?
      end
    end
  end
end
