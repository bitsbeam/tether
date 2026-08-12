# frozen_string_literal: true

require "uri"

module Tether
  module Connection
    # One server a client may connect to.
    class Server
      # @return [URI::Generic] where the server listens
      attr_reader :uri

      # @return [Integer] consecutive failed attempts
      attr_accessor :attempts

      # @return [Integer] consecutive attempts refused for the same authentication reason
      attr_accessor :auth_failures

      # @return [Exception, nil] why the last attempt failed
      attr_accessor :last_error

      # @return [String, nil] the name to verify a TLS certificate against when the host is a bare address
      attr_reader :tls_hostname

      # @param uri [URI::Generic] where the server listens
      # @param discovered [Boolean] whether the cluster advertised it
      # @param tls_hostname [String, nil] the name to verify a TLS certificate against
      def initialize(uri, discovered: false, tls_hostname: nil)
        @uri = uri
        @discovered = discovered
        @tls_hostname = tls_hostname
        @attempts = 0
        @auth_failures = 0
        @last_error = nil
      end

      # @return [Boolean] whether the cluster advertised this server
      def discovered?
        @discovered
      end

      # @return [String] the URL without any credentials
      def url
        "#{@uri.scheme}://#{@uri.host}:#{@uri.port}"
      end

      # @return [String] the URL and how the server was learned
      def inspect
        "#<Tether::Server url=#{url.inspect} discovered=#{@discovered} attempts=#{@attempts}>"
      end
    end

    # The seed servers and the ones the cluster advertised, in the order they
    # will be tried.
    class ServerPool
      DEFAULT_PORTS = { "nats" => 4222, "tls" => 4222, "ws" => 80, "wss" => 443 }.freeze
      IP_ADDRESS = /\A(?:\d{1,3}(?:\.\d{1,3}){3}|\[?[0-9a-fA-F:]+\]?)\z/

      # @param urls [String, Array<String>, nil] one URL, a comma separated list, or an array
      # @param randomize [Boolean] shuffle the servers once
      # @param max_attempts [Integer] consecutive failures after which a server is dropped; negative never drops
      def initialize(urls, randomize: true, max_attempts: 60)
        @lock = Mutex.new
        @randomize = randomize
        @max_attempts = max_attempts
        @servers = build(urls)
      end

      # @return [Array<Server>] the servers in the order they will be tried
      def to_a
        @lock.synchronize { @servers.dup }
      end

      # @return [Integer] how many servers are in the pool
      def size
        @lock.synchronize { @servers.size }
      end

      # Replaces the pool.
      #
      # @param urls [String, Array<String>] the new servers
      # @return [void] nothing
      def replace(urls)
        servers = build(urls)
        @lock.synchronize { @servers = servers }
        nil
      end

      # Takes the next server to try, dropping the ones that have failed too often.
      #
      # @return [Server, nil] nil when no server is left
      def next_server
        @lock.synchronize do
          @servers.reject! { |server| server.attempts >= @max_attempts } unless @max_attempts.negative?
          server = @servers.shift
          @servers.push(server) if server
          server
        end
      end

      # Records a successful connect, and keeps that server last in line.
      #
      # @param server [Server] the server now connected
      # @return [void] nothing
      def connected(server)
        @lock.synchronize do
          server.attempts = 0
          server.auth_failures = 0
          server.last_error = nil
          @servers.push(server) if @servers.delete(server)
        end
        nil
      end

      # Records a failed attempt.
      #
      # @param server [Server] the server that could not be reached
      # @param error [Exception] why
      # @return [void] nothing
      def failed(server, error)
        @lock.synchronize do
          same = error.is_a?(ServerError) && server.last_error.instance_of?(error.class)
          server.auth_failures = same ? server.auth_failures + 1 : 1
          server.attempts += 1
          server.last_error = error
        end
        nil
      end

      # Makes every server eligible again.
      #
      # @return [void] nothing
      def reset_attempts
        @lock.synchronize { @servers.each { |server| server.attempts = 0 } }
        nil
      end

      # Reconciles the pool with the servers the cluster advertises: new ones
      # are added, discovered ones no longer advertised are removed, and seeds
      # are never removed.
      #
      # @param urls [Array<String>] `connect_urls` from an INFO received over an authenticated connection
      # @param from [Server] the server that advertised them
      # @return [Array<Server>] the servers that were added
      def discover(urls, from)
        advertised = urls.map { |url| parse(url, from.uri.scheme) }
        @lock.synchronize do
          @servers.reject! do |server|
            server.discovered? && !server.equal?(from) && advertised.none? do |uri|
              same?(uri, server.uri)
            end
          end
          fresh = advertised.reject { |uri| @servers.any? { |server| same?(uri, server.uri) } }
          added = fresh.map { |uri| discovered_server(uri, from) }
          @servers.concat(@randomize ? added.shuffle : added)
          added
        end
      end

      private

      def build(urls)
        list = Array(urls).flat_map { |url| url.to_s.split(",") }.map(&:strip).reject(&:empty?)
        raise ArgumentError, "no server URL given" if list.empty?

        servers = list.map { |url| Server.new(parse(url, "nats")) }
        websocket = servers.count { |server| server.uri.scheme.start_with?("ws") }
        raise ArgumentError, "WebSocket and TCP URLs cannot be mixed" unless [0, servers.size].include?(websocket)

        @randomize ? servers.shuffle : servers
      end

      def parse(url, default_scheme)
        uri = URI.parse(url.include?("://") ? url : "#{default_scheme}://#{url}")
        port = DEFAULT_PORTS[uri.scheme] or raise ArgumentError, "unsupported URL scheme: #{url.inspect}"
        raise ArgumentError, "URL has no host: #{url.inspect}" if uri.host.to_s.empty?

        uri.port ||= port
        uri
      rescue URI::InvalidURIError
        raise ArgumentError, "invalid server URL: #{url.inspect}"
      end

      def same?(left, right)
        left.host == right.host && left.port == right.port
      end

      def discovered_server(uri, from)
        uri.userinfo = from.uri.userinfo if from.uri.userinfo
        hostname = uri.host.match?(IP_ADDRESS) ? (from.tls_hostname || from.uri.host) : nil
        Server.new(uri, discovered: true, tls_hostname: hostname)
      end
    end
  end
end
