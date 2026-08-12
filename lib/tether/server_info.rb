# frozen_string_literal: true

module Tether
  # The INFO document a server announced, as a frozen value.
  class ServerInfo
    FIELDS = {
      id: "server_id", name: "server_name", version: "version", host: "host", port: "port",
      max_payload: "max_payload", proto: "proto", client_id: "client_id", client_ip: "client_ip",
      cluster: "cluster", domain: "domain", nonce: "nonce", git_commit: "git_commit", go: "go",
      account: "acc", xkey: "xkey"
    }.freeze

    # @return [Hash{String => Object}] the document exactly as the server sent it
    attr_reader :raw

    FIELDS.each do |name, key|
      define_method(name) { @raw[key] }
    end

    # @param raw [Hash{String => Object}] the decoded INFO document
    def initialize(raw)
      @raw = raw.freeze
      freeze
    end

    # @return [Boolean] whether the server accepts and delivers message headers
    def headers?
      @raw["headers"] == true
    end

    # @return [Boolean] whether JetStream is enabled for this connection's account
    def jetstream?
      @raw["jetstream"] == true
    end

    # @return [Boolean] whether the server asks for credentials
    def auth_required?
      @raw["auth_required"] == true
    end

    # @return [Boolean] whether the server requires TLS
    def tls_required?
      @raw["tls_required"] == true
    end

    # @return [Boolean] whether the server offers TLS at all
    def tls_available?
      tls_required? || @raw["tls_available"] == true
    end

    # @return [Boolean] whether the server is shutting down gracefully
    def lame_duck?
      @raw["ldm"] == true
    end

    # @return [Integer] the JetStream API level the server implements, 0 when it does not say
    def api_level
      @raw["api_lvl"] || 0
    end

    # @return [Array<String>] the other servers of the cluster a client may connect to
    def connect_urls
      @raw["connect_urls"] || []
    end

    # @param key [String] a key of the INFO document
    # @return [Object, nil] the raw value
    def [](key)
      @raw[key.to_s]
    end

    # @return [Hash{String => Object}] the raw document
    def to_h
      @raw
    end

    # @return [String] the identity of the server without the whole document
    def inspect
      "#<Tether::ServerInfo name=#{name.inspect} version=#{version.inspect} host=#{host.inspect} port=#{port}>"
    end
  end
end
