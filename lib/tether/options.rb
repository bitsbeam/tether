# frozen_string_literal: true

module Tether
  # The validated, frozen options of one client.
  #
  # Every key is checked once, here, so a typo raises when the client is built
  # instead of being ignored for the life of the connection.
  class Options
    MEBIBYTE = 1024 * 1024

    DEFAULTS = {
      name: nil, connect_timeout: 2, write_timeout: 10, reconnect: true, retry_on_failed_connect: false,
      randomize: true, ignore_discovered_servers: false, ping_interval: 120, max_pings_out: 2, echo: true,
      inbox_prefix: "_INBOX", max_pending: 65_536, max_pending_bytes: 64 * MEBIBYTE, drain_timeout: 30,
      worker_threads: 16, validate_subjects: true, token: nil, user: nil, password: nil, jwt: nil, nkey_seed: nil,
      nkey_seed_file: nil, credentials: nil, credentials_file: nil, auth: nil, tls: nil, websocket: nil,
      logger: nil
    }.freeze

    RECONNECT_DEFAULTS = {
      max_attempts: 60, wait: 2, jitter: 0.1, tls_jitter: 1, delay: nil, server: nil,
      buffer_size: 8 * MEBIBYTE, on_lame_duck: false, abort_on_auth_error: true
    }.freeze

    TLS_DEFAULTS = {
      ca_file: nil, ca_path: nil, cert_file: nil, key_file: nil, verify: true, first: false,
      min_version: nil, context: nil, hostname: nil
    }.freeze

    WEBSOCKET_DEFAULTS = { compression: false, path: nil, headers: nil }.freeze

    AUTH_KEYS = %i[token user jwt nkey_seed nkey_seed_file credentials credentials_file auth].freeze

    # @return [Hash{Symbol => Object}, nil] reconnect settings, or nil when reconnect is off
    attr_reader :reconnect

    # @return [Hash{Symbol => Object}, nil] TLS settings, or nil when TLS was not asked for
    attr_reader :tls

    # @return [Hash{Symbol => Object}] WebSocket settings
    attr_reader :websocket

    DEFAULTS.each_key do |key|
      next if %i[reconnect tls websocket].include?(key)

      define_method(key) { @values[key] }
    end

    # @param options [Hash{Symbol => Object}] the keyword options given to the client
    # @raise [ArgumentError] if a key is unknown or a value cannot be used
    def initialize(**options)
      reject_unknown(options, DEFAULTS, "option")
      @values = DEFAULTS.merge(options).freeze
      @reconnect = nested(@values[:reconnect], RECONNECT_DEFAULTS, "reconnect")
      @tls = nested(@values[:tls], TLS_DEFAULTS, "tls")
      @websocket = nested(@values[:websocket] || {}, WEBSOCKET_DEFAULTS, "websocket")
      validate
      freeze
    end

    # @return [Boolean] whether a lost connection is re-established
    def reconnect?
      !@reconnect.nil?
    end

    # @return [Hash{Symbol => Object}] every top-level option with its value
    def to_h
      @values
    end

    private

    def nested(value, defaults, name)
      case value
      when nil, false then nil
      when true then defaults
      when Hash
        reject_unknown(value, defaults, "#{name} option")
        defaults.merge(value).freeze
      else raise ArgumentError, "#{name}: must be true, false, or a Hash, got #{value.inspect}"
      end
    end

    def reject_unknown(given, known, kind)
      unknown = given.keys - known.keys
      raise ArgumentError, "unknown #{kind}: #{unknown.map(&:inspect).join(", ")}" unless unknown.empty?
    end

    def validate
      positive(:connect_timeout, :write_timeout, :max_pings_out, :max_pending, :max_pending_bytes, :worker_threads)
      validate_auth
      prefix = inbox_prefix.to_s
      return unless prefix.empty? || prefix.match?(/[\s*>]/) || prefix.end_with?(".")

      raise ArgumentError, "inbox_prefix: #{inbox_prefix.inspect} is not a literal subject"
    end

    def positive(*keys)
      keys.each do |key|
        value = @values[key]
        raise ArgumentError, "#{key}: must be a positive number, got #{value.inspect}" unless
          value.is_a?(Numeric) && value.positive?
      end
    end

    def validate_auth
      given = AUTH_KEYS.select { |key| @values[key] }
      given -= %i[nkey_seed nkey_seed_file] if given.include?(:jwt)
      return if given.size <= 1

      raise ArgumentError, "conflicting authentication options: #{given.map(&:inspect).join(", ")}"
    end
  end
end
