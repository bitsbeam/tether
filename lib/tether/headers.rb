# frozen_string_literal: true

module Tether
  # NATS message headers, plus the optional status and description that the
  # server encodes on the version line (`NATS/1.0 503`).
  #
  # Keys are case sensitive and used exactly as given: JetStream relies on
  # literal names such as `Nats-Msg-Id` and `Nats-Expected-Last-Sequence`.
  # A key may carry several values, as in Go's `nats.Header`.
  class Headers
    VERSION = "NATS/1.0"
    CRLF = "\r\n"

    # Status codes the server puts on the version line. Taken from the
    # nats-server source rather than from HTTP: the numbers overlap with HTTP
    # but the meanings are the server's own.
    CONTROL = 100
    NO_CONTENT = 204
    BAD_REQUEST = 400
    NOT_FOUND = 404
    REQUEST_TIMEOUT = 408
    CONFLICT = 409
    REQUIRED_API_LEVEL = 412
    TOO_MANY_RESULTS = 413
    PIN_ID_MISMATCH = 423
    TOO_MANY_REQUESTS = 429
    INTERNAL_ERROR = 500
    NO_RESPONDERS = 503

    # A header name is an RFC 7230 token. A value may not contain CR or LF:
    # the server does not validate the block it forwards, so an unescaped CRLF
    # in a value forges a header line that its own lookup will honor.
    TOKEN = /\A[!#$%&'*+\-.^_`|~0-9A-Za-z]+\z/
    FORBIDDEN_IN_VALUE = /[\r\n]/
    private_constant :CRLF, :TOKEN, :FORBIDDEN_IN_VALUE

    # @return [Integer, nil] status code from the version line
    attr_reader :status

    # @return [String, nil] status description from the version line
    attr_reader :description

    # Parses a header block as it appears on the wire, including the trailing
    # blank line.
    #
    # @param bytes [String] the header section of an HMSG or HPUB frame
    # @return [Headers]
    # @raise [ProtocolError] if the version line is missing or malformed
    def self.parse(bytes)
      lines = bytes.to_s.split(CRLF, -1)
      version = lines.shift.to_s
      raise ProtocolError, "invalid header version line: #{version.inspect}" unless version.start_with?(VERSION)

      status, description = parse_version_line(version)
      headers = new(status: status, description: description)
      lines.each do |line|
        next if line.empty?

        key, _, value = line.partition(":")
        raise ProtocolError, "invalid header line: #{line.inspect}" if value.empty? && !line.include?(":")

        headers.add(key.strip, value.strip)
      end
      headers
    end

    # @return [Array(Integer, nil), Array(nil, nil)]
    def self.parse_version_line(version)
      remainder = version[VERSION.length..].to_s.strip
      return [nil, nil] if remainder.empty?

      code, _, text = remainder.partition(" ")
      [Integer(code, exception: false), text.strip.empty? ? nil : text.strip]
    end
    private_class_method :parse_version_line

    # Builds headers from a hash, so the common case needs no braces.
    #
    # @param entries [Hash{String => String, Array<String>}]
    # @return [Headers]
    def self.[](entries)
      new(entries: entries)
    end

    # @param entries [Hash{String => String, Array<String>}] initial headers
    # @param status [Integer, nil]
    # @param description [String, nil]
    def initialize(entries: {}, status: nil, description: nil)
      @entries = {}
      @status = status
      @description = description && validate_value(description)
      entries.each { |key, value| Array(value).each { |item| add(key.to_s, item.to_s) } }
    end

    # @param key [String]
    # @return [String, nil] the first value for the key
    def [](key)
      @entries[key]&.first
    end

    # Replaces every value for a key.
    #
    # @param key [String]
    # @param value [String]
    # @return [String]
    # @raise [ArgumentError] if the key is not a token or the value spans lines
    def []=(key, value)
      @entries[validate_key(key)] = [validate_value(value)]
      value
    end

    # Appends a value, keeping any already present for the key.
    #
    # Repeated keys are legal on the wire, but every server side lookup stops at
    # the first match, so a second value for a `Nats-` header is silently
    # ignored by the server rather than merged.
    #
    # @param key [String]
    # @param value [String]
    # @return [self]
    # @raise [ArgumentError] if the key is not a token or the value spans lines
    def add(key, value)
      (@entries[validate_key(key)] ||= []) << validate_value(value)
      self
    end

    # @param key [String]
    # @return [Array<String>]
    def values(key)
      @entries.fetch(key, []).dup
    end

    # @param key [String]
    # @return [Array<String>, nil] the removed values
    def delete(key)
      @entries.delete(key)
    end

    # @param key [String]
    # @return [Boolean]
    def key?(key)
      @entries.key?(key)
    end

    # @yieldparam key [String]
    # @yieldparam value [String]
    # @return [Enumerator, self]
    def each(&block)
      return to_enum(:each) unless block

      @entries.each { |key, values| values.each { |value| block.call(key, value) } }
      self
    end

    # @return [Array<String>]
    def keys
      @entries.keys
    end

    # @return [Boolean] whether there are no headers, status, or description
    def empty?
      @entries.empty? && @status.nil? && @description.nil?
    end

    # @return [Boolean] whether the server reported that nobody is listening
    def no_responders?
      @status == NO_RESPONDERS
    end

    # Whether this is a JetStream control frame of either kind.
    #
    # @return [Boolean]
    def control?
      @status == CONTROL
    end

    # An idle heartbeat carries no reply subject and must not be answered,
    # unless it carries `Nats-Consumer-Stalled`, whose value is a subject to
    # publish to. Distinguished from flow control by description, not by code,
    # because both are status 100.
    #
    # @return [Boolean]
    def idle_heartbeat?
      control? && @description.to_s.start_with?("Idle")
    end

    # A flow control request carries a reply subject and the consumer stalls
    # until the client publishes to it.
    #
    # @return [Boolean]
    def flow_control?
      control? && @description.to_s.start_with?("Flow")
    end

    # @return [Hash{String => Array<String>}]
    def to_h
      @entries.transform_values(&:dup)
    end

    # Encodes the header block for the wire, including the trailing blank line.
    #
    # @return [String] binary encoded header section
    def to_wire
      out = "#{VERSION}#{status_suffix}#{CRLF}"
      each { |key, value| out << key << ": " << value << CRLF }
      out << CRLF
      out.force_encoding(Encoding::BINARY)
    end

    # @return [String]
    def inspect
      "#<Tether::Headers status=#{@status.inspect} #{to_h.inspect}>"
    end

    private

    def validate_key(key)
      key = key.to_s
      raise ArgumentError, "invalid header name: #{key.inspect}" unless key.match?(TOKEN)

      key
    end

    def validate_value(value)
      value = value.to_s
      raise ArgumentError, "header value may not span lines: #{value.inspect}" if value.match?(FORBIDDEN_IN_VALUE)

      value
    end

    def status_suffix
      return "" if @status.nil?

      @description ? " #{@status} #{@description}" : " #{@status}"
    end
  end
end
