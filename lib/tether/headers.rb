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
    NO_RESPONDERS = 503
    IDLE_HEARTBEAT = 100
    private_constant :CRLF

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
      @description = description
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
    def []=(key, value)
      @entries[key] = [value.to_s]
      value
    end

    # Appends a value, keeping any already present for the key.
    #
    # @param key [String]
    # @param value [String]
    # @return [self]
    def add(key, value)
      (@entries[key] ||= []) << value.to_s
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

    # @return [Boolean] whether this is a JetStream idle heartbeat or flow control frame
    def idle_heartbeat?
      @status == IDLE_HEARTBEAT
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

    def status_suffix
      return "" if @status.nil?

      @description ? " #{@status} #{@description}" : " #{@status}"
    end
  end
end
