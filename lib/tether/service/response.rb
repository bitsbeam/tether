# frozen_string_literal: true

module Tether
  class Service
    # What every discovery response carries: who answered. Frozen, and keeps
    # the document it was read from so fields added later are not lost.
    class Response
      # Copies a decoded JSON value into frozen hashes, arrays and strings.
      #
      # @api private
      # @param value [Object] a decoded JSON value
      # @return [Object] a deeply frozen copy
      def self.frozen_copy(value)
        case value
        when Hash then value.to_h { |key, item| [frozen_copy(key.to_s), frozen_copy(item)] }.freeze
        when Array then value.map { |item| frozen_copy(item) }.freeze
        when String then value.frozen? ? value : value.dup.freeze
        else value
        end
      end

      # @return [Hash{String => Object}] the document as it travels on the wire
      attr_reader :raw

      # @param raw [Hash{String => Object}] the decoded response document
      def initialize(raw)
        @raw = Response.frozen_copy(raw)
        freeze
      end

      # @return [String, nil] the response type, such as `io.nats.micro.v1.ping_response`
      def type = @raw["type"]

      # @return [String, nil] the kind of service
      def name = @raw["name"]

      # @return [String, nil] the id of the instance that answered
      def id = @raw["id"]

      # @return [String, nil] the service version
      def version = @raw["version"]

      # @return [Hash{String => String}] the service metadata, empty when there is none
      def metadata = @raw["metadata"] || {}.freeze

      # @return [Hash{String => Object}] the document as it travels on the wire
      def to_h = @raw

      # @param other [Object] what to compare with
      # @return [Boolean] whether the other is the same kind of response with the same document
      def ==(other)
        other.class == self.class && other.raw == @raw
      end
      alias eql? ==

      # @return [Integer] a hash of the document
      def hash = [self.class, @raw].hash

      # @return [String] the kind of response and who sent it
      def inspect
        "#<#{self.class.name} name=#{name.inspect} id=#{id.inspect} version=#{version.inspect}>"
      end
    end

    # A `PING` response: a service instance announcing itself.
    class Ping < Response; end
  end
end
