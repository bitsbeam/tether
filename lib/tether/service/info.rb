# frozen_string_literal: true

module Tether
  class Service
    # An `INFO` response: a service instance and the endpoints it serves.
    class Info < Response
      # @return [Array<EndpointInfo>] the endpoints in the order they were added
      attr_reader :endpoints

      # @param raw [Hash{String => Object}] the decoded response document
      def initialize(raw)
        @endpoints = Array(raw["endpoints"]).map { |entry| EndpointInfo.new(entry) }.freeze
        super
      end

      # @return [String] the service description, empty when there is none
      def description = @raw["description"].to_s
    end

    # One endpoint as an `INFO` response lists it.
    class EndpointInfo
      # @return [Hash{String => Object}] the entry as it travels on the wire
      attr_reader :raw

      # @param raw [Hash{String => Object}] the decoded endpoint entry
      def initialize(raw)
        @raw = Response.frozen_copy(raw)
        freeze
      end

      # @return [String, nil] the endpoint name
      def name = @raw["name"]

      # @return [String, nil] the subject the endpoint is subscribed to
      def subject = @raw["subject"]

      # @return [String, nil] the queue group in use, nil when the endpoint has none
      def queue_group
        group = @raw["queue_group"].to_s
        group.empty? ? nil : group
      end

      # @return [Hash{String => String}] the endpoint metadata, empty when there is none
      def metadata = @raw["metadata"] || {}.freeze

      # @return [Hash{String => Object}] the entry as it travels on the wire
      def to_h = @raw

      # @param other [Object] what to compare with
      # @return [Boolean] whether the other is the same kind of entry with the same document
      def ==(other)
        other.class == self.class && other.raw == @raw
      end
      alias eql? ==

      # @return [Integer] a hash of the document
      def hash = [self.class, @raw].hash

      # @return [String] the name and where the endpoint listens
      def inspect
        "#<#{self.class.name} name=#{name.inspect} subject=#{subject.inspect} queue_group=#{queue_group.inspect}>"
      end
    end
  end
end
