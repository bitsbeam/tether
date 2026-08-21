# frozen_string_literal: true

module Tether
  class Service
    # A `STATS` response: the counters of every endpoint of a service instance.
    class Stats < Response
      # @return [Array<EndpointStats>] the endpoints in the order they were added
      attr_reader :endpoints

      # @return [Time, nil] when the service started or its counters were last reset
      attr_reader :started

      # @param raw [Hash{String => Object}] the decoded response document
      def initialize(raw)
        @endpoints = Array(raw["endpoints"]).map { |entry| EndpointStats.new(entry) }.freeze
        @started = parse_time(raw["started"])
        super
      end

      private

      def parse_time(text)
        text && Time.iso8601(text).utc.freeze
      rescue ArgumentError
        nil
      end
    end

    # The counters of one endpoint. Durations are seconds; the wire's
    # nanoseconds stay in {#raw}.
    class EndpointStats < EndpointInfo
      NANOSECONDS = 1_000_000_000.0

      # @return [Integer] requests whose handler has returned
      def requests = @raw["num_requests"].to_i

      # @return [Integer] requests answered with an error, or whose handler raised
      def errors = @raw["num_errors"].to_i

      # @return [String, nil] the last error as `"<code>:<description>"`, nil when there has been none
      def last_error
        text = @raw["last_error"].to_s
        text.empty? ? nil : text
      end

      # @return [Float] seconds spent inside the handler in total
      def processing_time = @raw["processing_time"].to_i / NANOSECONDS

      # @return [Float] seconds an average request spent inside the handler
      def average_processing_time = @raw["average_processing_time"].to_i / NANOSECONDS

      # @return [Object, nil] what the service's stats handler reported for this endpoint
      def data = @raw["data"]

      # @return [String] the name and the counters
      def inspect
        "#<#{self.class.name} name=#{name.inspect} requests=#{requests} errors=#{errors} " \
          "last_error=#{last_error.inspect}>"
      end
    end
  end
end
