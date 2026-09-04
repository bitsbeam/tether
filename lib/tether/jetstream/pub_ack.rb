# frozen_string_literal: true

module Tether
  class JetStream
    # The server's acknowledgment that a published message was stored.
    class PubAck < Schema
      field :stream, :string
      field :sequence, :integer, key: "seq"
      field :domain, :string
      field :duplicate, :boolean
      field :value, :string, key: "val"
      field :batch_id, :string, key: "batch"
      field :count, :integer

      # Decodes the reply to a publish.
      #
      # @param data [String] the body of the reply
      # @return [PubAck] the acknowledgment
      # @raise [APIError] the mapped error when the server refused the message
      # @raise [Error] if the reply is not a JetStream acknowledgment
      def self.parse(data)
        response = JSON.parse(data)
        raise APIError.for(response["error"]) if response.is_a?(Hash) && response["error"]
        return from_h(response) if response.is_a?(Hash) && !response["stream"].to_s.empty?

        raise Error, "the reply to a publish is not a JetStream acknowledgment"
      rescue JSON::ParserError
        raise Error, "the reply to a publish is not a JetStream acknowledgment"
      end

      # @return [Boolean] whether the server recognized the message id and stored nothing new
      def duplicate?
        duplicate == true
      end
    end

    # Builds the headers that carry JetStream publish options.
    module PublishHeaders
      MSG_ID = "Nats-Msg-Id"
      EXPECTED = {
        stream: "Nats-Expected-Stream", last_sequence: "Nats-Expected-Last-Sequence",
        last_subject_sequence: "Nats-Expected-Last-Subject-Sequence",
        subject: "Nats-Expected-Last-Subject-Sequence-Subject", last_msg_id: "Nats-Expected-Last-Msg-Id"
      }.freeze
      TTL = "Nats-TTL"
      ROLLUP = "Nats-Rollup"
      ROLLUPS = { subject: "sub", all: "all" }.freeze

      # @param headers [Protocol::Headers, Hash, nil] the caller's own headers, which are not modified
      # @param msg_id [String, nil] a deduplication id
      # @param expect [Hash{Symbol => Object}, nil] preconditions: see {EXPECTED}
      # @param ttl [Numeric, Symbol, nil] seconds the message lives, or :never
      # @param rollup [Symbol, nil] :subject or :all
      # @param schedule [Hash{Symbol => Object}, nil] makes the message a schedule: see {Schedule.apply}
      # @param cancel_schedule [String, nil] the subject of a schedule to remove when this message is stored
      # @return [Protocol::Headers, nil] the headers to publish with, or nil when there are none
      # @raise [ArgumentError] if an option is not recognized
      def self.build(headers = nil, msg_id: nil, expect: nil, ttl: nil, rollup: nil, schedule: nil,
                     cancel_schedule: nil)
        result = copy(headers)
        Schedule.apply(result, schedule) if schedule
        Schedule.cancel(result, cancel_schedule) if cancel_schedule
        result[MSG_ID] = msg_id.to_s if msg_id
        expectations(result, expect) if expect
        result[TTL] = ttl == :never ? "never" : duration(ttl) if ttl
        result[ROLLUP] = ROLLUPS.fetch(rollup) { raise ArgumentError, "rollup: must be :subject or :all" } if rollup
        result.empty? ? nil : result
      end

      # @param headers [Protocol::Headers, Hash, nil] headers to copy
      # @return [Protocol::Headers] a copy that can be added to
      def self.copy(headers)
        entries = headers.respond_to?(:to_h) ? headers.to_h : {}
        Protocol::Headers.new(entries: entries)
      end

      # @param seconds [Numeric] a duration in seconds
      # @return [String] the duration as the server parses it
      # @raise [ArgumentError] if it is not a number
      def self.duration(seconds)
        raise ArgumentError, "ttl: must be a number of seconds or :never" unless seconds.is_a?(Numeric)

        whole = seconds.to_i
        whole == seconds ? "#{whole}s" : "#{(seconds * 1000).round}ms"
      end

      def self.expectations(result, expect)
        expect.each do |name, value|
          key = EXPECTED.fetch(name) { raise ArgumentError, "unknown expectation: #{name.inspect}" }
          raise ArgumentError, "expect: subject: cannot be empty" if name == :subject && value == ""

          result[key] = value.to_s unless value.nil?
        end
      end
      private_class_method :expectations
    end
  end
end
