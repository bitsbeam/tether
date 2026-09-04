# frozen_string_literal: true

require "json"

module Tether
  class JetStream
    # An event the server published on `$JS.EVENT.ADVISORY.>`.
    class Advisory
      PREFIX = "$JS.EVENT.ADVISORY"
      KINDS = { api: "API", stream: "STREAM.>", consumer: "CONSUMER.>", server: "SERVER.>", domain: "DOMAIN.>" }.freeze

      # @return [String, nil] the schema name, for example `io.nats.jetstream.advisory.v1.stream_action`
      attr_reader :type

      # @return [String] the subject the advisory was published on
      attr_reader :subject

      # @return [Hash{String => Object}] the decoded event with every key the server sent
      attr_reader :data

      # @param kind [Symbol, String, nil] nil for every advisory, one of {KINDS}, or the subject after the prefix
      # @return [String] the subject to subscribe to
      # @raise [ArgumentError] if the kind is not known
      def self.subject_for(kind)
        return "#{PREFIX}.>" if kind.nil?
        return "#{PREFIX}.#{kind}" unless kind.is_a?(Symbol)

        "#{PREFIX}.#{KINDS.fetch(kind) { raise ArgumentError, "unknown advisory kind: #{kind.inspect}" }}"
      end

      # @param msg [Protocol::Msg] a message from an advisory subject
      # @return [Advisory] the decoded advisory
      # @raise [JSON::ParserError] if the payload is not JSON
      def self.from_msg(msg)
        new(msg.subject, JSON.parse(msg.data))
      end

      # @param subject [String] the subject the advisory was published on
      # @param data [Hash{String => Object}] the decoded event
      def initialize(subject, data)
        @subject = subject
        @data = data.freeze
        @type = data["type"]
        freeze
      end

      # @param key [String, Symbol] a key of the event
      # @return [Object, nil] its value
      def [](key)
        @data[key.to_s]
      end

      # @return [String, nil] the event's unique id
      def id
        @data["id"]
      end

      # @return [Time, nil] when the server produced the event
      def time
        stamp = @data["timestamp"]
        stamp && Time.iso8601(stamp)
      end

      # @return [Hash{String => Object}] the decoded event
      def to_h
        @data
      end

      # @return [String] the type and subject
      def inspect
        "#<Tether::JetStream::Advisory type=#{@type.inspect} subject=#{@subject.inspect}>"
      end
    end
  end
end
