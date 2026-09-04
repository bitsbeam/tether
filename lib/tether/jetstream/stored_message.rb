# frozen_string_literal: true

require "time"

module Tether
  class JetStream
    # A message read back from a stream by sequence or subject, outside of any consumer.
    class StoredMessage
      STREAM = "Nats-Stream"
      SUBJECT = "Nats-Subject"
      SEQUENCE = "Nats-Sequence"
      TIME = "Nats-Time-Stamp"

      # @return [String] the subject the message is stored under
      attr_reader :subject

      # @return [Integer] the message's sequence in the stream
      attr_reader :sequence

      # @return [String] the payload, always binary
      attr_reader :data

      # @return [Protocol::Headers] the stored headers; a Direct Get adds the `Nats-*` headers that describe the message
      attr_reader :headers

      # @return [Time] when the message was stored
      attr_reader :time

      # Builds a message from a Direct Get reply.
      #
      # @param msg [Protocol::Msg] a reply that carries no status
      # @return [StoredMessage] the stored message
      # @raise [Error] if the reply lacks the headers that describe the message
      def self.from_direct(msg)
        headers = msg.headers
        subject, sequence, time = headers && [SUBJECT, SEQUENCE, TIME].map { |name| headers[name] }
        sequence = Integer(sequence, 10, exception: false)
        raise Error, "a direct get reply lacks its #{SUBJECT}, #{SEQUENCE} or #{TIME} header" unless subject && sequence

        new(subject: subject, sequence: sequence, data: msg.data, headers: headers, time: Time.iso8601(time.to_s))
      rescue ArgumentError
        raise Error, "a direct get reply carries an invalid #{TIME} header: #{time.inspect}"
      end

      # Builds a message from the `message` member of a `STREAM.MSG.GET` response.
      #
      # @param message [Hash{String => Object}] the decoded member
      # @return [StoredMessage] the stored message
      def self.from_api(message)
        block = message["hdrs"]
        new(subject: message["subject"], sequence: message["seq"], data: message["data"].to_s.unpack1("m"),
            headers: block ? Protocol::Headers.parse(block.unpack1("m")) : Protocol::Headers.new,
            time: Time.iso8601(message["time"]))
      end

      # @param subject [String] the subject the message is stored under
      # @param sequence [Integer] the message's sequence in the stream
      # @param data [String] the payload
      # @param headers [Protocol::Headers] the stored headers
      # @param time [Time] when the message was stored
      def initialize(subject:, sequence:, data:, headers:, time:)
        @subject = subject
        @sequence = sequence
        @data = data
        @headers = headers
        @time = time
        freeze
      end

      # @return [String] the subject, sequence and payload size
      def inspect
        "#<Tether::JetStream::StoredMessage subject=#{@subject.inspect} sequence=#{@sequence} bytes=#{@data.bytesize}>"
      end
    end
  end
end
