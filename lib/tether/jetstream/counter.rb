# frozen_string_literal: true

require "json"

module Tether
  class JetStream
    # A distributed counter: one subject of a stream with `allow_msg_counter`.
    #
    # The server keeps the running total with arbitrary precision and sends
    # it as a decimal string, which a Ruby Integer holds without loss.
    class Counter
      INCREMENT = "Nats-Incr"
      SOURCES = "Nats-Counter-Sources"

      # @return [String] the subject the counter lives on
      attr_reader :subject

      # @param jetstream [JetStream] the context
      # @param subject [String] the subject the counter lives on
      # @param stream [String, nil] the stream that captures the subject; looked up on first read when nil
      # @raise [InvalidSubjectError] if the subject holds a wildcard
      def initialize(jetstream, subject, stream: nil)
        @jetstream = jetstream
        @subject = Protocol::Subject.literal!(subject)
        @stream = stream && jetstream.check_name(stream)
        @lock = Mutex.new
      end

      # Adds to the counter.
      #
      # @param delta [Integer] what to add; may be negative, zero or arbitrarily large
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @return [Integer] the new total
      # @raise [ArgumentError] if the delta is not an Integer
      # @raise [APIError] the mapped error, for example when the stream does not allow counters
      # @raise [NoStreamResponseError] if no stream captures the subject
      def increment(delta = 1, timeout: nil)
        raise ArgumentError, "a counter is incremented by an Integer, not #{delta.inspect}" unless delta.is_a?(Integer)

        ack = @jetstream.publish(@subject, nil, headers: { INCREMENT => delta.to_s }, timeout: timeout)
        @lock.synchronize { @stream ||= ack.stream }
        Integer(ack.value, 10)
      end

      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @return [Integer, nil] the total, or nil when the counter was never incremented
      # @raise [NoStreamResponseError] if no stream captures the subject
      def value(timeout: nil)
        message = last(Deadline.coerce(timeout || @jetstream.timeout))
        message && Integer(JSON.parse(decode(message["data"])).fetch("val"), 10)
      rescue JSON::ParserError, KeyError, ArgumentError, TypeError
        raise Error, "the last message on #{@subject} is not a counter"
      end

      # Reads what an aggregating stream recorded for the counters it sources.
      #
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @return [Hash{String => Hash{String => Integer}}] the last total seen per origin stream and subject;
      #   empty when the stream sources no counter
      # @raise [NoStreamResponseError] if no stream captures the subject
      def sources(timeout: nil)
        message = last(Deadline.coerce(timeout || @jetstream.timeout))
        recorded = message && message["hdrs"] && Protocol::Headers.parse(decode(message["hdrs"]))[SOURCES]
        return {} unless recorded

        JSON.parse(recorded).transform_values { |subjects| subjects.transform_values { |total| Integer(total, 10) } }
      rescue JSON::ParserError, ArgumentError, TypeError, NoMethodError
        raise Error, "the last message on #{@subject} carries malformed counter sources"
      end

      # @return [String] the subject and the stream once it is known
      def inspect
        "#<Tether::JetStream::Counter subject=#{@subject.inspect} stream=#{@stream.inspect}>"
      end

      private

      def last(deadline)
        @jetstream.api.request("STREAM.MSG.GET.#{stream(deadline)}", { "last_by_subj" => @subject },
                               timeout: deadline)["message"]
      rescue MessageNotFoundError
        nil
      end

      def stream(deadline)
        @lock.synchronize { @stream } || begin
          name = @jetstream.stream_name_for(@subject, timeout: deadline)
          raise NoStreamResponseError, "no stream captures #{@subject}" unless name

          @lock.synchronize { @stream ||= name }
        end
      end

      def decode(encoded)
        encoded.to_s.unpack1("m")
      end
    end
  end
end
