# frozen_string_literal: true

require "json"

module Tether
  class JetStream
    # A message delivered by a consumer.
    #
    # It may be acknowledged terminally once, with {#ack}, {#ack_sync}, {#nak}
    # or {#term}. On a message from a consumer with `ack_policy: :none`, which
    # includes ordered consumers, the acknowledgment methods do nothing, so the
    # same handler works under either policy.
    class Message
      ACK = "+ACK"
      NAK = "-NAK"
      IN_PROGRESS = "+WPI"
      TERM = "+TERM"
      NEXT = "+NXT"
      ACK_PREFIX = "$JS.ACK."
      MARKER_REASON = "Nats-Marker-Reason"
      NANOSECONDS = 1_000_000_000

      # What the acknowledgment subject says about one delivery.
      Metadata = Data.define(:stream, :consumer, :domain, :stream_sequence, :consumer_sequence,
                             :delivered, :pending, :time)

      # @param msg [Protocol::Msg] the message as it arrived
      # @param client [Client] the connection acknowledgments are published on
      # @param acks [Boolean] false when the consumer's ack policy is `:none`
      # @param pull [Pull, nil] the loop that delivered it, which serves `ack(next:)`
      def initialize(msg, client, acks: true, pull: nil)
        @msg = msg
        @client = client
        @acks = acks
        @pull = pull
        @state = :open
        @lock = Mutex.new
      end

      # @return [String] the subject the message was stored under
      def subject = @msg.subject

      # @return [String] the payload, always binary
      def data = @msg.data

      # @return [Protocol::Headers, nil] the headers, or nil when it has none
      def headers = @msg.headers

      # @return [String, nil] the acknowledgment subject
      def reply_to = @msg.reply_to

      # Acknowledges the message without waiting for the server.
      #
      # @param next [Integer, nil] also ask for this many more messages, in the same publish
      # @return [self] this message
      # @raise [AlreadyAckedError] if it was acknowledged terminally before
      # @raise [NotJetStreamMessageError] if it carries no acknowledgment subject
      # @raise [ArgumentError] if `next:` is used on a message that no `consume` or `messages` loop delivered
      def ack(next: nil)
        batch = binding.local_variable_get(:next)
        return terminal { publish(ACK) } unless batch
        raise ArgumentError, "ack(next:) needs a message delivered by consume or messages" unless @pull

        terminal do
          inbox, request = @pull.next_request(Integer(batch))
          publish("#{NEXT} #{request}", reply_to: inbox)
        end
      end

      # Acknowledges the message and waits for the server to confirm it.
      #
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @return [self] this message
      # @raise [TimeoutError] if no confirmation arrives; the message may be acknowledged again
      # @raise [AlreadyAckedError] if it was acknowledged terminally before
      def ack_sync(timeout: 5)
        deadline = Deadline.coerce(timeout)
        terminal { @client.request(ack_subject, ACK, timeout: deadline) }
      end

      # Asks for the message to be delivered again.
      #
      # @param delay [Numeric, nil] seconds to wait before the redelivery; nil redelivers at once
      # @return [self] this message
      # @raise [AlreadyAckedError] if it was acknowledged terminally before
      def nak(delay: nil)
        payload = delay&.positive? ? %(#{NAK} {"delay": #{(delay * NANOSECONDS).round}}) : NAK
        terminal { publish(payload) }
      end

      # Tells the server the message is still being worked on, restarting its
      # acknowledgment wait. May be sent any number of times.
      #
      # @return [self] this message
      def in_progress
        publish(IN_PROGRESS) if @acks
        self
      end

      # Tells the server never to deliver the message again.
      #
      # @param reason [String, nil] recorded in the server's advisory
      # @return [self] this message
      # @raise [AlreadyAckedError] if it was acknowledged terminally before
      def term(reason: nil)
        terminal { publish(reason.to_s.strip.empty? ? TERM : "#{TERM} #{reason.to_s.strip}") }
      end

      # @return [Boolean] whether the message was acknowledged terminally
      def acked?
        @state == :acked
      end

      # @return [Boolean] whether this is a marker the server left for removed messages, not user data
      def marker?
        !marker_reason.nil?
      end

      # @return [String, nil] why the server left this marker: `MaxAge`, `Purge` or `Remove`
      def marker_reason
        headers&.[](MARKER_REASON)
      end

      # @return [Metadata] the stream, consumer, sequences, delivery count, pending count and storage time
      # @raise [NotJetStreamMessageError] if the message carries no acknowledgment subject
      def metadata
        @metadata ||= parse_metadata
      end

      # @return [String] the subject and payload size without the bytes
      def inspect
        "#<Tether::JetStream::Message subject=#{subject.inspect} bytes=#{data.bytesize}>"
      end

      private

      def terminal(&)
        return self unless @acks

        ack_subject
        @lock.synchronize do
          raise AlreadyAckedError, "message on #{subject} was already acknowledged" unless @state == :open

          @state = :acking
        end
        settle(&)
        self
      end

      def settle
        done = false
        yield
        done = true
      ensure
        @lock.synchronize { @state = done ? :acked : :open }
      end

      def publish(payload, reply_to: nil)
        @client.publish(ack_subject, payload, reply_to: reply_to)
      end

      def ack_subject
        subject = reply_to
        return subject if subject&.start_with?(ACK_PREFIX)

        raise NotJetStreamMessageError, "message on #{self.subject} did not come from a consumer"
      end

      def parse_metadata
        tokens = ack_subject.split(".")
        start = metadata_offset(tokens.size)
        delivered, stream_sequence, consumer_sequence, stored, pending = tokens[start + 2, 5].map { |t| Integer(t, 10) }
        Metadata.new(stream: tokens[start], consumer: tokens[start + 1], domain: domain(tokens, start),
                     stream_sequence: stream_sequence, consumer_sequence: consumer_sequence, delivered: delivered,
                     pending: pending, time: Time.at(stored / NANOSECONDS, stored % NANOSECONDS, :nsec).utc)
      rescue ArgumentError, TypeError
        raise NotJetStreamMessageError, "malformed acknowledgment subject #{reply_to.inspect}"
      end

      def domain(tokens, start)
        tokens[2] if start == 4 && tokens[2] != "_"
      end

      def metadata_offset(count)
        return 2 if count == 9
        return 4 if count >= 11

        raise NotJetStreamMessageError, "malformed acknowledgment subject #{reply_to.inspect}"
      end
    end
  end
end
