# frozen_string_literal: true

RSpec.describe Tether::JetStream::Message do
  let(:client) { FakePullClient.new }
  let(:current) { "$JS.ACK.hub.szMpdrwD.ORDERS.worker.2.34.13.1763800000000000000.5" }

  def message(reply_to, headers: nil, **options)
    block = headers && "NATS/1.0\r\n#{headers.map { |key, value| "#{key}: #{value}\r\n" }.join}\r\n"
    msg = Tether::Protocol::Msg.new(subject: "ORDERS.new", data: "data", reply_to: reply_to, header_bytes: block)
    described_class.new(msg, client, **options)
  end

  def published
    client.published.map { |frame| [frame.subject, frame.payload] }
  end

  describe "#metadata" do
    it "parses the eleven-token acknowledgment subject" do
      expect(message(current).metadata)
        .to have_attributes(stream: "ORDERS", consumer: "worker", domain: "hub", delivered: 2, stream_sequence: 34,
                            consumer_sequence: 13, pending: 5, time: Time.at(1_763_800_000).utc)
    end

    it "reads the domain placeholder as no domain" do
      expect(message("$JS.ACK._.szMpdrwD.ORDERS.worker.1.1.1.1763800000000000000.0").metadata.domain).to be_nil
    end

    it "parses the nine-token acknowledgment subject of older servers" do
      expect(message("$JS.ACK.ORDERS.worker.2.34.13.1763800000000000000.5").metadata)
        .to have_attributes(stream: "ORDERS", consumer: "worker", domain: nil, delivered: 2, stream_sequence: 34,
                            consumer_sequence: 13, pending: 5)
    end

    it "ignores tokens after the pending count" do
      expect(message("#{current}.random").metadata.pending).to eq(5)
    end

    it "keeps the nanoseconds of the storage time" do
      expect(message("$JS.ACK.ORDERS.worker.1.1.1.1763800000123456789.0").metadata.time.nsec).to eq(123_456_789)
    end

    it "raises NotJetStreamMessageError for ten tokens" do
      expect { message("$JS.ACK.x.ORDERS.worker.2.34.13.1763800000000000000.5").metadata }
        .to raise_error(Tether::JetStream::NotJetStreamMessageError)
    end

    it "raises NotJetStreamMessageError for a number that is not decimal" do
      expect { message("$JS.ACK.ORDERS.worker.2.x.13.1763800000000000000.5").metadata }
        .to raise_error(Tether::JetStream::NotJetStreamMessageError)
    end

    it "raises NotJetStreamMessageError for a message without a reply subject" do
      expect { message(nil).metadata }.to raise_error(Tether::JetStream::NotJetStreamMessageError)
    end
  end

  describe "#ack" do
    it "publishes +ACK to the reply subject exactly as received" do
      message(current).ack

      expect(published).to eq([[current, "+ACK"]])
    end

    it "raises NotJetStreamMessageError for a message that did not come from a consumer" do
      expect { message("_INBOX.reply").ack }.to raise_error(Tether::JetStream::NotJetStreamMessageError)
      expect(published).to be_empty
    end

    it "publishes nothing when the consumer does not take acknowledgments" do
      message(current, acks: false).ack

      expect(published).to be_empty
    end
  end

  describe "#nak" do
    it "publishes -NAK without a delay" do
      message(current).nak

      expect(published).to eq([[current, "-NAK"]])
    end

    it "publishes a fractional delay as whole nanoseconds" do
      message(current).nak(delay: 1.5)

      expect(published).to eq([[current, '-NAK {"delay": 1500000000}']])
    end

    it "publishes a plain -NAK for a delay of zero" do
      message(current).nak(delay: 0)

      expect(published).to eq([[current, "-NAK"]])
    end
  end

  describe "#in_progress" do
    it "publishes +WPI" do
      message(current).in_progress

      expect(published).to eq([[current, "+WPI"]])
    end
  end

  describe "#term" do
    it "publishes +TERM without a reason" do
      message(current).term

      expect(published).to eq([[current, "+TERM"]])
    end

    it "publishes the reason after +TERM" do
      message(current).term(reason: " poison ")

      expect(published).to eq([[current, "+TERM poison"]])
    end
  end

  describe "#marker?" do
    it "is true for a marker the server left for removed messages" do
      expect(message(current, headers: { "Nats-Marker-Reason" => "MaxAge" })).to be_marker
    end

    it "is false for a message with other headers" do
      expect(message(current, headers: { "X-Kind" => "order" })).not_to be_marker
    end
  end

  describe "#marker_reason" do
    it "returns why the server left the marker" do
      expect(message(current, headers: { "Nats-Marker-Reason" => "Purge" }).marker_reason).to eq("Purge")
    end
  end
end
