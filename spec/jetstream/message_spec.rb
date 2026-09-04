# frozen_string_literal: true

RSpec.describe Tether::JetStream::Message do
  include ConsumerHelper

  let(:consumer) { durable(ack_wait: 30) }
  let(:message) do
    store(1)
    consumer.fetch(1, timeout: 2).first
  end

  def unacknowledged
    stream.create_consumer(durable: "plain", ack_policy: :none).fetch(1, timeout: 2).first
  end

  def advisories(kind)
    collect(nc, "$JS.EVENT.ADVISORY.CONSUMER.#{kind}.#{name}.worker")
  end

  describe "#subject" do
    it "returns the subject the message was stored under" do
      expect(message.subject).to eq("#{name}.a")
    end
  end

  describe "#data" do
    it "returns the payload" do
      expect(message.data).to eq("m1")
    end
  end

  describe "#headers" do
    it "returns the headers the message was published with" do
      stream
      js.publish("#{name}.a", "m1", headers: { "X-Kind" => "order" })

      expect(consumer.fetch(1, timeout: 2).first.headers["X-Kind"]).to eq("order")
    end

    it "returns nil for a message without headers" do
      expect(message.headers).to be_nil
    end
  end

  describe "#reply_to" do
    it "returns the acknowledgment subject" do
      expect(message.reply_to).to match(/\A\$JS\.ACK\..*#{name}\.worker\.1\.1\.1\.\d+\.0\z/)
    end
  end

  describe "#ack" do
    it "removes the message from the consumer's pending acknowledgments" do
      message.ack

      eventually { consumer.info.ack_pending_count.zero? }
      expect(consumer.cached_info.ack_floor.stream_sequence).to eq(1)
    end

    it "returns the message" do
      expect(message.ack).to be(message)
    end

    it "raises AlreadyAckedError the second time" do
      message.ack

      expect { message.ack }.to raise_error(Tether::JetStream::AlreadyAckedError)
    end

    it "lets one of many concurrent calls through" do
      message
      outcomes = Array.new(8) { Thread.new { error_from { message.ack } } }.map(&:value)

      expect(outcomes.count(&:nil?)).to eq(1)
      expect(outcomes.compact).to all(be_a(Tether::JetStream::AlreadyAckedError))
    end

    it "does nothing on a message from a consumer with ack_policy: :none" do
      store(1)
      plain = unacknowledged
      allow(nc).to receive(:publish).and_call_original

      2.times { plain.ack }

      expect(nc).not_to have_received(:publish)
      expect(plain).not_to be_acked
    end

    it "acknowledges and asks for more messages with next: on a message from a loop" do
      store(4)
      iterator = consumer.messages(max_messages: 1)
      first = iterator.next(timeout: 2)
      eventually { consumer.info.delivered.consumer_sequence == 2 }

      first.ack(next: 2)

      eventually { consumer.info.delivered.consumer_sequence == 4 }
      expect(first).to be_acked
      expect(Array.new(3) { iterator.next(timeout: 2).data }).to eq(%w[m2 m3 m4])
      iterator.stop
    end

    it "raises ArgumentError with next: on a fetched message" do
      expect { message.ack(next: 2) }.to raise_error(ArgumentError, /consume or messages/)
    end

    it "can be retried when the publish failed" do
      message
      allow(nc).to receive(:publish).and_raise(Tether::DisconnectedError, "connection lost")
      error_from { message.ack }
      allow(nc).to receive(:publish).and_call_original

      expect(message.ack).to be_acked
    end
  end

  describe "#ack_sync" do
    it "returns once the server has confirmed the acknowledgment" do
      message.ack_sync(timeout: 2)

      expect(consumer.info.ack_pending_count).to eq(0)
      expect(message).to be_acked
    end

    it "raises AlreadyAckedError after a terminal acknowledgment" do
      message.ack

      expect { message.ack_sync }.to raise_error(Tether::JetStream::AlreadyAckedError)
    end

    it "leaves the message unacknowledged when no confirmation arrives" do
      message
      allow(nc).to receive(:request).and_raise(Tether::TimeoutError, "no reply")

      expect { message.ack_sync(timeout: 0.1) }.to raise_error(Tether::TimeoutError)
      expect(message).not_to be_acked
    end

    it "does nothing on a message from a consumer with ack_policy: :none" do
      store(1)

      expect(unacknowledged.ack_sync(timeout: 0.2)).not_to be_acked
    end
  end

  describe "#nak" do
    it "makes the server deliver the message again at once" do
      message.nak

      again = consumer.fetch(1, timeout: 2).first

      expect(again.metadata).to have_attributes(stream_sequence: 1, consumer_sequence: 2, delivered: 2)
    end

    it "delays the redelivery by delay: seconds" do
      message.nak(delay: 0.5)
      started = Tether::Deadline.now

      expect(consumer.fetch(1, wait: false)).to be_empty
      expect(consumer.fetch(1, timeout: 5).first.metadata.delivered).to eq(2)
      expect(Tether::Deadline.now - started).to be_between(0.3, 3)
    end

    it "sends the delay as nanoseconds in JSON" do
      message
      allow(nc).to receive(:publish).and_call_original

      message.nak(delay: 30)

      expect(nc).to have_received(:publish).with(message.reply_to, '-NAK {"delay": 30000000000}', reply_to: nil)
    end

    it "raises AlreadyAckedError after a terminal acknowledgment" do
      message.ack

      expect { message.nak }.to raise_error(Tether::JetStream::AlreadyAckedError)
    end
  end

  describe "#in_progress" do
    it "restarts the acknowledgment wait, so the message is not redelivered meanwhile" do
      waiting = stream.create_consumer(durable: "slow", ack_wait: 1)
      store(1)
      slow = waiting.fetch(1, timeout: 2).first
      started = Tether::Deadline.now
      eventually { Tether::Deadline.now - started > 0.6 }
      slow.in_progress

      redelivered = waiting.fetch(1, timeout: 5).first

      expect(redelivered.metadata.delivered).to eq(2)
      expect(Tether::Deadline.now - started).to be > 1.5
    end

    it "may be sent any number of times and does not acknowledge" do
      3.times { message.in_progress }

      expect(message).not_to be_acked
      expect(consumer.info.ack_pending_count).to eq(1)
    end

    it "does nothing on a message from a consumer with ack_policy: :none" do
      store(1)
      plain = unacknowledged
      allow(nc).to receive(:publish).and_call_original

      plain.in_progress

      expect(nc).not_to have_received(:publish)
    end
  end

  describe "#term" do
    it "stops the server from delivering the message again" do
      terminated = advisories("MSG_TERMINATED")
      message.term

      expect(JSON.parse(terminated.pop(timeout: 2).data)).to include("stream_seq" => 1, "deliveries" => 1)
      expect(consumer.info.ack_pending_count).to eq(0)
      expect(consumer.fetch(1, wait: false)).to be_empty
    end

    it "passes a reason on to the server's advisory" do
      terminated = advisories("MSG_TERMINATED")
      message.term(reason: "poison")

      expect(JSON.parse(terminated.pop(timeout: 2).data)).to include("reason" => "poison")
    end

    it "raises AlreadyAckedError after a terminal acknowledgment" do
      message.term

      expect { message.ack }.to raise_error(Tether::JetStream::AlreadyAckedError)
    end
  end

  describe "#acked?" do
    it "is false until the message is acknowledged terminally" do
      expect(message).not_to be_acked
    end

    it "is true after ack" do
      expect(message.ack).to be_acked
    end

    it "is true after nak" do
      expect(message.nak).to be_acked
    end

    it "is true after term" do
      expect(message.term).to be_acked
    end
  end

  describe "#marker?" do
    it "is false for a message a client published" do
      expect(message).not_to be_marker
    end
  end

  describe "#marker_reason" do
    it "is nil for a message a client published" do
      expect(message.marker_reason).to be_nil
    end
  end

  describe "#metadata" do
    it "returns the stream, consumer, sequences, delivery count and pending count" do
      store(3)
      second = consumer.fetch(2, timeout: 2).to_a.last

      expect(second.metadata).to have_attributes(stream: name, consumer: "worker", domain: nil, stream_sequence: 2,
                                                 consumer_sequence: 2, delivered: 1, pending: 1)
    end

    it "returns the time the message was stored" do
      expect(message.metadata.time).to be_within(5).of(Time.now)
    end
  end

  describe "#inspect" do
    it "shows the subject and the size of the payload" do
      expect(message.inspect).to eq(%(#<Tether::JetStream::Message subject="#{name}.a" bytes=2>))
    end
  end
end
