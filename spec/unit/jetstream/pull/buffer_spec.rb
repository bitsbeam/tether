# frozen_string_literal: true

RSpec.describe Tether::JetStream::Pull::Buffer do
  let(:buffer) { described_class.new(max_messages: 10) }
  let(:now) { Tether::Deadline.after(0) }

  def msg(payload = "data")
    Tether::Protocol::Msg.new(subject: "ORDERS.new", data: payload, reply_to: "$JS.ACK.reply")
  end

  def pulled(target = buffer)
    target.refill
    target
  end

  describe "#refill" do
    it "asks for the whole limit first" do
      expect(buffer.refill).to eq(batch: 10)
    end

    it "asks for nothing while the outstanding count is at or above the threshold" do
      expect(pulled.refill).to be_nil
    end

    it "asks for the difference once the outstanding count is below half of the limit" do
      pulled
      6.times { buffer.push(msg) }
      6.times { buffer.take(now) }

      expect(buffer.refill).to eq(batch: 6)
    end

    it "asks for a byte budget and a fixed batch when the limit is in bytes" do
      expect(described_class.new(max_messages: 1_000_000, max_bytes: 4096).refill)
        .to eq(batch: 1_000_000, max_bytes: 4096)
    end

    it "asks for nothing once the buffer is draining" do
      buffer.drain

      expect(buffer.refill).to be_nil
    end
  end

  describe "#push" do
    it "makes a message available to take" do
      first = msg("a")
      buffer.push(first)

      expect(buffer.take(now)).to be(first)
    end

    it "drops the message once the buffer is stopped" do
      buffer.stop
      buffer.push(msg)

      expect(buffer.size).to eq(0)
    end
  end

  describe "#post" do
    it "makes a control event available before buffered messages" do
      buffer.push(msg)
      buffer.post([:warning, nil, 1])

      expect(buffer.take(now)).to eq([:warning, nil, 1])
    end

    it "keeps one event of a kind with once: until it is taken" do
      2.times { buffer.post([:refill, nil, 1], once: true) }
      buffer.take(now)

      expect(buffer.take(now)).to be_nil
    end

    it "accepts the kind again after it was taken" do
      buffer.post([:refill, nil, 1], once: true)
      buffer.take(now)
      buffer.post([:refill, nil, 1], once: true)

      expect(buffer.take(now)).to eq([:refill, nil, 1])
    end
  end

  describe "#take" do
    it "returns nil when the deadline passes with nothing to take" do
      expect(buffer.take(Tether::Deadline.after(0.01))).to be_nil
    end

    it "returns messages in the order pushed" do
      messages = [msg("a"), msg("b")]
      messages.each { |message| buffer.push(message) }

      expect([buffer.take(now), buffer.take(now)]).to eq(messages)
    end

    it "wakes a waiting thread when a message is pushed" do
      taking = Thread.new { buffer.take(Tether::Deadline.after(5)) }
      eventually { taking.status == "sleep" }
      buffer.push(msg("late"))

      expect(taking.value.data).to eq("late")
    end

    it "returns :drained once a draining buffer is empty" do
      buffer.push(msg)
      buffer.drain

      expect([buffer.take(now).class, buffer.take(now)]).to eq([Tether::Protocol::Msg, :drained])
    end

    it "returns :stopped once the buffer is stopped" do
      buffer.push(msg)
      buffer.stop

      expect(buffer.take(now)).to eq(:stopped)
    end
  end

  describe "#correct" do
    it "returns :refill when what an ended pull will not deliver leaves less than the threshold outstanding" do
      expect(pulled.correct(6, 0)).to eq(:refill)
    end

    it "returns nil while enough is still outstanding" do
      expect(pulled.correct(5, 0)).to be_nil
    end

    it "never lets the outstanding count fall below what is buffered" do
      pulled
      3.times { buffer.push(msg) }
      buffer.correct(100, 0)

      expect(buffer.refill).to eq(batch: 7)
    end

    it "counts in bytes when the limit is in bytes" do
      bytes = pulled(described_class.new(max_messages: 100, max_bytes: 1000))

      expect([bytes.correct(100, 400), bytes.correct(0, 200)]).to eq([nil, :refill])
    end

    it "returns :stalled when a full byte budget ended without holding one message" do
      bytes = pulled(described_class.new(max_messages: 100, max_bytes: 1000))

      expect(bytes.correct(1, 1000, too_large: true)).to eq(:stalled)
    end
  end

  describe "#reset" do
    it "forgets what the server owed and keeps what is buffered" do
      pulled
      3.times { buffer.push(msg) }
      buffer.reset

      expect(buffer.refill).to eq(batch: 7)
    end
  end

  describe "#add" do
    it "counts messages asked for outside the refill cycle" do
      pulled
      buffer.add(5)
      buffer.correct(9, 0)

      expect(buffer.refill).to be_nil
    end
  end

  describe "#clear" do
    it "discards buffered messages and what the server owed" do
      pulled
      buffer.push(msg)
      buffer.clear

      expect(buffer.size).to eq(0)
      expect(buffer.refill).to eq(batch: 10)
    end

    it "keeps control events" do
      buffer.post([:terminal, nil, nil])
      buffer.clear

      expect(buffer.take(now)).to eq([:terminal, nil, nil])
    end
  end

  describe "#owed?" do
    it "is false before anything was pulled" do
      expect(buffer).not_to be_owed
    end

    it "is true while a pull has not delivered everything" do
      expect(pulled).to be_owed
    end

    it "is false once everything pulled is buffered" do
      pulled
      10.times { buffer.push(msg) }

      expect(buffer).not_to be_owed
    end
  end

  describe "#drain" do
    it "returns true and moves a running buffer to draining" do
      expect(buffer.drain).to be(true)
      expect(buffer.state).to eq(:draining)
    end

    it "returns false when the buffer is not running" do
      buffer.stop

      expect(buffer.drain).to be(false)
    end
  end

  describe "#stop" do
    it "discards messages and control events" do
      buffer.push(msg)
      buffer.post([:warning, nil, 1])
      buffer.stop

      expect(buffer.state).to eq(:stopped)
      expect(buffer.size).to eq(0)
    end
  end

  describe "#pause" do
    it "returns true when the deadline passes while the buffer runs" do
      expect(buffer.pause(Tether::Deadline.after(0.01))).to be(true)
    end

    it "returns false as soon as the buffer stops" do
      pausing = Thread.new { buffer.pause(Tether::Deadline.after(5)) }
      eventually { pausing.status == "sleep" }
      buffer.stop

      expect(pausing.value).to be(false)
    end
  end

  describe "#size" do
    it "counts the messages waiting to be taken" do
      2.times { buffer.push(msg) }
      buffer.take(now)

      expect(buffer.size).to eq(1)
    end
  end
end
