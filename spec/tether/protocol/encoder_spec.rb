# frozen_string_literal: true

RSpec.describe Tether::Protocol::Encoder do
  describe ".pub" do
    it "encodes a publish" do
      expect(described_class.pub("foo", "bar")).to eq("PUB foo 3\r\nbar\r\n")
    end

    it "encodes an empty payload" do
      expect(described_class.pub("foo")).to eq("PUB foo 0\r\n\r\n")
    end

    it "encodes a reply subject" do
      expect(described_class.pub("foo", "bar", "_INBOX.abc.1")).to eq("PUB foo _INBOX.abc.1 3\r\nbar\r\n")
    end

    it "counts bytes rather than characters" do
      expect(described_class.pub("foo", "héllo")).to eq("PUB foo 6\r\nh\xC3\xA9llo\r\n".b)
    end

    it "returns one binary frame so the writer can lock around a single write" do
      frame = described_class.pub("foo", "héllo")

      expect(frame.encoding).to eq(Encoding::BINARY)
    end

    it "keeps arbitrary bytes intact" do
      payload = "\x00\x01\r\n\xFF".b

      expect(described_class.pub("foo", payload)).to eq("PUB foo 5\r\n#{payload}\r\n".b)
    end
  end

  describe ".hpub" do
    it "encodes the header size and the total size" do
      frame = described_class.hpub("foo", "bar", Tether::Headers["Nats-Msg-Id" => "1"])

      expect(frame).to eq("HPUB foo 28 31\r\nNATS/1.0\r\nNats-Msg-Id: 1\r\n\r\nbar\r\n")
    end

    it "encodes a reply subject" do
      frame = described_class.hpub("foo", "", Tether::Headers.new, "_INBOX.r")

      expect(frame).to eq("HPUB foo _INBOX.r 12 12\r\nNATS/1.0\r\n\r\n\r\n")
    end

    it "accepts a pre-encoded header block" do
      frame = described_class.hpub("foo", "x", "NATS/1.0\r\n\r\n")

      expect(frame).to eq("HPUB foo 12 13\r\nNATS/1.0\r\n\r\nx\r\n")
    end
  end

  describe ".sub" do
    it "encodes a subscription" do
      expect(described_class.sub("foo.>", 7)).to eq("SUB foo.> 7\r\n")
    end

    it "encodes a queue group" do
      expect(described_class.sub("foo", 8, queue: "workers")).to eq("SUB foo workers 8\r\n")
    end
  end

  describe ".unsub" do
    it "encodes an unsubscribe" do
      expect(described_class.unsub(7)).to eq("UNSUB 7\r\n")
    end

    it "encodes an auto unsubscribe after a message count" do
      expect(described_class.unsub(7, 10)).to eq("UNSUB 7 10\r\n")
    end
  end

  describe ".connect" do
    it "encodes the handshake payload as JSON" do
      expect(described_class.connect(verbose: false, protocol: 1))
        .to eq(%(CONNECT {"verbose":false,"protocol":1}\r\n))
    end
  end
end
