# frozen_string_literal: true

RSpec.describe Tether::Protocol::Headers do
  describe ".parse" do
    it "reads a plain header block" do
      headers = described_class.parse("NATS/1.0\r\nNats-Msg-Id: abc-1\r\nNats-Expected-Last-Sequence: 42\r\n\r\n")

      expect(headers["Nats-Msg-Id"]).to eq("abc-1")
      expect(headers["Nats-Expected-Last-Sequence"]).to eq("42")
      expect(headers.status).to be_nil
    end

    it "reads the no responders status the server sends for an unanswered request" do
      headers = described_class.parse("NATS/1.0 503\r\n\r\n")

      expect(headers.status).to eq(503)
      expect(headers).to be_no_responders
    end

    it "reads a status with a description" do
      headers = described_class.parse("NATS/1.0 409 Exceeded MaxWaiting\r\n\r\n")

      expect(headers.status).to eq(409)
      expect(headers.description).to eq("Exceeded MaxWaiting")
    end

    it "recognizes a JetStream idle heartbeat" do
      headers = described_class.parse("NATS/1.0 100 Idle Heartbeat\r\nNats-Last-Consumer: 5\r\n\r\n")

      expect(headers).to be_idle_heartbeat
      expect(headers).not_to be_flow_control
      expect(headers["Nats-Last-Consumer"]).to eq("5")
    end

    it "tells flow control apart from a heartbeat, which share status 100" do
      headers = described_class.parse("NATS/1.0 100 FlowControl Request\r\n\r\n")

      expect(headers).to be_control
      expect(headers).to be_flow_control
      expect(headers).not_to be_idle_heartbeat
    end

    it "keeps every value of a repeated key" do
      headers = described_class.parse("NATS/1.0\r\nX-Trace: a\r\nX-Trace: b\r\n\r\n")

      expect(headers.values("X-Trace")).to eq(%w[a b])
      expect(headers["X-Trace"]).to eq("a")
    end

    it "treats keys as case sensitive because JetStream relies on literal names" do
      headers = described_class.parse("NATS/1.0\r\nNats-Msg-Id: a\r\nnats-msg-id: b\r\n\r\n")

      expect(headers["Nats-Msg-Id"]).to eq("a")
      expect(headers["nats-msg-id"]).to eq("b")
    end

    it "rejects a block that is not a NATS header block" do
      expect { described_class.parse("HTTP/1.1 200 OK\r\n\r\n") }
        .to raise_error(Tether::ProtocolError, /version line/)
    end
  end

  describe "#to_wire" do
    it "round trips through the parser" do
      headers = described_class["X-Trace" => "t1"]
      headers.add("X-Trace", "t2")
      headers["Nats-Msg-Id"] = "id-1"

      expect(described_class.parse(headers.to_wire).to_h)
        .to eq("X-Trace" => %w[t1 t2], "Nats-Msg-Id" => ["id-1"])
    end

    it "encodes as binary so it can be concatenated into a frame" do
      expect(described_class["K" => "v"].to_wire.encoding).to eq(Encoding::BINARY)
    end

    it "emits the status on the version line" do
      expect(described_class.new(status: 503).to_wire).to eq("NATS/1.0 503\r\n\r\n")
      expect(described_class.new(status: 409, description: "Consumer Deleted").to_wire)
        .to eq("NATS/1.0 409 Consumer Deleted\r\n\r\n")
    end

    it "ends with a blank line so the byte count matches the wire format" do
      expect(described_class["K" => "v"].to_wire).to end_with("\r\n\r\n")
    end
  end

  describe "rejecting values that would forge header lines" do
    it "refuses a value containing CRLF, which the server would read as another header" do
      expect { described_class.new["X"] = "a\r\nNats-Msg-Id: forged" }
        .to raise_error(ArgumentError, /may not span lines/)
    end

    it "refuses a value containing a bare LF" do
      expect { described_class.new.add("X", "a\nb") }.to raise_error(ArgumentError, /may not span lines/)
    end

    it "refuses a key that is not an RFC 7230 token" do
      expect { described_class.new["Bad Key"] = "v" }.to raise_error(ArgumentError, /invalid header name/)
      expect { described_class.new["X:Y"] = "v" }.to raise_error(ArgumentError, /invalid header name/)
    end

    it "keeps the irregular casing the server matches byte for byte" do
      expect(described_class["Nats-TTL" => "60"].to_wire).to include("Nats-TTL: 60")
      expect(described_class["Nats-UpTo-Sequence" => "9"].to_wire).to include("Nats-UpTo-Sequence: 9")
    end
  end

  describe "#[]=" do
    it "replaces every value for the key" do
      headers = described_class["K" => "a"]
      headers.add("K", "b")
      headers["K"] = "c"

      expect(headers.values("K")).to eq(["c"])
    end
  end

  it "reports emptiness only when there is no status either" do
    expect(described_class.new).to be_empty
    expect(described_class.new(status: 503)).not_to be_empty
    expect(described_class["K" => "v"]).not_to be_empty
  end
end
