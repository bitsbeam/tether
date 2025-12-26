# frozen_string_literal: true

RSpec.describe Tether::Protocol::Parser do
  subject(:parser) { described_class.new(sink) }

  let(:sink) { RecordingSink.new }

  def feed(*chunks)
    chunks.each { parser << _1 }
    sink.events
  end

  describe "control lines" do
    it "parses INFO and leaves the JSON unparsed for the client" do
      expect(feed(%(INFO {"server_id":"x","max_payload":1048576}\r\n)))
        .to eq([[:info, %({"server_id":"x","max_payload":1048576})]])
    end

    it "parses keepalive and acknowledgement lines" do
      expect(feed("PING\r\nPONG\r\n+OK\r\n")).to eq([[:ping], [:pong], [:ok]])
    end

    it "unquotes an -ERR description" do
      expect(feed("-ERR 'Authorization Violation'\r\n"))
        .to eq([[:error, "Authorization Violation"]])
    end

    it "accepts lowercase verbs" do
      expect(feed("ping\r\npong\r\n")).to eq([[:ping], [:pong]])
    end
  end

  describe "messages" do
    it "parses MSG" do
      expect(feed("MSG foo 1 3\r\nbar\r\n")).to eq([[:msg, "foo", "1", nil, nil, "bar"]])
    end

    it "parses MSG with a reply subject" do
      expect(feed("MSG foo 2 _INBOX.a.b 3\r\nbaz\r\n"))
        .to eq([[:msg, "foo", "2", "_INBOX.a.b", nil, "baz"]])
    end

    it "parses an empty payload" do
      expect(feed("MSG foo 1 0\r\n\r\n")).to eq([[:msg, "foo", "1", nil, nil, ""]])
    end

    it "parses HMSG and hands the header block over unparsed" do
      expect(feed("HMSG foo 3 28 31\r\nNATS/1.0\r\nNats-Msg-Id: 1\r\n\r\nbar\r\n"))
        .to eq([[:msg, "foo", "3", nil, "NATS/1.0\r\nNats-Msg-Id: 1\r\n\r\n", "bar"]])
    end

    it "parses HMSG with a reply subject and no payload" do
      expect(feed("HMSG q 4 _INBOX.r 16 16\r\nNATS/1.0 503\r\n\r\n\r\n"))
        .to eq([[:msg, "q", "4", "_INBOX.r", "NATS/1.0 503\r\n\r\n", ""]])
    end

    it "keeps a payload that contains CRLF and high bytes intact" do
      payload = "\x00\x01\r\n\xFF".b

      expect(feed("MSG bin 5 5\r\n#{payload}\r\n".b))
        .to eq([[:msg, "bin", "5", nil, nil, payload]])
    end

    it "yields binary payloads regardless of the encoding of the bytes fed in" do
      parser << "MSG foo 1 2\r\nhi\r\n"

      expect(sink.events.first.last.encoding).to eq(Encoding::BINARY)
    end

    it "treats the sid as the opaque token the server echoes back, not a number" do
      expect(feed("MSG foo abcd 5\r\nhello\r\n"))
        .to eq([[:msg, "foo", "abcd", nil, nil, "hello"]])
      expect(parser).not_to be_broken
    end

    it "parses back to back messages from a single chunk" do
      expect(feed("MSG a 1 1\r\nx\r\nMSG b 2 1\r\ny\r\n"))
        .to eq([[:msg, "a", "1", nil, nil, "x"], [:msg, "b", "2", nil, nil, "y"]])
    end
  end

  describe "framing across reads" do
    let(:stream) do
      [
        %(INFO {"server_id":"x"}\r\n),
        "PING\r\n",
        "MSG foo 1 3\r\nbar\r\n",
        "MSG foo.bar.baz 2 _INBOX.a.b 0\r\n\r\n",
        "HMSG foo 3 28 31\r\nNATS/1.0\r\nNats-Msg-Id: 1\r\n\r\nbar\r\n",
        "MSG bin 5 5\r\n\x00\x01\r\n\xFF\r\n",
        "-ERR 'Permissions Violation for Publish to \"x\"'\r\n"
      ].join.b
    end

    let(:expected) { described_class.new(RecordingSink.new).then { |_| feed(stream) }.dup }

    it "produces the same events for every possible split point" do
      whole = expected

      (1...stream.bytesize).each do |cut|
        fresh = RecordingSink.new
        split = described_class.new(fresh)
        split << stream.byteslice(0, cut)
        split << stream.byteslice(cut..)

        expect(fresh.events).to eq(whole), "mismatch when split at byte #{cut}"
      end
    end

    it "produces the same events when fed one byte at a time" do
      whole = expected

      fresh = RecordingSink.new
      slow = described_class.new(fresh)
      stream.each_byte { slow << _1.chr }

      expect(fresh.events).to eq(whole)
      expect(slow.pending_bytes).to eq(0)
    end

    it "reports nothing until a frame is complete" do
      parser << "MSG foo 1 3\r\nba"

      expect(sink.events).to be_empty
      expect(parser.pending_bytes).to eq(2)
    end

    it "drains its buffer once every frame is consumed" do
      feed("MSG foo 1 3\r\nbar\r\n")

      expect(parser.pending_bytes).to eq(0)
    end
  end

  describe "protocol violations" do
    it "reports an unknown operation instead of raising into the reader thread" do
      expect { feed("WAT\r\n") }.not_to raise_error
      expect(sink.events).to eq([[:protocol_error, %(unknown protocol operation: "WAT")]])
    end

    it "reports a malformed message line" do
      feed("MSG foo\r\n")

      expect(sink.events).to eq([[:protocol_error, %(malformed control line: "MSG foo")]])
    end

    it "rejects a header size larger than the total size" do
      feed("HMSG foo 1 40 10\r\n")

      expect(sink.events).to eq([[:protocol_error, "header size exceeds total size on foo"]])
    end

    it "rejects a payload that is not terminated by CRLF" do
      feed("MSG foo 1 3\r\nbarX\r\n")

      expect(sink.events).to eq([[:protocol_error, "payload not terminated by CRLF on foo"]])
    end

    it "stops consuming once broken so the client can tear the connection down" do
      feed("WAT\r\n", "MSG foo 1 3\r\nbar\r\n")

      expect(parser).to be_broken
      expect(sink.events.size).to eq(1)
    end

    it "refuses a payload size the server itself would refuse to parse" do
      feed("MSG foo 1 9999999999\r\n")

      expect(sink.events.first.first).to eq(:protocol_error)
    end

    it "refuses a frame larger than the max_payload the server advertised" do
      bounded = described_class.new(sink, max_payload: 1024)
      bounded << "MSG foo 1 2048\r\n"

      expect(sink.events).to eq([[:protocol_error, "frame of 2048 bytes exceeds max_payload 1024 on foo"]])
    end

    it "refuses a control line past the sanity bound" do
      bounded = described_class.new(sink, max_control_line: 32)
      bounded << "INFO #{"x" * 64}"

      expect(sink.events).to eq([[:protocol_error, "control line exceeds 32 bytes"]])
    end

    it "can be reset and reused after a violation" do
      feed("WAT\r\n")
      parser.reset!
      parser << "PING\r\n"

      expect(parser).not_to be_broken
      expect(sink.events.last).to eq([:ping])
    end
  end
end
