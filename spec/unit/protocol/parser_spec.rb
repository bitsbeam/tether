# frozen_string_literal: true

RSpec.describe Tether::Protocol::Parser do
  subject(:parser) { described_class.new(dispatcher) }

  let(:dispatcher) { RecordingDispatcher.new }

  def feed(*chunks)
    chunks.each { |chunk| parser.feed(chunk) }
    dispatcher.events
  end

  describe "#feed" do
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
      parser.feed("MSG foo 1 2\r\nhi\r\n")

      expect(dispatcher.events.first.last.encoding).to eq(Encoding::BINARY)
    end

    it "treats the sid as the opaque token the server echoes back, not a number" do
      expect(feed("MSG foo abcd 5\r\nhello\r\n"))
        .to eq([[:msg, "foo", "abcd", nil, nil, "hello"]])
    end

    it "parses back to back messages from a single chunk" do
      expect(feed("MSG a 1 1\r\nx\r\nMSG b 2 1\r\ny\r\n"))
        .to eq([[:msg, "a", "1", nil, nil, "x"], [:msg, "b", "2", nil, nil, "y"]])
    end

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

    let(:expected) { described_class.new(RecordingDispatcher.new).then { |_| feed(stream) }.dup }

    it "produces the same events for every possible split point" do
      whole = expected

      (1...stream.bytesize).each do |cut|
        fresh = RecordingDispatcher.new
        split = described_class.new(fresh)
        split.feed(stream.byteslice(0, cut))
        split.feed(stream.byteslice(cut..))

        expect(fresh.events).to eq(whole), "mismatch when split at byte #{cut}"
      end
    end

    it "produces the same events when fed one byte at a time" do
      whole = expected

      fresh = RecordingDispatcher.new
      slow = described_class.new(fresh)
      stream.each_byte { slow.feed(_1.chr) }

      expect(fresh.events).to eq(whole)
      expect(slow.pending_bytes).to eq(0)
    end

    it "reports nothing until a frame is complete" do
      parser.feed("MSG foo 1 3\r\nba")

      expect(dispatcher.events).to be_empty
    end

    it "returns itself so calls can be chained" do
      expect(parser.feed("PING\r\n")).to be(parser)
    end

    it "ignores nil and empty input" do
      expect(feed(nil, "")).to be_empty
    end

    it "reports an unknown operation instead of raising into the reader thread" do
      expect { feed("WAT\r\n") }.not_to raise_error
      expect(dispatcher.events).to eq([[:protocol_error, %(unknown protocol operation: "WAT")]])
    end

    it "reports a malformed message line" do
      feed("MSG foo\r\n")

      expect(dispatcher.events).to eq([[:protocol_error, %(malformed control line: "MSG foo")]])
    end

    it "rejects a header size larger than the total size" do
      feed("HMSG foo 1 40 10\r\n")

      expect(dispatcher.events).to eq([[:protocol_error, "header size exceeds total size on foo"]])
    end

    it "rejects a payload that is not terminated by CRLF" do
      feed("MSG foo 1 3\r\nbarX\r\n")

      expect(dispatcher.events).to eq([[:protocol_error, "payload not terminated by CRLF on foo"]])
    end

    it "stops consuming once broken so the client can tear the connection down" do
      feed("WAT\r\n", "MSG foo 1 3\r\nbar\r\n")

      expect(dispatcher.events.size).to eq(1)
    end

    it "refuses a payload size the server itself would refuse to parse" do
      feed("MSG foo 1 9999999999\r\n")

      expect(dispatcher.events.first.first).to eq(:protocol_error)
    end

    it "refuses a frame larger than the max_payload the server advertised" do
      bounded = described_class.new(dispatcher, max_payload: 1024)
      bounded.feed("MSG foo 1 2048\r\n")

      expect(dispatcher.events).to eq([[:protocol_error, "frame of 2048 bytes exceeds max_payload 1024 on foo"]])
    end

    it "refuses a control line past the sanity bound" do
      bounded = described_class.new(dispatcher, max_control_line: 32)
      bounded.feed("INFO #{"x" * 64}")

      expect(dispatcher.events).to eq([[:protocol_error, "control line exceeds 32 bytes"]])
    end
  end

  describe "#max_payload=" do
    it "bounds the frames accepted from then on" do
      parser.max_payload = 8
      feed("MSG foo 1 16\r\n")

      expect(dispatcher.events).to eq([[:protocol_error, "frame of 16 bytes exceeds max_payload 8 on foo"]])
    end
  end

  describe "#dispatcher=" do
    it "hands later frames to the new dispatcher" do
      replacement = RecordingDispatcher.new
      feed("PING\r\n")
      parser.dispatcher = replacement
      feed("PONG\r\n")

      expect(replacement.events).to eq([[:pong]])
    end
  end

  describe "#reset!" do
    it "makes a broken parser usable again" do
      feed("WAT\r\n")
      parser.reset!
      parser.feed("PING\r\n")

      expect(parser).not_to be_broken
      expect(dispatcher.events.last).to eq([:ping])
    end

    it "discards a partial frame" do
      parser.feed("MSG foo 1 3\r\nba")
      parser.reset!

      expect(parser.pending_bytes).to eq(0)
    end
  end

  describe "#broken?" do
    it "returns false while the stream is well formed" do
      feed("PING\r\n")

      expect(parser).not_to be_broken
    end

    it "returns true after a protocol violation" do
      feed("WAT\r\n")

      expect(parser).to be_broken
    end
  end

  describe "#pending_bytes" do
    it "counts the bytes of a frame still arriving" do
      parser.feed("MSG foo 1 3\r\nba")

      expect(parser.pending_bytes).to eq(2)
    end

    it "returns zero once every frame is consumed" do
      feed("MSG foo 1 3\r\nbar\r\n")

      expect(parser.pending_bytes).to eq(0)
    end
  end

  describe "#state" do
    it "returns :control between frames" do
      expect(parser.state).to eq(:control)
    end

    it "returns :payload while a payload is arriving" do
      parser.feed("MSG foo 1 3\r\nba")

      expect(parser.state).to eq(:payload)
    end
  end
end
