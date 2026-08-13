# frozen_string_literal: true

require "spec_helper"
require "zlib"

RSpec.describe Tether::Connection::WebSocket do
  let(:deflate) do
    { "Sec-WebSocket-Extensions" => "permessage-deflate; server_no_context_takeover; client_no_context_takeover" }
  end

  def deadline(seconds = 5)
    Tether::Deadline.after(seconds)
  end

  def transport_for(server, url_path: "", **websocket)
    described_class.new(URI("#{server.uri}#{url_path}"), Tether::Options.new(websocket: websocket))
  end

  def connected(websocket: {}, **server_options)
    FakeWebSocketServer.run(**server_options) do |server|
      transport = transport_for(server, **websocket).open(deadline)
      yield transport, server
    ensure
      transport&.close
    end
  end

  def read_bytes(transport, size)
    data = String.new(encoding: Encoding::BINARY)
    data << transport.read(deadline) while data.bytesize < size
    data
  end

  def raw_deflate(data)
    deflater = Zlib::Deflate.new(Zlib::DEFAULT_COMPRESSION, -Zlib::MAX_WBITS)
    deflater.deflate(data, Zlib::SYNC_FLUSH)[0...-4]
  end

  def raw_inflate(data)
    Zlib::Inflate.new(-Zlib::MAX_WBITS).inflate(data + "\x00\x00\xff\xff".b)
  end

  def open_error(**server_options)
    FakeWebSocketServer.run(**server_options) do |server|
      transport_for(server).open(deadline)
    rescue Tether::Error => e
      return e
    end
  end

  describe "#initialize" do
    it "rejects a header that would inject a line into the request" do
      options = Tether::Options.new(websocket: { headers: { "X-Test" => "a\r\nHost: evil" } })

      expect { described_class.new(URI("ws://127.0.0.1:1"), options) }.to raise_error(ArgumentError, /X-Test/)
    end

    it "rejects a path with whitespace in it" do
      options = Tether::Options.new(websocket: { path: "/a b" })

      expect { described_class.new(URI("ws://127.0.0.1:1"), options) }.to raise_error(ArgumentError, /path/)
    end
  end

  describe "#open" do
    it "sends an HTTP upgrade request for the root path" do
      connected do |_transport, server|
        expect(server.request).to start_with("GET / HTTP/1.1\r\nHost: 127.0.0.1:#{server.port}\r\n")
          .and include("Upgrade: websocket\r\n", "Connection: Upgrade\r\n", "Sec-WebSocket-Version: 13\r\n")
      end
    end

    it "sends a random 16 byte key" do
      keys = Array.new(2) { connected { |_transport, server| server.request[/^Sec-WebSocket-Key: (.+)\r$/, 1] } }

      expect(keys.map { |key| key.unpack1("m0").bytesize }).to eq([16, 16])
      expect(keys.uniq.size).to eq(2)
    end

    it "requests the path of the URL" do
      FakeWebSocketServer.run do |server|
        transport_for(server, url_path: "/nats?tenant=a").open(deadline)

        expect(server.request).to start_with("GET /nats?tenant=a HTTP/1.1\r\n")
      end
    end

    it "requests the configured path instead of the path of the URL" do
      FakeWebSocketServer.run do |server|
        transport_for(server, url_path: "/ignored", path: "/proxy/nats").open(deadline)

        expect(server.request).to start_with("GET /proxy/nats HTTP/1.1\r\n")
      end
    end

    it "adds the configured headers to the request" do
      connected(websocket: { headers: { "Cookie" => "jwt=abc", "X-Tenant" => "a" } }) do |_transport, server|
        expect(server.request).to include("Cookie: jwt=abc\r\n", "X-Tenant: a\r\n")
      end
    end

    it "offers permessage-deflate without context takeover when compression is on" do
      connected(websocket: { compression: true }) do |_transport, server|
        expect(server.request).to include(
          "Sec-WebSocket-Extensions: permessage-deflate; server_no_context_takeover; client_no_context_takeover\r\n"
        )
      end
    end

    it "offers no extension when compression is off" do
      connected { |_transport, server| expect(server.request).not_to include("Sec-WebSocket-Extensions") }
    end

    it "raises with the status line when the server refuses the upgrade" do
      error = open_error(status: "HTTP/1.1 403 Forbidden")

      expect(error).to be_a(Tether::ConnectionError).and have_attributes(message: %r{HTTP/1\.1 403 Forbidden})
    end

    it "raises when Sec-WebSocket-Accept does not match the key" do
      error = open_error(accept: "c3RhbGUgYWNjZXB0IGtleQ==")

      expect(error).to be_a(Tether::ConnectionError).and have_attributes(message: /Sec-WebSocket-Accept/)
    end

    it "raises when the response does not upgrade to websocket" do
      expect(open_error(headers: { "Upgrade" => "h2c" })).to be_a(Tether::ConnectionError)
    end

    it "raises when the server keeps a compression context" do
      error = FakeWebSocketServer.run(headers: { "Sec-WebSocket-Extensions" => "permessage-deflate" }) do |server|
        transport_for(server, compression: true).open(deadline)
      rescue Tether::Error => e
        e
      end

      expect(error).to be_a(Tether::ConnectionError).and have_attributes(message: /context takeover/)
    end

    it "raises when the server chooses an extension that was not offered" do
      expect(open_error(headers: deflate)).to be_a(Tether::ConnectionError)
    end

    it "raises TimeoutError when the server does not answer within the deadline" do
      FakeWebSocketServer.run(silent: true) do |server|
        expect { transport_for(server).open(deadline(0.05)) }.to raise_error(Tether::TimeoutError)
      end
    end

    it "raises ConnectionError when nothing listens on the port" do
      port = TCPServer.open("127.0.0.1", 0) { |listener| listener.addr[1] }
      transport = described_class.new(URI("ws://127.0.0.1:#{port}"), Tether::Options.new)

      expect { transport.open(deadline) }.to raise_error(Tether::ConnectionError)
    end

    it "closes the socket when the upgrade fails" do
      FakeWebSocketServer.run(status: "HTTP/1.1 400 Bad Request") do |server|
        transport = transport_for(server)

        expect { transport.open(deadline) }.to raise_error(Tether::ConnectionError)
        expect(transport).to be_closed
        expect(server).to be_eof
      end
    end
  end

  describe "#upgrade_to_tls" do
    it "refuses to start TLS inside an established WebSocket connection" do
      connected do |transport|
        expect { transport.upgrade_to_tls(OpenSSL::SSL::SSLContext.new, "127.0.0.1", deadline) }
          .to raise_error(Tether::TLSError, /wss/)
      end
    end
  end

  describe "#read" do
    it "returns the payload of a binary frame" do
      connected do |transport, server|
        server.send_frame(0x2, "PING\r\n")

        expect(transport.read(deadline)).to eq("PING\r\n")
      end
    end

    it "returns the payload of a text frame" do
      connected do |transport, server|
        server.send_frame(0x1, "PONG\r\n")

        expect(transport.read(deadline)).to eq("PONG\r\n")
      end
    end

    it "returns bytes in binary encoding" do
      connected do |transport, server|
        server.send_frame(0x2, "\xff\x00".b)

        expect(transport.read(deadline).encoding).to eq(Encoding::BINARY)
      end
    end

    it "joins the frames of a fragmented message" do
      connected do |transport, server|
        server.send_frame(0x2, "MSG a 1 ", fin: false)
        server.send_frame(0x0, "5\r\nhe", fin: false)
        server.send_frame(0x0, "llo\r\n")

        expect(read_bytes(transport, 18)).to eq("MSG a 1 5\r\nhello\r\n")
      end
    end

    it "joins several frames that arrive together" do
      connected do |transport, server|
        server.send_raw("\x82\x02ab\x82\x02cd\x82\x02ef".b)

        expect(read_bytes(transport, 6)).to eq("abcdef")
      end
    end

    it "reassembles a frame whose header arrives one byte at a time" do
      connected do |transport, server|
        reader = Thread.new { read_bytes(transport, 300) }
        ([0x82, 126, 300].pack("CCn") + ("x" * 300)).each_char { |byte| server.send_raw(byte) }

        expect(reader.value).to eq("x" * 300)
      end
    end

    it "reads a frame with a 16 bit length" do
      payload = Random.bytes(60_000)

      connected do |transport, server|
        server.send_frame(0x2, payload)

        expect(read_bytes(transport, payload.bytesize)).to eq(payload)
      end
    end

    it "reads a frame with a 64 bit length" do
      payload = Random.bytes(200_000)

      connected do |transport, server|
        writer = Thread.new { server.send_frame(0x2, payload) }

        expect(read_bytes(transport, payload.bytesize)).to eq(payload)
        writer.join
      end
    end

    it "inflates a compressed message" do
      payload = "MSG a 1 5\r\nhello\r\n" * 50

      connected(websocket: { compression: true }, headers: deflate) do |transport, server|
        server.send_frame(0x2, raw_deflate(payload), rsv1: true)

        expect(read_bytes(transport, payload.bytesize)).to eq(payload)
      end
    end

    it "inflates a compressed message split across frames" do
      payload = "MSG a 1 5\r\nhello\r\n" * 50
      compressed = raw_deflate(payload)

      connected(websocket: { compression: true }, headers: deflate) do |transport, server|
        server.send_frame(0x2, compressed[0, 10], fin: false, rsv1: true)
        server.send_frame(0x0, compressed[10..])

        expect(read_bytes(transport, payload.bytesize)).to eq(payload)
      end
    end

    it "inflates each message without the context of the previous one" do
      connected(websocket: { compression: true }, headers: deflate) do |transport, server|
        2.times { server.send_frame(0x2, raw_deflate("a" * 500), rsv1: true) }

        expect(read_bytes(transport, 1000)).to eq("a" * 1000)
      end
    end

    it "passes an uncompressed message through when compression is negotiated" do
      connected(websocket: { compression: true }, headers: deflate) do |transport, server|
        server.send_frame(0x2, "PING\r\n")

        expect(transport.read(deadline)).to eq("PING\r\n")
      end
    end

    it "answers a ping with a masked pong carrying the same payload" do
      connected do |transport, server|
        server.send_frame(0x9, "are you there")
        server.send_frame(0x2, "PING\r\n")

        expect(transport.read(deadline)).to eq("PING\r\n")
        expect(server.read_frame).to have_attributes(opcode: 0xa, payload: "are you there", masked: true, fin: true)
      end
    end

    it "answers a ping that arrives in the middle of a fragmented message" do
      connected do |transport, server|
        server.send_frame(0x2, "PI", fin: false)
        server.send_frame(0x9, "mid")
        server.send_frame(0x0, "NG\r\n")

        expect(read_bytes(transport, 6)).to eq("PING\r\n")
        expect(server.read_frame).to have_attributes(opcode: 0xa, payload: "mid")
      end
    end

    it "ignores a pong" do
      connected do |transport, server|
        server.send_frame(0xa, "unsolicited")
        server.send_frame(0x2, "PING\r\n")

        expect(transport.read(deadline)).to eq("PING\r\n")
      end
    end

    it "returns nil after a close frame" do
      connected do |transport, server|
        server.send_frame(0x8, "#{[1001].pack("n")}Server Shutdown")

        expect(transport.read(deadline)).to be_nil
        expect(transport.read(deadline)).to be_nil
      end
    end

    it "returns the data that came before a close frame first" do
      connected do |transport, server|
        server.send_raw("\x82\x06-ERR\r\n\x88\x02\x03\xf0".b)

        expect(read_bytes(transport, 6)).to eq("-ERR\r\n")
        expect(transport.read(deadline)).to be_nil
      end
    end

    it "records the status and the reason of a close frame" do
      connected do |transport, server|
        server.send_frame(0x8, "#{[1008].pack("n")}Authentication Timeout")
        transport.read(deadline)

        expect(transport).to have_attributes(close_code: 1008, close_reason: "Authentication Timeout")
      end
    end

    it "answers a close frame with a close frame of the same status" do
      connected do |transport, server|
        server.send_frame(0x8, [1001].pack("n"))
        transport.read(deadline)

        expect(server.read_frame).to have_attributes(opcode: 0x8, payload: [1001].pack("n"), masked: true)
      end
    end

    it "returns nil when the server drops the connection without a close frame" do
      connected do |transport, server|
        server.drop

        expect(transport.read(deadline)).to be_nil
      end
    end

    it "raises ConnectionClosedError when a server frame is masked" do
      connected do |transport, server|
        server.send_raw("\x82\x81\x00\x00\x00\x00a".b)

        expect { transport.read(deadline) }.to raise_error(Tether::ConnectionClosedError, /masked/)
      end
    end

    it "raises ConnectionClosedError when a compressed frame arrives without compression negotiated" do
      connected do |transport, server|
        server.send_frame(0x2, raw_deflate("hello"), rsv1: true)

        expect { transport.read(deadline) }.to raise_error(Tether::ConnectionClosedError, /compress/)
      end
    end

    it "raises ConnectionClosedError when a continuation frame continues nothing" do
      connected do |transport, server|
        server.send_frame(0x0, "orphan")

        expect { transport.read(deadline) }.to raise_error(Tether::ConnectionClosedError, /continuation/)
      end
    end

    it "raises ConnectionClosedError when a compressed message is not valid deflate" do
      connected(websocket: { compression: true }, headers: deflate) do |transport, server|
        server.send_frame(0x2, "\xff\xff\xff\xff\xff".b, rsv1: true)

        expect { transport.read(deadline) }.to raise_error(Tether::ConnectionClosedError, /inflate/)
      end
    end

    it "sends a close frame with status 1002 when the server breaks the protocol" do
      connected do |transport, server|
        server.send_frame(0x3, "reserved opcode")

        expect { transport.read(deadline) }.to raise_error(Tether::ConnectionClosedError)
        expect(server.read_frame).to have_attributes(opcode: 0x8, payload: [1002].pack("n"))
      end
    end

    it "raises TimeoutError when nothing arrives within the deadline" do
      connected do |transport|
        expect { transport.read(deadline(0.05)) }.to raise_error(Tether::TimeoutError)
      end
    end

    it "raises ConnectionClosedError after the transport is closed" do
      connected do |transport|
        transport.close

        expect { transport.read(deadline) }.to raise_error(Tether::ConnectionClosedError)
      end
    end
  end

  describe "#write" do
    it "sends the bytes in a masked binary frame" do
      connected do |transport, server|
        transport.write("PUB a 5\r\nhello\r\n", deadline)

        expect(server.read_frame).to have_attributes(opcode: 0x2, payload: "PUB a 5\r\nhello\r\n", masked: true,
                                                     fin: true, rsv1: false)
      end
    end

    it "returns the number of bytes of data" do
      connected do |transport|
        expect(transport.write("PING\r\n", deadline)).to eq(6)
      end
    end

    it "masks payloads whose size is not a multiple of four" do
      payloads = (0..9).map { |size| Random.bytes(size) }

      connected do |transport, server|
        payloads.each { |payload| transport.write(payload, deadline) }

        expect(payloads.map { server.read_frame.payload }).to eq(payloads)
      end
    end

    it "sends a payload that needs a 16 bit length" do
      payload = Random.bytes(20_000)

      connected do |transport, server|
        transport.write(payload, deadline)

        expect(server.read_frame.payload).to eq(payload)
      end
    end

    it "splits data larger than the message limit into complete messages" do
      payload = Random.bytes((described_class::MAX_MESSAGE_SIZE * 2) + 10)

      connected do |transport, server|
        writer = Thread.new { transport.write(payload, deadline) }
        frames = Array.new(3) { server.read_frame }
        writer.join

        expect(frames.map(&:fin)).to eq([true, true, true])
        expect(frames.map { |frame| frame.payload.bytesize }).to eq([32_768, 32_768, 10])
        expect(frames.map(&:payload).join).to eq(payload)
      end
    end

    it "compresses a message when the server agreed to it" do
      payload = "PUB a 5\r\nhello\r\n" * 100

      connected(websocket: { compression: true }, headers: deflate) do |transport, server|
        transport.write(payload, deadline)
        frame = server.read_frame

        expect(frame).to have_attributes(opcode: 0x2, rsv1: true, masked: true)
        expect(frame.payload.bytesize).to be < payload.bytesize
        expect(raw_inflate(frame.payload)).to eq(payload)
      end
    end

    it "compresses every message without the context of the previous one" do
      payload = "PUB a 5\r\nhello\r\n" * 100

      connected(websocket: { compression: true }, headers: deflate) do |transport, server|
        2.times { transport.write(payload, deadline) }

        expect(Array.new(2) { raw_inflate(server.read_frame.payload) }).to eq([payload, payload])
      end
    end

    it "leaves a small message uncompressed when compression is negotiated" do
      connected(websocket: { compression: true }, headers: deflate) do |transport, server|
        transport.write("PING\r\n", deadline)

        expect(server.read_frame).to have_attributes(rsv1: false, payload: "PING\r\n")
      end
    end

    it "sends uncompressed frames when the server ignores the compression offer" do
      payload = "PUB a 5\r\nhello\r\n" * 100

      connected(websocket: { compression: true }) do |transport, server|
        transport.write(payload, deadline)

        expect(server.read_frame).to have_attributes(rsv1: false, payload: payload)
      end
    end

    it "sends unmasked frames when the server agrees to no masking" do
      connected(websocket: { headers: { "Nats-No-Masking" => "true" } },
                headers: { "Nats-No-Masking" => "true" }) do |transport, server|
        transport.write("PING\r\n", deadline)

        expect(server.read_frame).to have_attributes(masked: false, payload: "PING\r\n")
      end
    end

    it "keeps masking when the server offers no masking that was not requested" do
      connected(headers: { "Nats-No-Masking" => "true" }) do |transport, server|
        transport.write("PING\r\n", deadline)

        expect(server.read_frame).to have_attributes(masked: true)
      end
    end

    it "keeps frames whole when a reader answers pings while a writer writes" do
      payload = "PUB a 5\r\nhello\r\n" * 50

      connected do |transport, server|
        50.times { |index| server.send_frame(0x9, index.to_s) }
        server.send_frame(0x2, "done")
        reader = Thread.new { read_bytes(transport, 4) }
        writer = Thread.new { 50.times { transport.write(payload, deadline) } }
        frames = []
        frames << server.read_frame until frames.size == 100
        [reader, writer].each(&:join)

        expect(frames.select { |frame| frame.opcode == 0xa }.map(&:payload)).to eq((0...50).map(&:to_s))
        expect(frames.select { |frame| frame.opcode == 0x2 }.map(&:payload)).to eq([payload] * 50)
      end
    end

    it "raises ConnectionClosedError after the transport is closed" do
      connected do |transport|
        transport.close

        expect { transport.write("PING\r\n", deadline) }.to raise_error(Tether::ConnectionClosedError)
      end
    end
  end

  describe "#close" do
    it "sends a close frame with status 1000 and ends the connection" do
      connected do |transport, server|
        transport.close

        expect(server.read_frame).to have_attributes(opcode: 0x8, payload: [1000].pack("n"), masked: true)
        expect(server).to be_eof
      end
    end

    it "sends no second close frame after answering the server's" do
      connected do |transport, server|
        server.send_frame(0x8, [1001].pack("n"))
        transport.read(deadline)
        transport.close

        expect(server.read_frame.payload).to eq([1001].pack("n"))
        expect(server).to be_eof
      end
    end

    it "reports the transport as closed" do
      connected do |transport|
        transport.close

        expect(transport).to be_closed
      end
    end

    it "does nothing the second time" do
      connected do |transport|
        transport.close

        expect(transport.close).to be_nil
      end
    end
  end

  describe "#abandon" do
    it "closes the descriptor without sending a close frame" do
      connected do |transport, server|
        transport.abandon

        expect(server).to be_eof
      end
    end
  end
end
