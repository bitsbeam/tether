# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tether::Connection::WebSocket do
  max_payload = 4 * 1024 * 1024

  before(:all) do
    listener = "max_payload: #{max_payload}\ndebug: true\nwebsocket { port: 8080, no_tls: true, compression: %s }"
    certificates = TlsCertificates.instance
    @plain = NatsServer.new(config: format(listener, false), websocket: true).start
    @compressing = NatsServer.new(config: format(listener, true), websocket: true).start
    @secure = NatsServer.new(
      config: "websocket { port: 8080, tls { cert_file: /certs/server.pem, key_file: /certs/server.key } }",
      files: { "/certs/server.pem" => certificates.server_pem, "/certs/server.key" => certificates.server_key_pem },
      websocket: true
    ).start
  end

  after(:all) do
    [@plain, @compressing, @secure].each { |server| server&.stop }
  end

  after { @clients&.each(&:close) }

  def connect(server = @plain, scheme: "ws", **options)
    client = Tether.connect(server.websocket_uri(scheme), **options)
    (@clients ||= []) << client
    client
  end

  def compressed
    connect(@compressing, websocket: { compression: true })
  end

  def round_trip(client, payload, headers: nil)
    subject = client.new_inbox
    subscription = client.subscribe(subject)
    client.publish(subject, payload, headers: headers, flush: true)
    subscription.next(timeout: 10)
  end

  def echo(client, payload)
    subject = client.new_inbox
    client.subscribe(subject) { |msg| msg.respond(msg.data) }
    client.flush
    client.request(subject, payload, timeout: 10).data
  end

  def publish_concurrently(client, publishers:, each:)
    subject = client.new_inbox
    subscription = client.subscribe(subject)
    Array.new(publishers) do |publisher|
      Thread.new { each.times { |index| client.publish(subject, "#{publisher}:#{index}:#{"x" * (index % 200)}") } }
    end.each(&:join)
    client.flush
    Array.new(publishers * each) { subscription.next(timeout: 10).data }
  end

  def expected_from(publishers:, each:)
    Array.new(publishers) do |publisher|
      Array.new(each) { |index| "#{publisher}:#{index}:#{"x" * (index % 200)}" }
    end
  end

  def per_publisher(messages)
    messages.group_by { |message| message.split(":").first.to_i }.sort.map(&:last)
  end

  def receive_stream(client, count)
    subject = client.new_inbox
    subscription = client.subscribe(subject)
    count.times { |index| client.publish(subject, "message #{index}") }
    client.flush
    Array.new(count) { subscription.next(timeout: 10).data }
  end

  def elapsed
    started = Tether::Deadline.now
    yield
    Tether::Deadline.now - started
  end

  describe "#open" do
    it "connects over ws://" do
      client = connect

      expect(client).to be_connected
      expect(client.server_info.max_payload).to eq(max_payload)
    end

    it "connects over wss:// and verifies the certificate" do
      client = connect(@secure, scheme: "wss", tls: { ca_file: TlsCertificates.instance.ca_file })

      expect(round_trip(client, "over tls").data).to eq("over tls")
    end

    it "raises TLSError when the certificate of a wss:// server is not trusted" do
      expect { connect(@secure, scheme: "wss", reconnect: false) }.to raise_error(Tether::TLSError)
    end

    it "raises ConnectionError when a wss:// URL points at a listener without TLS" do
      expect { connect(@plain, scheme: "wss", reconnect: false) }.to raise_error(Tether::ConnectionError)
    end

    it "connects with compression when the server offers it" do
      expect(round_trip(compressed, "squeezed").data).to eq("squeezed")
    end

    it "continues uncompressed when the server does not agree to compression" do
      client = connect(@plain, websocket: { compression: true })

      expect(round_trip(client, "a" * 10_000).data).to eq("a" * 10_000)
    end

    it "connects uncompressed to a server that offers compression" do
      expect(round_trip(connect(@compressing), "a" * 10_000).data).to eq("a" * 10_000)
    end

    it "connects with a custom path and headers" do
      client = connect(websocket: { path: "/behind/a/proxy", headers: { "X-Forwarded-For" => "203.0.113.7" } })

      expect(round_trip(client, "hello").data).to eq("hello")
      expect(eventually { @plain.log[%r{203\.0\.113\.7/\S+ - wid:\d+}] }).to be_a(String)
    end

    it "asks a headers callable again on every connect" do
      calls = Thread::Queue.new
      client = connect(reconnect: { wait: 0.05 },
                       websocket: { headers: -> { { "X-Forwarded-For" => "203.0.113.#{calls.push(1).size}" } } })
      client.force_reconnect
      eventually { client.connected? && client.stats.reconnects == 1 }

      expect(eventually { @plain.log[%r{203\.0\.113\.2/\S+ - wid:\d+}] }).to be_a(String)
    end

    it "connects without masking when it is requested and the server agrees" do
      client = connect(websocket: { headers: { "Nats-No-Masking" => "true" } })

      expect(round_trip(client, Random.bytes(100_000)).size).to eq(100_000)
    end

    it "raises ConnectionError within the connect timeout when the port speaks plain NATS" do
      error = nil
      taken = elapsed do
        Tether.connect("ws://127.0.0.1:#{@plain.port}", connect_timeout: 2, reconnect: false)
      rescue Tether::Error => e
        error = e
      end

      expect(error).to be_a(Tether::ConnectionError).and have_attributes(message: /WebSocket upgrade .* failed/)
      expect(taken).to be < 2
    end

    it "connects again after the server restarts" do
      NatsServer.run("websocket { port: 8080, no_tls: true }", websocket: true) do |server|
        client = connect(server, reconnect: { wait: 0.05, max_attempts: -1 })
        server.restart
        eventually(timeout: 20) { client.connected? && client.stats.reconnects.positive? }

        expect(round_trip(client, "back").data).to eq("back")
      end
    end
  end

  describe "#write" do
    it "publishes a message a subscriber receives" do
      expect(round_trip(connect, "hello").data).to eq("hello")
    end

    it "publishes an empty message" do
      expect(round_trip(connect, "").data).to eq("")
    end

    it "publishes headers" do
      msg = round_trip(connect, "body", headers: { "X-Kind" => "test" })

      expect(msg.headers["X-Kind"]).to eq("test")
    end

    it "publishes a payload with every byte value" do
      payload = (0..255).to_a.pack("C*") * 16

      expect(round_trip(connect, payload).data).to eq(payload)
    end

    it "publishes a payload larger than 64 KB" do
      payload = Random.bytes(70_000)

      expect(round_trip(connect, payload).data).to eq(payload)
    end

    it "publishes a payload larger than 1 MB" do
      payload = Random.bytes(1_500_000)

      expect(round_trip(connect, payload).data).to eq(payload)
    end

    it "publishes a payload of exactly the maximum size" do
      payload = Random.bytes(max_payload)

      expect(round_trip(connect, payload).data).to eq(payload)
    end

    it "publishes a compressed message" do
      payload = "compressible " * 1_000

      expect(round_trip(compressed, payload).data).to eq(payload)
    end

    it "publishes a compressed payload with every byte value" do
      payload = (0..255).to_a.pack("C*") * 16

      expect(round_trip(compressed, payload).data).to eq(payload)
    end

    it "publishes an incompressible payload larger than 1 MB with compression" do
      payload = Random.bytes(1_500_000)

      expect(round_trip(compressed, payload).data).to eq(payload)
    end

    it "publishes a compressible payload of exactly the maximum size with compression" do
      payload = "0123456789abcdef" * (max_payload / 16)

      expect(round_trip(compressed, payload).data).to eq(payload)
    end

    it "keeps the messages of many concurrent publishers whole and in order" do
      received = publish_concurrently(connect, publishers: 8, each: 500)

      expect(per_publisher(received)).to eq(expected_from(publishers: 8, each: 500))
    end

    it "keeps the messages of many concurrent publishers whole and in order with compression" do
      received = publish_concurrently(compressed, publishers: 8, each: 500)

      expect(per_publisher(received)).to eq(expected_from(publishers: 8, each: 500))
    end
  end

  describe "#read" do
    it "receives the reply to a request" do
      expect(echo(connect, "ping")).to eq("ping")
    end

    it "receives the reply to a request with compression" do
      expect(echo(compressed, "ping " * 100)).to eq("ping " * 100)
    end

    it "receives a reply larger than 1 MB" do
      payload = Random.bytes(2_000_000)

      expect(echo(connect, payload)).to eq(payload)
    end

    it "receives a reply larger than 1 MB with compression" do
      payload = Random.bytes(1_000) * 2_000

      expect(echo(compressed, payload)).to eq(payload)
    end

    it "receives a stream of small messages in order" do
      expect(receive_stream(connect, 20_000)).to eq(Array.new(20_000) { |index| "message #{index}" })
    end

    it "receives a stream of small messages in order with compression" do
      expect(receive_stream(compressed, 20_000)).to eq(Array.new(20_000) { |index| "message #{index}" })
    end

    it "answers the server's pings while the connection is idle" do
      NatsServer.run("debug: true\nping_interval: \"50ms\"\nping_max: 1\nwebsocket { port: 8080, no_tls: true }",
                     websocket: true) do |server|
        client = connect(server, reconnect: false)
        eventually { server.log.scan("Client Ping Timer").size >= 4 }

        expect(client).to be_connected
        expect(server.log).not_to include("Stale Client Connection")
      end
    end

    it "reports the connection as lost when the server goes away" do
      NatsServer.run("websocket { port: 8080, no_tls: true }", websocket: true) do |server|
        client = connect(server, reconnect: false)
        server.halt

        expect(eventually(timeout: 10) { client.closed? }).to be(true)
      end
    end
  end

  describe "#close" do
    it "ends the connection as closed by the client" do
      client = connect(name: "closing-over-websocket")
      client.close

      expect(eventually { @plain.log[/wid:\d+ - "[^"]*closing-over-websocket".*Client Closed/] }).to be_a(String)
    end

    it "leaves the client closed" do
      client = connect
      client.close

      expect(client).to be_closed
    end
  end
end
