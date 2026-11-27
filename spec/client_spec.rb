# frozen_string_literal: true

RSpec.describe Tether::Client do
  include ClientHelper

  let(:nc) { connect }
  let(:other) { connect }
  let(:certificates) { TlsCertificates.instance }

  def outage(client)
    proxy.stop
    eventually { client.reconnecting? }
  end

  def restore(client)
    proxy.start
    eventually { client.connected? }
  end

  describe ".connect" do
    it "returns a connected client" do
      expect(nc).to be_connected
    end

    it "yields the client, returns the block's value and closes the client afterwards" do
      yielded = nil
      value = described_class.connect(server.uri) do |client|
        yielded = client
        client.status
      end

      expect(value).to eq(:connected)
      expect(yielded).to be_closed
    end

    it "closes the client when the block raises" do
      yielded = nil
      block = lambda do |client|
        yielded = client
        raise ArgumentError, "boom"
      end

      expect { described_class.connect(server.uri, &block) }.to raise_error(ArgumentError, "boom")
      expect(yielded).to be_closed
    end

    it "defaults to NATS_URL when no URL is given" do
      previous = ENV.fetch("NATS_URL", nil)
      ENV["NATS_URL"] = server.uri
      client = described_class.connect
      opened_clients << client

      expect(client.servers.map(&:url)).to eq([server.uri])
    ensure
      ENV["NATS_URL"] = previous
    end

    it "raises ArgumentError naming an unknown option" do
      expect { described_class.connect(server.uri, pingg_interval: 1) }
        .to raise_error(ArgumentError, "unknown option: :pingg_interval")
    end

    it "raises ConnectionError when nothing is listening" do
      expect { described_class.connect("nats://127.0.0.1:1", connect_timeout: 1) }
        .to raise_error(Tether::ConnectionError, /could not connect to 127.0.0.1:1/)
    end

    it "raises TimeoutError when the server never greets" do
      silent = TCPServer.new("127.0.0.1", 0)

      expect { described_class.connect("nats://127.0.0.1:#{silent.addr[1]}", connect_timeout: 0.1) }
        .to raise_error(Tether::TimeoutError)
    ensure
      silent.close
    end

    it "raises ConnectionError when the server hangs up during the handshake" do
      rude = fake_server(greeting: "")
      accepted = Thread.new { eventually { rude.connections.positive? } && rude.drop }

      expect { described_class.connect(rude.uri) }
        .to raise_error(Tether::ConnectionError, /closed the connection during the handshake/)
      accepted.join
    end

    it "raises ProtocolError when the server greets with a malformed INFO" do
      expect { described_class.connect(fake_server(greeting: "INFO {nope\r\n").uri) }
        .to raise_error(Tether::ProtocolError, /malformed INFO/)
    end

    it "raises the server's error when it refuses the connection before INFO" do
      full = fake_server(greeting: "-ERR 'maximum connections exceeded'\r\n")

      expect { described_class.connect(full.uri) }.to raise_error(Tether::MaxConnectionsError)
    end

    it "sends its name and asks for headers and no_responders in CONNECT" do
      connect(fake_server.uri, name: "billing", echo: false)
      options = JSON.parse(fake_server.received[/^CONNECT (\{.*\})\r$/, 1])

      expect(options).to include("name" => "billing", "echo" => false, "verbose" => false, "pedantic" => false,
                                 "headers" => true, "no_responders" => true, "lang" => "ruby", "protocol" => 1)
    end

    it "answers the server's PING, so the server keeps the connection" do
      connect(fake_server.uri)
      fake_server.push("PING\r\n")

      expect(eventually { fake_server.received.end_with?("PONG\r\n") }).to be(true)
    end

    it "waits what the reconnect delay callable returns between passes over the pool" do
      attempts = Thread::Queue.new
      delay = lambda do |attempt|
        attempts.push(attempt)
        0.01
      end
      client = connect(proxy.uri, reconnect: { delay: delay, max_attempts: -1 })
      outage(client)

      expect(take(attempts, 3)).to eq([1, 2, 3])
    end

    it "asks the reconnect server callable which server to try next" do
      asked = Thread::Queue.new
      chooser = lambda do |servers, info|
        asked.push(info)
        servers.find { |candidate| candidate.url == proxy.uri }
      end
      settings = ClientHelper::FAST_RECONNECT.merge(server: chooser)
      client = connect(["nats://127.0.0.1:1", proxy.uri], randomize: false, reconnect: settings)
      proxy.drop

      expect(asked.pop(timeout: 2)).to be_a(Tether::ServerInfo)
      expect(eventually { client.stats.reconnects == 1 }).to be(true)
    end

    it "connects to the first reachable server of several" do
      client = connect(["nats://127.0.0.1:1", server.uri], randomize: false)

      expect(client).to be_connected
      expect(client.servers.map(&:url)).to contain_exactly("nats://127.0.0.1:1", server.uri)
    end

    it "accepts a comma separated list of servers" do
      client = connect("nats://127.0.0.1:1,#{server.uri}")

      expect(client.servers.size).to eq(2)
    end

    it "returns a reconnecting client with retry_on_failed_connect and connects once the server appears" do
      @proxy = TcpProxy.new(server.port)
      client = connect(@proxy.uri, retry_on_failed_connect: true, reconnect: ClientHelper::FAST_RECONNECT)
      status = client.status
      @proxy.start

      expect(status).to eq(:reconnecting)
      expect(eventually { client.connected? }).to be(true)
    end

    it "authenticates with a token" do
      expect(connect(SharedNatsServer.token.uri, token: "s3cr3t")).to be_connected
    end

    it "authenticates with a token in the URL" do
      expect(connect("nats://s3cr3t@127.0.0.1:#{SharedNatsServer.token.port}")).to be_connected
    end

    it "authenticates with a user and password" do
      expect(connect(SharedNatsServer.users.uri, user: "alice", password: "wonderland")).to be_connected
    end

    it "authenticates with a user and password in the URL" do
      expect(connect("nats://alice:wonderland@127.0.0.1:#{SharedNatsServer.users.port}")).to be_connected
    end

    it "authenticates by signing the server's nonce with an NKEY seed" do
      expect(connect(SharedNatsServer.users.uri, nkey_seed: SharedNatsServer::NKEY_SEED)).to be_connected
    end

    it "raises AuthorizationError for a wrong password" do
      expect { described_class.connect(SharedNatsServer.users.uri, user: "alice", password: "nope") }
        .to raise_error(Tether::AuthorizationError, "Authorization Violation")
    end

    it "raises AuthorizationError when the server wants credentials and none are given" do
      expect { described_class.connect(SharedNatsServer.token.uri) }.to raise_error(Tether::AuthorizationError)
    end

    it "connects over tls:// and verifies the certificate against the given CA" do
      client = connect(SharedNatsServer.tls.tls_uri, tls: { ca_file: certificates.ca_file })

      expect(client).to be_connected
      expect(client.server_info).to be_tls_required
    end

    it "upgrades to TLS on a nats:// URL when the server requires it" do
      client = connect(SharedNatsServer.tls.uri, tls: { ca_file: certificates.ca_file })

      expect(client).to be_connected
    end

    it "raises TLSError when the server's certificate is not trusted" do
      expect { described_class.connect(SharedNatsServer.tls.tls_uri) }
        .to raise_error(Tether::TLSError, /certificate verify failed/)
    end

    it "connects without verification when asked to" do
      expect(connect(SharedNatsServer.tls.tls_uri, tls: { verify: false })).to be_connected
    end

    it "raises TLSError when TLS is asked of a server that does not offer it" do
      expect { described_class.connect(server.uri, tls: true) }
        .to raise_error(Tether::TLSError, /does not offer it/)
    end

    it "performs the TLS handshake before INFO with tls first" do
      client = connect(SharedNatsServer.tls_first.tls_uri, tls: { first: true, ca_file: certificates.ca_file })

      expect(client.publish(topic, "over tls").flush).to be_connected
    end
  end

  describe "#connect" do
    it "returns the client" do
      client = build

      expect(client.connect).to be(client)
    end

    it "raises when the client is already connected" do
      expect { nc.connect }.to raise_error(Tether::Error, "already connected")
    end

    it "raises ConnectionClosedError once the client is closed" do
      nc.close

      expect { nc.connect }.to raise_error(Tether::ConnectionClosedError)
    end

    it "leaves the client disconnected after a failure, so it can be tried again" do
      @proxy = TcpProxy.new(server.port)
      client = build(@proxy.uri)
      failure = error_from { client.connect }
      status = client.status
      @proxy.start

      expect(failure).to be_a(Tether::ConnectionError)
      expect(status).to eq(:disconnected)
      expect(client.connect).to be_connected
    end
  end

  describe "#publish" do
    it "raises when the client was never connected" do
      client = described_class.new(server.uri)

      expect { client.publish("never.connected", "x") }.to raise_error(Tether::ConnectionError, /not connected/)
    end

    def expect_invalid_subject(subject)
      expect { nc.publish(subject, "x") }.to raise_error(Tether::InvalidSubjectError)
    end

    it "delivers the payload to a subscriber" do
      received = collect(other, topic)
      nc.publish(topic, "hello")

      expect(received.pop(timeout: 2)).to have_attributes(subject: topic, data: "hello", reply_to: nil, headers: nil)
    end

    it "returns the client" do
      expect(nc.publish(topic, "x")).to be(nc)
    end

    it "sends an empty message for a nil payload" do
      received = collect(other, topic)
      nc.publish(topic)

      expect(received.pop(timeout: 2).data).to eq("")
    end

    it "delivers arbitrary bytes unchanged, including CRLF inside the payload" do
      payload = "\x00\x01\r\n\xFF binary".b
      received = collect(other, topic)
      nc.publish(topic, payload)

      expect(received.pop(timeout: 2).data).to eq(payload)
    end

    it "sends a UTF-8 payload as its bytes" do
      received = collect(other, topic)
      nc.publish(topic, "héllo wörld")

      expect(received.pop(timeout: 2).data).to eq("héllo wörld".b)
    end

    it "keeps payloads of different encodings intact within one flush" do
      received = collect(other, topic)
      nc.publish(topic, "ünïcödé").publish(topic, "\xFF\xFE".b).publish(topic, "ascii")

      expect(take(received, 3).map(&:data)).to eq(["ünïcödé".b, "\xFF\xFE".b, "ascii"])
    end

    it "delivers headers given as a Hash" do
      received = collect(other, topic)
      nc.publish(topic, "body", headers: { "Trace-Id" => "abc", "Tags" => %w[a b] })
      msg = received.pop(timeout: 2)

      expect(msg.data).to eq("body")
      expect(msg.headers.to_h).to eq("Trace-Id" => ["abc"], "Tags" => %w[a b])
    end

    it "delivers headers given as Headers" do
      received = collect(other, topic)
      nc.publish(topic, "body", headers: Tether::Headers["Nats-Msg-Id" => "m-1"])

      expect(received.pop(timeout: 2).headers["Nats-Msg-Id"]).to eq("m-1")
    end

    it "delivers headers with an empty payload" do
      received = collect(other, topic)
      nc.publish(topic, nil, headers: { "Only" => "headers" })

      msg = received.pop(timeout: 2)

      expect(msg.data).to eq("")
      expect(msg.headers.to_h).to eq("Only" => ["headers"])
    end

    it "sends a plain message for empty headers" do
      received = collect(other, topic)
      nc.publish(topic, "body", headers: {})

      expect(received.pop(timeout: 2)).to have_attributes(data: "body", headers: nil)
    end

    it "raises ArgumentError for a header value that would forge another header" do
      expect { nc.publish(topic, "x", headers: { "A" => "1\r\nB: 2" }) }.to raise_error(ArgumentError)
    end

    it "carries the reply subject through" do
      received = collect(other, topic)
      nc.publish(topic, "question", reply_to: "_INBOX.somewhere")

      expect(received.pop(timeout: 2).reply_to).to eq("_INBOX.somewhere")
    end

    it "delivers a message written with flush: true" do
      received = collect(other, topic)
      nc.publish(topic, "now", flush: true)

      expect(received.pop(timeout: 2).data).to eq("now")
    end

    it "accepts a payload of exactly the server's max_payload" do
      received = collect(other, topic)
      nc.publish(topic, "x" * nc.max_payload)

      expect(received.pop(timeout: 5).data.bytesize).to eq(nc.max_payload)
    end

    it "raises MaxPayloadError for a payload over the server's limit" do
      expect { nc.publish(topic, "x" * (nc.max_payload + 1)) }
        .to raise_error(Tether::MaxPayloadError, /the server accepts #{nc.max_payload}/)
    end

    it "raises MaxPayloadError when headers push a payload over the server's limit" do
      expect { nc.publish(topic, "x" * nc.max_payload, headers: { "A" => "b" }) }
        .to raise_error(Tether::MaxPayloadError)
    end

    it "stays connected after refusing an oversized payload" do
      error_from { nc.publish(topic, "x" * (nc.max_payload + 1)) }

      expect(nc.flush).to be_connected
    end

    it("raises InvalidSubjectError for an empty subject") { expect_invalid_subject("") }
    it("raises InvalidSubjectError for a nil subject") { expect_invalid_subject(nil) }
    it("raises InvalidSubjectError for a subject with a space") { expect_invalid_subject("a b") }
    it("raises InvalidSubjectError for a subject with CRLF") { expect_invalid_subject("a\r\nPING") }
    it("raises InvalidSubjectError for a subject with a * token") { expect_invalid_subject("a.*.c") }
    it("raises InvalidSubjectError for a subject with a > token") { expect_invalid_subject("a.>") }
    it("raises InvalidSubjectError for a subject with an empty token") { expect_invalid_subject("a..b") }
    it("raises InvalidSubjectError for a subject ending in a dot") { expect_invalid_subject("a.") }

    it "raises InvalidSubjectError for a reply subject with whitespace" do
      expect { nc.publish(topic, "x", reply_to: "in box") }
        .to raise_error(Tether::InvalidSubjectError, /invalid reply subject/)
    end

    it "publishes to a wildcard token with validate_subjects: false" do
      relaxed = connect(validate_subjects: false)
      received = collect(other, "#{topic}.*")
      relaxed.publish("#{topic}.*", "literal star")

      expect(received.pop(timeout: 2).subject).to eq("#{topic}.*")
    end

    it "raises InvalidSubjectError for whitespace even with validate_subjects: false" do
      relaxed = connect(validate_subjects: false)

      expect { relaxed.publish("a b", "x") }.to raise_error(Tether::InvalidSubjectError)
    end

    it "raises HeadersNotSupportedError when the server does not support headers" do
      client = connect(fake_server(info: { headers: false }).uri)

      expect { client.publish(topic, "x", headers: { "A" => "b" }) }.to raise_error(Tether::HeadersNotSupportedError)
    end

    it "raises ConnectionClosedError once the client is closed" do
      nc.close

      expect { nc.publish(topic, "x") }.to raise_error(Tether::ConnectionClosedError)
    end

    it "keeps frames from threads publishing at once from interleaving" do
      received = collect(other, topic)
      payload = ->(thread, index) { "#{thread}:#{index}:#{"payload" * ((index % 7) + 1)}" }
      8.times.map { |t| Thread.new { 250.times { |i| nc.publish(topic, payload.call(t, i)) } } }.each(&:join)
      expected = 8.times.flat_map { |t| 250.times.map { |i| payload.call(t, i) } }

      expect(take(received, 2000, timeout: 5).map(&:data)).to match_array(expected)
    end

    it "delivers one thread's publishes in the order they were made" do
      received = collect(other, topic)
      500.times { |index| nc.publish(topic, index.to_s) }

      expect(take(received, 500, timeout: 5).map { |msg| msg.data.to_i }).to eq((0...500).to_a)
    end

    it "buffers publishes made during an outage and delivers them after the reconnect" do
      received = collect(other, topic)
      client = connect(proxy.uri, reconnect: ClientHelper::FAST_RECONNECT)
      outage(client)
      3.times { |index| client.publish(topic, "buffered #{index}") }
      restore(client)

      expect(take(received, 3).map(&:data)).to eq(["buffered 0", "buffered 1", "buffered 2"])
    end

    it "raises ReconnectBufferFullError once an outage has filled the reconnect buffer" do
      client = connect(proxy.uri, reconnect: ClientHelper::FAST_RECONNECT.merge(buffer_size: 64))
      outage(client)
      client.publish(topic, "x" * 20)

      expect { client.publish(topic, "x" * 64) }.to raise_error(Tether::ReconnectBufferFullError)
    end

    it "raises DisconnectedError during an outage with buffer_size: 0" do
      client = connect(proxy.uri, reconnect: ClientHelper::FAST_RECONNECT.merge(buffer_size: 0))
      outage(client)

      expect { client.publish(topic, "x") }.to raise_error(Tether::DisconnectedError)
    end

    it "keeps accepting publishes from a thread while the connection drops and returns" do
      received = collect(other, topic)
      client = connect(proxy.uri, reconnect: ClientHelper::FAST_RECONNECT)
      publisher = Thread.new do
        error_from { Thread.pass while client.publish("#{topic}.load", "x" * 64).stats.reconnects.zero? }
      end
      eventually { client.stats.messages_out > 100 }
      proxy.drop
      failure = publisher.value
      eventually { client.connected? }
      client.publish(topic, "after")

      expect(failure).to be_nil
      expect(received.pop(timeout: 2).data).to eq("after")
    end

    it "delivers publishes made before a retried first connect succeeds" do
      received = collect(other, topic)
      @proxy = TcpProxy.new(server.port)
      client = connect(@proxy.uri, retry_on_failed_connect: true, reconnect: ClientHelper::FAST_RECONNECT)
      client.publish(topic, "early")
      @proxy.start

      expect(received.pop(timeout: 2).data).to eq("early")
    end

    it "replaces the inherited connection on first use in a forked child" do
      other.subscribe(topic) { |msg| msg.respond("from parent: #{msg.data}") }
      other.flush
      nc.flush
      reply = in_child { nc.request(topic, "child").data }

      expect(reply).to eq("from parent: child")
    end

    it "raises ConnectionClosedError in a forked child when reconnect is disabled" do
      client = connect(reconnect: false)

      expect(in_child { client.publish(topic, "x") }).to match(["Tether::ConnectionClosedError", anything])
    end
  end

  describe "#subscribe" do
    it "delivers each message to the block on a worker thread" do
      names = Thread::Queue.new
      nc.subscribe(topic) { names.push(Thread.current.name) }
      nc.publish(topic, "x")

      expect(names.pop(timeout: 2)).to eq("tether:worker")
    end

    it "runs the blocks of every subscription on at most worker_threads threads" do
      client = connect(worker_threads: 3)
      threads = Thread::Queue.new
      20.times { |index| client.subscribe("#{topic}.#{index}") { threads.push(Thread.current) } }
      client.flush
      20.times { |index| client.publish("#{topic}.#{index}", "x") }

      expect(take(threads, 20).uniq.size).to be <= 3
    end

    it "keeps delivering to other subscriptions while a block is busy" do
      client = connect(worker_threads: 2)
      received = Thread::Queue.new
      client.subscribe("#{topic}.busy") { gate.pop }
      client.subscribe("#{topic}.free") { |msg| received.push(msg.data) }
      client.flush
      client.publish("#{topic}.busy", "x").publish("#{topic}.free", "through")

      expect(received.pop(timeout: 2)).to eq("through")
    end

    it "runs a dedicated subscription on a thread named after its subject" do
      names = Thread::Queue.new
      nc.subscribe(topic, dedicated: true) { names.push(Thread.current.name) }
      nc.publish(topic, "x")

      expect(names.pop(timeout: 2)).to eq("tether:sub:#{topic}")
    end

    it "delivers to a dedicated subscription while every shared worker is busy" do
      client = connect(worker_threads: 1)
      received = Thread::Queue.new
      client.subscribe("#{topic}.busy") { gate.pop }
      client.subscribe("#{topic}.own", dedicated: true) { |msg| received.push(msg.data) }
      client.flush
      client.publish("#{topic}.busy", "x").publish("#{topic}.own", "through")

      expect(received.pop(timeout: 2)).to eq("through")
    end

    it "returns a subscription for the subject" do
      expect(nc.subscribe(topic, queue: "workers")).to have_attributes(subject: topic, queue: "workers", client: nc)
    end

    it "matches one token with *" do
      received = collect(nc, "#{topic}.*.leaf")
      nc.publish("#{topic}.middle.leaf", "one").publish("#{topic}.a.b.leaf", "two").publish("#{topic}.x.leaf", "three")

      expect(take(received, 2).map(&:data)).to eq(%w[one three])
    end

    it "matches every trailing token with >" do
      received = collect(nc, "#{topic}.>")
      nc.publish("#{topic}.a", "one").publish("#{topic}.a.b.c", "two").publish(topic, "none")
      nc.flush

      expect(take(received, 2).map(&:subject)).to eq(["#{topic}.a", "#{topic}.a.b.c"])
      expect(received.pop(timeout: 0.05)).to be_nil
    end

    it "delivers the messages in the order they were published" do
      received = collect(nc, topic)
      200.times { |index| nc.publish(topic, index.to_s) }

      expect(take(received, 200).map { |msg| msg.data.to_i }).to eq((0...200).to_a)
    end

    it "gives each message to one member of a queue group" do
      members = Array.new(3) { connect }
      subscriptions = members.map { |member| member.subscribe(topic, queue: "workers") }
      members.each(&:flush)
      30.times { |index| nc.publish(topic, index.to_s) }
      nc.flush
      members.each(&:flush)

      expect(subscriptions.sum(&:pending)).to eq(30)
    end

    it "uses the client's max_pending when the subscription gives none" do
      client = connect(max_pending: 2)
      subscription = client.subscribe(topic)
      5.times { client.publish(topic, "x") }
      client.flush

      expect(subscription.dropped).to eq(3)
    end

    it "ends the subscription after max messages" do
      received = Thread::Queue.new
      subscription = nc.subscribe(topic, max: 2) { |msg| received.push(msg.data) }
      4.times { |index| nc.publish(topic, index.to_s) }
      nc.flush

      expect(take(received, 2)).to eq(%w[0 1])
      expect(eventually { subscription.closed? }).to be(true)
      expect(received.pop(timeout: 0.05)).to be_nil
    end

    it "asks the server to stop after max messages, so no more arrive" do
      nc.subscribe(topic, max: 2) { nil }
      before = nc.flush.stats.messages_in
      5.times { nc.publish(topic, "x") }

      expect(nc.flush.stats.messages_in - before).to eq(2)
    end

    it "runs the block on several workers at once with concurrency" do
      entered = Thread::Queue.new
      nc.subscribe(topic, concurrency: 3) do
        entered.push(Thread.current)
        gate.pop
      end
      3.times { nc.publish(topic, "x") }

      expect(take(entered, 3).uniq.size).to eq(3)
    end

    it "runs the block one message at a time by default" do
      running = 0
      overlaps = Thread::Queue.new
      done = Thread::Queue.new
      nc.subscribe(topic) do
        overlaps.push(true) if (running += 1) > 1
        Thread.pass
        running -= 1
        done.push(true)
      end
      50.times { nc.publish(topic, "x") }
      take(done, 50)

      expect(overlaps).to be_empty
    end

    it "drops messages past max_pending instead of blocking the reader" do
      subscription = nc.subscribe(topic, max_pending: 2) { gate.pop }
      10.times { nc.publish(topic, "x") }
      nc.flush

      expect(eventually { subscription.dropped >= 7 }).to be(true)
      expect(nc.flush).to be_connected
    end

    it "reports one SlowConsumerError for an episode of dropped messages" do
      errors = record(nc, :error)
      nc.subscribe(topic, max_pending: 2) { gate.pop }
      10.times { nc.publish(topic, "x") }
      nc.flush

      expect(errors.pop(timeout: 2)).to match([:error, an_instance_of(Tether::SlowConsumerError)])
      expect(errors.pop(timeout: 0.05)).to be_nil
    end

    it "drops messages past max_pending_bytes" do
      subscription = nc.subscribe(topic, max_pending_bytes: 100) { gate.pop }
      10.times { nc.publish(topic, "x" * 60) }
      nc.flush

      expect(eventually { subscription.dropped >= 7 }).to be(true)
    end

    it "keeps delivering to other subscriptions while one block is stuck" do
      entered = Thread::Queue.new
      nc.subscribe("#{topic}.stuck") do
        entered.push(true)
        gate.pop
      end
      healthy = collect(nc, "#{topic}.healthy")
      nc.publish("#{topic}.stuck", "blocks")
      entered.pop(timeout: 2)
      nc.publish("#{topic}.healthy", "still flowing")

      expect(healthy.pop(timeout: 2).data).to eq("still flowing")
    end

    it "reports a HandlerError when the block raises and keeps delivering" do
      errors = record(nc, :error)
      received = Thread::Queue.new
      nc.subscribe(topic) { |msg| msg.data == "bad" ? raise(ArgumentError, "cannot handle") : received.push(msg.data) }
      nc.publish(topic, "bad").publish(topic, "good")
      _, error = errors.pop(timeout: 2)

      expect(error).to be_a(Tether::HandlerError).and have_attributes(message: /ArgumentError: cannot handle/)
      expect(error.msg.data).to eq("bad")
      expect(error.cause).to be_a(ArgumentError)
      expect(received.pop(timeout: 2)).to eq("good")
    end

    it "receives its own publishes by default" do
      received = collect(nc, topic)
      nc.publish(topic, "echo")

      expect(received.pop(timeout: 2).data).to eq("echo")
    end

    it "receives only other connections' publishes with echo: false" do
      quiet = connect(echo: false)
      received = collect(quiet, topic)
      quiet.publish(topic, "mine").flush
      other.publish(topic, "theirs")

      expect(received.pop(timeout: 2).data).to eq("theirs")
    end

    it "raises InvalidSubjectError for a subject with an empty token" do
      expect { nc.subscribe("a..b") }.to raise_error(Tether::InvalidSubjectError, /empty token/)
    end

    it "raises InvalidSubjectError for a subject with whitespace" do
      expect { nc.subscribe("a b") }.to raise_error(Tether::InvalidSubjectError)
    end

    it "raises InvalidSubjectError for a queue group with whitespace" do
      expect { nc.subscribe(topic, queue: "my group") }
        .to raise_error(Tether::InvalidSubjectError, /invalid queue group/)
    end

    it "raises ConnectionClosedError once the client is closed" do
      nc.close

      expect { nc.subscribe(topic) }.to raise_error(Tether::ConnectionClosedError)
    end

    it "raises ConnectionDrainingError while the client is draining" do
      entered = Thread::Queue.new
      nc.subscribe(topic) do
        entered.push(true)
        gate.pop
      end
      nc.publish(topic, "x")
      entered.pop(timeout: 2)
      draining = Thread.new { nc.drain }
      eventually { nc.draining? }

      expect { nc.subscribe("#{topic}.late") }.to raise_error(Tether::ConnectionDrainingError)
      gate.close
      draining.join
    end

    it "restores the subscription after the connection is dropped" do
      client = connect(proxy.uri, reconnect: ClientHelper::FAST_RECONNECT)
      received = collect(client, topic)
      proxy.drop
      eventually { client.stats.reconnects == 1 }
      other.publish(topic, "after reconnect")

      expect(received.pop(timeout: 2).data).to eq("after reconnect")
    end

    it "restores a queue subscription after the connection is dropped" do
      client = connect(proxy.uri, reconnect: ClientHelper::FAST_RECONNECT)
      received = collect(client, topic, queue: "workers")
      proxy.drop
      eventually { client.stats.reconnects == 1 }
      other.publish(topic, "after reconnect")

      expect(received.pop(timeout: 2).data).to eq("after reconnect")
    end

    it "registers a subscription made during an outage once reconnected" do
      client = connect(proxy.uri, reconnect: ClientHelper::FAST_RECONNECT)
      outage(client)
      received = Thread::Queue.new
      client.subscribe(topic) { |msg| received.push(msg) }
      restore(client)
      client.flush
      other.publish(topic, "subscribed while down")

      expect(received.pop(timeout: 2).data).to eq("subscribed while down")
    end

    it "restores only the remainder of an auto-unsubscribe limit after a reconnect" do
      client = connect(proxy.uri, reconnect: ClientHelper::FAST_RECONNECT)
      received = collect(client, topic, max: 3)
      other.publish(topic, "before")
      received.pop(timeout: 2)
      proxy.drop
      eventually { client.stats.reconnects == 1 }
      before = client.flush.stats.messages_in
      5.times { other.publish(topic, "after") }
      other.flush

      expect(take(received, 2).map(&:data)).to eq(%w[after after])
      expect(client.flush.stats.messages_in - before).to eq(2)
    end

    it "restores subscriptions after the server itself restarts" do
      NatsServer.run do |restartable|
        client = connect(restartable.uri, reconnect: ClientHelper::FAST_RECONNECT)
        received = collect(client, topic)
        restartable.restart
        eventually(timeout: 10) { client.stats.reconnects == 1 }
        client.publish(topic, "after restart")

        expect(received.pop(timeout: 2).data).to eq("after restart")
      end
    end

    it "reports a PermissionsViolationError for a subject the user may not subscribe to and stays connected" do
      limited = connect(SharedNatsServer.users.uri, user: "limited", password: "wonderland")
      errors = record(limited, :error)
      limited.subscribe("forbidden.subject")

      expect(errors.pop(timeout: 2).last).to be_a(Tether::PermissionsViolationError)
      expect(limited.flush).to be_connected
    end
  end

  describe "#request" do
    def respond_on(client, subject, &handler)
      client.subscribe(subject) { |msg| msg.respond(handler.call(msg)) }
      client.flush
    end

    it "returns the first reply" do
      respond_on(other, topic) { |msg| "echo: #{msg.data}" }

      expect(nc.request(topic, "hello")).to have_attributes(data: "echo: hello")
    end

    it "sends an empty request without a payload" do
      respond_on(other, topic) { |msg| msg.data.bytesize.to_s }

      expect(nc.request(topic).data).to eq("0")
    end

    it "carries headers in both directions" do
      other.subscribe(topic) { |msg| msg.respond(msg.data, headers: { "X-Seen" => msg.headers["X-Sent"] }) }
      other.flush

      expect(nc.request(topic, "body", headers: { "X-Sent" => "abc" }).headers["X-Seen"]).to eq("abc")
    end

    it "raises NoRespondersError at once when nobody is subscribed" do
      started = Tether::Deadline.now

      expect { nc.request(topic, "x", timeout: 5) }.to raise_error(Tether::NoRespondersError, /#{topic}/)
      expect(Tether::Deadline.now - started).to be < 2
    end

    it "raises TimeoutError when no reply arrives in time" do
      other.subscribe(topic) { nil }
      other.flush

      expect { nc.request(topic, "x", timeout: 0.05) }.to raise_error(Tether::TimeoutError, /#{topic}/)
    end

    it "accepts a Deadline as the timeout" do
      other.subscribe(topic) { nil }
      other.flush

      expect { nc.request(topic, "x", timeout: Tether::Deadline.after(0.05)) }.to raise_error(Tether::TimeoutError)
    end

    it "answers concurrent requests from many threads without crossing replies" do
      respond_on(other, topic, &:data)
      replies = 32.times.map { |i| Thread.new { nc.request(topic, "req-#{i}", timeout: 5).data } }.map(&:value)

      expect(replies).to eq(32.times.map { |i| "req-#{i}" })
    end

    it "serves every request through one subscription" do
      client = connect(fake_server.uri)
      3.times { error_from { client.request(topic, "x", timeout: 0.01) } }
      client.flush

      expect(fake_server.received.scan(/^SUB /).size).to eq(1)
    end

    it "keeps working after a request timed out and its reply arrived late" do
      late = Thread::Queue.new
      other.subscribe(topic) { |msg| msg.data == "slow" ? late.push(msg) : msg.respond("fast") }
      other.flush
      error_from { nc.request(topic, "slow", timeout: 0.05) }
      late.pop(timeout: 2).respond("too late")
      other.flush

      expect(nc.request(topic, "quick").data).to eq("fast")
    end

    it "is not delayed by a stuck subscription block on the same connection" do
      entered = Thread::Queue.new
      nc.subscribe("#{topic}.stuck") do
        entered.push(true)
        gate.pop
      end
      respond_on(other, topic) { "prompt" }
      nc.publish("#{topic}.stuck", "x")
      entered.pop(timeout: 2)

      expect(nc.request(topic, "x").data).to eq("prompt")
    end

    it "raises DisconnectedError when the connection is lost while waiting" do
      arrived = Thread::Queue.new
      other.subscribe(topic) { arrived.push(true) }
      other.flush
      client = connect(proxy.uri, reconnect: ClientHelper::FAST_RECONNECT)
      waiting = Thread.new { error_from { client.request(topic, "x", timeout: 10) } }
      arrived.pop(timeout: 2)
      proxy.drop

      expect(waiting.value).to be_a(Tether::DisconnectedError)
    end

    it "keeps its deadline running during an outage and raises TimeoutError" do
      client = connect(proxy.uri, reconnect: ClientHelper::FAST_RECONNECT)
      outage(client)

      expect { client.request(topic, "x", timeout: 0.05) }.to raise_error(Tether::TimeoutError)
    end

    it "is answered after the reconnect when made during an outage" do
      respond_on(other, topic) { "worth the wait" }
      client = connect(proxy.uri, reconnect: ClientHelper::FAST_RECONNECT)
      outage(client)
      waiting = Thread.new { client.request(topic, "x", timeout: 5).data }
      eventually { waiting.status == "sleep" }
      proxy.start

      expect(waiting.value).to eq("worth the wait")
    end

    it "raises ConnectionClosedError once the client is closed" do
      nc.close

      expect { nc.request(topic, "x") }.to raise_error(Tether::ConnectionClosedError)
    end
  end

  describe "#request_many" do
    def respond_with(client, subject, *replies)
      client.subscribe(subject) { |msg| replies.each { |reply| msg.respond(reply) } }
      client.flush
    end

    it "collects replies until the timeout and returns them without raising" do
      respond_with(other, topic, "one", "two")

      expect(nc.request_many(topic, "q", timeout: 0.2).map(&:data)).to eq(%w[one two])
    end

    it "returns as soon as max replies have arrived" do
      respond_with(other, topic, "one", "two", "three")
      started = Tether::Deadline.now
      replies = nc.request_many(topic, "q", max: 2, timeout: 5)

      expect(replies.map(&:data)).to eq(%w[one two])
      expect(Tether::Deadline.now - started).to be < 2
    end

    it "returns once no reply has arrived for the stall interval" do
      respond_with(other, topic, "one", "two", "three")
      started = Tether::Deadline.now
      replies = nc.request_many(topic, "q", stall: 0.1, timeout: 5)

      expect(replies.size).to eq(3)
      expect(Tether::Deadline.now - started).to be < 2
    end

    it "stops at the reply the sentinel accepts, which is not included" do
      respond_with(other, topic, "one", "two", "", "ignored")
      replies = nc.request_many(topic, "q", sentinel: ->(msg) { msg.data.empty? }, timeout: 5)

      expect(replies.map(&:data)).to eq(%w[one two])
    end

    it "collects replies from several responders" do
      respond_with(other, topic, "a")
      respond_with(connect, topic, "b")

      expect(nc.request_many(topic, "q", max: 2, timeout: 5).map(&:data)).to match_array(%w[a b])
    end

    it "yields each reply and returns the count when a block is given" do
      respond_with(other, topic, "one", "two")
      yielded = []
      count = nc.request_many(topic, "q", max: 2, timeout: 5) { |msg| yielded << msg.data }

      expect(count).to eq(2)
      expect(yielded).to eq(%w[one two])
    end

    it "raises NoRespondersError when nobody is subscribed" do
      expect { nc.request_many(topic, "q", timeout: 5) }.to raise_error(Tether::NoRespondersError, /#{topic}/)
    end

    it "returns an empty array when the responder never answers" do
      other.subscribe(topic) { nil }
      other.flush

      expect(nc.request_many(topic, "q", timeout: 0.05)).to eq([])
    end

    it "sends the request's headers" do
      other.subscribe(topic) { |msg| msg.respond(msg.headers["Kind"]) }
      other.flush

      expect(nc.request_many(topic, "q", headers: { "Kind" => "search" }, max: 1).first.data).to eq("search")
    end

    it "unsubscribes from its reply subject when it returns" do
      inboxes = Thread::Queue.new
      other.subscribe(topic) do |msg|
        inboxes.push(msg.reply_to)
        msg.respond("one")
      end
      other.flush
      nc.request_many(topic, "q", max: 1)
      before = nc.flush.stats.messages_in
      other.publish(inboxes.pop(timeout: 2), "stray").flush

      expect(nc.flush.stats.messages_in).to eq(before)
    end
  end

  describe "#new_inbox" do
    it "shares its prefix with the reply subjects of requests" do
      client = connect(inbox_prefix: "replies.billing")
      other.subscribe(topic) { |msg| msg.respond(msg.reply_to) }
      other.flush

      expect(client.request(topic).data).to start_with("replies.billing.")
    end

    it "returns the inbox prefix followed by a 22 character identifier" do
      expect(nc.new_inbox).to match(/\A_INBOX\.[0-9A-Za-z]{22}\z/)
    end

    it "returns a different subject each time" do
      expect(nc.new_inbox).not_to eq(nc.new_inbox)
    end

    it "uses the configured inbox_prefix" do
      expect(build(inbox_prefix: "replies.billing").new_inbox).to start_with("replies.billing.")
    end
  end

  describe "#flush" do
    it "returns the client once the server has processed everything sent before it" do
      received = Thread::Queue.new
      other.subscribe(topic) { |msg| received.push(msg) }
      other.flush
      200.times { nc.publish(topic, "x") }

      expect(nc.flush).to be(nc)
      expect(take(received, 200).compact.size).to eq(200)
    end

    it "answers flushes from several threads at once" do
      results = 8.times.map { Thread.new { nc.flush(timeout: 5) } }.map(&:value)

      expect(results).to all(be(nc))
    end

    it "raises TimeoutError when the server does not answer in time" do
      client = connect(fake_server(pongs: 1).uri)

      expect { client.flush(timeout: 0.05) }.to raise_error(Tether::TimeoutError, /did not answer a PING/)
    end

    it "accepts a Deadline as the timeout" do
      client = connect(fake_server(pongs: 1).uri)

      expect { client.flush(timeout: Tether::Deadline.after(0.05)) }.to raise_error(Tether::TimeoutError)
    end

    it "raises DisconnectedError at once during an outage" do
      client = connect(proxy.uri, reconnect: ClientHelper::FAST_RECONNECT)
      outage(client)

      expect { client.flush }.to raise_error(Tether::DisconnectedError)
    end

    it "raises DisconnectedError when the connection is lost while waiting" do
      client = connect(fake_server(pongs: 1).uri, reconnect: ClientHelper::FAST_RECONNECT)
      waiting = Thread.new { error_from { client.flush(timeout: 10) } }
      eventually { fake_server.received.scan("PING\r\n").size == 2 }
      fake_server.drop

      expect(waiting.value).to be_a(Tether::DisconnectedError)
    end

    it "raises ConnectionClosedError once the client is closed" do
      nc.close

      expect { nc.flush }.to raise_error(Tether::ConnectionClosedError)
    end
  end

  describe "#rtt" do
    it "returns the seconds a round trip to the server takes" do
      expect(nc.rtt).to be_a(Float).and be_between(0, 2).exclusive
    end

    it "raises ConnectionClosedError once the client is closed" do
      nc.close

      expect { nc.rtt }.to raise_error(Tether::ConnectionClosedError)
    end
  end

  describe "#drain" do
    it "lets the block finish everything already received, then closes" do
      handled = Thread::Queue.new
      nc.subscribe(topic) do |msg|
        gate.pop(timeout: 0.01)
        handled.push(msg.data)
      end
      20.times { |index| nc.publish(topic, index.to_s) }
      nc.flush
      nc.drain

      expect(handled.size).to eq(20)
      expect(nc).to be_closed
    end

    it "returns the client" do
      expect(nc.drain).to be(nc)
    end

    it "sends what blocks publish while draining" do
      forwarded = collect(other, "#{topic}.out")
      nc.subscribe("#{topic}.in") { |msg| nc.publish("#{topic}.out", "forwarded #{msg.data}") }
      10.times { |index| nc.publish("#{topic}.in", index.to_s) }
      nc.flush
      nc.drain

      expect(take(forwarded, 10).map(&:data)).to eq(Array.new(10) { |index| "forwarded #{index}" })
    end

    it "sends publishes buffered before the drain" do
      received = collect(other, topic)
      100.times { nc.publish(topic, "x") }
      nc.drain

      expect(take(received, 100).compact.size).to eq(100)
    end

    it "stops delivery of messages published after it started" do
      received = collect(nc, topic)
      nc.drain
      other.publish(topic, "too late").flush

      expect(received.pop(timeout: 0.05)).to be_nil
    end

    it "closes the subscriptions" do
      subscription = nc.subscribe(topic) { nil }
      nc.drain

      expect(subscription).to be_closed
    end

    it "starts the drain and returns when called from a subscription block" do
      closed = record(nc, :close)
      handled = Thread::Queue.new
      nc.subscribe(topic) do |msg|
        nc.drain if msg.data == "0"
        handled.push(msg.data)
      end
      3.times { |index| nc.publish(topic, index.to_s) }

      expect(closed.pop(timeout: 5)).to eq([:close])
      expect(take(handled, 3)).to eq(%w[0 1 2])
      expect(nc).to be_closed
    end

    it "raises TimeoutError when a block does not finish in time, and closes anyway" do
      entered = Thread::Queue.new
      nc.subscribe(topic) do
        entered.push(true)
        gate.pop
      end
      nc.publish(topic, "x")
      entered.pop(timeout: 2)

      expect { nc.drain(timeout: 0.05) }.to raise_error(Tether::TimeoutError, /did not drain/)
      expect(nc).to be_closed
    end

    it "uses drain_timeout when no timeout is given" do
      client = connect(drain_timeout: 0.05)
      entered = Thread::Queue.new
      client.subscribe(topic) do
        entered.push(true)
        gate.pop
      end
      client.publish(topic, "x")
      entered.pop(timeout: 2)

      expect { client.drain }.to raise_error(Tether::TimeoutError)
    end

    it "raises DisconnectedError during an outage and closes without draining" do
      client = connect(proxy.uri, reconnect: ClientHelper::FAST_RECONNECT)
      outage(client)

      expect { client.drain }.to raise_error(Tether::DisconnectedError)
      expect(client).to be_closed
    end

    it "does nothing on a closed client" do
      nc.close

      expect(nc.drain).to be(nc)
    end
  end

  describe "#close" do
    it "returns the client, closed" do
      expect(nc.close).to be(nc).and be_closed
    end

    it "tolerates being called twice" do
      nc.close

      expect { nc.close }.not_to raise_error
    end

    it "tolerates being called on a client that never connected" do
      expect(build.close).to be_closed
    end

    it "writes what is buffered before closing" do
      received = collect(other, topic)
      100.times { nc.publish(topic, "x") }
      nc.close

      expect(take(received, 100).compact.size).to eq(100)
    end

    it "closes the subscriptions" do
      subscription = nc.subscribe(topic)
      nc.close

      expect(subscription).to be_closed
    end

    it "releases a caller blocked in a request with ConnectionClosedError" do
      arrived = Thread::Queue.new
      other.subscribe(topic) { arrived.push(true) }
      other.flush
      waiting = Thread.new { error_from { nc.request(topic, "x", timeout: 10) } }
      arrived.pop(timeout: 2)
      nc.close

      expect(waiting.value).to be_a(Tether::ConnectionClosedError)
    end

    it "releases a caller blocked in a flush with ConnectionClosedError" do
      client = connect(fake_server(pongs: 1).uri)
      waiting = Thread.new { error_from { client.flush(timeout: 10) } }
      eventually { fake_server.received.scan("PING\r\n").size == 2 }
      client.close

      expect(waiting.value).to be_a(Tether::ConnectionClosedError)
    end

    it "releases a caller blocked waiting for the next message" do
      subscription = nc.subscribe(topic)
      waiting = Thread.new { error_from { subscription.next(timeout: 10) } }
      eventually { waiting.status == "sleep" }
      nc.close

      expect(waiting.value).to be_a(Tether::SubscriptionClosedError)
    end

    it "leaves none of its threads running" do
      before = tether_threads
      client = connect
      record(client, :error)
      client.subscribe(topic) { nil }
      client.request_many(topic, "x", max: 1, timeout: 0.01)
      client.close

      expect(eventually { (tether_threads - before).empty? }).to be(true)
    end

    it "leaves none of its threads running when closed during an outage" do
      before = tether_threads
      client = connect(proxy.uri, reconnect: ClientHelper::FAST_RECONNECT)
      outage(client)
      client.close

      expect(eventually { (tether_threads - before).empty? }).to be(true)
    end

    it "closes the client when called from a subscription block" do
      closed = record(nc, :close)
      nc.subscribe(topic) { nc.close }
      nc.publish(topic, "x")

      expect(closed.pop(timeout: 5)).to eq([:close])
      expect(nc).to be_closed
    end

    it "closes the client when called from an event handler" do
      client = connect(proxy.uri, reconnect: ClientHelper::FAST_RECONNECT)
      client.on_disconnect { client.close }
      proxy.drop

      expect(eventually { client.closed? }).to be(true)
    end

    it "ends the connection on the server" do
      client = connect(fake_server.uri)
      client.close

      expect(eventually { fake_server.connections.zero? }).to be(true)
    end
  end

  describe "#force_reconnect" do
    it "drops the socket and reconnects, keeping subscriptions" do
      client = connect(reconnect: ClientHelper::FAST_RECONNECT)
      received = collect(client, topic)
      client.force_reconnect
      eventually { client.stats.reconnects == 1 && client.connected? }
      other.publish(topic, "after")

      expect(received.pop(timeout: 2).data).to eq("after")
    end

    it "returns the client" do
      client = connect(reconnect: ClientHelper::FAST_RECONNECT)

      expect(client.force_reconnect).to be(client)
    end

    it "reports the disconnect without an error, since it was requested" do
      client = connect(reconnect: ClientHelper::FAST_RECONNECT)
      events = record(client, :disconnect)
      client.force_reconnect

      expect(events.pop(timeout: 2)).to eq([:disconnect, nil])
    end

    it "cuts short the wait of a reconnect in progress" do
      client = connect(proxy.uri, reconnect: { wait: 30, jitter: 0 })
      errors = record(client, :reconnect_error)
      outage(client)
      errors.pop(timeout: 2)
      proxy.start
      client.force_reconnect

      expect(eventually { client.connected? }).to be(true)
    end

    it "raises ConfigurationError when reconnect is disabled" do
      client = connect(reconnect: false)

      expect { client.force_reconnect }.to raise_error(Tether::ConfigurationError)
    end

    it "raises ConnectionClosedError once the client is closed" do
      nc.close

      expect { nc.force_reconnect }.to raise_error(Tether::ConnectionClosedError)
    end
  end

  describe "#after_fork" do
    it "gives the child a working client of its own" do
      nc.flush
      result = in_child do
        nc.after_fork
        subscription = nc.subscribe(topic)
        nc.publish(topic, "from child")
        [nc.status, nc.generation == Process.pid, subscription.next(timeout: 2).data]
      end

      expect(result).to eq([:connected, true, "from child"])
    end

    it "returns the client" do
      expect(nc.after_fork).to be(nc)
    end

    it "closes inherited subscriptions in the child without running their blocks" do
      delivered = Thread::Queue.new
      with_block = nc.subscribe(topic) { |msg| delivered.push(msg) }
      without_block = nc.subscribe("#{topic}.pull")
      nc.flush
      result = in_child do
        nc.after_fork
        nc.publish(topic, "x").flush
        [with_block.closed?, without_block.closed?, error_from { without_block.next(timeout: 0.05) }.class.name]
      end

      expect(result).to eq([true, true, "Tether::SubscriptionClosedError"])
    end

    it "leaves the parent's connection and subscriptions untouched" do
      received = collect(nc, topic)
      in_child { nc.after_fork.publish(topic, "from child").flush.close.status }
      nc.publish(topic, "from parent")

      expect(take(received, 2).map(&:data)).to eq(["from child", "from parent"])
      expect(nc.stats.reconnects).to eq(0)
    end

    it "gives the child an inbox of its own, so replies do not cross processes" do
      other.subscribe(topic) { |msg| msg.respond(msg.reply_to) }
      other.flush
      parent_inbox = nc.request(topic).data
      child_inbox = in_child { nc.after_fork.request(topic).data }

      expect(child_inbox.split(".")[1]).not_to eq(parent_inbox.split(".")[1])
    end
  end

  describe "#status" do
    it "returns :disconnected before the client connects" do
      expect(build.status).to eq(:disconnected)
    end

    it "returns :connected once connected" do
      expect(nc.status).to eq(:connected)
    end

    it "returns :reconnecting during an outage" do
      client = connect(proxy.uri, reconnect: ClientHelper::FAST_RECONNECT)
      outage(client)

      expect(client.status).to eq(:reconnecting)
    end

    it "returns :closed after close" do
      expect(nc.close.status).to eq(:closed)
    end

    it "returns :closed when the connection is lost with reconnect disabled" do
      client = connect(proxy.uri, reconnect: false)
      proxy.drop

      expect(eventually { client.status == :closed }).to be(true)
    end

    it "returns :closed once every server has failed max_attempts times" do
      client = connect(proxy.uri, reconnect: { wait: 0.01, jitter: 0, max_attempts: 2 })
      proxy.stop

      expect(eventually { client.status == :closed }).to be(true)
      expect(eventually { client.last_error }).to be_a(Tether::NoServersError)
    end
  end

  describe "#connected?" do
    it "returns true with a live connection" do
      expect(nc).to be_connected
    end

    it "returns false after close" do
      expect(nc.close).not_to be_connected
    end
  end

  describe "#reconnecting?" do
    it "returns true during an outage" do
      client = connect(proxy.uri, reconnect: ClientHelper::FAST_RECONNECT)
      proxy.stop

      expect(eventually { client.reconnecting? }).to be(true)
    end

    it "returns false with a live connection" do
      expect(nc).not_to be_reconnecting
    end
  end

  describe "#draining?" do
    it "returns true while a drain waits for a block" do
      entered = Thread::Queue.new
      nc.subscribe(topic) do
        entered.push(true)
        gate.pop
      end
      nc.publish(topic, "x")
      entered.pop(timeout: 2)
      draining = Thread.new { nc.drain }

      expect(eventually { nc.draining? }).to be(true)
      gate.close
      draining.join
    end

    it "returns false with a live connection" do
      expect(nc).not_to be_draining
    end
  end

  describe "#closed?" do
    it "returns false with a live connection" do
      expect(nc).not_to be_closed
    end

    it "returns true after close" do
      expect(nc.close).to be_closed
    end
  end

  describe "#server" do
    it "returns the server the client connected to" do
      expect(nc.server.url).to eq(server.uri)
    end

    it "returns a URL without the credentials it connected with" do
      client = connect(SharedNatsServer.users.uri.sub("://", "://alice:wonderland@"))

      expect(client.server.url).not_to include("wonderland")
    end

    it "returns nil before the client connects" do
      expect(build.server).to be_nil
    end
  end

  describe "#server_info" do
    it "returns what the connected server announced" do
      expect(nc.server_info).to be_a(Tether::ServerInfo).and have_attributes(
        version: /\A\d+\.\d+/, max_payload: a_value > 0, client_id: an_instance_of(Integer), headers?: true
      )
    end

    it "returns nil before the client connects" do
      expect(build.server_info).to be_nil
    end
  end

  describe "#max_payload" do
    it "returns the limit the server announced" do
      expect(nc.max_payload).to eq(nc.server_info.max_payload)
    end

    it "returns nil before the client connects" do
      expect(build.max_payload).to be_nil
    end
  end

  describe "#servers" do
    it "returns the pool without credentials in the URLs" do
      client = build("nats://user:secret@127.0.0.1:4222")

      expect(client.servers.map(&:url)).to eq(["nats://127.0.0.1:4222"])
    end

    it "keeps the given order with randomize: false" do
      client = build("nats://a:1,nats://b:2,nats://c:3", randomize: false)

      expect(client.servers.map(&:url)).to eq(%w[nats://a:1 nats://b:2 nats://c:3])
    end
  end

  describe "#servers=" do
    it "replaces the pool the next reconnect uses" do
      client = connect("nats://127.0.0.1:#{proxy.port}", reconnect: ClientHelper::FAST_RECONNECT)
      client.servers = server.uri
      proxy.stop

      expect(eventually { client.connected? && client.stats.reconnects == 1 }).to be(true)
      expect(client.servers.map(&:url)).to eq([server.uri])
    end
  end

  describe "#stats" do
    it "counts messages and bytes in both directions" do
      received = collect(nc, topic)
      before = nc.stats
      nc.publish(topic, "hello")
      received.pop(timeout: 2)
      after = nc.flush.stats

      expect(after.messages_out - before.messages_out).to eq(1)
      expect(after.messages_in - before.messages_in).to eq(1)
      expect(after.bytes_out - before.bytes_out).to eq("PUB #{topic} 5\r\nhello\r\nPING\r\n".bytesize)
      expect(after.bytes_in - before.bytes_in).to eq("MSG #{topic} 1 5\r\nhello\r\nPONG\r\n".bytesize)
    end

    it "returns a frozen snapshot" do
      expect(nc.stats).to be_frozen.and have_attributes(reconnects: 0)
    end

    it "counts reconnects" do
      client = connect(proxy.uri, reconnect: ClientHelper::FAST_RECONNECT)
      proxy.drop

      expect(eventually { client.stats.reconnects == 1 }).to be(true)
    end
  end

  describe "#last_error" do
    it "returns nil when nothing has gone wrong" do
      expect(nc.last_error).to be_nil
    end

    it "returns the most recent asynchronous error without a handler registered" do
      nc.subscribe(topic) { raise "boom" }
      nc.publish(topic, "x")

      expect(eventually { nc.last_error }).to be_a(Tether::HandlerError)
    end

    it "returns the authorization error that ended reconnecting" do
      tokens = ["s3cr3t"]
      token = -> { tokens.shift || "wrong" }
      client = connect(proxy(SharedNatsServer.token).uri, token: token, reconnect: ClientHelper::FAST_RECONNECT)
      proxy.drop

      expect(eventually { client.closed? }).to be(true)
      expect(eventually { client.last_error }).to be_a(Tether::AuthorizationError)
    end

    it "returns StaleConnectionError when the server stops answering PINGs" do
      client = connect(fake_server(pongs: 1).uri, ping_interval: 0.02, max_pings_out: 2, reconnect: false)

      expect(eventually { client.closed? }).to be(true)
      expect(eventually { client.last_error }).to be_a(Tether::StaleConnectionError)
    end
  end

  describe "#user_info" do
    it "returns who the server says this connection is" do
      client = connect(SharedNatsServer.users.uri, user: "alice", password: "wonderland")

      expect(client.user_info).to include("user" => "alice", "account" => "$G")
    end

    it "includes the permissions of a restricted user" do
      client = connect(SharedNatsServer.users.uri, user: "limited", password: "wonderland")

      expect(client.user_info.dig("permissions", "publish", "allow")).to include("allowed.>")
    end
  end

  describe "#on_error" do
    it "receives an -ERR the server sends without closing the connection" do
      client = connect(fake_server.uri)
      errors = record(client, :error)
      fake_server.push(%(-ERR 'Permissions Violation for Publish to "orders.new"'\r\n))

      expect(errors.pop(timeout: 2).last).to be_a(Tether::PermissionsViolationError)
      expect(client.flush).to be_connected
    end

    it "receives a permissions violation for a publish the user may not make and stays connected" do
      limited = connect(SharedNatsServer.users.uri, user: "limited", password: "wonderland")
      errors = record(limited, :error)
      limited.publish("forbidden.subject", "x")

      expect(errors.pop(timeout: 2).last).to have_attributes(message: /Permissions Violation for Publish/)
      expect(limited.flush).to be_connected
    end

    it "calls every registered handler" do
      first = record(nc, :error)
      second = record(nc, :error)
      nc.subscribe(topic) { raise "boom" }
      nc.publish(topic, "x")

      expect([first.pop(timeout: 2), second.pop(timeout: 2)]).to all(match([:error, a_kind_of(Tether::HandlerError)]))
    end

    it "returns a registration whose cancel removes the handler" do
      canceled = Thread::Queue.new
      nc.on_error { |error| canceled.push(error) }.cancel
      kept = record(nc, :error)
      nc.subscribe(topic) { raise "boom" }
      nc.publish(topic, "x")
      kept.pop(timeout: 2)

      expect(canceled).to be_empty
    end

    it "runs handlers on the event thread, where they may call back into the client" do
      results = Thread::Queue.new
      nc.on_error { results.push([Thread.current.name, nc.rtt.positive?]) }
      nc.subscribe(topic) { raise "boom" }
      nc.publish(topic, "x")

      expect(results.pop(timeout: 2)).to eq(["tether:events", true])
    end

    it "keeps delivering events after a handler raises" do
      nc.on_error { raise "handler bug" }
      errors = record(nc, :error)
      nc.subscribe(topic) { raise "boom" }
      nc.publish(topic, "one").publish(topic, "two")

      expect(take(errors, 2).compact.size).to eq(2)
    end
  end

  describe "#on_connect" do
    it "receives the server of the first successful connect" do
      client = build
      events = record(client, :connect)
      client.connect

      expect(events.pop(timeout: 2)).to match([:connect, have_attributes(url: server.uri)])
    end
  end

  describe "#on_disconnect" do
    it "receives the reason the connection was lost" do
      client = connect(proxy.uri, reconnect: ClientHelper::FAST_RECONNECT)
      events = record(client, :disconnect)
      proxy.drop

      expect(events.pop(timeout: 2)).to match([:disconnect, a_kind_of(Tether::ConnectionError)])
    end

    it "receives nil when the client itself closes the connection" do
      events = record(nc, :disconnect)
      nc.close

      expect(events.pop(timeout: 2)).to eq([:disconnect, nil])
    end

    it "receives a ProtocolError when the server sends bytes that are not NATS protocol" do
      client = connect(fake_server.uri, reconnect: ClientHelper::FAST_RECONNECT)
      events = record(client, :disconnect)
      fake_server.push("WAT\r\n")

      expect(events.pop(timeout: 2)).to match([:disconnect, an_instance_of(Tether::ProtocolError)])
      expect(eventually { client.stats.reconnects == 1 }).to be(true)
    end

    it "receives a StaleConnectionError when the server stops answering PINGs" do
      client = build(fake_server(pongs: 1).uri, ping_interval: 0.02, max_pings_out: 1, reconnect: false)
      events = record(client, :disconnect)
      client.connect

      expect(events.pop(timeout: 2)).to match([:disconnect, an_instance_of(Tether::StaleConnectionError)])
    end
  end

  describe "#on_reconnect" do
    it "receives the server reconnected to" do
      client = connect(proxy.uri, reconnect: ClientHelper::FAST_RECONNECT)
      events = record(client, :reconnect)
      proxy.drop

      expect(events.pop(timeout: 2)).to match([:reconnect, have_attributes(url: proxy.uri)])
    end
  end

  describe "#on_reconnect_error" do
    it "receives the error of each failed attempt" do
      client = connect(proxy.uri, reconnect: ClientHelper::FAST_RECONNECT)
      events = record(client, :reconnect_error)
      proxy.stop

      expect(take(events, 2)).to all(match([:reconnect_error, a_kind_of(Tether::ConnectionError)]))
    end
  end

  describe "#on_reconnect_error" do
    it "keeps retrying through authorization errors with abort_on_auth_error: false" do
      tokens = ["s3cr3t"]
      token = -> { tokens.shift || "wrong" }
      settings = ClientHelper::FAST_RECONNECT.merge(abort_on_auth_error: false)
      client = connect(proxy(SharedNatsServer.token).uri, token: token, reconnect: settings)
      events = record(client, :reconnect_error)
      proxy.drop

      expect(take(events, 4).map(&:last)).to all(be_a(Tether::AuthorizationError))
      expect(client).to be_reconnecting
    end
  end

  describe "#on_close" do
    it "runs once the client is closed" do
      events = record(nc, :close)
      nc.close

      expect(events.pop(timeout: 2)).to eq([:close])
    end

    it "runs when the connection is lost with reconnect disabled" do
      client = connect(proxy.uri, reconnect: false)
      events = record(client, :disconnect, :close)
      proxy.drop

      expect(take(events, 2)).to match([[:disconnect, a_kind_of(Tether::ConnectionError)], [:close]])
    end
  end

  describe "#on_lame_duck" do
    it "receives the server that announced it is shutting down" do
      client = connect(fake_server.uri)
      events = record(client, :lame_duck)
      fake_server.push(%(INFO {"server_id":"FAKE","ldm":true,"headers":true,"max_payload":1048576}\r\n))

      expect(events.pop(timeout: 2)).to match([:lame_duck, have_attributes(url: fake_server.uri)])
      expect(client).to be_connected
    end

    it "reconnects at once with reconnect: { on_lame_duck: true }" do
      client = connect(fake_server.uri, reconnect: ClientHelper::FAST_RECONNECT.merge(on_lame_duck: true))
      fake_server.push(%(INFO {"server_id":"FAKE","ldm":true,"headers":true,"max_payload":1048576}\r\n))

      expect(eventually { client.stats.reconnects == 1 }).to be(true)
    end

    it "fires when a real server enters lame duck mode" do
      NatsServer.run do |retiring|
        client = connect(retiring.uri, reconnect: false)
        events = record(client, :lame_duck)
        retiring.signal("ldm")

        expect(events.pop(timeout: 10)).to match([:lame_duck, have_attributes(url: retiring.uri)])
      end
    end
  end

  describe "#on_servers_discovered" do
    def advertise(*urls)
      document = { server_id: "FAKE", headers: true, max_payload: 1_048_576, connect_urls: urls }
      fake_server.push("INFO #{JSON.generate(document)}\r\n")
    end

    it "receives the servers the cluster advertised and adds them to the pool" do
      client = connect(fake_server.uri)
      events = record(client, :servers_discovered)
      advertise("10.0.0.9:4222")

      expect(events.pop(timeout: 2)).to match([:servers_discovered, [have_attributes(url: "nats://10.0.0.9:4222")]])
      expect(client.servers.map(&:url)).to contain_exactly(fake_server.uri, "nats://10.0.0.9:4222")
    end

    it "leaves the pool alone with ignore_discovered_servers: true" do
      client = connect(fake_server.uri, ignore_discovered_servers: true)
      advertise("10.0.0.9:4222")
      client.flush

      expect(client.servers.map(&:url)).to eq([fake_server.uri])
    end
  end

  describe "#on_status" do
    it "receives every transition in order, interleaved with the connection events" do
      client = build(proxy.uri, reconnect: ClientHelper::FAST_RECONNECT)
      events = record(client, :status, :connect, :disconnect, :reconnect, :close)
      client.connect
      proxy.drop
      eventually { client.stats.reconnects == 1 }
      client.close

      expect(take(events, 10).map { |name, detail| name == :status ? detail : name }).to eq(
        %i[connecting connected connect reconnecting disconnect connected reconnect closed disconnect close]
      )
    end
  end

  describe "#inspect" do
    it "shows the status and server without credentials" do
      client = connect("nats://alice:wonderland@127.0.0.1:#{SharedNatsServer.users.port}")

      expect(client.inspect)
        .to eq(%(#<Tether::Client status=connected server="nats://127.0.0.1:#{SharedNatsServer.users.port}">))
    end
  end

  describe "#options" do
    it "returns the validated options the client was built with" do
      expect(build(name: "billing").options).to be_a(Tether::Options).and have_attributes(name: "billing")
    end
  end
end
