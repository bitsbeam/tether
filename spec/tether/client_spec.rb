# frozen_string_literal: true

RSpec.describe Tether::Client do
  subject(:nc) { described_class.connect(server.uri) }

  let(:server) { SharedNatsServer.instance }

  after { nc.close }

  def collector(subject_pattern, **options)
    received = Thread::Queue.new
    nc.subscribe(subject_pattern, **options) { |msg| received.push(msg) }
    nc.flush
    received
  end

  describe "connecting" do
    it "completes the handshake and reads the server's INFO" do
      expect(nc).to be_connected
      expect(nc.server_info).to include("max_payload", "version")
    end

    it "reports the max_payload the server advertised" do
      expect(nc.max_payload).to be > 0
    end

    it "raises rather than hanging when nothing is listening" do
      expect { described_class.connect("nats://127.0.0.1:1", connect_timeout: 1) }
        .to raise_error(Tether::ConnectionError)
    end
  end

  describe "publish and subscribe" do
    it "round trips a payload" do
      received = collector("round.trip")
      nc.publish("round.trip", "hello")

      expect(received.pop(timeout: 2).data).to eq("hello")
    end

    it "round trips headers without disturbing the payload" do
      received = collector("with.headers")
      nc.publish("with.headers", "body", headers: Tether::Protocol::Headers["Nats-Msg-Id" => "m-1", "Nats-TTL" => "60"])
      msg = received.pop(timeout: 2)

      expect(msg.data).to eq("body")
      expect(msg.headers["Nats-Msg-Id"]).to eq("m-1")
      expect(msg.headers["Nats-TTL"]).to eq("60")
    end

    it "delivers arbitrary bytes unchanged, including CRLF inside the payload" do
      payload = "\x00\x01\r\n\xFF binary".b
      received = collector("binary.safe")
      nc.publish("binary.safe", payload)

      expect(received.pop(timeout: 2).data).to eq(payload)
    end

    it "matches wildcard subjects" do
      received = collector("wild.*.leaf")
      nc.publish("wild.middle.leaf", "matched")

      expect(received.pop(timeout: 2).subject).to eq("wild.middle.leaf")
    end

    it "carries a reply subject through" do
      received = collector("needs.reply")
      nc.publish("needs.reply", "q", reply_to: "_INBOX.somewhere")

      expect(received.pop(timeout: 2).reply_to).to eq("_INBOX.somewhere")
    end

    it "spreads a queue group across its members" do
      counts = Hash.new(0)
      lock = Mutex.new
      2.times do |n|
        nc.subscribe("work.items", queue: "workers") { lock.synchronize { counts[n] += 1 } }
      end
      nc.flush
      20.times { nc.publish("work.items", "job") }
      nc.flush
      sleep 0.2

      expect(lock.synchronize { counts.values.sum }).to eq(20)
    end

    it "refuses a payload the server would reject" do
      expect { nc.publish("too.big", "x" * (nc.max_payload + 1)) }
        .to raise_error(Tether::MaxPayloadError, /max_payload/)
    end
  end

  describe "subscriptions without a callback" do
    it "hands messages over on the caller's thread" do
      subscription = nc.subscribe("pull.me")
      nc.flush
      nc.publish("pull.me", "pulled")

      expect(subscription.next_message(Tether::Deadline.after(2)).data).to eq("pulled")
    end

    it "times out rather than blocking forever" do
      subscription = nc.subscribe("never.arrives")
      nc.flush

      expect { subscription.next_message(Tether::Deadline.after(0.1)) }
        .to raise_error(Tether::TimeoutError)
    end
  end

  describe "unsubscribing" do
    it "stops delivery" do
      received = Thread::Queue.new
      subscription = nc.subscribe("stop.me") { |msg| received.push(msg) }
      nc.flush
      nc.unsubscribe(subscription)
      nc.publish("stop.me", "after")
      nc.flush
      sleep 0.1

      expect(received).to be_empty
    end
  end

  describe "backpressure" do
    it "drops rather than blocking the reader, and reports a slow consumer" do
      errors = []
      nc.on_error { |e| errors << e }
      release = Thread::Queue.new
      subscription = nc.subscribe("slow.one", max_pending: 2) { release.pop }
      nc.flush

      20.times { nc.publish("slow.one", "x") }
      nc.flush
      sleep 0.3

      expect(subscription.dropped).to be > 0
      expect(errors).to include(an_instance_of(Tether::SlowConsumerError))
      3.times { release.push(:go) }
    end

    it "keeps delivering to other subscriptions while one is stuck" do
      blocked = Thread::Queue.new
      nc.subscribe("stuck.one") { blocked.pop }
      healthy = collector("healthy.one")
      nc.flush

      nc.publish("stuck.one", "blocks")
      sleep 0.1
      nc.publish("healthy.one", "still flowing")

      expect(healthy.pop(timeout: 2).data).to eq("still flowing")
      blocked.push(:go)
    end
  end

  describe "concurrent use of one connection" do
    it "never interleaves frames from threads publishing at once" do
      threads = 8
      per_thread = 250
      received = Thread::Queue.new
      nc.subscribe("concurrent.pub") { |msg| received.push(msg.data) }
      nc.flush

      threads.times.map do |t|
        Thread.new do
          per_thread.times { |i| nc.publish("concurrent.pub", "#{t}:#{i}:#{"payload" * ((i % 7) + 1)}") }
        end
      end.each(&:join)
      nc.flush

      collected = []
      collected << received.pop(timeout: 5) while collected.size < threads * per_thread
      expected = threads.times.flat_map { |t| per_thread.times.map { |i| "#{t}:#{i}:#{"payload" * ((i % 7) + 1)}" } }

      expect(collected.sort).to eq(expected.sort)
    end

    it "survives many threads subscribing and unsubscribing at once" do
      threads = 8.times.map do |t|
        Thread.new do
          10.times do |i|
            subscription = nc.subscribe("churn.#{t}.#{i}")
            nc.unsubscribe(subscription)
          end
        end
      end
      threads.each(&:join)
      nc.flush

      expect(nc).to be_connected
    end

    it "answers a flush from several threads at once" do
      results = 8.times.map { Thread.new { nc.flush(timeout: 5) } }.map(&:value)

      expect(results).to all(be(true))
    end
  end

  describe "closing" do
    it "stops accepting publishes" do
      nc.close

      expect { nc.publish("after.close", "x") }.to raise_error(Tether::ConnectionClosedError)
    end

    it "is safe to call twice" do
      nc.close

      expect { nc.close }.not_to raise_error
    end
  end
end
