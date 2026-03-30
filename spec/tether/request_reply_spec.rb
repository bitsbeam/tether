# frozen_string_literal: true

RSpec.describe "request and reply" do
  subject(:nc) { Tether::Client.connect(server.uri) }

  let(:server) { SharedNatsServer.instance }

  after { nc.close }

  def serve(subject_pattern, &handler)
    nc.subscribe(subject_pattern) { |msg| msg.respond(handler.call(msg)) }
    nc.flush
  end

  it "round trips a request" do
    serve("echo.one") { |msg| "echo: #{msg.data}" }

    expect(nc.request("echo.one", "hello").data).to eq("echo: hello")
  end

  it "carries headers in both directions" do
    nc.subscribe("echo.headers") do |msg|
      msg.respond(msg.data, headers: Tether::Protocol::Headers["X-Seen" => msg.headers["X-Sent"]])
    end
    nc.flush

    reply = nc.request("echo.headers", "body", headers: Tether::Protocol::Headers["X-Sent" => "abc"])

    expect(reply.headers["X-Seen"]).to eq("abc")
  end

  it "fails fast when nobody is subscribed rather than waiting out the timeout" do
    started = Tether::Deadline.now

    expect { nc.request("nobody.listening", "x", timeout: 5) }
      .to raise_error(Tether::NoRespondersError, /nobody.listening/)
    expect(Tether::Deadline.now - started).to be < 1
  end

  it "times out when the responder is too slow" do
    nc.subscribe("too.slow") { sleep 2 }
    nc.flush

    expect { nc.request("too.slow", "x", timeout: 0.2) }
      .to raise_error(Tether::TimeoutError, /too.slow/)
  end

  it "adds exactly one subscription for request/reply, however many requests follow" do
    serve("counted.service") { "ok" }
    expect { nc.request("counted.service", "x") }.to change(nc, :subscription_count).by(1)

    expect { 8.times.map { Thread.new { nc.request("counted.service", "x") } }.each(&:value) }
      .not_to change(nc, :subscription_count)
  end

  it "answers concurrent requests on one connection without crossing replies" do
    serve("mirror", &:data)

    replies = 32.times.map { |i| Thread.new { nc.request("mirror", "req-#{i}", timeout: 5).data } }.map(&:value)

    expect(replies.sort).to eq(32.times.map { "req-#{_1}" }.sort)
  end

  it "keeps working after a request times out and its reply arrives late" do
    nc.subscribe("late.reply") do |msg|
      Thread.new do
        sleep 0.4
        msg.respond("late")
      end
    end
    nc.flush

    expect { nc.request("late.reply", "x", timeout: 0.1) }.to raise_error(Tether::TimeoutError)
    sleep 0.6

    serve("still.alive") { "yes" }
    expect(nc.request("still.alive", "x").data).to eq("yes")
  end

  it "does not leak a waiter once a request settles" do
    serve("settled") { "ok" }
    4.times { nc.request("settled", "x") }
    begin
      nc.request("gone", "x", timeout: 0.1)
    rescue Tether::NoRespondersError, Tether::TimeoutError
      nil
    end

    expect(nc.send(:inbox).pending).to eq(0)
  end

  describe "reply subjects" do
    it "gives every request a distinct token under one shared prefix" do
      seen = Thread::Queue.new
      nc.subscribe("collect.replies") { |msg| seen.push(msg.reply_to) }
      nc.flush
      4.times do |_i|
        nc.request("collect.replies", "x", timeout: 0.2)
      rescue StandardError
        nil
      end

      tokens = 4.times.map { seen.pop(timeout: 2) }
      prefixes = tokens.map { _1[0..._1.rindex(".")] }

      expect(prefixes.uniq.size).to eq(1)
      expect(tokens.uniq.size).to eq(4)
    end

    it "builds a fresh inbox on demand for subscribe-then-publish patterns" do
      expect(nc.build_inbox_subject).to start_with("_INBOX.")
      expect(nc.build_inbox_subject).not_to eq(nc.build_inbox_subject)
    end
  end

  describe "Msg#respond" do
    it "refuses when there is no reply subject" do
      received = Thread::Queue.new
      nc.subscribe("no.reply.here") { |msg| received.push(msg) }
      nc.flush
      nc.publish("no.reply.here", "x")

      expect { received.pop(timeout: 2).respond("nope") }
        .to raise_error(Tether::Error, /no reply subject/)
    end
  end
end
