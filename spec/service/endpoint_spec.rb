# frozen_string_literal: true

RSpec.describe Tether::Service::Endpoint do
  let(:nc) { Tether.connect(SharedNatsServer.instance.uri) }
  let(:service) { nc.services.add(name: "calc", version: "1.2.0") }

  after { nc.close }

  def settled(endpoint, requests: 1)
    eventually { (stats = endpoint.stats).requests == requests && stats }
  end

  describe "#name" do
    it "returns the name given" do
      expect(service.endpoint("add", subject: "calc.add") { nil }.name).to eq("add")
    end
  end

  describe "#subject" do
    it "returns the subject the endpoint is subscribed to" do
      expect(service.group("math").endpoint("sqrt") { nil }.subject).to eq("math.sqrt")
    end
  end

  describe "#queue_group" do
    it "returns q by default" do
      expect(service.endpoint("add") { nil }.queue_group).to eq("q")
    end

    it "returns nil for an endpoint without a queue group" do
      expect(service.endpoint("add", queue: false) { nil }.queue_group).to be_nil
    end
  end

  describe "#metadata" do
    it "returns a frozen copy with string keys and values" do
      endpoint = service.endpoint("add", metadata: { unit: :int }) { nil }

      expect(endpoint.metadata).to eq("unit" => "int").and be_frozen
    end
  end

  describe "#info" do
    it "returns the name, subject, queue group and metadata" do
      info = service.endpoint("add", subject: "calc.add", metadata: { "unit" => "int" }) { nil }.info

      expect([info.name, info.subject, info.queue_group, info.metadata])
        .to eq(["add", "calc.add", "q", { "unit" => "int" }])
    end
  end

  describe "#stats" do
    it "counts a request once its handler has returned" do
      endpoint = service.endpoint("add") { |request| request.respond("ok") }
      nc.request("add")
      stats = settled(endpoint)

      expect([stats.requests, stats.errors, stats.last_error]).to eq([1, 0, nil])
    end

    it "counts a request that got no response" do
      endpoint = service.endpoint("quiet") { nil }
      nc.publish("quiet")

      expect(settled(endpoint).errors).to eq(0)
    end

    it "adds up the time spent in the handler and its truncated average" do
      endpoint = service.endpoint("add") { |request| request.respond("ok") }
      3.times { nc.request("add") }
      stats = settled(endpoint, requests: 3)

      expect(stats.processing_time).to be_positive
      expect(stats.to_h["average_processing_time"]).to eq(stats.to_h["processing_time"] / 3)
      expect(stats.average_processing_time).to be_within(1e-9).of(stats.processing_time / 3)
    end

    it "counts an error response and records its code and description" do
      endpoint = service.endpoint("div") { |request| request.respond_error(400, "division by zero") }
      nc.request("div")
      stats = settled(endpoint)

      expect([stats.errors, stats.last_error]).to eq([1, "400:division by zero"])
    end

    it "answers a handler that raises with a 500 error response" do
      service.endpoint("boom") { raise "kaboom" }
      reply = nc.request("boom")

      expect([reply.headers["Nats-Service-Error"], reply.headers["Nats-Service-Error-Code"], reply.data])
        .to eq(["kaboom", "500", ""])
    end

    it "counts a handler that raises and records it as the last error" do
      endpoint = service.endpoint("boom") { raise "kaboom" }
      nc.request("boom")
      stats = settled(endpoint)

      expect([stats.requests, stats.errors, stats.last_error]).to eq([1, 1, "500:kaboom"])
    end

    it "keeps serving after a handler raised" do
      endpoint = service.endpoint("flaky") { |request| request.data == "bad" ? raise("kaboom") : request.respond("ok") }
      nc.request("flaky", "bad")

      expect(nc.request("flaky", "good").data).to eq("ok")
      expect(settled(endpoint, requests: 2).errors).to eq(1)
    end

    it "sends no second response when the handler raises after responding" do
      endpoint = service.endpoint("late") do |request|
        request.respond("done")
        raise "too late"
      end
      replies = nc.request_many("late", stall: 0.2)

      expect(replies.map(&:data)).to eq(["done"])
      expect(settled(endpoint).errors).to eq(1)
    end

    it "folds a multi-line exception message into one header line" do
      service.endpoint("boom") { raise "first\r\nsecond" }

      expect(nc.request("boom").headers["Nats-Service-Error"]).to eq("first second")
    end

    it "uses the exception class when the message is empty" do
      service.endpoint("boom") { raise ArgumentError, "" }

      expect(nc.request("boom").headers["Nats-Service-Error"]).to eq("ArgumentError")
    end

    it "reports a handler that raises to the connection's error handlers" do
      errors = Thread::Queue.new
      nc.on_error { |error| errors << error }
      service.endpoint("boom") { raise "kaboom" }
      nc.request("boom")

      expect(errors.pop(timeout: 2)).to be_a(Tether::HandlerError).and have_attributes(message: /kaboom/)
    end

    it "counts a handler that raises on a request without a reply subject" do
      endpoint = service.endpoint("boom") { raise "kaboom" }
      nc.publish("boom")

      expect(settled(endpoint).last_error).to eq("500:kaboom")
    end

    it "ignores a response sent after the handler returned" do
      requests = Thread::Queue.new
      endpoint = service.endpoint("deferred") { |request| requests << request }
      inbox = nc.new_inbox
      replies = nc.subscribe(inbox)
      nc.publish("deferred", reply_to: inbox)
      request = requests.pop(timeout: 2)
      settled(endpoint)
      request.respond_error(503, "too slow")

      expect(replies.next(timeout: 2).headers["Nats-Service-Error-Code"]).to eq("503")
      expect(endpoint.stats.errors).to eq(0)
    end

    it "keeps separate counters for endpoints with the same name" do
      first = service.endpoint("same", subject: "one", &:respond)
      second = service.endpoint("same", subject: "two", &:respond)
      nc.request("one")
      settled(first)

      expect(second.stats.requests).to eq(0)
    end
  end
end
