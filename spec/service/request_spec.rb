# frozen_string_literal: true

require "json"

RSpec.describe Tether::Service::Request do
  let(:nc) { Tether.connect(SharedNatsServer.instance.uri) }
  let(:service) { nc.services.add(name: "calc", version: "1.2.0") }

  after { nc.close }

  def received(subject = "probe", payload = nil, headers: nil, reply: true)
    requests = Thread::Queue.new
    service.endpoint("probe", subject: subject) { |request| requests << request }
    nc.publish(subject, payload, reply_to: reply ? nc.new_inbox : nil, headers: headers)
    requests.pop(timeout: 2)
  end

  describe "#data" do
    it "returns the request payload" do
      expect(received("probe", "21").data).to eq("21")
    end
  end

  describe "#subject" do
    it "returns the subject the request was sent to, not the pattern served" do
      requests = Thread::Queue.new
      service.endpoint("probe", subject: "probe.*") { |request| requests << request }
      nc.publish("probe.one")

      expect(requests.pop(timeout: 2).subject).to eq("probe.one")
    end
  end

  describe "#headers" do
    it "returns the request headers" do
      expect(received(headers: { "Trace" => "t1" }).headers["Trace"]).to eq("t1")
    end

    it "returns nil for a request without headers" do
      expect(received.headers).to be_nil
    end
  end

  describe "#size" do
    it "counts header and payload bytes" do
      expect(received("probe", "12345", headers: { "A" => "b" }).size).to eq("NATS/1.0\r\nA: b\r\n\r\n".bytesize + 5)
    end
  end

  describe "#respond" do
    it "publishes the payload to the requester" do
      service.endpoint("echo") { |request| request.respond(request.data) }

      expect(nc.request("echo", "hello").data).to eq("hello")
    end

    it "publishes an empty response without a payload" do
      service.endpoint("ack", &:respond)
      reply = nc.request("ack")

      expect([reply.data, reply.headers]).to eq(["", nil])
    end

    it "sends the headers given" do
      service.endpoint("echo") { |request| request.respond("ok", headers: { "Trace" => "t1" }) }

      expect(nc.request("echo").headers["Trace"]).to eq("t1")
    end

    it "returns the request" do
      request = received

      expect(request.respond("ok")).to be(request)
    end

    it "raises when the request has no reply subject" do
      expect { received(reply: false).respond("ok") }.to raise_error(Tether::Service::Error, /no reply subject/)
    end
  end

  describe "#respond_json" do
    it "publishes the object encoded as JSON" do
      service.endpoint("sum") { |request| request.respond_json({ "sum" => request.data.split(",").sum(&:to_i) }) }

      expect(JSON.parse(nc.request("sum", "1,2,3").data)).to eq("sum" => 6)
    end

    it "sends the headers given" do
      service.endpoint("sum") { |request| request.respond_json([1], headers: { "Content-Type" => "application/json" }) }

      expect(nc.request("sum").headers["Content-Type"]).to eq("application/json")
    end
  end

  describe "#respond_error" do
    it "publishes the error headers with an empty payload" do
      service.endpoint("div") { |request| request.respond_error(400, "division by zero") }
      reply = nc.request("div")

      expect(reply.headers.to_h).to eq("Nats-Service-Error" => ["division by zero"],
                                       "Nats-Service-Error-Code" => ["400"])
      expect(reply.data).to eq("")
    end

    it "sends the data given as the payload" do
      service.endpoint("div") { |request| request.respond_error(400, "division by zero", "details") }

      expect(nc.request("div").data).to eq("details")
    end

    it "sends further headers along" do
      service.endpoint("div") { |request| request.respond_error(400, "bad", headers: { "Trace" => "t1" }) }
      headers = nc.request("div").headers

      expect([headers["Trace"], headers["Nats-Service-Error-Code"]]).to eq(%w[t1 400])
    end

    it "raises no exception on the requesting side" do
      service.endpoint("div") { |request| request.respond_error(500, "broken") }

      expect { nc.request("div") }.not_to raise_error
    end

    it "raises on a code that is not an integer" do
      expect { received.respond_error("400", "bad") }.to raise_error(ArgumentError, /must be an Integer/)
    end

    it "raises on an empty description" do
      expect { received.respond_error(400, "") }.to raise_error(ArgumentError, /description/)
    end

    it "raises when the request has no reply subject" do
      expect { received(reply: false).respond_error(400, "bad") }
        .to raise_error(Tether::Service::Error, /no reply subject/)
    end
  end

  describe "#responded?" do
    it "returns false before a response" do
      expect(received).not_to be_responded
    end

    it "returns true after a response" do
      expect(received.respond("ok")).to be_responded
    end

    it "returns true after an error response" do
      expect(received.respond_error(400, "bad")).to be_responded
    end
  end
end
