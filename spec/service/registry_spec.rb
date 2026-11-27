# frozen_string_literal: true

require "json"

RSpec.describe Tether::Service::Registry do
  let(:nc) { Tether.connect(SharedNatsServer.instance.uri) }
  let(:services) { nc.services }

  after { nc.close }

  def add(name = "calc", **options)
    services.add(name: name, version: "1.2.0", **options)
  end

  def ping_reply(service, subject)
    replies = nc.request_many(subject, stall: 0.2).map(&:data)
    replies.find { |data| JSON.parse(data)["id"] == service.id }
  end

  def expected_ping(service)
    [%({"name":"calc","id":"#{service.id}","version":"1.2.0","metadata":{"team":"core"},),
     %("type":"io.nats.micro.v1.ping_response"})].join
  end

  describe "#add" do
    it "returns a running service with the given identity" do
      service = services.add(name: "calc", version: "1.2.0-rc.1+build.5", description: "arithmetic")

      expect([service.name, service.version, service.description]).to eq(["calc", "1.2.0-rc.1+build.5", "arithmetic"])
      expect(service).not_to be_stopped
    end

    it "calls on_error when the server refuses a subscription of the service" do
      limited = Tether.connect(SharedNatsServer.users.uri, user: "limited", password: "wonderland")
      errors = Thread::Queue.new
      limited.services.add(name: "calc", version: "1.2.0", on_error: ->(error, _endpoint) { errors << error })

      expect(errors.pop(timeout: 2)).to be_a(Tether::PermissionsViolationError)
    ensure
      limited&.close
    end

    it "answers PING requests to every service with the exact reference document" do
      service = add(metadata: { "team" => "core" })

      expect(ping_reply(service, "$SRV.PING")).to eq(expected_ping(service))
    end

    it "answers PING requests to the service name with the exact reference document" do
      service = add(metadata: { "team" => "core" })

      expect(ping_reply(service, "$SRV.PING.calc")).to eq(expected_ping(service))
    end

    it "answers PING requests to the instance with the exact reference document" do
      service = add(metadata: { "team" => "core" })

      expect(nc.request("$SRV.PING.calc.#{service.id}").data).to eq(expected_ping(service))
    end

    it "answers every instance's discovery outside any queue group" do
      ids = Array.new(2) { add.id }

      expect(nc.request_many("$SRV.PING.calc", stall: 0.2).map { |reply| JSON.parse(reply.data)["id"] })
        .to match_array(ids)
    end

    it "answers discovery under another prefix" do
      service = nc.services(prefix: "Imported.srv").add(name: "calc", version: "1.2.0")

      expect(JSON.parse(nc.request("Imported.srv.PING.calc.#{service.id}").data)["id"]).to eq(service.id)
      expect { nc.request("$SRV.PING.calc.#{service.id}", timeout: 1) }.to raise_error(Tether::NoRespondersError)
    end

    it "raises on a name with characters outside letters, digits, dash and underscore" do
      expect { services.add(name: "my.service", version: "1.0.0") }
        .to raise_error(ArgumentError, /invalid service name/)
    end

    it "raises on an empty name" do
      expect { services.add(name: "", version: "1.0.0") }.to raise_error(ArgumentError, /invalid service name/)
    end

    it "raises on a version that is not a semantic version" do
      expect { services.add(name: "calc", version: "1.0") }.to raise_error(ArgumentError, /invalid service version/)
    end

    it "raises on a version with a leading zero" do
      expect { services.add(name: "calc", version: "01.0.0") }.to raise_error(ArgumentError, /invalid service version/)
    end

    it "raises on a version followed by a newline" do
      expect { services.add(name: "calc", version: "1.0.0\n") }.to raise_error(ArgumentError, /invalid service version/)
    end

    it "raises on metadata that is not a hash" do
      expect { add(metadata: "team=core") }.to raise_error(ArgumentError, /metadata must be a Hash/)
    end

    it "raises on a queue group with a space" do
      expect { add(queue: "my group") }.to raise_error(Tether::InvalidSubjectError)
    end

    it "raises on an unknown keyword" do
      expect { add(colour: "red") }.to raise_error(ArgumentError, /unknown keyword/)
    end

    it "subscribes to nothing when validation fails" do
      expect { services.add(name: "bad name", version: "1.0.0") }.to raise_error(ArgumentError)
      expect(services.ping("calc", stall: 0.1, timeout: 1)).to be_empty
    end

    it "raises when the connection is closed" do
      nc.close

      expect { add }.to raise_error(Tether::ConnectionClosedError)
    end
  end

  describe "#ping" do
    it "returns every instance of every service" do
      ids = [add("calc").id, add("echo").id]

      expect(services.ping(stall: 0.2).map(&:id)).to include(*ids)
    end

    it "returns the instances of the named service" do
      ids = Array.new(2) { add("calc").id }
      add("echo")

      expect(services.ping("calc", stall: 0.2).map(&:id)).to match_array(ids)
    end

    it "returns the one instance asked for without waiting for the timeout" do
      service = add(metadata: { "team" => "core" })
      add
      found = services.ping("calc", id: service.id, timeout: 30)

      expect(found.map { |ping| [ping.type, ping.name, ping.id, ping.version, ping.metadata] })
        .to eq([["io.nats.micro.v1.ping_response", "calc", service.id, "1.2.0", { "team" => "core" }]])
    end

    it "stops after max replies" do
      3.times { add }

      expect(services.ping("calc", max: 2, timeout: 30).size).to eq(2)
    end

    it "returns an empty list when no service answers" do
      expect(services.ping("nobody")).to eq([])
    end

    it "raises on an id without a name" do
      expect { services.ping(id: "abc") }.to raise_error(ArgumentError, /needs a name/)
    end

    it "raises when a reply is not JSON" do
      nc.subscribe("$SRV.PING.broken") { |msg| msg.respond("not json") }

      expect { services.ping("broken", max: 1) }.to raise_error(Tether::Service::Error, /not JSON/)
    end

    it "finds services under another prefix only" do
      service = nc.services(prefix: "Imported.srv").add(name: "calc", version: "1.2.0")

      expect(nc.services(prefix: "Imported.srv").ping("calc", max: 1).map(&:id)).to eq([service.id])
      expect(services.ping("calc")).to eq([])
    end
  end

  describe "#info" do
    it "returns the description and endpoints of each instance" do
      service = add(description: "arithmetic")
      service.group("math", queue: "math-q").endpoint("sqrt", metadata: { "unit" => "int" }) { nil }
      info = services.info("calc", max: 1).first
      endpoint = info.endpoints.first

      expect([info.type, info.id, info.description]).to eq(["io.nats.micro.v1.info_response", service.id, "arithmetic"])
      expect([endpoint.name, endpoint.subject, endpoint.queue_group, endpoint.metadata])
        .to eq(["sqrt", "math.sqrt", "math-q", { "unit" => "int" }])
    end

    it "returns every instance of every service" do
      ids = [add("calc").id, add("echo").id]

      expect(services.info(stall: 0.2).map(&:id)).to include(*ids)
    end

    it "returns the one instance asked for" do
      service = add
      add

      expect(services.info("calc", id: service.id, timeout: 30)).to eq([service.info])
    end

    it "returns an empty list when no service answers" do
      expect(services.info("nobody")).to eq([])
    end
  end

  describe "#stats" do
    it "returns the counters of each instance" do
      service = add
      service.endpoint("bad") { |request| request.respond_error(400, "empty request") }
      nc.request("bad")
      eventually { service.stats.endpoints.first.requests == 1 }
      entry = services.stats("calc", max: 1).first.endpoints.first

      expect([entry.name, entry.subject, entry.queue_group, entry.requests, entry.errors, entry.last_error])
        .to eq(["bad", "bad", "q", 1, 1, "400:empty request"])
    end

    it "returns every instance of every service" do
      ids = [add("calc").id, add("echo").id]

      expect(services.stats(stall: 0.2).map(&:id)).to include(*ids)
    end

    it "returns the one instance asked for" do
      service = add
      add

      expect(services.stats("calc", id: service.id, timeout: 30)).to eq([service.stats])
    end

    it "returns an empty list when no service answers" do
      expect(services.stats("nobody")).to eq([])
    end
  end

  describe "#prefix" do
    it "defaults to $SRV" do
      expect(services.prefix).to eq("$SRV")
    end

    it "returns the prefix given, with its case kept" do
      expect(nc.services(prefix: "Imported.srv").prefix).to eq("Imported.srv")
    end
  end
end
