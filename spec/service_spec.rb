# frozen_string_literal: true

require "json"

RSpec.describe Tether::Service do
  let(:nc) { Tether.connect(SharedNatsServer.instance.uri) }

  after { nc.close }

  def add(**options)
    nc.services.add(name: "calc", version: "1.2.0", **options)
  end

  def document(subject)
    JSON.parse(nc.request(subject).data)
  end

  describe "#id" do
    it "returns a 22 character identifier" do
      expect(add.id).to match(/\A[A-Za-z0-9]{22}\z/)
    end

    it "differs between two instances of the same service" do
      expect(add.id).not_to eq(add.id)
    end
  end

  describe "#metadata" do
    it "returns a frozen copy with string keys and values" do
      given = { region: "eu", shard: 2 }
      service = add(metadata: given)

      expect(service.metadata).to eq("region" => "eu", "shard" => "2").and be_frozen
      expect(given).to eq(region: "eu", shard: 2)
    end
  end

  describe "#description" do
    it "returns an empty string when none was given" do
      expect(add.description).to eq("")
    end
  end

  describe "#endpoint" do
    it "serves requests on a subject named after the endpoint" do
      add.endpoint("double") { |request| request.respond((request.data.to_i * 2).to_s) }

      expect(nc.request("double", "21").data).to eq("42")
    end

    it "serves requests on the subject given" do
      endpoint = add.endpoint("div", subject: "calc.div") { |request| request.respond("ok") }

      expect(nc.request("calc.div").data).to eq("ok")
      expect(endpoint.subject).to eq("calc.div")
    end

    it "joins the queue group q by default, so one instance answers each request" do
      2.times { add.endpoint("one") { |request| request.respond("ok") } }

      expect(nc.request_many("one", stall: 0.2).size).to eq(1)
    end

    it "joins the service's queue group" do
      endpoint = add(queue: "workers").endpoint("one", &:respond)

      expect(endpoint.queue_group).to eq("workers")
    end

    it "joins the queue group given" do
      endpoint = add(queue: "workers").endpoint("one", queue: "special", &:respond)

      expect(endpoint.queue_group).to eq("special")
    end

    it "delivers every request to every instance when the queue is false" do
      2.times { add.endpoint("all", queue: false) { |request| request.respond("ok") } }

      expect(nc.request_many("all", stall: 0.2).size).to eq(2)
    end

    it "inherits no queue group from a service that has none" do
      endpoint = add(queue: false).endpoint("all", &:respond)

      expect(endpoint.queue_group).to be_nil
    end

    it "keeps serving the other endpoints of the service" do
      service = add
      service.endpoint("a") { |request| request.respond("from a") }
      service.endpoint("b") { |request| request.respond("from b") }

      expect([nc.request("a").data, nc.request("b").data]).to eq(["from a", "from b"])
    end

    it "raises on a name with characters outside letters, digits, dash and underscore" do
      expect { add.endpoint("not.valid") { nil } }.to raise_error(ArgumentError, /invalid endpoint name/)
    end

    it "raises on a subject with a space" do
      expect { add.endpoint("a", subject: "has space") { nil } }.to raise_error(Tether::InvalidSubjectError)
    end

    it "raises on a subject with > before the end" do
      expect { add.endpoint("a", subject: "a.>.b") { nil } }.to raise_error(Tether::InvalidSubjectError)
    end

    it "raises on a subject under the discovery prefix" do
      expect { add.endpoint("a", subject: "$SRV.mine") { nil } }.to raise_error(Tether::InvalidSubjectError, /reserved/)
    end

    it "raises on a queue group with a space" do
      expect { add.endpoint("a", queue: "my group") { nil } }.to raise_error(Tether::InvalidSubjectError)
    end

    it "raises without a block" do
      expect { add.endpoint("a") }.to raise_error(ArgumentError, /needs a block/)
    end

    it "raises once the service is stopped" do
      service = add.stop

      expect { service.endpoint("late") { nil } }.to raise_error(Tether::Service::Error, /stopped/)
    end
  end

  describe "#group" do
    it "prefixes the subject of its endpoints" do
      add.group("math").endpoint("sqrt") { |request| request.respond(Integer.sqrt(request.data.to_i).to_s) }

      expect(nc.request("math.sqrt", "49").data).to eq("7")
    end

    it "passes its queue group to its endpoints" do
      endpoint = add.group("math", queue: "math-q").endpoint("sqrt", &:respond)

      expect(endpoint.queue_group).to eq("math-q")
    end

    it "raises on a name with >" do
      expect { add.group("math.>") }.to raise_error(Tether::InvalidSubjectError, /group name/)
    end
  end

  describe "#endpoints" do
    it "lists the endpoints in the order they were added, groups included" do
      service = add
      service.endpoint("a") { nil }
      service.group("g").endpoint("b") { nil }

      expect(service.endpoints.map(&:subject)).to eq(%w[a g.b])
    end

    it "returns a copy" do
      service = add
      service.endpoints << :other

      expect(service.endpoints).to be_empty
    end
  end

  describe "#info" do
    it "describes the service and the subject and queue group each endpoint uses" do
      service = add(description: "arithmetic", metadata: { "team" => "core" })
      service.endpoint("add", metadata: { "unit" => "int" }) { nil }
      service.group("math", queue: false).endpoint("sqrt", subject: "root") { nil }

      expect(service.info.to_h).to eq(
        "name" => "calc", "id" => service.id, "version" => "1.2.0", "metadata" => { "team" => "core" },
        "type" => "io.nats.micro.v1.info_response", "description" => "arithmetic",
        "endpoints" => [
          { "name" => "add", "subject" => "add", "queue_group" => "q", "metadata" => { "unit" => "int" } },
          { "name" => "sqrt", "subject" => "math.root", "queue_group" => "", "metadata" => {} }
        ]
      )
    end

    it "answers INFO requests to every service with the same document" do
      service = add(description: "arithmetic")
      service.endpoint("add") { nil }
      replies = nc.request_many("$SRV.INFO", stall: 0.2).map { |reply| JSON.parse(reply.data) }

      expect(replies).to include(service.info.to_h)
    end

    it "answers INFO requests to the service name with the same document" do
      service = add(description: "arithmetic")
      service.endpoint("add") { nil }

      expect(document("$SRV.INFO.calc")).to eq(service.info.to_h)
    end

    it "answers INFO requests to the instance with the keys in the reference order" do
      service = add(description: "arithmetic")
      service.endpoint("add") { nil }

      expect(nc.request("$SRV.INFO.calc.#{service.id}").data).to eq(
        [%({"name":"calc","id":"#{service.id}","version":"1.2.0","metadata":{},),
         %("type":"io.nats.micro.v1.info_response","description":"arithmetic",),
         %("endpoints":[{"name":"add","subject":"add","queue_group":"q","metadata":{}}]})].join
      )
    end

    it "includes an endpoint added after the service started" do
      service = add
      before = document("$SRV.INFO.calc.#{service.id}")
      service.endpoint("late") { nil }

      expect(before["endpoints"]).to eq([])
      expect(document("$SRV.INFO.calc.#{service.id}")["endpoints"].map { |entry| entry["name"] }).to eq(["late"])
    end
  end

  describe "#stats" do
    it "reports zeroed counters and an empty last error before any request" do
      service = add
      service.endpoint("add") { nil }

      expect(service.stats.to_h).to eq(
        "name" => "calc", "id" => service.id, "version" => "1.2.0", "metadata" => {},
        "type" => "io.nats.micro.v1.stats_response", "started" => service.stats.to_h["started"],
        "endpoints" => [{ "name" => "add", "subject" => "add", "queue_group" => "q", "num_requests" => 0,
                          "num_errors" => 0, "last_error" => "", "processing_time" => 0,
                          "average_processing_time" => 0 }]
      )
    end

    it "reports when the service started as an RFC 3339 UTC timestamp" do
      started = add.stats

      expect(started.to_h["started"]).to match(/\A\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{9}Z\z/)
      expect(started.started).to be_within(5).of(Time.now)
    end

    it "counts requests and error responses per endpoint" do
      service = add
      service.endpoint("ok", &:respond)
      service.endpoint("bad") { |request| request.respond_error(400, "empty request") }
      2.times { nc.request("ok") }
      nc.request("bad")
      stats = eventually { service.stats.endpoints.sum(&:requests) == 3 && service.stats }

      expect(stats.endpoints.map { |entry| [entry.name, entry.requests, entry.errors, entry.last_error] })
        .to eq([["ok", 2, 0, nil], ["bad", 1, 1, "400:empty request"]])
    end

    it "answers STATS requests to every service" do
      service = add
      replies = nc.request_many("$SRV.STATS", stall: 0.2).map { |reply| JSON.parse(reply.data) }

      expect(replies).to include(service.stats.to_h)
    end

    it "answers STATS requests to the service name" do
      service = add

      expect(document("$SRV.STATS.calc")).to eq(service.stats.to_h)
    end

    it "answers STATS requests to the instance with integer nanoseconds in the reference key order" do
      service = add
      service.endpoint("bad") { |request| request.respond_error(400, "empty request") }
      nc.request("bad")
      stats = eventually { service.stats.endpoints.first.requests == 1 && service.stats.to_h }
      entry = stats["endpoints"].first

      expect(nc.request("$SRV.STATS.calc.#{service.id}").data).to eq(
        [%({"name":"calc","id":"#{service.id}","version":"1.2.0","metadata":{},),
         %("type":"io.nats.micro.v1.stats_response","started":"#{stats["started"]}",),
         %("endpoints":[{"name":"bad","subject":"bad","queue_group":"q","num_requests":1,"num_errors":1,),
         %("last_error":"400:empty request","processing_time":#{entry["processing_time"]},),
         %("average_processing_time":#{entry["average_processing_time"]}}]})].join
      )
      expect(entry["processing_time"]).to be_a(Integer).and be_positive
    end

    it "embeds what the stats handler returns as the endpoint's data" do
      service = add(stats_handler: ->(endpoint) { { "endpoint" => endpoint.name, "load" => 3 } })
      service.endpoint("add") { nil }

      expect(document("$SRV.STATS.calc.#{service.id}")["endpoints"].first["data"])
        .to eq("endpoint" => "add", "load" => 3)
    end

    it "leaves the data out when the stats handler raises" do
      service = add(stats_handler: ->(_endpoint) { raise "no data" })
      service.endpoint("add") { nil }

      expect(document("$SRV.STATS.calc.#{service.id}")["endpoints"].first).not_to have_key("data")
    end
  end

  describe "#reset_stats" do
    it "zeroes the counters and the last error" do
      service = add
      service.endpoint("bad") { |request| request.respond_error(400, "empty request") }
      nc.request("bad")
      eventually { service.stats.endpoints.first.errors == 1 }
      entry = service.reset_stats.stats.endpoints.first

      expect([entry.requests, entry.errors, entry.last_error, entry.processing_time]).to eq([0, 0, nil, 0.0])
    end

    it "restarts the window the counters cover" do
      service = add
      before = service.stats.started

      expect(eventually { service.reset_stats.stats.started > before }).to be(true)
    end
  end

  describe "#on_error" do
    let(:limited) { Tether.connect(SharedNatsServer.users.uri, user: "limited", password: "wonderland") }

    after { limited.close }

    def refused_service(errors)
      service = limited.services.add(name: "calc", version: "1.2.0").on_error { |*refusal| errors << refusal }
      service.endpoint("sum", subject: "denied.sum") { |request| request.respond("0") }
      service
    end

    def endpoint_refusal(errors)
      eventually { (refusal = errors.pop(timeout: 2)) && refusal.last && refusal }
    end

    it "calls the block with the endpoint whose subscription the server refuses" do
      errors = Thread::Queue.new
      service = refused_service(errors)

      expect(endpoint_refusal(errors))
        .to match([a_kind_of(Tether::PermissionsViolationError), service.endpoints.first])
    end

    it "calls the block without an endpoint when a discovery subscription is refused" do
      errors = Thread::Queue.new
      refused_service(errors)

      expect(errors.pop(timeout: 2)).to match([an_object_having_attributes(subscription_subject: /\A\$SRV\./), nil])
    end

    it "counts the refusal as an error of the endpoint" do
      service = refused_service(Thread::Queue.new)

      stats = eventually { service.stats.endpoints.first.errors == 1 && service.stats.endpoints.first }
      expect(stats.last_error).to match(/Permissions Violation for Subscription to "denied.sum"/)
    end

    it "keeps the service running" do
      errors = Thread::Queue.new
      service = refused_service(errors)
      endpoint_refusal(errors)

      expect(service).not_to be_stopped
    end
  end

  describe "#on_stop" do
    it "calls the block once with nil when the service stops" do
      calls = Thread::Queue.new
      service = add.on_stop { |error| calls << [:stopped, error] }
      2.times { service.stop }

      expect(calls.pop(timeout: 2)).to eq([:stopped, nil])
      expect(calls).to be_empty
    end

    it "calls the block with the error given to stop" do
      calls = Thread::Queue.new
      failure = RuntimeError.new("database is gone")
      add.on_stop { |error| calls << error }.stop(failure)

      expect(calls.pop(timeout: 2)).to be(failure)
    end

    it "calls the block at once when the service has already stopped" do
      calls = Thread::Queue.new
      add.stop.on_stop { |error| calls << [:stopped, error] }

      expect(calls.pop(timeout: 0)).to eq([:stopped, nil])
    end

    it "calls every registered block, the one given when adding included" do
      calls = Thread::Queue.new
      service = add(on_stop: ->(_error) { calls << :first })
      service.on_stop { calls << :second }
      service.stop

      expect([calls.pop(timeout: 2), calls.pop(timeout: 2)]).to eq(%i[first second])
    end

    it "calls the block when the connection closes" do
      calls = Thread::Queue.new
      service = add.on_stop { |error| calls << [:stopped, error] }
      nc.close

      expect(calls.pop(timeout: 2)).to eq([:stopped, nil])
      expect(service).to be_stopped
    end

    it "calls only the blocks of the service that stopped" do
      calls = Thread::Queue.new
      other = add.on_stop { calls << :other }
      add.on_stop { calls << :stopped }.stop

      expect(calls.pop(timeout: 2)).to eq(:stopped)
      expect(other).not_to be_stopped
    end
  end

  describe "#stop" do
    it "stops serving the endpoints" do
      service = add
      service.endpoint("add") { |request| request.respond("ok") }
      service.stop

      expect { nc.request("add", timeout: 1) }.to raise_error(Tether::NoRespondersError)
    end

    it "stops answering discovery" do
      service = add.stop

      expect { nc.request("$SRV.PING.calc.#{service.id}", timeout: 1) }.to raise_error(Tether::NoRespondersError)
    end

    it "answers the requests already received before it returns" do
      gate = Thread::Queue.new
      replies = Thread::Queue.new
      service = add
      service.endpoint("slow") do |request|
        gate.pop(timeout: 2)
        service.stop
        request.respond(request.data)
      end
      inbox = nc.new_inbox
      nc.subscribe(inbox) { |msg| replies << msg.data }
      %w[1 2].each { |payload| nc.publish("slow", payload, reply_to: inbox) }
      nc.flush
      gate.close

      expect([replies.pop(timeout: 2), replies.pop(timeout: 2)]).to eq(%w[1 2])
      expect(eventually { service.stopped? }).to be(true)
    end

    it "raises when a handler is still busy at the deadline and stops anyway" do
      gate = Thread::Queue.new
      entered = Thread::Queue.new
      service = add
      service.endpoint("stuck") do
        entered << true
        gate.pop(timeout: 5)
      end
      nc.publish("stuck")
      entered.pop(timeout: 2)

      expect { service.stop(timeout: 0.05) }.to raise_error(Tether::TimeoutError)
      expect(service).to be_stopped
      gate.close
    end

    it "removes the endpoints" do
      service = add
      service.endpoint("add") { nil }

      expect(service.stop.endpoints).to be_empty
    end

    it "leaves another service on the same connection running" do
      other = nc.services.add(name: "other", version: "1.0.0")
      other.endpoint("still") { |request| request.respond("here") }
      add.stop

      expect(nc.request("still").data).to eq("here")
    end

    it "returns the service when called again" do
      service = add.stop

      expect(service.stop).to be(service)
    end
  end

  describe "#stopped?" do
    it "returns false while the service runs" do
      expect(add).not_to be_stopped
    end

    it "returns true after stop" do
      expect(add.stop).to be_stopped
    end
  end
end
