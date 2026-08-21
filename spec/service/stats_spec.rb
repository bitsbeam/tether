# frozen_string_literal: true

RSpec.describe Tether::Service::Stats do
  let(:nc) { Tether.connect(SharedNatsServer.instance.uri) }
  let(:service) { nc.services.add(name: "calc", version: "1.2.0", metadata: { "team" => "core" }) }
  let(:stats) { nc.services.stats("calc", id: service.id).first }

  after { nc.close }

  describe "#type" do
    it "returns the STATS response type" do
      expect(stats.type).to eq("io.nats.micro.v1.stats_response")
    end
  end

  describe "#metadata" do
    it "returns the service metadata" do
      expect(stats.metadata).to eq("team" => "core")
    end
  end

  describe "#started" do
    it "returns when the service started as a UTC time" do
      expect(stats.started).to be_within(5).of(Time.now).and be_utc
    end
  end

  describe "#endpoints" do
    it "returns one entry per endpoint with its subject and queue group" do
      service.group("math", queue: false).endpoint("sqrt") { nil }
      entry = stats.endpoints.first

      expect([entry.name, entry.subject, entry.queue_group, entry.requests, entry.last_error])
        .to eq(["sqrt", "math.sqrt", nil, 0, nil])
    end

    it "returns the data the stats handler reported" do
      service = nc.services.add(name: "meter", version: "1.0.0", stats_handler: ->(_endpoint) { { "load" => 3 } })
      service.endpoint("a") { nil }

      expect(nc.services.stats("meter", id: service.id).first.endpoints.first.data).to eq("load" => 3)
    end
  end

  describe "#raw" do
    it "keeps the nanoseconds as integers" do
      service.endpoint("add", &:respond)
      nc.request("add")
      eventually { service.stats.endpoints.first.requests == 1 }

      expect(stats.raw["endpoints"].first.values_at("processing_time", "average_processing_time"))
        .to all(be_a(Integer).and(be_positive))
    end
  end
end
