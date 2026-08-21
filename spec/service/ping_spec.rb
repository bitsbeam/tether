# frozen_string_literal: true

RSpec.describe Tether::Service::Ping do
  let(:nc) { Tether.connect(SharedNatsServer.instance.uri) }
  let(:service) { nc.services.add(name: "calc", version: "1.2.0", metadata: { "team" => "core" }) }
  let(:ping) { nc.services.ping("calc", id: service.id).first }

  after { nc.close }

  describe "#type" do
    it "returns the PING response type" do
      expect(ping.type).to eq("io.nats.micro.v1.ping_response")
    end
  end

  describe "#name" do
    it "returns the service name" do
      expect(ping.name).to eq("calc")
    end
  end

  describe "#id" do
    it "returns the id of the instance that answered" do
      expect(ping.id).to eq(service.id)
    end
  end

  describe "#version" do
    it "returns the service version" do
      expect(ping.version).to eq("1.2.0")
    end
  end

  describe "#metadata" do
    it "returns the frozen service metadata" do
      expect(ping.metadata).to eq("team" => "core").and be_frozen
    end
  end

  describe "#to_h" do
    it "returns exactly the keys of the PING schema" do
      expect(ping.to_h.keys).to eq(%w[name id version metadata type])
    end
  end
end
