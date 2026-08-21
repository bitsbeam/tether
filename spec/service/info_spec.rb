# frozen_string_literal: true

RSpec.describe Tether::Service::Info do
  let(:nc) { Tether.connect(SharedNatsServer.instance.uri) }
  let(:service) do
    nc.services.add(name: "calc", version: "1.2.0", description: "arithmetic", metadata: { "team" => "core" })
  end
  let(:info) { nc.services.info("calc", id: service.id).first }

  after { nc.close }

  describe "#type" do
    it "returns the INFO response type" do
      expect(info.type).to eq("io.nats.micro.v1.info_response")
    end
  end

  describe "#name" do
    it "returns the service name" do
      expect(info.name).to eq("calc")
    end
  end

  describe "#id" do
    it "returns the id of the instance that answered" do
      expect(info.id).to eq(service.id)
    end
  end

  describe "#version" do
    it "returns the service version" do
      expect(info.version).to eq("1.2.0")
    end
  end

  describe "#metadata" do
    it "returns the service metadata" do
      expect(info.metadata).to eq("team" => "core")
    end
  end

  describe "#description" do
    it "returns the service description" do
      expect(info.description).to eq("arithmetic")
    end
  end

  describe "#endpoints" do
    it "returns the endpoints in the order they were added" do
      service.endpoint("b") { nil }
      service.endpoint("a") { nil }

      expect(info.endpoints.map(&:name)).to eq(%w[b a])
    end

    it "returns an empty list for a service without endpoints" do
      expect(info.endpoints).to eq([])
    end
  end

  describe "#raw" do
    it "is frozen all the way down" do
      service.endpoint("a") { nil }

      expect(info).to be_frozen
      expect([info.raw, info.raw["endpoints"], info.raw["endpoints"].first, info.endpoints]).to all(be_frozen)
    end
  end

  describe "#==" do
    it "returns true for the same document read twice" do
      expect(info).to eq(service.info)
    end

    it "returns false for another instance" do
      other = nc.services.add(name: "calc", version: "1.2.0", description: "arithmetic")

      expect(info).not_to eq(other.info)
    end
  end
end
