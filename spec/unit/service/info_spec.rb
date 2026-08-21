# frozen_string_literal: true

RSpec.describe Tether::Service::Info do
  describe "#metadata" do
    it "returns an empty hash when another client sent null" do
      expect(described_class.new("name" => "calc", "metadata" => nil).metadata).to eq({})
    end
  end

  describe "#endpoints" do
    it "accepts an endpoint whose metadata another client sent as null" do
      entry = { "name" => "echo", "subject" => "svc.echo", "queue_group" => "q", "metadata" => nil }

      expect(described_class.new("endpoints" => [entry]).endpoints.first.metadata).to eq({})
    end

    it "returns an empty list when another client sent none" do
      expect(described_class.new("name" => "calc").endpoints).to eq([])
    end
  end

  describe "#raw" do
    it "keeps fields this client does not know" do
      expect(described_class.new("name" => "calc", "future" => { "x" => 1 }).raw["future"]).to eq("x" => 1)
    end

    it "leaves the hash it was given unfrozen" do
      given = { "name" => "calc", "endpoints" => [] }
      described_class.new(given)

      expect([given, given["endpoints"]]).to all(satisfy { |value| !value.frozen? })
    end
  end
end
