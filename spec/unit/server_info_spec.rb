# frozen_string_literal: true

RSpec.describe Tether::ServerInfo do
  subject(:info) { described_class.new(document) }

  let(:document) do
    { "server_id" => "NABC", "server_name" => "n1", "version" => "2.15.3", "host" => "0.0.0.0", "port" => 4222,
      "max_payload" => 1_048_576, "proto" => 1, "client_id" => 9, "client_ip" => "10.0.0.2", "cluster" => "east",
      "domain" => "hub", "nonce" => "abc", "acc" => "APP", "headers" => true, "jetstream" => true,
      "auth_required" => true, "tls_available" => true, "api_lvl" => 3,
      "connect_urls" => ["10.0.0.3:4222", "10.0.0.4:4222"] }
  end

  describe "#initialize" do
    it "freezes itself and the document" do
      expect(info).to be_frozen
      expect(info.raw).to be_frozen
    end
  end

  describe "#id" do
    it "returns server_id" do
      expect(info.id).to eq("NABC")
    end
  end

  describe "#name" do
    it "returns server_name" do
      expect(info.name).to eq("n1")
    end
  end

  describe "#max_payload" do
    it "returns the limit the server announced" do
      expect(info.max_payload).to eq(1_048_576)
    end
  end

  describe "#account" do
    it "returns the acc field" do
      expect(info.account).to eq("APP")
    end
  end

  describe "#headers?" do
    it "returns true when the server supports headers" do
      expect(info).to be_headers
    end

    it "returns false when the field is absent" do
      expect(described_class.new({})).not_to be_headers
    end
  end

  describe "#jetstream?" do
    it "returns true when JetStream is enabled" do
      expect(info).to be_jetstream
    end

    it "returns false when the field is absent" do
      expect(described_class.new({})).not_to be_jetstream
    end
  end

  describe "#auth_required?" do
    it "returns true when the server asks for credentials" do
      expect(info).to be_auth_required
    end
  end

  describe "#tls_required?" do
    it "returns false when TLS is only available" do
      expect(info).not_to be_tls_required
    end
  end

  describe "#tls_available?" do
    it "returns true when the server offers TLS" do
      expect(info).to be_tls_available
    end

    it "returns true when the server requires TLS" do
      expect(described_class.new("tls_required" => true)).to be_tls_available
    end

    it "returns false when the server says nothing about TLS" do
      expect(described_class.new({})).not_to be_tls_available
    end
  end

  describe "#lame_duck?" do
    it "returns true when the server is shutting down" do
      expect(described_class.new("ldm" => true)).to be_lame_duck
    end

    it "returns false otherwise" do
      expect(info).not_to be_lame_duck
    end
  end

  describe "#api_level" do
    it "returns the level the server reports" do
      expect(info.api_level).to eq(3)
    end

    it "returns zero when the server does not say" do
      expect(described_class.new({}).api_level).to eq(0)
    end
  end

  describe "#connect_urls" do
    it "returns the other servers of the cluster" do
      expect(info.connect_urls).to eq(["10.0.0.3:4222", "10.0.0.4:4222"])
    end

    it "returns an empty array when none are advertised" do
      expect(described_class.new({}).connect_urls).to eq([])
    end
  end

  describe "#[]" do
    it "returns a raw value by string key" do
      expect(info["cluster"]).to eq("east")
    end

    it "returns a raw value by symbol key" do
      expect(info[:cluster]).to eq("east")
    end
  end

  describe "#to_h" do
    it "returns the document as the server sent it" do
      expect(info.to_h).to eq(document)
    end
  end

  describe "#inspect" do
    it "shows the identity of the server without the whole document" do
      expect(info.inspect).to eq('#<Tether::ServerInfo name="n1" version="2.15.3" host="0.0.0.0" port=4222>')
    end
  end
end
