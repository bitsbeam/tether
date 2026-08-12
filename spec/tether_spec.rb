# frozen_string_literal: true

RSpec.describe Tether do
  let(:server) { SharedNatsServer.instance }

  describe "::VERSION" do
    it "holds a semantic version number" do
      expect(Tether::VERSION).to match(/\A\d+\.\d+\.\d+/)
    end
  end

  describe ".connect" do
    it "returns a connected client" do
      client = described_class.connect(server.uri)

      expect(client).to be_a(Tether::Client).and be_connected
    ensure
      client&.close
    end

    it "passes its options to the client" do
      client = described_class.connect(server.uri, name: "billing")

      expect(client.options.name).to eq("billing")
    ensure
      client&.close
    end

    it "yields the client, returns the block's value and closes the client afterwards" do
      yielded = nil
      value = described_class.connect(server.uri) do |client|
        yielded = client
        client.rtt.class
      end

      expect(value).to eq(Float)
      expect(yielded).to be_closed
    end

    it "raises ArgumentError naming an unknown option" do
      expect { described_class.connect(server.uri, nmae: "typo") }
        .to raise_error(ArgumentError, "unknown option: :nmae")
    end
  end
end
