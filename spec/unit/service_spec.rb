# frozen_string_literal: true

RSpec.describe Tether::Service do
  describe ".control_subject" do
    it "builds the subject every service answers on" do
      expect(described_class.control_subject("PING")).to eq("$SRV.PING")
    end

    it "builds the subject every instance of one service answers on" do
      expect(described_class.control_subject(:info, "calc")).to eq("$SRV.INFO.calc")
    end

    it "builds the subject one instance answers on" do
      expect(described_class.control_subject("STATS", "calc", "abc")).to eq("$SRV.STATS.calc.abc")
    end

    it "uses another prefix as given" do
      expect(described_class.control_subject("PING", "calc", prefix: "Imported.srv")).to eq("Imported.srv.PING.calc")
    end

    it "raises on an unknown verb" do
      expect { described_class.control_subject("HELLO") }.to raise_error(ArgumentError, /unknown service verb/)
    end

    it "raises on an id without a name" do
      expect { described_class.control_subject("PING", nil, "abc") }.to raise_error(ArgumentError, /needs a name/)
    end
  end

  describe ".error_headers" do
    it "encodes the description and the decimal code byte for byte" do
      wire = described_class.error_headers(400, "empty request").to_wire

      expect(wire).to eq("NATS/1.0\r\nNats-Service-Error: empty request\r\nNats-Service-Error-Code: 400\r\n\r\n".b)
    end

    it "keeps further headers" do
      headers = described_class.error_headers(404, "no such user", { "Trace" => "t1" })

      expect(headers.to_h).to eq("Trace" => ["t1"], "Nats-Service-Error" => ["no such user"],
                                 "Nats-Service-Error-Code" => ["404"])
    end

    it "raises on a code that is not an integer" do
      expect { described_class.error_headers("400", "bad") }.to raise_error(ArgumentError, /must be an Integer/)
    end

    it "raises on an empty description" do
      expect { described_class.error_headers(400, "") }.to raise_error(ArgumentError, /description/)
    end

    it "raises on a description that spans lines" do
      expect { described_class.error_headers(400, "a\r\nb") }.to raise_error(ArgumentError, /description/)
    end
  end
end
