# frozen_string_literal: true

RSpec.describe Tether::Auth::Credentials do
  let(:text) do
    <<~CREDS
      -----BEGIN NATS USER JWT-----
      eyJ0eXAiOiJKV1QifQ.payload.signature
      ------END NATS USER JWT------

      ************************* IMPORTANT *************************
      NKEY Seed printed below can be used to sign and prove identity.

      -----BEGIN USER NKEY SEED-----
      SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY
      ------END USER NKEY SEED------
    CREDS
  end

  describe ".parse" do
    it "returns the jwt and the seed" do
      expect(described_class.parse(text))
        .to eq(["eyJ0eXAiOiJKV1QifQ.payload.signature", "SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY"])
    end

    it "reads a file with CRLF line endings" do
      expect(described_class.parse(text.gsub("\n", "\r\n")).first).to eq("eyJ0eXAiOiJKV1QifQ.payload.signature")
    end

    it "raises when the seed block is missing" do
      expect { described_class.parse(text.split("*").first) }
        .to raise_error(Tether::CredentialsError, /must hold a JWT and an NKEY seed/)
    end

    it "raises for nil" do
      expect { described_class.parse(nil) }.to raise_error(Tether::CredentialsError)
    end
  end
end
