# frozen_string_literal: true

RSpec.describe Tether::Auth::NKey do
  subject(:nkey) { described_class.new(seed) }

  let(:seed) { SharedNatsServer::NKEY_SEED }
  let(:public_key) { SharedNatsServer::NKEY_PUBLIC }

  def raw_public_key
    described_class.decode(public_key).byteslice(1, 32)
  end

  describe "#initialize" do
    it "accepts a seed surrounded by whitespace" do
      expect(described_class.new("  #{seed}\n").public_key).to eq(public_key)
    end

    it "raises for a public key given in place of a seed" do
      expect { described_class.new(public_key) }.to raise_error(Tether::CredentialsError, "not an NKEY seed")
    end

    it "raises for a seed with a corrupted character" do
      expect { described_class.new(seed.sub("SUAC", "SUAD")) }
        .to raise_error(Tether::CredentialsError, "NKEY checksum mismatch")
    end

    it "raises for characters outside base32" do
      expect { described_class.new("not-a-seed!") }.to raise_error(Tether::CredentialsError, /invalid character/)
    end
  end

  describe "#public_key" do
    it "derives the user key that belongs to the seed" do
      expect(nkey.public_key).to eq(public_key)
    end
  end

  describe "#sign" do
    it "produces an Ed25519 signature the public key verifies" do
      signature = nkey.sign("nonce").tr("-_", "+/").unpack1("m")
      key = OpenSSL::PKey.new_raw_public_key("ED25519", raw_public_key)

      expect(key.verify(nil, signature, "nonce")).to be(true)
    end

    it "encodes the signature as base64url without padding" do
      expect(nkey.sign("nonce")).to match(/\A[A-Za-z0-9_-]{86}\z/)
    end
  end

  describe ".encode" do
    it "produces text that decodes back to the same bytes" do
      bytes = "\x01\x02\x03\xFF".b

      expect(described_class.decode(described_class.encode(bytes))).to eq(bytes)
    end
  end

  describe ".decode" do
    it "returns the bytes before the checksum" do
      expect(described_class.decode(public_key).bytesize).to eq(33)
    end

    it "raises when the checksum does not match" do
      expect { described_class.decode(public_key.sub("UDXU", "UDXV")) }
        .to raise_error(Tether::CredentialsError, "NKEY checksum mismatch")
    end
  end

  describe ".crc16" do
    it "computes the XMODEM variant" do
      expect(described_class.crc16("123456789")).to eq(0x31C3)
    end
  end
end
