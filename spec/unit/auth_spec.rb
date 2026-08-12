# frozen_string_literal: true

require "tempfile"

RSpec.describe Tether::Auth do
  let(:seed) { SharedNatsServer::NKEY_SEED }
  let(:public_key) { SharedNatsServer::NKEY_PUBLIC }
  let(:plain) { URI.parse("nats://127.0.0.1:4222") }

  def fields(uri = plain, nonce = "nonce", **options)
    described_class.new(Tether::Options.new(**options)).fields(uri, nonce)
  end

  def credentials_file(text)
    Tempfile.create("tether-creds").tap do |file|
      file.write(text)
      file.close
    end
  end

  describe "#fields" do
    it "returns nothing when no credentials were given" do
      expect(fields).to eq({})
    end

    it "returns the token" do
      expect(fields(token: "s3cr3t")).to eq(auth_token: "s3cr3t")
    end

    it "calls a token callable on every connect" do
      tokens = %w[first second]
      auth = described_class.new(Tether::Options.new(token: -> { tokens.shift }))

      expect([auth.fields(plain, nil), auth.fields(plain, nil)])
        .to eq([{ auth_token: "first" }, { auth_token: "second" }])
    end

    it "returns the user and password" do
      expect(fields(user: "alice", password: "wonderland")).to eq(user: "alice", pass: "wonderland")
    end

    it "sends an empty password when only a user was given" do
      expect(fields(user: "alice")).to eq(user: "alice", pass: "")
    end

    it "prefers the user and password of the server's URL" do
      uri = URI.parse("nats://bob:p%40ss@127.0.0.1:4222")

      expect(fields(uri, "nonce", token: "ignored")).to eq(user: "bob", pass: "p@ss")
    end

    it "reads URL userinfo without a password as a token" do
      expect(fields(URI.parse("nats://s3cr3t@127.0.0.1:4222"))).to eq(auth_token: "s3cr3t")
    end

    it "signs the nonce with an NKEY seed" do
      result = fields(nkey_seed: seed)

      expect(result).to eq(nkey: public_key, sig: Tether::Auth::NKey.new(seed).sign("nonce"))
    end

    it "reads the seed from a file on every connect" do
      file = credentials_file("#{seed}\n")

      expect(fields(nkey_seed_file: file.path)).to include(nkey: public_key)
    ensure
      File.unlink(file.path)
    end

    it "omits the signature when the server sent no nonce" do
      expect(fields(plain, nil, nkey_seed: seed)).to eq(nkey: public_key)
    end

    it "sends a bearer jwt without a signature" do
      expect(fields(jwt: "header.payload.sig")).to eq(jwt: "header.payload.sig")
    end

    it "signs the nonce for a jwt given with its seed" do
      expect(fields(jwt: "the.jwt", nkey_seed: seed)).to include(jwt: "the.jwt", sig: a_string_matching(/\A[\w-]+\z/))
    end

    it "reads the jwt and seed from credentials text" do
      text = "-----BEGIN NATS USER JWT-----\nthe.jwt\n------END NATS USER JWT------\n\n" \
             "-----BEGIN USER NKEY SEED-----\n#{seed}\n------END USER NKEY SEED------\n"

      expect(fields(credentials: text)).to include(jwt: "the.jwt", sig: a_kind_of(String))
    end

    it "raises CredentialsError for a credentials file that does not exist" do
      expect { fields(credentials_file: "/nonexistent/user.creds") }
        .to raise_error(Tether::CredentialsError, /could not read/)
    end

    it "returns whatever the auth callable builds from the nonce" do
      expect(fields(auth: ->(nonce) { { "jwt" => "j", "sig" => nonce.upcase } })).to eq(jwt: "j", sig: "NONCE")
    end
  end
end
