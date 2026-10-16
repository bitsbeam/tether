# frozen_string_literal: true

RSpec.describe Tether::ObjectStore::Status do
  include ObjectStoreHelper

  def status_of(**options)
    make_store(**options).status
  end

  describe "#bucket" do
    it "returns the bucket's name" do
      expect(store.status.bucket).to eq(bucket)
    end
  end

  describe "#description" do
    it "returns the description the bucket was created with" do
      expect(status_of(description: "build artifacts").description).to eq("build artifacts")
    end

    it "returns nil for a bucket without one" do
      expect(store.status.description).to be_nil
    end
  end

  describe "#metadata" do
    it "returns the metadata the bucket was created with" do
      expect(status_of(metadata: { "team" => "ops" }).metadata).to include("team" => "ops")
    end

    it "returns an empty hash for a bucket without any" do
      expect(store.status.metadata.to_h.reject { |key, _| key.start_with?("_nats.") }).to eq({})
    end
  end

  describe "#ttl" do
    it "returns the seconds an object lives" do
      expect(status_of(ttl: 3600).ttl).to eq(3600)
    end

    it "returns nil when objects never expire" do
      expect(store.status.ttl).to be_nil
    end
  end

  describe "#storage" do
    it "returns :memory for a memory bucket" do
      expect(store.status.storage).to eq(:memory)
    end

    it "returns :file for a file bucket" do
      expect(status_of(storage: :file).storage).to eq(:file)
    end
  end

  describe "#replicas" do
    it "returns how many servers hold the bucket" do
      expect(store.status.replicas).to eq(1)
    end
  end

  describe "#max_bytes" do
    it "returns the limit the bucket was created with" do
      expect(status_of(max_bytes: 1_000_000).max_bytes).to eq(1_000_000)
    end

    it "returns nil for an unlimited bucket" do
      expect(store.status.max_bytes).to be_nil
    end
  end

  describe "#sealed?" do
    it "returns false for a bucket that takes writes" do
      expect(store.status.sealed?).to be(false)
    end

    it "returns true for a sealed bucket" do
      expect(store.seal.status.sealed?).to be(true)
    end
  end

  describe "#compressed?" do
    it "returns false for a bucket without compression" do
      expect(store.status.compressed?).to be(false)
    end

    it "returns true for a compressed bucket" do
      expect(status_of(storage: :file, compression: true).compressed?).to be(true)
    end
  end

  describe "#size" do
    it "returns zero for an empty bucket" do
      expect(store.status.size).to eq(0)
    end

    it "returns the bytes the bucket holds, chunks and metadata together" do
      store.put("object", binary(50_000))

      expect(store.status.size).to eq(backing_stream.info.state.bytes).and be > 50_000
    end
  end

  describe "#backing_store" do
    it "returns JetStream" do
      expect(store.status.backing_store).to eq("JetStream")
    end
  end

  describe "#stream" do
    it "returns the name of the stream behind the bucket" do
      expect(store.status.stream).to eq("OBJ_#{bucket}")
    end
  end

  describe "#stream_info" do
    it "returns the info of the stream behind the bucket" do
      store.put("object", "hello world", chunk_size: 4)

      expect(store.status.stream_info.state.messages).to eq(4)
    end
  end

  describe "#inspect" do
    it "shows the bucket, its size and whether it is sealed" do
      expect(store.status.inspect)
        .to eq(%(#<Tether::ObjectStore::Status bucket="#{bucket}" size=0 sealed=false>))
    end
  end
end
