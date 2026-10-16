# frozen_string_literal: true

RSpec.describe Tether::ObjectStore::Info do
  include ObjectStoreHelper

  let(:info) do
    store.put("greeting.txt", "hello world", chunk_size: 4, description: "a greeting",
                                             headers: { "Content-Type" => "text/plain", "Tag" => %w[a b] },
                                             metadata: { "owner" => "ops" })
    store.info("greeting.txt")
  end

  let(:tombstone) do
    store.put("gone", "x")
    store.delete("gone")
    store.info("gone", show_deleted: true)
  end

  let(:link) { store.add_link("alias", info) }

  describe "#name" do
    it "returns the object's name" do
      expect(info.name).to eq("greeting.txt")
    end
  end

  describe "#bucket" do
    it "returns the bucket that holds the entry" do
      expect(info.bucket).to eq(bucket)
    end
  end

  describe "#nuid" do
    it "returns the id the object's chunks are stored under" do
      subject = chunk_subject(info.nuid)

      expect(stored_subjects).to include(subject => 3)
    end
  end

  describe "#size" do
    it "returns the content's size in bytes" do
      expect(info.size).to eq(11)
    end

    it "returns zero for a deleted object" do
      expect(tombstone.size).to eq(0)
    end
  end

  describe "#chunks" do
    it "returns how many chunk messages hold the content" do
      expect(info.chunks).to eq(3)
    end
  end

  describe "#chunk_size" do
    it "returns the chunk size the upload used" do
      expect(info.chunk_size).to eq(4)
    end

    it "returns 128 KiB for an upload with the default" do
      expect(store.put("object", "x").chunk_size).to eq(131_072)
    end

    it "returns nil for a link" do
      expect(link.chunk_size).to be_nil
    end
  end

  describe "#digest" do
    it "returns SHA-256= and the URL-safe padded base64 of the content's hash" do
      expect(info.digest).to eq("SHA-256=uU0nuZNNPgilLlLX2n2r-sSE7-N6U4DukIj3rOLvzek=")
    end

    it "returns the digest of nothing for an empty object" do
      expect(store.put("empty", "").digest).to eq(digest_of(""))
    end

    it "returns nil for a deleted object" do
      expect(tombstone.digest).to be_nil
    end

    it "returns nil for a link" do
      expect(link.digest).to be_nil
    end
  end

  describe "#digest_bytes" do
    it "returns the raw SHA-256 of the content" do
      expect(info.digest_bytes).to eq(Digest::SHA256.digest("hello world"))
    end

    it "returns nil when no digest is recorded" do
      expect(tombstone.digest_bytes).to be_nil
    end
  end

  describe "#description" do
    it "returns the description stored with the object" do
      expect(info.description).to eq("a greeting")
    end

    it "returns nil for an object without one" do
      expect(store.put("object", "x").description).to be_nil
    end
  end

  describe "#headers" do
    it "returns the headers stored with the object, each with all its values" do
      expect(info.headers).to eq("Content-Type" => ["text/plain"], "Tag" => %w[a b])
    end

    it "returns an empty hash for an object without headers" do
      expect(store.put("object", "x").headers).to eq({})
    end

    it "takes headers given as Tether::Headers" do
      stored = store.put("object", "x", headers: Tether::Headers["K" => "v"])

      expect(stored.headers).to eq("K" => ["v"])
    end
  end

  describe "#metadata" do
    it "returns the metadata stored with the object" do
      expect(info.metadata).to eq("owner" => "ops")
    end

    it "returns an empty hash for an object without metadata" do
      expect(store.put("object", "x").metadata).to eq({})
    end
  end

  describe "#modified_at" do
    it "returns the time the server stored the metadata" do
      expect(info.modified_at).to eq(backing_stream.get(last_for: meta_subject("greeting.txt")).time)
    end
  end

  describe "#deleted?" do
    it "returns false for a live object" do
      expect(info.deleted?).to be(false)
    end

    it "returns true for the tombstone of a deleted object" do
      expect(tombstone.deleted?).to be(true)
    end
  end

  describe "#link?" do
    it "returns false for an object with content" do
      expect(info.link?).to be(false)
    end

    it "returns true for a link to an object" do
      expect(link.link?).to be(true)
    end

    it "returns true for a link to a bucket" do
      expect(store.add_bucket_link("elsewhere", "other").link?).to be(true)
    end
  end

  describe "#bucket_link?" do
    it "returns false for a link to an object" do
      expect(link.bucket_link?).to be(false)
    end

    it "returns true for a link to a bucket" do
      expect(store.add_bucket_link("elsewhere", "other").bucket_link?).to be(true)
    end
  end

  describe "#link" do
    it "returns the bucket and name a link points at" do
      expect(link.link).to have_attributes(bucket: bucket, name: "greeting.txt")
    end

    it "returns nil for an object with content" do
      expect(info.link).to be_nil
    end

    it "returns the link read back from the server" do
      link

      expect(store.info("alias").link).to have_attributes(bucket: bucket, name: "greeting.txt")
    end
  end

  describe "#raw" do
    it "returns the stored document, unknown keys included" do
      store
      raw_object("object", "hello world", "future" => { "a" => 1 })

      expect(store.info("object").raw).to include("future" => { "a" => 1 }, "mtime" => "0001-01-01T00:00:00Z")
    end
  end

  describe "#to_h" do
    it "returns the document as it is stored" do
      expect(info.to_h).to eq(stored_meta("greeting.txt")[:document])
    end

    it "keeps unknown keys of a document another client wrote" do
      store
      raw_object("object", "hello world", "future" => { "a" => 1 })
      store.update_meta("object", description: "changed")

      expect(stored_meta("object")[:document]).to include("future" => { "a" => 1 }, "description" => "changed")
    end
  end

  describe "#==" do
    it "returns true for the same entry read twice" do
      expect(info).to eq(store.info("greeting.txt"))
    end

    it "returns false for entries of different objects" do
      expect(info).not_to eq(store.put("other", "hello world", chunk_size: 4))
    end
  end

  describe "#inspect" do
    it "shows the bucket, name and size" do
      expect(info.inspect)
        .to eq(%(#<Tether::ObjectStore::Info bucket="#{bucket}" name="greeting.txt" size=11>))
    end

    it "marks a tombstone" do
      expect(tombstone.inspect).to end_with("size=0 deleted>")
    end
  end
end
