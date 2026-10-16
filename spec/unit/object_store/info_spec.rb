# frozen_string_literal: true

require "json"

RSpec.describe Tether::ObjectStore::Info do
  let(:go_document) do
    '{"name":"greeting.txt","options":{"max_chunk_size":4},"bucket":"DOCS","nuid":"Vq3bTnHcDpLx0a8YkWm2Ze",' \
      '"size":11,"mtime":"0001-01-01T00:00:00Z","chunks":3,' \
      '"digest":"SHA-256=uU0nuZNNPgilLlLX2n2r-sSE7-N6U4DukIj3rOLvzek="}'
  end

  describe ".parse" do
    it "decodes the document nats.go writes" do
      expect(described_class.parse(go_document))
        .to have_attributes(name: "greeting.txt", bucket: "DOCS", nuid: "Vq3bTnHcDpLx0a8YkWm2Ze", size: 11,
                            chunks: 3, chunk_size: 4, deleted?: false, link?: false)
    end

    it "ignores the stored modification time in favor of the one given" do
      time = Time.at(1_700_000_000).utc

      expect(described_class.parse(go_document, modified_at: time).modified_at).to eq(time)
    end

    it "tolerates a document without options, mtime, digest and metadata" do
      document = '{"name":"a","bucket":"B","nuid":"n","size":0,"chunks":0,"metadata":null}'

      expect(described_class.parse(document)).to have_attributes(chunk_size: nil, digest: nil, metadata: {})
    end

    it "decodes a link to an object" do
      document = '{"name":"latest","options":{"link":{"bucket":"DOCS","name":"greeting.txt"}},"bucket":"DOCS",' \
                 '"nuid":"n","size":0,"chunks":0}'

      expect(described_class.parse(document).link).to have_attributes(bucket: "DOCS", name: "greeting.txt")
    end

    it "decodes a link with an empty name as a link to a bucket" do
      document = '{"name":"archive","options":{"link":{"bucket":"OLD","name":""}},"bucket":"DOCS","nuid":"n"}'

      expect(described_class.parse(document)).to have_attributes(bucket_link?: true)
    end

    it "decodes a tombstone" do
      expect(described_class.parse('{"name":"a","bucket":"B","nuid":"n","deleted":true}').deleted?).to be(true)
    end

    it "raises Error for a document that is not JSON" do
      expect { described_class.parse("nope") }.to raise_error(Tether::ObjectStore::Error, /not valid JSON/)
    end

    it "raises Error for JSON that is not an object" do
      expect { described_class.parse("[1]") }.to raise_error(Tether::ObjectStore::Error, /not a JSON object/)
    end
  end

  describe ".digest_of" do
    it "returns the digest string ADR-20 gives as its example" do
      hash = "IdgP4UYMGt47rgecOqFoLrd24AXukHf5-SVzqQ5Psg8=".tr("-_", "+/").unpack1("m")

      expect(described_class.digest_of(hash)).to eq("SHA-256=IdgP4UYMGt47rgecOqFoLrd24AXukHf5-SVzqQ5Psg8=")
    end
  end

  describe "#digest_bytes" do
    it "returns the 32 bytes behind the digest" do
      expect(described_class.parse(go_document).digest_bytes.bytesize).to eq(32)
    end

    it "returns nil for another algorithm" do
      info = described_class.new(name: "a", bucket: "B", nuid: "n", digest: "MD5=rL0Y20zC-Fzt72VPzMSk2A==")

      expect(info.digest_bytes).to be_nil
    end

    it "returns nil for a digest without a separator" do
      expect(described_class.new(name: "a", bucket: "B", nuid: "n", digest: "garbage").digest_bytes).to be_nil
    end

    it "returns nil for a value that is not base64" do
      expect(described_class.new(name: "a", bucket: "B", nuid: "n", digest: "SHA-256=!!!").digest_bytes).to be_nil
    end

    it "returns nil for a hash of the wrong length" do
      expect(described_class.new(name: "a", bucket: "B", nuid: "n", digest: "SHA-256=YWJj").digest_bytes).to be_nil
    end
  end

  describe "#to_h" do
    it "writes the keys in the order nats.go does, without the modification time" do
      expect(JSON.generate(described_class.parse(go_document).to_h))
        .to eq(go_document.sub('"mtime":"0001-01-01T00:00:00Z",', ""))
    end

    it "leaves empty optional fields out" do
      info = described_class.new(name: "a", bucket: "B", nuid: "n", description: "", headers: {}, metadata: {})

      expect(info.to_h).to eq("name" => "a", "bucket" => "B", "nuid" => "n", "size" => 0, "chunks" => 0)
    end

    it "keeps unknown keys, at the top level and under options" do
      document = '{"name":"a","options":{"max_chunk_size":4,"later":true},"bucket":"B","nuid":"n","future":1}'

      expect(described_class.parse(document).to_h)
        .to include("future" => 1, "options" => { "max_chunk_size" => 4, "later" => true })
    end
  end

  describe "#with" do
    it "returns a changed copy" do
      expect(described_class.parse(go_document).with(size: 0, deleted: true))
        .to have_attributes(size: 0, deleted?: true, name: "greeting.txt")
    end

    it "leaves the original alone" do
      info = described_class.parse(go_document)
      info.with(size: 0)

      expect(info.size).to eq(11)
    end
  end

  describe "#hash" do
    it "is the same for equal entries" do
      expect(described_class.parse(go_document).hash).to eq(described_class.parse(go_document).hash)
    end
  end

  describe "#to_json" do
    it "returns the stored form as JSON" do
      expect(JSON.parse(described_class.parse(go_document).to_json)).to include("name" => "greeting.txt", "size" => 11)
    end
  end
end
