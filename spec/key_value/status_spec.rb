# frozen_string_literal: true

RSpec.describe Tether::KeyValue::Status do
  include KeyValueHelper

  describe "#bucket" do
    it "returns the bucket's name" do
      expect(kv.status.bucket).to eq(bucket)
    end
  end

  describe "#values" do
    it "counts every stored revision and tombstone" do
      history = make_bucket(history: 5)
      history.put("color", "blue")
      history.put("color", "green")
      history.delete("color")
      history.put("shape", "round")

      expect(history.status.values).to eq(4)
    end

    it "is zero for an empty bucket" do
      expect(kv.status.values).to eq(0)
    end
  end

  describe "#history" do
    it "returns the revisions kept per key" do
      expect(make_bucket(history: 12).status.history).to eq(12)
    end

    it "is one by default" do
      expect(kv.status.history).to eq(1)
    end
  end

  describe "#ttl" do
    it "returns the seconds a revision lives" do
      expect(make_bucket(ttl: 90).status.ttl).to eq(90)
    end

    it "is nil when revisions never expire" do
      expect(kv.status.ttl).to be_nil
    end
  end

  describe "#limit_marker_ttl" do
    it "returns the seconds a limit marker lives" do
      require_limit_markers

      expect(make_bucket(limit_marker_ttl: 3).status.limit_marker_ttl).to eq(3)
    end

    it "is nil without limit markers" do
      expect(kv.status.limit_marker_ttl).to be_nil
    end
  end

  describe "#bytes" do
    it "grows with what the bucket stores" do
      kv.put("color", "blue")

      expect { kv.put("shape", "x" * 1000) }.to change { kv.status.bytes }.by_at_least(1000)
    end
  end

  describe "#compressed?" do
    it "is true for a bucket created with compression" do
      expect(make_bucket(compression: true).status).to be_compressed
    end

    it "is false by default" do
      expect(kv.status).not_to be_compressed
    end
  end

  describe "#backing_store" do
    it "is JetStream" do
      expect(kv.status.backing_store).to eq("JetStream")
    end
  end

  describe "#metadata" do
    it "returns the metadata the bucket was created with" do
      expect(make_bucket(metadata: { "owner" => "spec" }).status.metadata).to include("owner" => "spec")
    end
  end

  describe "#stream_info" do
    it "returns the info of the stream behind the bucket" do
      kv.put("color", "blue")

      expect(kv.status.stream_info).to be_a(Tether::JetStream::StreamInfo)
      expect(kv.status.stream_info.state.last_sequence).to eq(1)
      expect(kv.status.stream_info.config.name).to eq("KV_#{bucket}")
    end
  end

  describe "#config" do
    it "returns the bucket's options" do
      status = make_bucket(history: 4, ttl: 300, max_bytes: 65_536, max_value_size: 512).status

      expect(status.config).to have_attributes(class: Tether::KeyValue::Config, bucket: bucket, history: 4, ttl: 300,
                                               max_bytes: 65_536, max_value_size: 512, storage: :memory)
    end
  end

  describe "#inspect" do
    it "shows the bucket and its counters" do
      expect(kv.status.inspect).to eq(
        %(#<Tether::KeyValue::Status bucket="#{bucket}" values=0 history=1 bytes=0>)
      )
    end
  end
end
