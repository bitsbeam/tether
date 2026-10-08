# frozen_string_literal: true

RSpec.describe Tether::KeyValue::Entry do
  include KeyValueHelper

  def watched(count, **options)
    collecting(**options)
    taken(count)
  end

  describe ".operation_of" do
    def operation_for(headers)
      kv.put("color", "blue")
      raw_put("color", headers: headers)

      watched(1).first.operation
    end

    it "reads a message without headers as a put" do
      kv.put("color", "blue")

      expect(watched(1).first.operation).to eq(:put)
    end

    it "reads the DEL operation as a delete" do
      expect(operation_for("KV-Operation" => "DEL")).to eq(:delete)
    end

    it "reads the PURGE operation as a purge" do
      expect(operation_for("KV-Operation" => "PURGE")).to eq(:purge)
    end

    it "reads an operation it does not know as a delete" do
      expect(operation_for("KV-Operation" => "ARCHIVE")).to eq(:delete)
    end

    it "reads a MaxAge marker as a purge" do
      expect(operation_for("Nats-Marker-Reason" => "MaxAge")).to eq(:purge)
    end

    it "reads a Purge marker as a purge" do
      expect(operation_for("Nats-Marker-Reason" => "Purge")).to eq(:purge)
    end

    it "reads a Remove marker as a delete" do
      expect(operation_for("Nats-Marker-Reason" => "Remove")).to eq(:delete)
    end

    it "lets the operation decide over the marker reason" do
      expect(operation_for("KV-Operation" => "DEL", "Nats-Marker-Reason" => "MaxAge")).to eq(:delete)
    end

    it "reads other headers as a put" do
      expect(operation_for("Trace" => "1")).to eq(:put)
    end
  end

  describe "#bucket" do
    it "returns the bucket the entry was read from" do
      kv.put("color", "blue")

      expect(kv.get("color").bucket).to eq(bucket)
      expect(watched(1).first.bucket).to eq(bucket)
    end
  end

  describe "#key" do
    it "returns the key without the bucket's subject prefix" do
      kv.put("config.eu.west", "1")

      expect(watched(1).first.key).to eq("config.eu.west")
    end
  end

  describe "#value" do
    it "returns the bytes that were written" do
      value = "\x00\xFFbinary\r\n".b
      kv.put("blob", value)

      expect(watched(1).first.value).to eq(value)
      expect(kv.get("blob").value).to eq(value)
    end

    it "is empty for a tombstone" do
      kv.delete("color")

      expect(watched(1).first.value).to eq("")
    end
  end

  describe "#revision" do
    it "returns the sequence of the message in the bucket's stream" do
      kv.put("color", "blue")
      kv.put("shape", "round")

      expect(kv.get("shape").revision).to eq(2)
      expect(watched(2).map(&:revision)).to eq([1, 2])
    end
  end

  describe "#created_at" do
    it "returns when the server stored the revision, the same from a get and from a watcher" do
      before = Time.now - 5
      kv.put("color", "blue")
      read = kv.get("color").created_at

      expect(read).to be_a(Time).and be_between(before, Time.now + 5)
      expect(watched(1).first.created_at).to eq(read)
    end
  end

  describe "#operation" do
    it "is :put for a value" do
      kv.put("color", "blue")

      expect(kv.get("color").operation).to eq(:put)
    end

    it "is :delete for the tombstone of a delete" do
      kv.delete("color")

      expect(watched(1).first.operation).to eq(:delete)
    end

    it "is :purge for the tombstone of a purge" do
      kv.purge("color")

      expect(watched(1).first.operation).to eq(:purge)
    end
  end

  describe "#delta" do
    it "is zero from a get" do
      kv.put("color", "blue")
      kv.put("shape", "round")

      expect(kv.get("color").delta).to eq(0)
    end

    it "counts the entries a watcher still had to deliver" do
      3.times { |index| kv.put("k#{index}", "v") }

      expect(watched(3).map(&:delta)).to eq([2, 1, 0])
    end
  end

  describe "#put?" do
    it "is true for a value" do
      kv.put("color", "blue")

      expect(kv.get("color")).to be_put
    end

    it "is false for a tombstone" do
      kv.delete("color")

      expect(watched(1).first).not_to be_put
    end
  end

  describe "#tombstone?" do
    it "is false for a value" do
      kv.put("color", "blue")

      expect(kv.get("color")).not_to be_tombstone
    end

    it "is true for a delete" do
      kv.delete("color")

      expect(watched(1).first).to be_tombstone
    end

    it "is true for a purge" do
      kv.purge("color")

      expect(watched(1).first).to be_tombstone
    end
  end

  describe "#==" do
    it "is true for the same revision of the same key read twice" do
      kv.put("color", "blue")

      expect(kv.get("color")).to eq(kv.get("color"))
    end

    it "is true for the same revision read by a get and by a watcher" do
      kv.put("color", "blue")

      expect(watched(1).first).to eq(kv.get("color"))
    end

    it "is false for another revision of the key" do
      kv.put("color", "blue")
      first = kv.get("color")
      kv.put("color", "green")

      expect(kv.get("color")).not_to eq(first)
    end
  end

  describe "#hash" do
    it "is the same for the same revision of the same key" do
      kv.put("color", "blue")

      expect(kv.get("color").hash).to eq(kv.get("color").hash)
    end
  end

  describe "#frozen?" do
    it "is true, since an entry is a value" do
      kv.put("color", "blue")

      expect(kv.get("color")).to be_frozen
    end
  end

  describe "#inspect" do
    it "shows the key, revision and operation without the value" do
      kv.put("color", "blue")

      expect(kv.get("color").inspect).to eq(
        %(#<Tether::KeyValue::Entry bucket="#{bucket}" key="color" revision=1 operation=:put bytes=4>)
      )
    end
  end
end
