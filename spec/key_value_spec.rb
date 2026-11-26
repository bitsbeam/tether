# frozen_string_literal: true

RSpec.describe Tether::KeyValue do
  include KeyValueHelper

  def tombstone(key)
    kv.history(key).last
  end

  describe ".create" do
    it "creates the stream every NATS client creates for a bucket" do
      make_bucket(history: 5)

      expect(stream_config).to include(
        "name" => "KV_#{bucket}", "subjects" => ["$KV.#{bucket}.>"], "retention" => "limits",
        "max_consumers" => -1, "max_msgs" => -1, "max_bytes" => -1, "discard" => "new", "max_age" => 0,
        "max_msgs_per_subject" => 5, "max_msg_size" => -1, "storage" => "memory", "num_replicas" => 1,
        "duplicate_window" => 120_000_000_000, "deny_delete" => true, "deny_purge" => false,
        "allow_rollup_hdrs" => true, "allow_direct" => true, "mirror_direct" => false, "compression" => "none",
        "sealed" => false, "allow_msg_ttl" => false
      )
    end

    it "applies every option to the stream" do
      require_limit_markers
      make_bucket(history: 64, ttl: 3600, max_bytes: 1_048_576, max_value_size: 1024, replicas: 1,
                  compression: true, description: "settings", limit_marker_ttl: 2,
                  republish: { source: ">", destination: "copy.#{bucket}.>" }, metadata: { "owner" => "spec" })

      expect(stream_config).to include(
        "description" => "settings", "max_msgs_per_subject" => 64, "max_age" => 3_600_000_000_000,
        "max_bytes" => 1_048_576, "max_msg_size" => 1024, "compression" => "s2", "allow_msg_ttl" => true,
        "subject_delete_marker_ttl" => 2_000_000_000, "duplicate_window" => 120_000_000_000,
        "republish" => { "src" => ">", "dest" => "copy.#{bucket}.>" }, "discard" => "new",
        "deny_delete" => true, "allow_rollup_hdrs" => true, "allow_direct" => true
      )
      expect(stream_config["metadata"]).to include("owner" => "spec")
    end

    it "enables per-key TTLs without limit markers" do
      require_limit_markers
      make_bucket(allow_message_ttl: true)

      expect(stream_config).to include("allow_msg_ttl" => true)
      expect(stream_config).not_to have_key("subject_delete_marker_ttl")
    end

    it "raises ArgumentError for per-key TTLs on a mirror" do
      expect { make_bucket(allow_message_ttl: true, mirror: { name: "origin" }) }
        .to raise_error(ArgumentError, /allow_message_ttl/)
    end

    it "uses the TTL as the duplicate window when it is below two minutes" do
      make_bucket(ttl: 30)

      expect(stream_config).to include("max_age" => 30_000_000_000, "duplicate_window" => 30_000_000_000)
    end

    it "stores on file by default" do
      made_buckets << bucket
      described_class.create(js, bucket: bucket)

      expect(stream_config).to include("storage" => "file")
    end

    it "returns a handle to the new bucket" do
      expect(make_bucket).to have_attributes(class: described_class, bucket: bucket, jetstream: js)
    end

    it "succeeds when the bucket exists with the same options" do
      make_bucket(history: 3)

      expect(make_bucket(history: 3).bucket).to eq(bucket)
    end

    it "raises BucketExistsError when the bucket exists with other options" do
      make_bucket(history: 3)

      expect { make_bucket(history: 4) }.to raise_error(Tether::KeyValue::BucketExistsError) do |error|
        expect(error.cause).to be_a(Tether::JetStream::StreamNameInUseError)
      end
    end

    it "raises InvalidBucketNameError before asking the server" do
      expect { described_class.create(js, bucket: "not valid") }
        .to raise_error(Tether::KeyValue::InvalidBucketNameError)
    end

    it "raises ArgumentError for a history above 64" do
      expect { described_class.create(js, bucket: bucket, history: 65) }.to raise_error(ArgumentError)
    end

    it "creates a mirror that serves the origin's keys and sends writes to the origin" do
      kv.put("color", "blue")
      mirror = make_bucket("#{bucket}_mirror", mirror: { name: bucket })

      expect(stream_config("#{bucket}_mirror")).to include("mirror_direct" => true)
      expect(stream_config("#{bucket}_mirror")).not_to have_key("subjects")
      expect(eventually { mirror.get("color") }).to have_attributes(key: "color", value: "blue", revision: 1)
      expect(mirror.put("shape", "round")).to eq(2)
      expect(kv["shape"]).to eq("round")
      expect(eventually { mirror.keys.to_a.sort == %w[color shape] }).to be(true)
    end

    it "creates a bucket that copies the keys of its sources" do
      kv.put("color", "blue")
      sink = make_bucket("#{bucket}_sink", sources: [{ name: bucket }])

      expect(eventually { sink["color"] }).to eq("blue")
    end
  end

  describe ".update" do
    it "replaces the options of the bucket" do
      make_bucket(history: 2)
      updated = described_class.update(js, bucket: bucket, storage: :memory, history: 10, description: "more")

      expect(updated.status).to have_attributes(history: 10)
      expect(stream_config).to include("max_msgs_per_subject" => 10, "description" => "more")
    end

    it "raises BucketNotFoundError when the bucket does not exist" do
      expect { described_class.update(js, bucket: bucket, history: 10) }
        .to raise_error(Tether::KeyValue::BucketNotFoundError) do |error|
          expect(error.cause).to be_a(Tether::JetStream::StreamNotFoundError)
        end
    end
  end

  describe ".create_or_update" do
    it "creates the bucket when it does not exist" do
      made_buckets << bucket
      described_class.create_or_update(js, bucket: bucket, storage: :memory, history: 4)

      expect(js.key_value(bucket).status.history).to eq(4)
    end

    it "updates the bucket when it exists" do
      make_bucket(history: 2)
      described_class.create_or_update(js, bucket: bucket, storage: :memory, history: 6)

      expect(js.key_value(bucket).status.history).to eq(6)
    end
  end

  describe ".bind" do
    it "returns a handle to an existing bucket" do
      kv.put("color", "blue")

      expect(described_class.bind(js, bucket)["color"]).to eq("blue")
    end

    it "raises BucketNotFoundError when the bucket does not exist" do
      expect { described_class.bind(js, bucket) }.to raise_error(Tether::KeyValue::BucketNotFoundError)
    end

    it "raises InvalidBucketNameError for a name with a dot" do
      expect { described_class.bind(js, "a.b") }.to raise_error(Tether::KeyValue::InvalidBucketNameError)
    end

    it "raises InvalidBucketNameError for an empty name" do
      expect { described_class.bind(js, "") }.to raise_error(Tether::KeyValue::InvalidBucketNameError)
    end

    it "raises BadBucketError for a KV_ stream that keeps no message per subject" do
      made_buckets << bucket
      js.create_stream(name: "KV_#{bucket}", subjects: ["$KV.#{bucket}.>"], storage: :memory,
                       max_messages_per_subject: -1)

      expect { described_class.bind(js, bucket) }.to raise_error(Tether::KeyValue::BadBucketError)
    end

    it "reads through the message API from a bucket without direct access" do
      made_buckets << bucket
      js.create_stream(name: "KV_#{bucket}", subjects: ["$KV.#{bucket}.>"], storage: :memory,
                       max_messages_per_subject: 1)
      legacy = described_class.bind(js, bucket)
      legacy.put("color", "blue")

      expect(legacy.get("color")).to have_attributes(value: "blue", revision: 1)
      expect(legacy.get("color", revision: 1)).to have_attributes(value: "blue")
      expect(legacy.get("shape")).to be_nil
    end
  end

  describe ".delete" do
    it "removes the bucket and its stream" do
      kv

      expect(described_class.delete(js, bucket)).to be(true)
      expect(js.stream?("KV_#{bucket}")).to be(false)
    end

    it "raises BucketNotFoundError when the bucket does not exist" do
      expect { described_class.delete(js, bucket) }.to raise_error(Tether::KeyValue::BucketNotFoundError)
    end
  end

  describe ".names" do
    it "lists the buckets by name, without the stream prefix" do
      kv
      make_bucket("#{bucket}_2")

      expect(described_class.names(js).to_a).to include(bucket, "#{bucket}_2")
    end

    it "leaves out streams that are not buckets" do
      kv
      made_buckets << "#{bucket}_plain"
      js.create_stream(name: "KV_#{bucket}_plain", subjects: ["plain.#{bucket}.>"], storage: :memory)

      expect(described_class.names(js).to_a).not_to include("#{bucket}_plain")
    end
  end

  describe ".list" do
    it "returns a handle for every bucket" do
      kv.put("color", "blue")
      listed = described_class.list(js).find { |candidate| candidate.bucket == bucket }

      expect(listed).to be_a(described_class)
      expect(listed["color"]).to eq("blue")
    end
  end

  describe "#put" do
    def accepts(key)
      kv.put(key, "v")

      expect(kv[key]).to eq("v")
    end

    def rejects(key)
      expect { kv.put(key, "v") }.to raise_error(Tether::KeyValue::InvalidKeyError)
      expect(kv.status.values).to eq(0)
    end

    it "returns the revision, which grows with every write to the bucket" do
      expect([kv.put("a", "1"), kv.put("b", "1"), kv.put("a", "2")]).to eq([1, 2, 3])
    end

    it "publishes the value to the key's subject without headers" do
      kv.put("color", "blue")

      expect(stored("color")).to eq(subject: "$KV.#{bucket}.color", data: "blue", headers: {})
    end

    it "overwrites a tombstone" do
      kv.put("color", "blue")
      kv.delete("color")
      kv.put("color", "green")

      expect(kv["color"]).to eq("green")
    end

    it "stores an empty value" do
      kv.put("empty", "")

      expect(kv.get("empty")).to have_attributes(value: "", operation: :put)
    end

    it "takes no TTL" do
      expect { kv.put("color", "blue", ttl: 5) }.to raise_error(ArgumentError, /unknown keyword/)
    end

    it "raises NoStreamResponseError once the bucket is gone" do
      kv
      js.delete_key_value(bucket)

      expect { kv.put("color", "blue") }.to raise_error(Tether::JetStream::NoStreamResponseError)
    end

    it "writes and reads under a JetStream domain" do
      NatsServer.run(%(jetstream { domain: "hub" })) do |server|
        store = connect(server.uri).jetstream(domain: "hub").create_key_value(bucket: bucket, storage: :memory)

        expect(store.put("color", "blue")).to eq(1)
        expect(store.get("color")).to have_attributes(value: "blue")
        expect(store.keys.to_a).to eq(["color"])
      end
    end

    it "publishes under the API prefix of its context" do
      prefixed = described_class.new(nc.jetstream(api_prefix: "JS.from-acc1"), kv.stream)
      nc.subscribe("JS.from-acc1.$KV.>") do |msg|
        entries.push(msg.subject)
        msg.respond(%({"stream":"KV_#{bucket}","seq":7}))
      end

      expect(prefixed.put("color", "blue")).to eq(7)
      expect(entries.pop(timeout: 2)).to eq("JS.from-acc1.$KV.#{bucket}.color")
    end

    it "accepts a key of letters and digits" do
      accepts("Region1")
    end

    it "accepts a key with a dash" do
      accepts("eu-west")
    end

    it "accepts a key with an underscore" do
      accepts("eu_west")
    end

    it "accepts a key with a slash" do
      accepts("eu/west")
    end

    it "accepts a key with an equals sign" do
      accepts("YQ==")
    end

    it "accepts a key of several dot-separated tokens" do
      accepts("config.eu.west")
    end

    it "rejects an empty key" do
      rejects("")
    end

    it "rejects a key that starts with a dot" do
      rejects(".region")
    end

    it "rejects a key that ends with a dot" do
      rejects("region.")
    end

    it "rejects a key with an empty token" do
      rejects("config..region")
    end

    it "rejects a key with a space" do
      rejects("eu west")
    end

    it "rejects a key with a star" do
      rejects("config.*")
    end

    it "rejects a key with a greater-than sign" do
      rejects("config.>")
    end

    it "rejects a key with a character outside ASCII" do
      rejects("région")
    end

    it "rejects a key with a line break" do
      rejects("region\n")
    end

    it "rejects a reserved key" do
      rejects("_kv.internal")
    end
  end

  describe "#get" do
    it "returns the latest entry of the key" do
      kv.put("color", "blue")
      kv.put("color", "green")

      expect(kv.get("color")).to have_attributes(bucket: bucket, key: "color", value: "green", revision: 2,
                                                 operation: :put, delta: 0)
    end

    it "returns a binary value byte for byte" do
      value = "#{(0..255).to_a.pack("C*")}\r\n\0".b
      kv.put("blob", value)

      expect(kv.get("blob").value).to eq(value)
      expect(kv.get("blob").value.encoding).to eq(Encoding::BINARY)
    end

    it "returns nil for a key that was never written" do
      expect(kv.get("color")).to be_nil
    end

    it "returns nil for a deleted key" do
      kv.put("color", "blue")
      kv.delete("color")

      expect(kv.get("color")).to be_nil
    end

    it "returns nil for a purged key" do
      kv.put("color", "blue")
      kv.purge("color")

      expect(kv.get("color")).to be_nil
    end

    it "returns an earlier revision that the history still holds" do
      history = make_bucket(history: 3)
      history.put("color", "blue")
      history.put("color", "green")

      expect(history.get("color", revision: 1)).to have_attributes(value: "blue", revision: 1)
    end

    it "returns nil for a revision that belongs to another key" do
      kv.put("color", "blue")
      kv.put("shape", "round")

      expect(kv.get("color", revision: 2)).to be_nil
    end

    it "returns nil for a revision the bucket does not hold" do
      kv.put("color", "blue")

      expect(kv.get("color", revision: 9)).to be_nil
    end

    it "returns nil for a revision that is a tombstone" do
      history = make_bucket(history: 3)
      history.put("color", "blue")
      revision = history.delete("color")

      expect(history.get("color", revision: revision)).to be_nil
    end

    it "reads a reserved key another client wrote" do
      kv
      raw_put("_kv.internal", "x")

      expect(kv.get("_kv.internal")).to have_attributes(value: "x")
    end

    it "reads a value another client published straight to the key's subject" do
      kv
      nc.request("$KV.#{bucket}.color", "blue", timeout: 2)

      expect(kv.get("color")).to have_attributes(value: "blue", revision: 1, operation: :put)
    end

    it "returns nil for a tombstone another client published" do
      kv.put("color", "blue")
      raw_put("color", headers: { "KV-Operation" => "DEL" })

      expect(kv.get("color")).to be_nil
    end

    it "treats an operation it does not know as a delete" do
      kv.put("color", "blue")
      raw_put("color", "x", headers: { "KV-Operation" => "ARCHIVE" })

      expect(kv.get("color")).to be_nil
    end

    it "raises InvalidKeyError for a wildcard" do
      expect { kv.get("config.*") }.to raise_error(Tether::KeyValue::InvalidKeyError)
    end

    it "raises ServiceUnavailableError once the bucket is gone" do
      kv
      js.delete_key_value(bucket)

      expect { kv.get("color") }.to raise_error(Tether::JetStream::ServiceUnavailableError)
    end
  end

  describe "#[]" do
    it "returns the value of the key" do
      kv.put("color", "blue")

      expect(kv["color"]).to eq("blue")
    end

    it "returns nil for a key without a value" do
      expect(kv["color"]).to be_nil
    end
  end

  describe "#[]=" do
    it "writes the value" do
      kv["color"] = "blue"

      expect(kv.get("color")).to have_attributes(value: "blue", revision: 1)
    end
  end

  describe "#fetch" do
    it "returns the value of the key" do
      kv.put("color", "blue")

      expect(kv.fetch("color")).to eq("blue")
    end

    it "raises KeyNotFoundError for a key that was never written" do
      expect { kv.fetch("color") }.to raise_error(Tether::KeyValue::KeyNotFoundError, /color/)
    end

    it "raises KeyNotFoundError for a deleted key" do
      kv.put("color", "blue")
      kv.delete("color")

      expect { kv.fetch("color") }.to raise_error(Tether::KeyValue::KeyNotFoundError)
    end

    it "returns what the block returns for a missing key" do
      expect(kv.fetch("color") { |key| "no #{key}" }).to eq("no color")
    end

    it "does not call the block for a key with a value" do
      kv.put("color", "blue")

      expect(kv.fetch("color") { raise "not expected" }).to eq("blue")
    end
  end

  describe "#create" do
    it "writes a key that does not exist and returns its revision" do
      expect(kv.create("color", "blue")).to eq(1)
      expect(kv["color"]).to eq("blue")
    end

    it "publishes with the expectation that the subject is empty" do
      kv.create("color", "blue")

      expect(stored("color")).to eq(subject: "$KV.#{bucket}.color", data: "blue",
                                    headers: { "Nats-Expected-Last-Subject-Sequence" => "0" })
    end

    it "raises KeyExistsError when the key holds a value, keeping the server's error as the cause" do
      kv.put("color", "blue")

      expect { kv.create("color", "green") }.to raise_error(Tether::KeyValue::KeyExistsError) do |error|
        expect(error.cause).to have_attributes(class: Tether::JetStream::WrongLastSequenceError, err_code: 10_071)
      end
      expect(kv["color"]).to eq("blue")
    end

    it "raises an error that is also a RevisionMismatchError" do
      kv.put("color", "blue")

      expect { kv.create("color", "green") }.to raise_error(Tether::KeyValue::RevisionMismatchError)
    end

    it "succeeds after the key was deleted, expecting the tombstone's revision" do
      kv.put("color", "blue")
      deleted = kv.delete("color")

      expect(kv.create("color", "green")).to eq(3)
      expect(stored("color")[:headers]).to eq("Nats-Expected-Last-Subject-Sequence" => deleted.to_s)
    end

    it "succeeds after the key was purged" do
      kv.put("color", "blue")
      kv.purge("color")

      expect(kv.create("color", "green")).to eq(3)
      expect(kv["color"]).to eq("green")
    end

    it "raises InvalidKeyError for a reserved key" do
      expect { kv.create("_kv.x", "v") }.to raise_error(Tether::KeyValue::InvalidKeyError, /reserved/)
    end

    it "raises ArgumentError for a TTL that is not positive" do
      expect { kv.create("color", "blue", ttl: 0) }.to raise_error(ArgumentError, /positive/)
    end

    it "passes the server's refusal on when the bucket has no per-key TTLs" do
      expect { kv.create("color", "blue", ttl: 5) }
        .to raise_error(Tether::JetStream::APIError) { |error| expect(error.err_code).to eq(10_166) }
    end

    context "with per-key TTLs and no limit markers" do
      let(:kv) { make_bucket(allow_message_ttl: true) }

      before { require_limit_markers }

      it "lets the key expire without leaving a marker" do
        kv.create("session", "token", ttl: 1)

        expect(eventually(timeout: 10) { kv.history("session").empty? }).to be(true)
      end

      it "succeeds again once the key has expired" do
        kv.create("session", "token", ttl: 1)
        eventually(timeout: 10) { kv.get("session").nil? }

        expect(kv.create("session", "again", ttl: 1)).to be > 1
      end
    end

    context "with limit markers" do
      let(:kv) { make_bucket(limit_marker_ttl: 1) }

      before { require_limit_markers }

      it "sends the TTL with the write" do
        kv.create("session", "token", ttl: 3600)

        expect(stored("session")[:headers]).to eq("Nats-Expected-Last-Subject-Sequence" => "0",
                                                  "Nats-TTL" => "3600s")
      end

      it "lets the key expire, leaving a marker that reads as a purge" do
        kv.create("session", "token", ttl: 1)
        marker = eventually(timeout: 10) { tombstone("session")&.then { |entry| entry if entry.tombstone? } }

        expect(marker).to have_attributes(key: "session", operation: :purge, value: "")
        expect(stored("session")[:headers]).to include("Nats-Marker-Reason" => "MaxAge")
        expect(kv.get("session")).to be_nil
      end

      it "tells a watcher that the key expired" do
        collecting
        kv.create("session", "token", ttl: 1)

        expect(summary(taken(2, timeout: 10))).to eq([["session", "token", :put], ["session", "", :purge]])
      end

      it "succeeds again once the key has expired" do
        kv.create("session", "token", ttl: 1)
        eventually(timeout: 10) { kv.get("session").nil? }

        expect(kv.create("session", "again")).to be > 1
      end
    end
  end

  describe "#update" do
    it "writes when the key is at the given revision" do
      revision = kv.put("color", "blue")

      expect(kv.update("color", "green", revision: revision)).to eq(2)
      expect(kv["color"]).to eq("green")
    end

    it "publishes with the expected revision" do
      kv.put("color", "blue")
      kv.update("color", "green", revision: 1)

      expect(stored("color")).to eq(subject: "$KV.#{bucket}.color", data: "green",
                                    headers: { "Nats-Expected-Last-Subject-Sequence" => "1" })
    end

    it "raises RevisionMismatchError when the key is at another revision" do
      kv.put("color", "blue")
      kv.put("color", "green")

      expect { kv.update("color", "red", revision: 1) }
        .to raise_error(Tether::KeyValue::RevisionMismatchError) do |error|
          expect(error).not_to be_a(Tether::KeyValue::KeyExistsError)
          expect(error.cause).to be_a(Tether::JetStream::WrongLastSequenceError)
        end
      expect(kv["color"]).to eq("green")
    end

    it "raises RevisionMismatchError for the revision of another key" do
      kv.put("color", "blue")
      other = kv.put("shape", "round")

      expect { kv.update("color", "red", revision: other) }.to raise_error(Tether::KeyValue::RevisionMismatchError)
    end

    it "writes a key that has no revision when the revision is zero" do
      expect(kv.update("color", "blue", revision: 0)).to eq(1)
    end

    it "raises ArgumentError without a revision" do
      expect { kv.update("color", "blue") }.to raise_error(ArgumentError, /revision/)
    end
  end

  describe "#delete" do
    it "leaves a tombstone after the earlier revisions and returns its revision" do
      history = make_bucket(history: 5)
      history.put("color", "blue")

      expect(history.delete("color")).to eq(2)
      expect(summary(history.history("color"))).to eq([["color", "blue", :put], ["color", "", :delete]])
    end

    it "publishes an empty message with the delete operation" do
      kv.put("color", "blue")
      kv.delete("color")

      expect(stored("color")).to eq(subject: "$KV.#{bucket}.color", data: "", headers: { "KV-Operation" => "DEL" })
    end

    it "deletes when the key is at the given revision" do
      revision = kv.put("color", "blue")
      kv.delete("color", revision: revision)

      expect(kv.get("color")).to be_nil
      expect(stored("color")[:headers]).to eq("KV-Operation" => "DEL",
                                              "Nats-Expected-Last-Subject-Sequence" => revision.to_s)
    end

    it "raises RevisionMismatchError when the key is at another revision" do
      kv.put("color", "blue")
      kv.put("color", "green")

      expect { kv.delete("color", revision: 1) }.to raise_error(Tether::KeyValue::RevisionMismatchError)
      expect(kv["color"]).to eq("green")
    end

    it "deletes a key that was never written" do
      expect(kv.delete("color")).to eq(1)
    end

    it "takes no TTL" do
      expect { kv.delete("color", ttl: 5) }.to raise_error(ArgumentError, /unknown keyword/)
    end
  end

  describe "#purge" do
    it "leaves only a tombstone and returns its revision" do
      history = make_bucket(history: 5)
      history.put("color", "blue")
      history.put("color", "green")

      expect(history.purge("color")).to eq(3)
      expect(summary(history.history("color"))).to eq([["color", "", :purge]])
    end

    it "publishes an empty message with the purge operation and a subject rollup" do
      kv.put("color", "blue")
      kv.purge("color")

      expect(stored("color")).to eq(subject: "$KV.#{bucket}.color", data: "",
                                    headers: { "KV-Operation" => "PURGE", "Nats-Rollup" => "sub" })
    end

    it "keeps the other keys" do
      kv.put("color", "blue")
      kv.put("shape", "round")
      kv.purge("color")

      expect(kv["shape"]).to eq("round")
    end

    it "purges when the key is at the given revision" do
      revision = kv.put("color", "blue")
      kv.purge("color", revision: revision)

      expect(stored("color")[:headers]).to eq("KV-Operation" => "PURGE", "Nats-Rollup" => "sub",
                                              "Nats-Expected-Last-Subject-Sequence" => revision.to_s)
    end

    it "raises RevisionMismatchError when the key is at another revision" do
      kv.put("color", "blue")
      kv.put("color", "green")

      expect { kv.purge("color", revision: 1) }.to raise_error(Tether::KeyValue::RevisionMismatchError)
      expect(kv["color"]).to eq("green")
    end

    it "passes the server's refusal on when the bucket has no per-key TTLs" do
      expect { kv.purge("color", ttl: 5) }
        .to raise_error(Tether::JetStream::APIError) { |error| expect(error.err_code).to eq(10_166) }
    end

    it "removes the tombstone after its TTL on a bucket with limit markers" do
      require_limit_markers
      expiring = make_bucket(limit_marker_ttl: 1)
      expiring.put("color", "blue")
      expiring.purge("color", ttl: 1)

      expect(stored("color")[:headers]).to include("KV-Operation" => "PURGE", "Nats-TTL" => "1s")
      expect(eventually(timeout: 10) { expiring.history("color").empty? }).to be(true)
    end
  end

  describe "#purge_deletes" do
    let(:kv) { make_bucket(history: 5) }

    def remove_keys
      kv.put("live", "1")
      kv.put("deleted", "1")
      kv.put("deleted", "2")
      kv.delete("deleted")
      kv.put("purged", "1")
      kv.purge("purged")
    end

    it "removes every tombstone and its key's revisions when the threshold is zero" do
      remove_keys

      expect(kv.purge_deletes(older_than: 0)).to eq(2)
      expect(kv.history("deleted")).to be_empty
      expect(kv.history("purged")).to be_empty
      expect(kv["live"]).to eq("1")
    end

    it "keeps tombstones younger than the threshold, without the revisions before them" do
      remove_keys

      expect(kv.purge_deletes(older_than: 3600)).to eq(0)
      expect(summary(kv.history("deleted"))).to eq([["deleted", "", :delete]])
      expect(summary(kv.history("purged"))).to eq([["purged", "", :purge]])
    end

    it "keeps tombstones for thirty minutes by default" do
      remove_keys

      expect(kv.purge_deletes).to eq(0)
      expect(summary(kv.history("deleted"))).to eq([["deleted", "", :delete]])
    end

    it "removes only the tombstones that have reached the threshold" do
      kv.put("old", "1")
      kv.delete("old")
      eventually { Time.now - tombstone("old").created_at > 0.3 }
      kv.put("young", "1")
      kv.delete("young")

      expect(kv.purge_deletes(older_than: 0.3)).to eq(1)
      expect(kv.history("old")).to be_empty
      expect(summary(kv.history("young"))).to eq([["young", "", :delete]])
    end

    it "returns zero for a bucket without tombstones" do
      kv.put("live", "1")

      expect(kv.purge_deletes(older_than: 0)).to eq(0)
      expect(eventually { server_consumers.empty? }).to be(true)
    end
  end

  describe "#keys" do
    it "lists the keys that hold a value" do
      kv.put("color", "blue")
      kv.put("shape", "round")
      kv.put("size", "big")
      kv.delete("shape")
      kv.purge("size")

      expect(kv.keys.to_a).to eq(["color"])
    end

    it "lists a key once however many revisions it has" do
      history = make_bucket(history: 5)
      3.times { |index| history.put("color", index.to_s) }

      expect(history.keys.to_a).to eq(["color"])
    end

    it "returns nothing for an empty bucket" do
      expect(kv.keys.to_a).to eq([])
    end

    it "returns nothing for a bucket that holds only tombstones" do
      kv.put("color", "blue")
      kv.delete("color")

      expect(kv.keys.to_a).to eq([])
    end

    it "lists only the keys matching a filter" do
      kv.put("config.eu", "1")
      kv.put("config.us", "1")
      kv.put("limits.eu", "1")

      expect(kv.keys("config.>").to_a).to contain_exactly("config.eu", "config.us")
    end

    it "lists the keys matching any of several filters" do
      kv.put("config.eu", "1")
      kv.put("limits.eu", "1")
      kv.put("other", "1")

      expect(kv.keys("config.*", "other").to_a).to contain_exactly("config.eu", "other")
    end

    it "lists more keys than one pull holds" do
      600.times { |index| js.publish_async("$KV.#{kv.bucket}.k#{index}", "v") }
      js.wait_for_publishes(timeout: 10)

      expect(kv.keys.to_a.size).to eq(600)
    end

    it "removes its server consumer when the listing is abandoned" do
      5.times { |index| kv.put("k#{index}", "v") }

      expect(kv.keys.first(2).size).to eq(2)
      expect(eventually { server_consumers.empty? }).to be(true)
    end

    it "reads nothing from the server until it is enumerated" do
      kv.put("color", "blue")
      kv.keys

      expect(server_consumers).to be_empty
    end

    it "raises InvalidKeyError for a filter with a space" do
      expect { kv.keys("config >") }.to raise_error(Tether::KeyValue::InvalidKeyError)
    end
  end

  describe "#history" do
    let(:kv) { make_bucket(history: 3) }

    it "returns the revisions of the key, oldest first" do
      kv.put("color", "blue")
      kv.put("shape", "round")
      kv.put("color", "green")

      expect(kv.history("color").map { |entry| [entry.value, entry.revision, entry.delta] })
        .to eq([["blue", 1, 1], ["green", 3, 0]])
    end

    it "keeps no more revisions than the bucket's history" do
      5.times { |index| kv.put("color", index.to_s) }

      expect(kv.history("color").map(&:value)).to eq(%w[2 3 4])
    end

    it "includes tombstones" do
      kv.put("color", "blue")
      kv.delete("color")

      expect(summary(kv.history("color"))).to eq([["color", "blue", :put], ["color", "", :delete]])
    end

    it "returns nothing for a key that was never written" do
      expect(kv.history("color")).to eq([])
    end

    it "raises InvalidKeyError for a wildcard" do
      expect { kv.history("config.>") }.to raise_error(Tether::KeyValue::InvalidKeyError)
    end

    it "leaves no server consumer behind" do
      kv.put("color", "blue")
      kv.history("color")

      expect(eventually { server_consumers.empty? }).to be(true)
    end
  end

  describe "#each" do
    it "yields the latest entry of every key that holds a value" do
      kv.put("color", "blue")
      kv.put("shape", "round")
      kv.put("color", "green")
      kv.delete("shape")
      yielded = Thread::Queue.new

      expect(kv.each { |entry| yielded.push(entry) }).to equal(kv)
      expect(summary([yielded.pop(timeout: 0)])).to eq([["color", "green", :put]])
      expect(yielded).to be_empty
    end

    it "returns an enumerator without a block" do
      kv.put("color", "blue")

      expect(kv.each.map(&:key)).to eq(["color"])
    end

    it "makes the bucket enumerable" do
      kv.put("color", "blue")
      kv.put("shape", "round")

      expect(kv.to_h { |entry| [entry.key, entry.value] }).to eq("color" => "blue", "shape" => "round")
    end

    it "yields nothing for an empty bucket" do
      expect(kv.to_a).to eq([])
    end
  end

  describe "#watch" do
    it "delivers the latest value of every key, then reports ready, then delivers updates" do
      kv.put("color", "blue")
      kv.put("color", "green")
      kv.put("shape", "round")
      collecting(on_ready: -> { entries.push(:ready) })
      initial = taken(3)
      kv.put("size", "big")

      expect(summary(initial[0, 2])).to eq([["color", "green", :put], ["shape", "round", :put]])
      expect(initial[2]).to eq(:ready)
      expect(summary(taken(1))).to eq([["size", "big", :put]])
    end

    it "delivers deletes and purges as entries" do
      collecting.wait_until_ready(timeout: 2)
      kv.put("color", "blue")
      kv.delete("color")
      kv.purge("color")

      expect(summary(taken(3))).to eq([["color", "blue", :put], ["color", "", :delete], ["color", "", :purge]])
    end

    it "delivers the tombstone of a deleted key among the initial values" do
      kv.put("color", "blue")
      kv.delete("color")
      collecting

      expect(summary(taken(1))).to eq([["color", "", :delete]])
    end

    it "delivers every stored revision with include_history" do
      history = make_bucket(history: 5)
      history.put("color", "blue")
      history.put("shape", "round")
      history.put("color", "green")
      history.watch(include_history: true, on_ready: -> { entries.push(:ready) }) { |entry| entries.push(entry) }
      delivered = taken(4)

      expect(delivered[0, 3].map { |entry| [entry.key, entry.value, entry.delta] })
        .to eq([["color", "blue", 2], ["shape", "round", 1], ["color", "green", 0]])
      expect(delivered[3]).to eq(:ready)
    end

    it "delivers only what is written afterwards with updates_only" do
      kv.put("color", "blue")
      watcher = collecting(updates_only: true)
      kv.put("shape", "round")

      expect(watcher.wait_until_ready(timeout: 2)).to be(true)
      expect(summary(taken(1))).to eq([["shape", "round", :put]])
      expect(entries).to be_empty
    end

    it "leaves tombstones out with ignore_deletes" do
      kv.put("color", "blue")
      kv.put("shape", "round")
      kv.delete("shape")
      watcher = collecting(ignore_deletes: true)
      watcher.wait_until_ready(timeout: 2)
      kv.delete("color")
      kv.put("size", "big")

      expect(summary(taken(2))).to eq([["color", "blue", :put], ["size", "big", :put]])
    end

    it "reports ready with ignore_deletes when the bucket holds only tombstones" do
      kv.put("color", "blue")
      kv.delete("color")

      expect(collecting(ignore_deletes: true).wait_until_ready(timeout: 2)).to be(true)
      expect(entries).to be_empty
    end

    it "delivers entries without their values with meta_only" do
      kv.put("color", "blue")
      collecting(meta_only: true)

      expect(taken(1).first).to have_attributes(key: "color", value: "", revision: 1, operation: :put)
    end

    it "delivers only the watched key" do
      kv.put("color", "blue")
      kv.put("shape", "round")
      collecting("shape").wait_until_ready(timeout: 2)
      kv.put("color", "green")
      kv.put("shape", "square")

      expect(summary(taken(2))).to eq([["shape", "round", :put], ["shape", "square", :put]])
    end

    it "delivers the keys matching a wildcard" do
      kv.put("config.eu", "1")
      kv.put("limits.eu", "1")
      kv.put("config.us", "1")
      collecting("config.*")

      expect(taken(2).map(&:key)).to eq(%w[config.eu config.us])
    end

    it "delivers the keys matching any of several filters" do
      kv.put("region", "eu")
      kv.put("limits.cpu", "4")
      kv.put("other", "x")
      kv.put("limits.memory", "8")
      watcher = collecting("region", "limits.>")

      expect(taken(3).map(&:key)).to eq(%w[region limits.cpu limits.memory])
      expect(watcher.filters).to eq(%w[region limits.>])
    end

    it "watches the whole bucket for an empty key" do
      kv.put("color", "blue")

      expect(collecting("").filters).to eq([">"])
      expect(taken(1).map(&:key)).to eq(["color"])
    end

    it "starts at the given revision with resume_from" do
      history = make_bucket(history: 5)
      4.times { |index| history.put("color", index.to_s) }
      history.watch(resume_from: 3, on_ready: -> { entries.push(:ready) }) { |entry| entries.push(entry) }
      delivered = taken(3)

      expect(delivered[0, 2].map(&:revision)).to eq([3, 4])
      expect(delivered[2]).to eq(:ready)
    end

    it "delivers a write another client published straight to the key's subject" do
      collecting.wait_until_ready(timeout: 2)
      raw_put("color", "blue")
      raw_put("color", headers: { "KV-Operation" => "PURGE", "Nats-Rollup" => "sub" })

      expect(summary(taken(2))).to eq([["color", "blue", :put], ["color", "", :purge]])
    end

    it "delivers on a worker thread of its own, one entry at a time" do
      threads = Thread::Queue.new
      kv.watch { |_| threads.push(Thread.current) }
      3.times { |index| kv.put("k#{index}", "v") }
      workers = Array.new(3) { threads.pop(timeout: 2) }

      expect(workers.uniq.size).to eq(1)
      expect(workers.first).not_to eq(Thread.current)
    end

    it "keeps delivering after the block raised, and reports the error" do
      errors = record(nc, :error)
      kv.put("a", "1")
      kv.put("b", "2")
      watcher = kv.watch do |entry|
        entries.push(entry)
        raise "boom"
      end

      expect(taken(2).map(&:key)).to eq(%w[a b])
      expect(errors.pop(timeout: 2).last).to be_a(Tether::HandlerError)
      expect(watcher.wait_until_ready(timeout: 2)).to be(true)
    end

    it "returns a watcher to take entries from when no block is given" do
      kv.put("color", "blue")
      watcher = kv.watch

      expect(watcher.next(timeout: 2)).to have_attributes(key: "color", value: "blue")
    ensure
      watcher&.stop
    end

    it "carries on without gaps or duplicates after its server consumer was deleted" do
      kv.put("k0", "v")
      watcher = collecting
      watcher.wait_until_ready(timeout: 2)
      js.delete_consumer("KV_#{bucket}", server_consumers.first)
      3.times { |index| kv.put("k#{index + 1}", "v") }

      expect(taken(4, timeout: 15).map(&:revision)).to eq([1, 2, 3, 4])
      kv.put("k4", "v")
      expect(taken(1, timeout: 15).map(&:revision)).to eq([5])
      expect(entries).to be_empty
    end

    it "carries on without gaps or duplicates after the connection dropped" do
      kv.put("k0", "v")
      client = connect(proxy.uri, reconnect: ClientHelper::FAST_RECONNECT)
      reconnected = record(client, :reconnect)
      watcher = client.jetstream.key_value(bucket).watch(on_ready: -> { entries.push(:ready) }) do |entry|
        entries.push(entry)
      end
      watcher.wait_until_ready(timeout: 2)
      proxy.drop
      3.times { |index| kv.put("k#{index + 1}", "v") }
      reconnected.pop(timeout: 5)
      kv.put("k4", "v")
      delivered = taken(6, timeout: 15)

      expect(delivered[1]).to eq(:ready)
      expect((delivered - [:ready]).map(&:revision)).to eq([1, 2, 3, 4, 5])
      expect(entries).to be_empty
    end

    it "raises InvalidKeyError for a filter with a space" do
      expect { kv.watch("config >") }.to raise_error(Tether::KeyValue::InvalidKeyError)
    end

    it "raises InvalidKeyError for a filter with a wildcard that is not last" do
      expect { kv.watch("config.>.eu") }.to raise_error(Tether::KeyValue::InvalidKeyError)
    end

    it "raises ArgumentError when include_history and updates_only are combined" do
      expect { kv.watch(include_history: true, updates_only: true) }
        .to raise_error(ArgumentError, /cannot be combined/)
    end

    it "raises ArgumentError for an option it does not have" do
      expect { kv.watch(history: true) }.to raise_error(ArgumentError, /unknown keyword/)
    end

    it "raises BucketNotFoundError once the bucket is gone" do
      kv
      js.delete_key_value(bucket)

      expect { kv.watch }.to raise_error(Tether::KeyValue::BucketNotFoundError)
    end
  end

  describe "#status" do
    it "reads the bucket's counters from the server" do
      kv.put("color", "blue")
      kv.put("shape", "round")

      expect(kv.status).to have_attributes(class: Tether::KeyValue::Status, bucket: bucket, values: 2)
    end

    it "raises BucketNotFoundError once the bucket is gone" do
      kv
      js.delete_key_value(bucket)

      expect { kv.status }.to raise_error(Tether::KeyValue::BucketNotFoundError)
    end
  end

  describe "#config" do
    it "returns the options the bucket was created with" do
      created = make_bucket(history: 7, ttl: 600, description: "settings")

      expect(created.config).to have_attributes(bucket: bucket, history: 7, ttl: 600, storage: :memory,
                                                description: "settings", compression: false, replicas: 1)
    end

    it "reads per-key TTLs without limit markers back" do
      require_limit_markers

      expect(make_bucket(allow_message_ttl: true).config).to have_attributes(allow_message_ttl: true,
                                                                             limit_marker_ttl: nil)
    end
  end

  describe "#stream" do
    it "returns the stream behind the bucket" do
      expect(kv.stream).to have_attributes(class: Tether::JetStream::Stream, name: "KV_#{bucket}")
    end
  end

  describe "#inspect" do
    it "names the bucket" do
      expect(kv.inspect).to eq(%(#<Tether::KeyValue bucket="#{bucket}">))
    end
  end
end
