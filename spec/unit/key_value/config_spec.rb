# frozen_string_literal: true

RSpec.describe Tether::KeyValue::Config do
  def wire(**options)
    described_class.new(bucket: "CFG", **options).stream_config.to_h
  end

  describe ".new" do
    it "raises InvalidBucketNameError for a name with a dot" do
      expect { described_class.new(bucket: "a.b") }.to raise_error(Tether::KeyValue::InvalidBucketNameError)
    end

    it "raises InvalidBucketNameError for a name longer than 252 characters" do
      expect { described_class.new(bucket: "a" * 253) }.to raise_error(Tether::KeyValue::InvalidBucketNameError)
    end

    it "accepts a name of 252 characters" do
      expect(described_class.new(bucket: "a" * 252).bucket.length).to eq(252)
    end

    it "raises ArgumentError for a history above 64" do
      expect { described_class.new(bucket: "CFG", history: 65) }.to raise_error(ArgumentError, /between 1 and 64/)
    end

    it "raises ArgumentError for a history below 1" do
      expect { described_class.new(bucket: "CFG", history: 0) }.to raise_error(ArgumentError, /between 1 and 64/)
    end

    it "raises ArgumentError for a limit marker TTL below one second" do
      expect { described_class.new(bucket: "CFG", limit_marker_ttl: 0.5) }
        .to raise_error(ArgumentError, /at least 1 second/)
    end

    it "raises ArgumentError for a limit marker TTL on a mirror" do
      expect { described_class.new(bucket: "CFG", limit_marker_ttl: 1, mirror: { name: "ORIGIN" }) }
        .to raise_error(ArgumentError, /mirror/)
    end

    it "raises ArgumentError for an option it does not have" do
      expect { described_class.new(bucket: "CFG", histroy: 2) }.to raise_error(ArgumentError, /unknown keyword/)
    end
  end

  describe ".from_stream" do
    it "reads the options back from the stream configuration they produced" do
      config = described_class.new(bucket: "CFG", history: 5, ttl: 60, max_bytes: 1024, max_value_size: 128,
                                   storage: :memory, compression: true, description: "settings",
                                   limit_marker_ttl: 2, metadata: { "owner" => "spec" })

      expect(described_class.from_stream(config.stream_config)).to eq(config)
    end

    it "reports absent limits as nil" do
      config = described_class.from_stream(described_class.new(bucket: "CFG").stream_config)

      expect(config).to have_attributes(ttl: nil, max_bytes: nil, max_value_size: nil, limit_marker_ttl: nil)
    end
  end

  describe "#stream_config" do
    it "builds the stream configuration other clients create for a plain bucket" do
      expect(wire(history: 5)).to eq(
        "name" => "KV_CFG", "subjects" => ["$KV.CFG.>"], "retention" => "limits", "max_consumers" => -1,
        "max_msgs" => -1, "max_bytes" => -1, "discard" => "new", "max_age" => 0, "max_msgs_per_subject" => 5,
        "max_msg_size" => -1, "storage" => "file", "num_replicas" => 1, "duplicate_window" => 120_000_000_000,
        "deny_delete" => true, "allow_rollup_hdrs" => true, "allow_direct" => true, "compression" => "none"
      )
    end

    it "never asks the server to deny purges" do
      expect(wire).not_to have_key("deny_purge")
    end

    it "shortens the duplicate window to a TTL below two minutes" do
      expect(wire(ttl: 30)).to include("max_age" => 30_000_000_000, "duplicate_window" => 30_000_000_000)
    end

    it "keeps the duplicate window at two minutes for a longer TTL" do
      expect(wire(ttl: 3600)).to include("max_age" => 3_600_000_000_000, "duplicate_window" => 120_000_000_000)
    end

    it "asks for s2 when compression is wanted" do
      expect(wire(compression: true)).to include("compression" => "s2")
    end

    it "enables message TTLs together with the limit marker TTL" do
      expect(wire(limit_marker_ttl: 5)).to include("allow_msg_ttl" => true,
                                                   "subject_delete_marker_ttl" => 5_000_000_000)
    end

    it "leaves message TTLs out without a limit marker TTL" do
      expect(wire.keys).not_to include("allow_msg_ttl", "subject_delete_marker_ttl")
    end

    it "sends the size limits, placement, republish and metadata that were given" do
      config = wire(max_bytes: 4096, max_value_size: 512, storage: :memory, replicas: 3, description: "settings",
                    placement: { cluster: "east" }, republish: { source: ">", destination: "copy.>" },
                    metadata: { "owner" => "spec" })

      expect(config).to include("max_bytes" => 4096, "max_msg_size" => 512, "storage" => "memory",
                                "num_replicas" => 3, "description" => "settings",
                                "placement" => { "cluster" => "east" },
                                "republish" => { "src" => ">", "dest" => "copy.>" },
                                "metadata" => { "owner" => "spec" })
    end

    it "mirrors without subjects, with direct access and the KV_ prefix on the origin" do
      config = wire(mirror: { name: "ORIGIN" })

      expect(config).to include("mirror" => { "name" => "KV_ORIGIN" }, "mirror_direct" => true)
      expect(config).not_to have_key("subjects")
    end

    it "keeps a mirror origin that already carries the KV_ prefix" do
      expect(wire(mirror: { name: "KV_ORIGIN" })).to include("mirror" => { "name" => "KV_ORIGIN" })
    end

    it "maps the keys of each source into the bucket" do
      expect(wire(sources: [{ name: "EU" }, { name: "KV_US" }])["sources"]).to eq(
        [{ "name" => "KV_EU", "subject_transforms" => [{ "src" => "$KV.EU.>", "dest" => "$KV.CFG.>" }] },
         { "name" => "KV_US", "subject_transforms" => [{ "src" => "$KV.US.>", "dest" => "$KV.CFG.>" }] }]
      )
    end

    it "passes a source with its own transforms through untouched" do
      source = { name: "ORDERS", subject_transforms: [{ source: "a.>", destination: "$KV.CFG.>" }] }

      expect(wire(sources: [source])["sources"]).to eq(
        [{ "name" => "ORDERS", "subject_transforms" => [{ "src" => "a.>", "dest" => "$KV.CFG.>" }] }]
      )
    end

    it "generates no transform for an external source with the bucket's own name" do
      source = { name: "CFG", external: { api: "$JS.hub.API" } }

      expect(wire(sources: [source])["sources"]).to eq(
        [{ "name" => "KV_CFG", "external" => { "api" => "$JS.hub.API" } }]
      )
    end
  end
end
