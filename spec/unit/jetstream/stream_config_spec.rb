# frozen_string_literal: true

RSpec.describe Tether::JetStream::StreamConfig do
  let(:created) do
    { "name" => "ORDERS", "subjects" => ["orders.>"], "retention" => "limits", "max_consumers" => -1, "max_msgs" => -1,
      "max_bytes" => -1, "max_age" => 0, "max_msgs_per_subject" => -1, "max_msg_size" => -1, "discard" => "old",
      "storage" => "file", "num_replicas" => 1, "duplicate_window" => 120_000_000_000, "compression" => "none",
      "allow_direct" => true, "mirror_direct" => false, "sealed" => false, "deny_delete" => false,
      "deny_purge" => false, "allow_rollup_hdrs" => false, "consumer_limits" => {}, "allow_msg_ttl" => false,
      "metadata" => { "_nats.level" => "5", "_nats.req.level" => "0", "_nats.ver" => "2.16.0-dev" } }
  end

  describe ".new" do
    it "rejects a field the server does not define" do
      expect { described_class.new(name: "ORDERS", subject: "orders.>") }
        .to raise_error(ArgumentError, "unknown StreamConfig field: subject")
    end

    it "rejects a retention policy that does not exist" do
      expect { described_class.new(retention: :forever) }.to raise_error(ArgumentError, /retention: :forever/)
    end
  end

  describe ".from_h" do
    it "reads the configuration the server echoes after a create" do
      config = described_class.from_h(created)

      expect(config).to have_attributes(
        name: "ORDERS", subjects: ["orders.>"], retention: :limits, max_consumers: -1, max_messages: -1,
        max_bytes: -1, max_age: 0, max_messages_per_subject: -1, max_message_size: -1, discard: :old,
        storage: :file, replicas: 1, duplicate_window: 120, compression: :none, allow_direct: true,
        mirror_direct: false, sealed: false, deny_delete: false, deny_purge: false, allow_rollup: false,
        allow_message_ttl: false
      )
    end

    it "reads the consumer limits as their own structure" do
      config = described_class.from_h("consumer_limits" => { "inactive_threshold" => 5_000_000_000 })

      expect(config.consumer_limits).to eq(Tether::JetStream::ConsumerLimits.new(inactive_threshold: 5))
    end

    it "reads the work queue retention string" do
      expect(described_class.from_h("retention" => "workqueue").retention).to eq(:work_queue)
    end

    it "reads a mirror with its start time and external stream" do
      mirror = described_class.from_h(
        "mirror" => { "name" => "ORIGIN", "opt_start_time" => "2026-01-01T00:00:00Z",
                      "external" => { "api" => "$JS.hub.API", "deliver" => "deliver.hub" } }
      ).mirror

      expect(mirror).to have_attributes(name: "ORIGIN", start_time: Time.utc(2026, 1, 1))
      expect(mirror.external).to have_attributes(api: "$JS.hub.API", deliver: "deliver.hub")
    end

    it "reads the subject transforms of a source" do
      sources = described_class.from_h(
        "sources" => [{ "name" => "A", "subject_transforms" => [{ "src" => "a.>", "dest" => "b.>" }] }]
      ).sources

      expect(sources.first.subject_transforms.first).to have_attributes(source: "a.>", destination: "b.>")
    end
  end

  describe "#to_h" do
    it "writes each field under the key the server defines" do
      config = described_class.new(
        name: "ORDERS", description: "all orders", subjects: ["orders.>"], retention: :work_queue, max_consumers: 2,
        max_messages: 10, max_bytes: 1024, max_age: 60, max_messages_per_subject: 5, max_message_size: 512,
        discard: :new, storage: :memory, replicas: 3, no_ack: true, duplicate_window: 30, compression: :s2,
        first_sequence: 100, allow_direct: true, mirror_direct: true, discard_new_per_subject: true, sealed: true,
        deny_delete: true, deny_purge: true, allow_rollup: true, metadata: { "owner" => "billing" }
      )

      expect(config.to_h).to eq(
        "name" => "ORDERS", "description" => "all orders", "subjects" => ["orders.>"], "retention" => "workqueue",
        "max_consumers" => 2, "max_msgs" => 10, "max_bytes" => 1024, "max_age" => 60_000_000_000,
        "max_msgs_per_subject" => 5, "max_msg_size" => 512, "discard" => "new", "storage" => "memory",
        "num_replicas" => 3, "no_ack" => true, "duplicate_window" => 30_000_000_000, "compression" => "s2",
        "first_seq" => 100, "allow_direct" => true, "mirror_direct" => true, "discard_new_per_subject" => true,
        "sealed" => true, "deny_delete" => true, "deny_purge" => true, "allow_rollup_hdrs" => true,
        "metadata" => { "owner" => "billing" }
      )
    end

    it "writes the per-message feature flags under the keys the server defines" do
      config = described_class.new(allow_message_ttl: true, subject_delete_marker_ttl: 60, allow_message_counter: true,
                                   allow_atomic: true, allow_message_schedules: true, persist_mode: :async,
                                   allow_batched: true)

      expect(config.to_h).to eq(
        "allow_msg_ttl" => true, "subject_delete_marker_ttl" => 60_000_000_000, "allow_msg_counter" => true,
        "allow_atomic" => true, "allow_msg_schedules" => true, "persist_mode" => "async", "allow_batched" => true
      )
    end

    it "writes placement, republish and the subject transform as nested objects" do
      config = described_class.new(
        placement: { cluster: "east", tags: ["ssd"] },
        republish: { source: "orders.>", destination: "copy.>", headers_only: true },
        subject_transform: { source: "orders.>", destination: "o.>" },
        consumer_limits: { inactive_threshold: 10, max_ack_pending: 100 }
      )

      expect(config.to_h).to eq(
        "placement" => { "cluster" => "east", "tags" => ["ssd"] },
        "republish" => { "src" => "orders.>", "dest" => "copy.>", "headers_only" => true },
        "subject_transform" => { "src" => "orders.>", "dest" => "o.>" },
        "consumer_limits" => { "inactive_threshold" => 10_000_000_000, "max_ack_pending" => 100 }
      )
    end

    it "writes a source with its start sequence, filter and consumer" do
      config = described_class.new(sources: [{ name: "A", start_sequence: 5, filter_subject: "a.>" }],
                                   mirror: { name: "B", consumer: { "name" => "copier" } })

      expect(config.to_h).to eq(
        "sources" => [{ "name" => "A", "opt_start_seq" => 5, "filter_subject" => "a.>" }],
        "mirror" => { "name" => "B", "consumer" => { "name" => "copier" } }
      )
    end

    it "sends back what the server echoed without the metadata the server owns" do
      expect(described_class.from_h(created).to_h).to eq(created.except("metadata"))
    end

    it "keeps a field of a newer server when one field is changed" do
      config = described_class.from_h(created.merge("allow_teleport" => true)).with(max_age: 3600)

      expect(config.to_h).to include("allow_teleport" => true, "max_age" => 3_600_000_000_000)
    end
  end

  describe "#api_level" do
    it "is 0 for a configuration that uses no newer feature" do
      expect(described_class.from_h(created).api_level).to eq(0)
    end

    it "is 1 when messages may carry a TTL" do
      expect(described_class.new(allow_message_ttl: true).api_level).to eq(1)
    end

    it "is 1 when subject delete markers are on" do
      expect(described_class.new(subject_delete_marker_ttl: 60).api_level).to eq(1)
    end

    it "is 0 when the subject delete marker TTL is zero" do
      expect(described_class.new(subject_delete_marker_ttl: 0).api_level).to eq(0)
    end

    it "is 2 for a counter stream" do
      expect(described_class.new(allow_message_counter: true).api_level).to eq(2)
    end

    it "is 2 when atomic batches are allowed" do
      expect(described_class.new(allow_atomic: true).api_level).to eq(2)
    end

    it "is 2 when message schedules are allowed" do
      expect(described_class.new(allow_message_schedules: true).api_level).to eq(2)
    end

    it "is 2 for the asynchronous persist mode" do
      expect(described_class.new(persist_mode: :async).api_level).to eq(2)
    end

    it "is 0 for the default persist mode" do
      expect(described_class.new(persist_mode: :default).api_level).to eq(0)
    end

    it "is 4 when fast batches are allowed" do
      expect(described_class.new(allow_batched: true, allow_atomic: true).api_level).to eq(4)
    end
  end
end
