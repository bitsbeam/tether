# frozen_string_literal: true

module Tether
  class JetStream
    # Where a stream's replicas should be placed.
    class Placement < Schema
      field :cluster, :string
      field :tags, [:string]
      field :preferred, :string
    end

    # A subject mapping: `src` selects, `dest` rewrites.
    class SubjectTransform < Schema
      field :source, :string, key: "src"
      field :destination, :string, key: "dest"
    end

    # Republishing of stored messages to another subject.
    class Republish < Schema
      field :source, :string, key: "src"
      field :destination, :string, key: "dest"
      field :headers_only, :boolean
    end

    # A stream in another account or domain.
    class ExternalStream < Schema
      field :api, :string
      field :deliver, :string
    end

    # A stream that a mirror or a sourcing stream copies from.
    class StreamSource < Schema
      field :name, :string
      field :start_sequence, :integer, key: "opt_start_seq"
      field :start_time, :time, key: "opt_start_time"
      field :filter_subject, :string
      field :subject_transforms, [SubjectTransform]
      field :external, ExternalStream
      field :consumer, :object
    end

    # Defaults and caps a stream applies to its consumers.
    class ConsumerLimits < Schema
      field :inactive_threshold, :duration
      field :max_ack_pending, :integer
    end

    # The configuration of a stream.
    class StreamConfig < Schema
      field :name, :string
      field :description, :string
      field :subjects, [:string]
      field :retention, :enum, values: { limits: "limits", interest: "interest", work_queue: "workqueue" }
      field :max_consumers, :integer
      field :max_messages, :integer, key: "max_msgs"
      field :max_bytes, :integer
      field :max_age, :duration
      field :max_messages_per_subject, :integer, key: "max_msgs_per_subject"
      field :max_message_size, :integer, key: "max_msg_size"
      field :discard, :enum, values: { old: "old", new: "new" }
      field :storage, :enum, values: { file: "file", memory: "memory" }
      field :replicas, :integer, key: "num_replicas"
      field :no_ack, :boolean
      field :duplicate_window, :duration
      field :placement, Placement
      field :mirror, StreamSource
      field :sources, [StreamSource]
      field :compression, :enum, values: { none: "none", s2: "s2" }
      field :first_sequence, :integer, key: "first_seq"
      field :subject_transform, SubjectTransform
      field :republish, Republish
      field :allow_direct, :boolean
      field :mirror_direct, :boolean
      field :discard_new_per_subject, :boolean
      field :sealed, :boolean
      field :deny_delete, :boolean
      field :deny_purge, :boolean
      field :allow_rollup, :boolean, key: "allow_rollup_hdrs"
      field :consumer_limits, ConsumerLimits
      field :allow_message_ttl, :boolean, key: "allow_msg_ttl", level: 1
      field :subject_delete_marker_ttl, :duration, level: 1
      field :allow_message_counter, :boolean, key: "allow_msg_counter", level: 2
      field :allow_atomic, :boolean, level: 2
      field :allow_message_schedules, :boolean, key: "allow_msg_schedules", level: 2
      field :persist_mode, :enum, values: { default: "default", async: "async" }, level: { async: 2 }
      field :allow_batched, :boolean, level: 4
      field :metadata, :object
    end

    # Counters describing what a stream holds.
    class StreamState < Schema
      field :messages, :integer
      field :bytes, :integer
      field :first_sequence, :integer, key: "first_seq"
      field :first_time, :time, key: "first_ts"
      field :last_sequence, :integer, key: "last_seq"
      field :last_time, :time, key: "last_ts"
      field :consumer_count, :integer
      field :subject_count, :integer, key: "num_subjects"
      field :subjects, :object
      field :deleted_count, :integer, key: "num_deleted"
      field :deleted, [:integer]
      field :lost, :object
    end

    # What the server knows about a stream at one moment.
    class StreamInfo < Schema
      field :config, StreamConfig
      field :created, :time
      field :state, StreamState
      field :domain, :string
      field :cluster, :object
      field :mirror, :object
      field :sources, [:object]
      field :alternates, [:object]
      field :time, :time, key: "ts"
    end

    # The limits of an account or of one of its tiers.
    class AccountLimits < Schema
      field :max_memory, :integer
      field :max_storage, :integer
      field :max_streams, :integer
      field :max_consumers, :integer
      field :max_ack_pending, :integer
      field :memory_max_stream_bytes, :integer
      field :storage_max_stream_bytes, :integer
      field :max_bytes_required, :boolean
    end

    # JetStream usage and limits of the connection's account.
    class AccountInfo < Schema
      field :memory, :integer
      field :storage, :integer
      field :reserved_memory, :integer
      field :reserved_storage, :integer
      field :streams, :integer
      field :consumers, :integer
      field :limits, AccountLimits
      field :domain, :string
      field :api, :object
      field :tiers, :object
    end
  end
end
