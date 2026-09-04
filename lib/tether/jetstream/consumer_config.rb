# frozen_string_literal: true

module Tether
  class JetStream
    # The configuration of a pull consumer.
    #
    # The fields that make a consumer a push consumer are not declared, so one
    # cannot be created by accident. A push consumer made by another client
    # still decodes: its extra keys are in `raw`, and {#push?} is true.
    class ConsumerConfig < Schema
      field :name, :string
      field :durable, :string, key: "durable_name"
      field :description, :string
      field :deliver_policy, :enum, values: {
        all: "all", last: "last", new: "new", by_start_sequence: "by_start_sequence",
        by_start_time: "by_start_time", last_per_subject: "last_per_subject"
      }
      field :start_sequence, :integer, key: "opt_start_seq"
      field :start_time, :time, key: "opt_start_time"
      field :ack_policy, :enum, values: { none: "none", all: "all", explicit: "explicit" }
      field :ack_wait, :duration
      field :max_deliver, :integer
      field :backoff, [:duration]
      field :filter_subject, :string
      field :filter_subjects, [:string]
      field :replay_policy, :enum, values: { instant: "instant", original: "original" }
      field :sample_frequency, :string, key: "sample_freq"
      field :max_waiting, :integer
      field :max_ack_pending, :integer
      field :headers_only, :boolean
      field :max_batch, :integer
      field :max_expires, :duration
      field :max_bytes, :integer
      field :inactive_threshold, :duration
      field :replicas, :integer, key: "num_replicas"
      field :memory_storage, :boolean, key: "mem_storage"
      field :metadata, :object
      field :pause_until, :time, level: 1
      field :priority_groups, [:string], level: 1
      field :priority_policy, :enum, level: 1, values: {
        none: "none", overflow: "overflow", pinned_client: "pinned_client", prioritized: "prioritized"
      }
      field :priority_timeout, :duration, level: 1

      # @return [Boolean] whether this describes a push consumer, which tether does not consume from
      def push?
        !raw["deliver_subject"].to_s.empty?
      end
    end

    # A pair of positions: one in the consumer's delivery order, one in the stream.
    class SequenceInfo < Schema
      field :consumer_sequence, :integer, key: "consumer_seq"
      field :stream_sequence, :integer, key: "stream_seq"
      field :last_active, :time
    end

    # What the server knows about a consumer at one moment.
    class ConsumerInfo < Schema
      field :stream, :string, key: "stream_name"
      field :name, :string
      field :created, :time
      field :config, ConsumerConfig
      field :delivered, SequenceInfo
      field :ack_floor, SequenceInfo
      field :ack_pending_count, :integer, key: "num_ack_pending"
      field :redelivered_count, :integer, key: "num_redelivered"
      field :waiting_count, :integer, key: "num_waiting"
      field :pending_count, :integer, key: "num_pending"
      field :cluster, :object
      field :push_bound, :boolean
      field :paused, :boolean
      field :pause_remaining, :duration
      field :time, :time, key: "ts"
      field :priority_groups, [:object]
    end
  end
end
