# frozen_string_literal: true

module Tether
  class KeyValue
    # One revision of a key: a value, or the tombstone a delete or purge left.
    class Entry
      OPERATION = "KV-Operation"
      MARKER_REASON = "Nats-Marker-Reason"
      OPERATIONS = { "DEL" => :delete, "PURGE" => :purge }.freeze
      MARKERS = { "MaxAge" => :purge, "Purge" => :purge, "Remove" => :delete }.freeze

      # @return [String] the bucket the entry came from
      attr_reader :bucket

      # @return [String] the key
      attr_reader :key

      # @return [String] the value, always binary; empty for a tombstone and for a watcher with `meta_only:`
      attr_reader :value

      # @return [Integer] the revision: the sequence of the message in the bucket's stream
      attr_reader :revision

      # @return [Time] when the server stored the revision
      attr_reader :created_at

      # @return [Symbol] :put, :delete or :purge
      attr_reader :operation

      # @return [Integer] how many revisions a watcher still had to deliver after this one; 0 from a get
      attr_reader :delta

      # Decides what a stored message is from its headers: `KV-Operation` first, then the reason of a
      # marker the server wrote. Any other `KV-Operation` value is a delete.
      #
      # @param headers [Protocol::Headers, nil] the headers of the stored message
      # @return [Symbol] :put, :delete or :purge
      def self.operation_of(headers)
        return :put unless headers

        operation = headers[OPERATION]
        return OPERATIONS.fetch(operation, :delete) unless operation.to_s.empty?

        reason = headers[MARKER_REASON]
        reason.to_s.empty? ? :put : MARKERS.fetch(reason, :delete)
      end

      # @param bucket [String] the bucket the entry came from
      # @param key [String] the key
      # @param value [String] the value
      # @param revision [Integer] the revision
      # @param created_at [Time] when the server stored the revision
      # @param operation [Symbol] :put, :delete or :purge
      # @param delta [Integer] revisions that were still to come when a watcher delivered this one
      def initialize(bucket:, key:, value:, revision:, created_at:, operation: :put, delta: 0)
        @bucket = bucket
        @key = key
        @value = value
        @revision = revision
        @created_at = created_at
        @operation = operation
        @delta = delta
        freeze
      end

      # @return [Boolean] whether the entry carries a value
      def put?
        @operation == :put
      end

      # @return [Boolean] whether the entry marks the key as deleted or purged
      def tombstone?
        @operation != :put
      end

      # @param other [Object] another object
      # @return [Boolean] whether both are the same revision of the same key in the same bucket
      def ==(other)
        other.is_a?(Entry) && other.bucket == @bucket && other.key == @key && other.revision == @revision
      end
      alias eql? ==

      # @return [Integer] a hash of the bucket, key and revision
      def hash
        [@bucket, @key, @revision].hash
      end

      # @return [String] the key, revision and operation without the value
      def inspect
        "#<Tether::KeyValue::Entry bucket=#{@bucket.inspect} key=#{@key.inspect} revision=#{@revision} " \
          "operation=#{@operation.inspect} bytes=#{@value.bytesize}>"
      end
    end
  end
end
