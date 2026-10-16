# frozen_string_literal: true

module Tether
  class ObjectStore
    # What the server knows about a bucket at one moment.
    class Status
      BACKING_STORE = "JetStream"

      # @return [String] the bucket's name
      attr_reader :bucket

      # @return [JetStream::StreamInfo] the info of the stream behind the bucket
      attr_reader :stream_info

      # @param bucket [String] the bucket's name
      # @param stream_info [JetStream::StreamInfo] the info of the stream behind the bucket
      def initialize(bucket, stream_info)
        @bucket = bucket
        @stream_info = stream_info
        freeze
      end

      # @return [String, nil] the bucket's description
      def description
        config.description
      end

      # @return [Hash{String => String}] the bucket's metadata
      def metadata
        config.metadata || {}
      end

      # @return [Numeric, nil] seconds an object lives; nil when objects never expire
      def ttl
        age = config.max_age
        age&.positive? ? age : nil
      end

      # @return [Symbol] :file or :memory
      def storage
        config.storage
      end

      # @return [Integer] how many servers hold the bucket
      def replicas
        config.replicas
      end

      # @return [Integer, nil] the most bytes the bucket may hold; nil when unlimited
      def max_bytes
        limit = config.max_bytes
        limit&.positive? ? limit : nil
      end

      # @return [Boolean] whether the bucket is sealed against further writes
      def sealed?
        config.sealed == true
      end

      # @return [Boolean] whether the bucket's data is compressed on the server
      def compressed?
        ![nil, :none].include?(config.compression)
      end

      # @return [Integer] bytes the bucket holds, metadata and chunks together
      def size
        @stream_info.state.bytes
      end

      # @return [String] the kind of storage behind the bucket, always `JetStream`
      def backing_store
        BACKING_STORE
      end

      # @return [String] the name of the stream behind the bucket
      def stream
        config.name
      end

      # @return [String] the bucket and its size
      def inspect
        "#<Tether::ObjectStore::Status bucket=#{@bucket.inspect} size=#{size} sealed=#{sealed?}>"
      end

      private

      def config
        @stream_info.config
      end
    end
  end
end
