# frozen_string_literal: true

module Tether
  class KeyValue
    # What the server knows about a bucket at one moment: a view over the info of its stream.
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

      # @return [Integer] stored revisions, tombstones included; not the number of keys
      def values
        @stream_info.state.messages.to_i
      end

      # @return [Integer] revisions kept per key
      def history
        @stream_info.config.max_messages_per_subject.to_i
      end

      # @return [Numeric, nil] seconds a revision lives, or nil when revisions never expire
      def ttl
        positive(@stream_info.config.max_age)
      end

      # @return [Numeric, nil] seconds the marker of an expired key lives, or nil without limit markers
      def limit_marker_ttl
        positive(@stream_info.config.subject_delete_marker_ttl)
      end

      # @return [Integer] bytes the bucket occupies
      def bytes
        @stream_info.state.bytes.to_i
      end

      # @return [Boolean] whether the bucket's data is compressed
      def compressed?
        ![nil, :none].include?(@stream_info.config.compression)
      end

      # @return [String] the kind of store behind the bucket, always "JetStream"
      def backing_store
        BACKING_STORE
      end

      # @return [Hash{String => String}] the bucket's metadata
      def metadata
        @stream_info.config.metadata || {}
      end

      # @return [Config] the bucket options the stream's configuration amounts to
      def config
        Config.from_stream(@stream_info.config)
      end

      # @return [String] the bucket and its counters
      def inspect
        "#<Tether::KeyValue::Status bucket=#{@bucket.inspect} values=#{values} history=#{history} bytes=#{bytes}>"
      end

      private

      def positive(seconds)
        seconds&.positive? ? seconds : nil
      end
    end
  end
end
