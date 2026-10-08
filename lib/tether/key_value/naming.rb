# frozen_string_literal: true

module Tether
  class KeyValue
    # The naming rules of ADR-6 and ADR-8 for buckets, keys and key filters.
    #
    # @api private
    module Naming
      STREAM_PREFIX = "KV_"
      SUBJECT_PREFIX = "$KV."
      ALL_BUCKETS = "$KV.*.>"
      BUCKET = /\A[a-zA-Z0-9_-]+\z/
      MAX_BUCKET_LENGTH = 252
      KEY = %r{\A[-/_=.a-zA-Z0-9]+\z}
      FILTER = %r{\A[-/_=.a-zA-Z0-9*]*>?\z}
      RESERVED = "_kv"
      EVERYTHING = ">"

      module_function

      # @param name [String] a bucket name
      # @return [String] the name
      # @raise [InvalidBucketNameError] if it breaks the rules
      def bucket!(name)
        text = name.to_s
        return text if text.match?(BUCKET) && text.length <= MAX_BUCKET_LENGTH

        raise InvalidBucketNameError,
              "invalid bucket name: #{name.inspect} (letters, digits, '-' and '_', at most #{MAX_BUCKET_LENGTH})"
      end

      # @param key [String] a key to read
      # @return [String] the key
      # @raise [InvalidKeyError] if it breaks the rules
      def key!(key)
        text = key.to_s
        return text if text.match?(KEY) && dots?(text)

        raise InvalidKeyError, "invalid key: #{key.inspect} (letters, digits and '-', '/', '_', '=', '.'; " \
                               "no leading, trailing or doubled '.')"
      end

      # @param key [String] a key to write
      # @return [String] the key
      # @raise [InvalidKeyError] if it breaks the rules or is reserved
      def writable_key!(key)
        text = key!(key)
        return text unless text.start_with?(RESERVED)

        raise InvalidKeyError, "invalid key: #{key.inspect} (keys starting with #{RESERVED} are reserved)"
      end

      # @param filter [String, nil] a key, or a pattern with `*` and a final `>`; empty selects every key
      # @return [String] the filter
      # @raise [InvalidKeyError] if it breaks the rules
      def filter!(filter)
        text = filter.to_s
        return EVERYTHING if text.empty?
        return text if text.match?(FILTER) && dots?(text)

        raise InvalidKeyError, "invalid key filter: #{filter.inspect}"
      end

      # @param name [String] a bucket name
      # @return [String] the name of the stream behind it
      def stream(name)
        "#{STREAM_PREFIX}#{name}"
      end

      # @param stream [String] a stream name
      # @return [String] the bucket it backs
      def bucket_of(stream)
        stream.to_s.delete_prefix(STREAM_PREFIX)
      end

      # @param bucket [String] a bucket name
      # @return [String] the subject prefix of its keys, with the trailing dot
      def prefix(bucket)
        "#{SUBJECT_PREFIX}#{bucket}."
      end

      def dots?(text)
        !text.start_with?(".") && !text.end_with?(".") && !text.include?("..")
      end
    end
  end
end
