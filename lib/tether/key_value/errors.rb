# frozen_string_literal: true

module Tether
  class KeyValue
    # Base class for every error Key/Value raises.
    class Error < Tether::Error; end

    # No bucket with this name exists.
    class BucketNotFoundError < Error; end

    # A bucket with this name exists with a different configuration.
    class BucketExistsError < Error; end

    # The stream behind the name is not usable as a bucket.
    class BadBucketError < Error; end

    # The key does not exist, or its latest revision is a tombstone.
    class KeyNotFoundError < Error; end

    # The key cannot be used: it breaks the character rules or is reserved.
    class InvalidKeyError < Error; end

    # The bucket name breaks the character rules or is too long.
    class InvalidBucketNameError < Error; end

    # The key's latest revision is not the one the write expected.
    class RevisionMismatchError < Error; end

    # The key already holds a value, so it cannot be created.
    class KeyExistsError < RevisionMismatchError; end
  end
end
