# frozen_string_literal: true

module Tether
  class ObjectStore
    # Base class for every error the Object Store raises.
    class Error < Tether::Error; end

    # No bucket with this name exists.
    class BucketNotFoundError < Error; end

    # A bucket with this name exists with a different configuration.
    class BucketExistsError < Error; end

    # The bucket name cannot be used.
    class InvalidBucketNameError < Error; end

    # The object name cannot be used.
    class InvalidObjectNameError < Error; end

    # No object with this name exists, or it was deleted.
    class ObjectNotFoundError < Error; end

    # A live object already uses the name a rename or a link asked for.
    class ObjectExistsError < Error; end

    # The bytes read do not match the size or the digest recorded for the object.
    class DigestMismatchError < Error; end

    # The bucket is sealed and takes no more writes.
    class BucketSealedError < Error; end

    # A link cannot be made or followed.
    class LinkError < Error; end
  end
end
