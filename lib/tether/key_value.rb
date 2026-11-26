# frozen_string_literal: true

module Tether
  # A Key/Value bucket: a stream named `KV_<bucket>` in which a key is a
  # subject, a value is the last message on it and a revision is that
  # message's sequence.
  #
  # Reads are answered by any replica or mirror of the bucket, so a read made
  # right after a write may still see the previous revision. Where the value
  # just written matters, use the revision the write returned.
  class KeyValue
    include Enumerable

    OPERATION = "KV-Operation"
    DELETE = "DEL"
    PURGE = "PURGE"
    PURGE_DELETES_AFTER = 1800
    MIN_API_LEVEL = 1

    # @return [String] the bucket's name
    attr_reader :bucket

    # @return [JetStream] the context this bucket belongs to
    attr_reader :jetstream

    # @return [JetStream::Stream] the stream behind the bucket
    attr_reader :stream

    class << self
      # Creates a bucket. Creating one that exists with the same options succeeds.
      #
      # @param jetstream [JetStream] the context
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @param options [Hash{Symbol => Object}] see {Config#initialize}
      # @return [KeyValue] the bucket
      # @raise [BucketExistsError] if the bucket exists with other options
      # @raise [Error] if per-key TTLs are asked of a server that does not implement them
      def create(jetstream, timeout: nil, **options)
        config = Config.new(**options)
        deadline = jetstream.api.deadline(timeout)
        require_key_ttls(jetstream, deadline) if config.limit_marker_ttl || config.allow_message_ttl
        new(jetstream, jetstream.create_stream(config.stream_config, timeout: deadline))
      rescue JetStream::StreamNameInUseError
        raise BucketExistsError, "bucket #{config.bucket} exists with a different configuration"
      end

      # Replaces the options of an existing bucket. Options that are not given return to their defaults.
      #
      # @param jetstream [JetStream] the context
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @param options [Hash{Symbol => Object}] see {Config#initialize}
      # @return [KeyValue] the bucket
      # @raise [BucketNotFoundError] if the bucket does not exist
      def update(jetstream, timeout: nil, **options)
        config = Config.new(**options)
        new(jetstream, jetstream.update_stream(config.stream_config, timeout: jetstream.api.deadline(timeout)))
      rescue JetStream::StreamNotFoundError
        raise BucketNotFoundError, "bucket #{config.bucket} not found"
      end

      # Replaces the options of a bucket, or creates it when it does not exist.
      #
      # @param jetstream [JetStream] the context
      # @param timeout [Numeric, Deadline, nil] how long both attempts together may take
      # @param options [Hash{Symbol => Object}] see {Config#initialize}
      # @return [KeyValue] the bucket
      def create_or_update(jetstream, timeout: nil, **options)
        deadline = jetstream.api.deadline(timeout)
        update(jetstream, timeout: deadline, **options)
      rescue BucketNotFoundError
        create(jetstream, timeout: deadline, **options)
      end

      # Looks a bucket up.
      #
      # @param jetstream [JetStream] the context
      # @param bucket [String] the bucket's name
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @return [KeyValue] the bucket
      # @raise [BucketNotFoundError] if the bucket does not exist
      # @raise [BadBucketError] if the stream behind the name is not a bucket
      # @raise [InvalidBucketNameError] if the name breaks the rules
      def bind(jetstream, bucket, timeout: nil)
        new(jetstream, jetstream.stream(Naming.stream(Naming.bucket!(bucket)), timeout: timeout))
      rescue JetStream::StreamNotFoundError
        raise BucketNotFoundError, "bucket #{bucket} not found"
      end

      # Deletes a bucket and everything in it.
      #
      # @param jetstream [JetStream] the context
      # @param bucket [String] the bucket's name
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @return [Boolean] whether the server confirmed the deletion
      # @raise [BucketNotFoundError] if the bucket does not exist
      # @raise [InvalidBucketNameError] if the name breaks the rules
      def delete(jetstream, bucket, timeout: nil)
        jetstream.delete_stream(Naming.stream(Naming.bucket!(bucket)), timeout: timeout)
      rescue JetStream::StreamNotFoundError
        raise BucketNotFoundError, "bucket #{bucket} not found"
      end

      # @param jetstream [JetStream] the context
      # @param timeout [Numeric, Deadline, nil] seconds each page request may take, or a deadline for the whole listing
      # @return [Enumerator::Lazy<String>] the name of every bucket, fetched a page at a time
      def names(jetstream, timeout: nil)
        jetstream.stream_names(subject: Naming::ALL_BUCKETS, timeout: timeout)
                 .select { |name| name.start_with?(Naming::STREAM_PREFIX) }
                 .map { |name| Naming.bucket_of(name) }
      end

      # @param jetstream [JetStream] the context
      # @param timeout [Numeric, Deadline, nil] seconds each page request may take, or a deadline for the whole listing
      # @return [Enumerator::Lazy<KeyValue>] every bucket, fetched a page at a time
      def list(jetstream, timeout: nil)
        jetstream.streams(subject: Naming::ALL_BUCKETS, timeout: timeout)
                 .select { |stream| stream.name.start_with?(Naming::STREAM_PREFIX) }
                 .map { |stream| new(jetstream, stream) }
      end

      private

      def require_key_ttls(jetstream, deadline)
        level = jetstream.account_info(timeout: deadline).api.to_h["level"].to_i
        return if level >= MIN_API_LEVEL

        raise Error, "per-key TTLs are not supported by the server (JetStream API level #{level})"
      end
    end

    # @param jetstream [JetStream] the context
    # @param stream [JetStream::Stream] the stream behind the bucket, as the server described it
    # @raise [BadBucketError] if the stream does not keep at least one message per subject
    def initialize(jetstream, stream)
      @jetstream = jetstream
      @stream = stream
      @bucket = Naming.bucket_of(stream.name)
      raise BadBucketError, "stream #{stream.name} is not a bucket" if stream.config.max_messages_per_subject.to_i < 1

      origin = stream.config.mirror
      @read_prefix = Naming.prefix(origin ? Naming.bucket_of(origin.name) : @bucket)
      @write_prefix = write_prefix(origin&.external&.api)
    end

    # @api private
    # @return [String] the subject prefix the bucket's keys are stored under, with the trailing dot
    attr_reader :read_prefix

    # Reads the latest revision of a key, or one particular revision.
    #
    # @param key [String] the key
    # @param revision [Integer, nil] the revision to read; nil reads the latest
    # @param timeout [Numeric, Deadline, nil] how long to wait
    # @return [Entry, nil] the entry; nil when the key is absent, deleted or purged, or when the
    #   revision is a tombstone or belongs to another key
    # @raise [InvalidKeyError] if the key breaks the rules
    def get(key, revision: nil, timeout: nil)
      entry = load(Naming.key!(key), revision, @jetstream.api.deadline(timeout))
      entry if entry&.put?
    end

    # @param key [String] the key
    # @return [String, nil] the key's value, or nil when the key is absent, deleted or purged
    # @raise [InvalidKeyError] if the key breaks the rules
    def [](key)
      get(key)&.value
    end

    # Writes a value, whatever the key holds.
    #
    # @param key [String] the key
    # @param value [String] the value
    # @return [Integer] the new revision
    # @raise [InvalidKeyError] if the key breaks the rules or is reserved
    def []=(key, value)
      put(key, value)
    end

    # Reads the value of a key that must exist.
    #
    # @param key [String] the key
    # @param timeout [Numeric, Deadline, nil] how long to wait
    # @yieldparam key [String] the key, when it is absent, deleted or purged
    # @return [String, Object] the value, or what the block returns for a missing key
    # @raise [KeyNotFoundError] if the key is missing and no block is given
    # @raise [InvalidKeyError] if the key breaks the rules
    def fetch(key, timeout: nil)
      entry = get(key, timeout: timeout)
      return entry.value if entry
      return yield(key) if block_given?

      raise KeyNotFoundError, "key #{key} not found in bucket #{@bucket}"
    end

    # Writes a value, whatever the key holds.
    #
    # @param key [String] the key
    # @param value [String] the value
    # @param timeout [Numeric, Deadline, nil] how long to wait
    # @return [Integer] the new revision
    # @raise [InvalidKeyError] if the key breaks the rules or is reserved
    def put(key, value, timeout: nil)
      write(Naming.writable_key!(key), value, @jetstream.api.deadline(timeout))
    end

    # Writes a value only if the key is absent, deleted or purged.
    #
    # @param key [String] the key
    # @param value [String] the value
    # @param ttl [Numeric, nil] seconds the key lives; the bucket must have a `limit_marker_ttl` or `allow_message_ttl`
    # @param timeout [Numeric, Deadline, nil] how long every step together may take
    # @return [Integer] the new revision
    # @raise [KeyExistsError] if the key holds a value
    # @raise [InvalidKeyError] if the key breaks the rules or is reserved
    # @raise [JetStream::APIError] if the server refuses the TTL
    def create(key, value, ttl: nil, timeout: nil)
      key = Naming.writable_key!(key)
      ttl = lifetime(ttl)
      deadline = @jetstream.api.deadline(timeout)
      begin
        write(key, value, deadline, revision: 0, ttl: ttl)
      rescue JetStream::WrongLastSequenceError
        create_over_tombstone(key, value, ttl, deadline)
      end
    end

    # Writes a value only if the key's latest revision is the given one.
    #
    # @param key [String] the key
    # @param value [String] the value
    # @param revision [Integer] the revision the key must be at; 0 means the key has no revision at all
    # @param timeout [Numeric, Deadline, nil] how long to wait
    # @return [Integer] the new revision
    # @raise [RevisionMismatchError] if the key is at another revision
    # @raise [InvalidKeyError] if the key breaks the rules or is reserved
    def update(key, value, revision:, timeout: nil)
      key = Naming.writable_key!(key)
      guarded(key) { write(key, value, @jetstream.api.deadline(timeout), revision: Integer(revision)) }
    end

    # Marks a key as deleted. Its earlier revisions stay in the history.
    #
    # @param key [String] the key
    # @param revision [Integer, nil] delete only if the key's latest revision is this one
    # @param timeout [Numeric, Deadline, nil] how long to wait
    # @return [Integer] the revision of the tombstone
    # @raise [RevisionMismatchError] if the key is at another revision
    # @raise [InvalidKeyError] if the key breaks the rules or is reserved
    def delete(key, revision: nil, timeout: nil)
      key = Naming.writable_key!(key)
      guarded(key) do
        write(key, nil, @jetstream.api.deadline(timeout), revision: expected(revision), operation: DELETE)
      end
    end

    # Marks a key as deleted and drops its earlier revisions.
    #
    # @param key [String] the key
    # @param revision [Integer, nil] purge only if the key's latest revision is this one
    # @param ttl [Numeric, nil] seconds after which the tombstone itself goes away; the bucket must have
    #   a `limit_marker_ttl` or `allow_message_ttl`
    # @param timeout [Numeric, Deadline, nil] how long to wait
    # @return [Integer] the revision of the tombstone
    # @raise [RevisionMismatchError] if the key is at another revision
    # @raise [InvalidKeyError] if the key breaks the rules or is reserved
    # @raise [JetStream::APIError] if the server refuses the TTL
    def purge(key, revision: nil, ttl: nil, timeout: nil)
      key = Naming.writable_key!(key)
      guarded(key) do
        write(key, nil, @jetstream.api.deadline(timeout), revision: expected(revision), operation: PURGE,
                                                          ttl: lifetime(ttl))
      end
    end

    # Reclaims the space of deleted keys: removes the tombstones older than the threshold together with
    # everything stored for their keys, and keeps younger tombstones, without the revisions before them,
    # so that watchers and mirrors that lag behind still see the delete.
    #
    # @param older_than [Numeric] the age in seconds from which a tombstone is removed; 0 or less removes all
    # @param timeout [Numeric, Deadline, nil] seconds each step may take, or a deadline for the whole call
    # @return [Integer] how many tombstones were removed
    def purge_deletes(older_than: PURGE_DELETES_AFTER, timeout: nil)
      tombstones = []
      initial([], timeout) { |entry| tombstones << entry if entry.tombstone? }
      limit = Time.now - older_than
      tombstones.count do |entry|
        keep = older_than.positive? && entry.created_at > limit
        @stream.purge(subject: "#{@read_prefix}#{entry.key}", keep: (1 if keep),
                      timeout: @jetstream.api.deadline(timeout))
        !keep
      end
    end

    # Lists the keys that hold a value.
    #
    # @param filters [Array<String>] only keys matching any of these: keys, or patterns with `*` and a final `>`
    # @param timeout [Numeric, Deadline, nil] seconds to wait for each key, or a deadline for the whole listing
    # @return [Enumerator::Lazy<String>] the keys, read from the server as they are consumed
    # @raise [InvalidKeyError] if a filter breaks the rules
    def keys(*filters, timeout: nil)
      filters = filters.flatten.map { |filter| Naming.filter!(filter) }
      Enumerator.new do |yielder|
        initial(filters, timeout, ignore_deletes: true, meta_only: true) { |entry| yielder << entry.key }
      end.lazy
    end

    # Reads the revisions the bucket still holds for a key, tombstones included.
    #
    # @param key [String] the key
    # @param timeout [Numeric, Deadline, nil] seconds to wait for each revision, or a deadline for all of them
    # @return [Array<Entry>] the revisions, oldest first; empty when the key was never written or was removed
    # @raise [InvalidKeyError] if the key breaks the rules
    def history(key, timeout: nil)
      entries = []
      initial([Naming.key!(key)], timeout, include_history: true) { |entry| entries << entry }
      entries
    end

    # Yields the latest entry of every key that holds a value.
    #
    # @param timeout [Numeric, Deadline, nil] seconds to wait for each entry, or a deadline for all of them
    # @yieldparam entry [Entry] each entry, on the calling thread
    # @return [self, Enumerator<Entry>] this bucket, or an enumerator when no block is given
    def each(timeout: nil, &block)
      return enum_for(:each, timeout: timeout) unless block

      initial([], timeout, ignore_deletes: true, &block)
      self
    end

    # Watches keys: first what the bucket holds for them, then every change.
    #
    # @param keys [Array<String>] keys, or patterns with `*` and a final `>`; none watches every key
    # @param options [Hash{Symbol => Object}] `include_history:`, `updates_only:`, `ignore_deletes:`,
    #   `meta_only:`, `resume_from:`, `on_ready:` and `on_error:`; see {Watcher#initialize}
    # @yieldparam entry [Entry] each entry, on the watcher's worker thread
    # @return [Watcher] the watcher; without a block, take entries from it with {Watcher#next} or {Watcher#each}
    # @raise [InvalidKeyError] if a filter breaks the rules
    # @raise [BucketNotFoundError] if the bucket is gone
    def watch(*keys, **options, &block)
      Watcher.new(self, keys.flatten, block, **options)
    end

    # Reads the bucket's status from the server.
    #
    # @param timeout [Numeric, Deadline, nil] how long to wait
    # @return [Status] the status
    # @raise [BucketNotFoundError] if the bucket is gone
    def status(timeout: nil)
      Status.new(@bucket, @stream.info(timeout: timeout))
    rescue JetStream::StreamNotFoundError
      raise BucketNotFoundError, "bucket #{@bucket} not found"
    end

    # @return [Config] the bucket's options as last read from the server
    def config
      Config.from_stream(@stream.config)
    end

    # @return [String] the bucket's name
    def inspect
      "#<Tether::KeyValue bucket=#{@bucket.inspect}>"
    end

    private

    def write_prefix(external_api)
      prefix = @jetstream.api.prefix
      return "#{external_api}.#{@read_prefix}" if external_api
      return @read_prefix if prefix == JetStream::DEFAULT_PREFIX

      "#{prefix}.#{@read_prefix}"
    end

    def write(key, value, deadline, revision: nil, operation: nil, ttl: nil)
      options = { headers: operation && { OPERATION => operation }, ttl: ttl, timeout: deadline,
                  expect: revision && { last_subject_sequence: revision }, rollup: (:subject if operation == PURGE) }
      @jetstream.publish("#{@write_prefix}#{key}", value, **options).sequence
    end

    def load(key, revision, deadline)
      subject = "#{@read_prefix}#{key}"
      request = revision ? [Integer(revision)] : []
      message = @stream.get(*request, last_for: (subject unless revision), timeout: deadline)
      return nil unless message.subject == subject

      Entry.new(bucket: @bucket, key: key, value: message.data, revision: message.sequence,
                created_at: message.time, operation: Entry.operation_of(message.headers))
    rescue JetStream::MessageNotFoundError
      nil
    end

    def create_over_tombstone(key, value, ttl, deadline)
      latest = load(key, nil, deadline)
      raise KeyExistsError, "key #{key} exists in bucket #{@bucket}" unless latest&.tombstone?

      write(key, value, deadline, revision: latest.revision, ttl: ttl)
    rescue JetStream::WrongLastSequenceError
      raise KeyExistsError, "key #{key} exists in bucket #{@bucket}"
    end

    def guarded(key)
      yield
    rescue JetStream::WrongLastSequenceError => e
      raise RevisionMismatchError, "key #{key} in bucket #{@bucket} is at another revision: #{e.description}"
    end

    def expected(revision)
      revision &&= Integer(revision)
      revision&.positive? ? revision : nil
    end

    def lifetime(ttl)
      return nil if ttl.nil?
      return ttl if ttl.is_a?(Numeric) && ttl.positive?

      raise ArgumentError, "ttl: must be a positive number of seconds, got #{ttl.inspect}"
    end

    def initial(filters, timeout, **options, &block)
      watcher = Watcher.new(self, filters, nil, **options)
      watcher.each_initial(timeout, &block)
    ensure
      watcher&.stop
    end
  end
end

require "tether/key_value/errors"
require "tether/key_value/naming"
require "tether/key_value/config"
require "tether/key_value/entry"
require "tether/key_value/status"
require "tether/key_value/watcher"
