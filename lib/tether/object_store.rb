# frozen_string_literal: true

require "json"

module Tether
  # An Object Store bucket: named binary objects of any size, kept in the
  # stream `OBJ_<bucket>` as chunk messages plus one metadata message per name.
  #
  # Calls that make several round trips take `timeout:` as seconds for each
  # wait on the server, or as a {Deadline} that bounds the whole call.
  class ObjectStore
    DEFAULT_CHUNK_SIZE = 128 * 1024
    STREAM_PREFIX = "OBJ_"
    BUCKET_NAME = /\A[a-zA-Z0-9_-]+\z/
    MAX_BUCKET_NAME = 251
    ALL_BUCKETS = "$O.*.C.>"

    # @return [String] the bucket's name
    attr_reader :name

    # @return [JetStream] the context this bucket is reached through
    attr_reader :jetstream

    # @return [JetStream::Stream] the stream behind the bucket
    attr_reader :stream

    class << self
      # Creates a bucket.
      #
      # @api private
      # @param jetstream [JetStream] the context
      # @param bucket [String] the bucket's name
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @param options [Hash{Symbol => Object}] see {JetStream#create_object_store}
      # @return [ObjectStore] the new bucket
      def create(jetstream, bucket:, timeout: nil, **options)
        new(jetstream, jetstream.create_stream(stream_config(check_bucket(bucket), **options), timeout: timeout))
      rescue JetStream::StreamNameInUseError
        raise BucketExistsError, "bucket #{bucket} exists with another configuration"
      end

      # Replaces the options of an existing bucket. Options that are not given return to their defaults.
      #
      # @api private
      # @param jetstream [JetStream] the context
      # @param bucket [String] the bucket's name
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @param options [Hash{Symbol => Object}] see {JetStream#create_object_store}
      # @return [ObjectStore] the bucket
      # @raise [BucketNotFoundError] if the bucket does not exist
      def update(jetstream, bucket:, timeout: nil, **options)
        new(jetstream, jetstream.update_stream(stream_config(check_bucket(bucket), **options), timeout: timeout))
      rescue JetStream::StreamNotFoundError
        raise BucketNotFoundError, "bucket #{bucket} does not exist"
      end

      # Replaces the options of a bucket, or creates it when it does not exist.
      #
      # @api private
      # @param jetstream [JetStream] the context
      # @param bucket [String] the bucket's name
      # @param timeout [Numeric, Deadline, nil] how long both attempts together may take
      # @param options [Hash{Symbol => Object}] see {JetStream#create_object_store}
      # @return [ObjectStore] the bucket
      def create_or_update(jetstream, bucket:, timeout: nil, **options)
        deadline = jetstream.api.deadline(timeout)
        update(jetstream, bucket: bucket, timeout: deadline, **options)
      rescue BucketNotFoundError
        create(jetstream, bucket: bucket, timeout: deadline, **options)
      end

      # Binds to an existing bucket.
      #
      # @api private
      # @param jetstream [JetStream] the context
      # @param bucket [String] the bucket's name
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @return [ObjectStore] the bucket
      # @raise [BucketNotFoundError] if the bucket does not exist
      def bind(jetstream, bucket, timeout: nil)
        new(jetstream, jetstream.stream(stream_name(check_bucket(bucket)), timeout: timeout))
      rescue JetStream::StreamNotFoundError
        raise BucketNotFoundError, "bucket #{bucket} does not exist"
      end

      # Deletes a bucket and everything in it.
      #
      # @api private
      # @param jetstream [JetStream] the context
      # @param bucket [String] the bucket's name
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @return [Boolean] whether the server confirmed the deletion
      # @raise [BucketNotFoundError] if the bucket does not exist
      def delete(jetstream, bucket, timeout: nil)
        jetstream.delete_stream(stream_name(check_bucket(bucket)), timeout: timeout)
      rescue JetStream::StreamNotFoundError
        raise BucketNotFoundError, "bucket #{bucket} does not exist"
      end

      # @api private
      # @param jetstream [JetStream] the context
      # @param timeout [Numeric, Deadline, nil] seconds each page request may take, or a deadline for the listing
      # @return [Enumerator::Lazy<String>] the name of every bucket
      def names(jetstream, timeout: nil)
        jetstream.stream_names(subject: ALL_BUCKETS, timeout: timeout)
                 .select { |name| name.start_with?(STREAM_PREFIX) }
                 .map { |name| name.delete_prefix(STREAM_PREFIX) }
      end

      # @api private
      # @param jetstream [JetStream] the context
      # @param timeout [Numeric, Deadline, nil] seconds each page request may take, or a deadline for the listing
      # @return [Enumerator::Lazy<ObjectStore>] every bucket
      def all(jetstream, timeout: nil)
        jetstream.streams(subject: ALL_BUCKETS, timeout: timeout)
                 .select { |stream| stream.name.start_with?(STREAM_PREFIX) }
                 .map { |stream| new(jetstream, stream) }
      end

      # @api private
      # @param bucket [String] a bucket name
      # @return [String] the name of the stream behind the bucket
      def stream_name(bucket)
        "#{STREAM_PREFIX}#{bucket}"
      end

      # @api private
      # @param bucket [String] a bucket name
      # @param nuid [String] the id of an upload
      # @return [String] the subject the upload's chunks are stored under
      def chunk_subject(bucket, nuid)
        "$O.#{bucket}.C.#{nuid}"
      end

      # @api private
      # @param bucket [String] a bucket name
      # @param name [String] an object name
      # @return [String] the subject the object's metadata is stored under
      def metadata_subject(bucket, name)
        "$O.#{bucket}.M.#{[name].pack("m0").tr("+/", "-_")}"
      end

      # @api private
      # @param bucket [String] a bucket name
      # @return [String] the filter that matches the metadata of every object in the bucket
      def metadata_filter(bucket)
        "$O.#{bucket}.M.>"
      end

      # @api private
      # @param bucket [Object] a bucket name
      # @return [String] the name
      # @raise [InvalidBucketNameError] if it is not made of letters, digits, `_` and `-`, or is too long
      def check_bucket(bucket)
        text = bucket.to_s
        return text if text.match?(BUCKET_NAME) && text.bytesize <= MAX_BUCKET_NAME

        raise InvalidBucketNameError,
              "invalid bucket name: #{bucket.inspect} (letters, digits, '_' and '-', at most #{MAX_BUCKET_NAME})"
      end

      private

      def stream_config(bucket, description: nil, ttl: nil, max_bytes: nil, storage: :file, replicas: 1,
                        compression: false, placement: nil, metadata: nil)
        { name: stream_name(bucket), description: description,
          subjects: [chunk_subject(bucket, ">"), metadata_filter(bucket)], max_age: ttl || 0,
          max_bytes: max_bytes || -1, storage: storage, replicas: replicas || 1, placement: placement,
          discard: :new, allow_rollup: true, allow_direct: true, compression: compression_of(compression),
          metadata: metadata }.compact
      end

      def compression_of(value)
        return nil unless value

        value == true ? :s2 : value
      end
    end

    # @api private
    # @param jetstream [JetStream] the context
    # @param stream [JetStream::Stream] the stream behind the bucket
    def initialize(jetstream, stream)
      @jetstream = jetstream
      @stream = stream
      @name = stream.name.delete_prefix(STREAM_PREFIX)
    end

    # Stores an object, replacing any object of the same name. The source is read in pieces of
    # `chunk_size:` bytes and never held in memory whole; the previous content stays readable until the
    # new one is complete. If anything fails, the chunks already written are removed.
    #
    # @param name [String] the object's name, any non-empty string
    # @param data [String, #read, nil] the content: a string, or anything `read(length)` can be called on
    # @param description [String, nil] free text describing the object
    # @param headers [Protocol::Headers, Hash, nil] headers to store with the object
    # @param metadata [Hash{String => String}, nil] metadata to store with the object
    # @param chunk_size [Integer] the most bytes one chunk message carries
    # @param timeout [Numeric, Deadline, nil] seconds each wait on the server may take, or a deadline for
    #   the whole upload
    # @return [Info] the metadata of the stored object
    # @raise [InvalidObjectNameError] if the name is empty
    # @raise [ArgumentError] if the data cannot be read or the chunk size is not positive
    # @raise [BucketSealedError] if the bucket is sealed
    # @raise [BucketNotFoundError] if the bucket is gone
    # @raise [JetStream::APIError] if the server refuses a chunk, as it does when the bucket is full
    # @raise [TimeoutError] if the server does not answer in time
    def put(name, data = nil, description: nil, headers: nil, metadata: nil, chunk_size: DEFAULT_CHUNK_SIZE,
            timeout: nil)
      name = check_name(name)
      check_source(data, chunk_size)
      meta = Info.new(name: name, bucket: @name, nuid: Connection::NUID.next, description: description,
                      headers: headers, metadata: metadata, chunk_size: chunk_size)
      writing(timeout) do
        previous = lookup(name, timeout, deleted: true)
        stored = upload(meta, data, timeout)
        discard_chunks(previous, timeout) if previous && !previous.deleted?
        stored
      end
    end

    # Stores the content of a file. See {#put}.
    #
    # @param name [String] the object's name
    # @param path [String] the path of the file to read
    # @param options [Hash{Symbol => Object}] the options of {#put}
    # @return [Info] the metadata of the stored object
    def put_file(name, path, **options)
      File.open(path, "rb") { |file| put(name, file, **options) }
    end

    # Reads an object. With a block or `into:` the content is streamed a chunk at a time and its size and
    # digest are verified before the call returns; otherwise a {Reader} is returned that streams and
    # verifies as it is read. A link to an object is followed, one level deep.
    #
    # @param name [String] the object's name
    # @param into [#write, nil] an IO to write the content to
    # @param show_deleted [Boolean] also find a deleted object, which has no content
    # @param timeout [Numeric, Deadline, nil] seconds each wait for the server may take, or a deadline for
    #   the whole download
    # @yieldparam chunk [String] the next bytes, binary
    # @return [Reader, Info] a reader; or the object's metadata when a block or `into:` took the content
    # @raise [ObjectNotFoundError] if no such object exists or it was deleted
    # @raise [DigestMismatchError] if the content does not match the recorded size or digest
    # @raise [LinkError] if the name is a link to a bucket, or leads to another link
    # @raise [BucketNotFoundError] if the bucket, or the bucket a link points into, does not exist
    # @raise [TimeoutError] if the server does not answer in time
    def get(name, into: nil, show_deleted: false, timeout: nil, &block)
      raise ArgumentError, "give either a block or into:, not both" if into && block

      reader = reader_for(check_name(name), show_deleted, timeout)
      return reader unless into || block

      begin
        reader.each { |chunk| into ? into.write(chunk) : block.call(chunk) }
      ensure
        reader.close
      end
      reader.info
    end

    # Reads an object into a file. The file appears under its name only once the content is verified.
    #
    # @param name [String] the object's name
    # @param path [String] the path of the file to write
    # @param options [Hash{Symbol => Object}] `show_deleted:` and `timeout:` of {#get}
    # @return [Info] the object's metadata
    # @raise [ObjectNotFoundError] if no such object exists or it was deleted
    # @raise [DigestMismatchError] if the content does not match the recorded size or digest
    def get_file(name, path, **options)
      partial = "#{path}.#{Connection::NUID.next}.part"
      info = File.open(partial, "wb") { |file| get(name, into: file, **options) }
      File.rename(partial, path)
      info
    ensure
      File.delete(partial) if partial && File.exist?(partial)
    end

    # Reads an object's metadata.
    #
    # @param name [String] the object's name
    # @param show_deleted [Boolean] also return the tombstone of a deleted object
    # @param timeout [Numeric, Deadline, nil] how long to wait
    # @return [Info, nil] the metadata, or nil when no such object exists or it was deleted
    # @raise [InvalidObjectNameError] if the name is empty
    # @raise [BucketNotFoundError] if the bucket is gone
    def info(name, show_deleted: false, timeout: nil)
      lookup(check_name(name), deadline(timeout), deleted: show_deleted)
    end

    # @param name [String] the object's name
    # @param timeout [Numeric, Deadline, nil] how long to wait
    # @return [Boolean] whether a live object or link has this name
    def exist?(name, timeout: nil)
      !info(name, timeout: timeout).nil?
    end

    # Deletes an object: its metadata is replaced by a tombstone and its chunks are removed.
    #
    # @param name [String] the object's name
    # @param timeout [Numeric, Deadline, nil] how long the whole call may take
    # @return [self] this bucket
    # @raise [ObjectNotFoundError] if no object ever had this name
    # @raise [BucketSealedError] if the bucket is sealed
    def delete(name, timeout: nil)
      name = check_name(name)
      limit = deadline(timeout)
      writing(limit) do
        found = lookup(name, limit, deleted: true) or raise missing(name)
        raise Error, "the metadata of #{name} in #{@name} names no chunks" if found.nuid.empty?

        store(found.with(deleted: true, size: 0, chunks: 0, digest: nil), limit)
        @stream.purge(subject: self.class.chunk_subject(@name, found.nuid), timeout: limit)
      end
      self
    end

    # Changes an object's name, description, headers or metadata. The content is not touched. A field
    # left nil keeps its value; give an empty string or hash to clear one.
    #
    # @param current [String] the object's name now
    # @param name [String, nil] a new name
    # @param description [String, nil] a new description
    # @param headers [Protocol::Headers, Hash, nil] new headers
    # @param metadata [Hash{String => String}, nil] new metadata
    # @param timeout [Numeric, Deadline, nil] how long the whole call may take
    # @return [Info] the new metadata
    # @raise [ObjectNotFoundError] if no such object exists or it was deleted
    # @raise [ObjectExistsError] if the new name belongs to a live object
    # @raise [BucketSealedError] if the bucket is sealed
    def update_meta(current, name: nil, description: nil, headers: nil, metadata: nil, timeout: nil)
      current = check_name(current)
      renamed = name.nil? ? current : check_name(name)
      limit = deadline(timeout)
      writing(limit) do
        found = lookup(current, limit) or raise missing(current)
        taken = renamed != current && lookup(renamed, limit, deleted: true)
        raise ObjectExistsError, "an object named #{renamed} exists in #{@name}" if taken && !taken.deleted?

        changes = { name: renamed, description: description, headers: headers, metadata: metadata }.compact
        stored = store(found.with(**changes), limit)
        @stream.purge(subject: self.class.metadata_subject(@name, current), timeout: limit) if renamed != current
        stored
      end
    end

    # Adds a link: a name in this bucket that reads the content of another object, here or in another
    # bucket. A link may replace a link or a deleted object, never a live object.
    #
    # @param name [String] the link's name
    # @param target [Info] the metadata of the object to point at
    # @param timeout [Numeric, Deadline, nil] how long the whole call may take
    # @return [Info] the metadata of the link
    # @raise [LinkError] if the target is deleted or is itself a link
    # @raise [ObjectExistsError] if the name belongs to a live object
    # @raise [BucketSealedError] if the bucket is sealed
    def add_link(name, target, timeout: nil)
      raise ArgumentError, "the target of a link is the Info of an object" unless target.is_a?(Info)
      raise LinkError, "#{target.name} is deleted and cannot be linked to" if target.deleted?
      raise LinkError, "#{target.name} is a link and cannot be linked to" if target.link?

      link(name, Info::Link.new(bucket: target.bucket, name: target.name), timeout)
    end

    # Adds a link to a whole bucket.
    #
    # @param name [String] the link's name
    # @param bucket [ObjectStore, String] the bucket to point at, or its name
    # @param timeout [Numeric, Deadline, nil] how long the whole call may take
    # @return [Info] the metadata of the link
    # @raise [ObjectExistsError] if the name belongs to a live object
    # @raise [BucketSealedError] if the bucket is sealed
    def add_bucket_link(name, bucket, timeout: nil)
      target = bucket.is_a?(ObjectStore) ? bucket.name : self.class.check_bucket(bucket)
      link(name, Info::Link.new(bucket: target, name: nil), timeout)
    end

    # Lists the bucket's objects and links. The consumer behind the listing is removed when the
    # enumeration ends, complete or not.
    #
    # @param deleted [Boolean] include the tombstones of deleted objects
    # @param timeout [Numeric, Deadline, nil] seconds each wait for the server may take, or a deadline for
    #   the whole listing
    # @return [Enumerator::Lazy<Info>] the entries, read as they are consumed; empty for an empty bucket
    # @raise [BucketNotFoundError] if the bucket is gone, when the enumeration starts
    def list(deleted: false, timeout: nil)
      Enumerator.new { |yielder| each_entry(deleted, timeout) { |info| yielder << info } }.lazy
    end

    # Watches the bucket's directory: the current entry of every object, then every change, deletions
    # included. With a block the entries are delivered on a worker thread; without one, take them with
    # {Watcher#next} or {Watcher#each}.
    #
    # @param include_history [Boolean] start from the oldest stored entry instead of the current ones
    # @param ignore_deletes [Boolean] leave the tombstones of deleted objects out
    # @param updates_only [Boolean] skip what exists and deliver only later changes
    # @param on_ready [#call, nil] called once when the initial entries have been delivered
    # @param on_error [#call, nil] receives the watcher's warnings and the error that ends it
    # @yieldparam info [Info] each entry, on the watcher's worker thread
    # @return [Watcher] a handle to stop the watcher and to ask whether it is ready
    # @raise [BucketNotFoundError] if the bucket is gone
    def watch(include_history: false, ignore_deletes: false, updates_only: false, on_ready: nil, on_error: nil,
              &block)
      Watcher.new(@stream, @name, include_history: include_history, ignore_deletes: ignore_deletes,
                                  updates_only: updates_only, on_ready: on_ready, on_error: on_error, &block)
    end

    # Makes the bucket read-only, for good: a sealed bucket cannot be unsealed.
    #
    # @param timeout [Numeric, Deadline, nil] how long the whole call may take
    # @return [self] this bucket
    # @raise [BucketNotFoundError] if the bucket is gone
    def seal(timeout: nil)
      @stream.update(sealed: true, timeout: timeout)
      self
    rescue JetStream::StreamNotFoundError
      raise gone
    end

    # @param timeout [Numeric, Deadline, nil] how long to wait
    # @return [Status] the bucket's configuration and size, read from the server
    # @raise [BucketNotFoundError] if the bucket is gone
    def status(timeout: nil)
      Status.new(@name, @stream.info(timeout: timeout))
    rescue JetStream::StreamNotFoundError
      raise gone
    end

    # @return [String] the bucket's name
    def inspect
      "#<Tether::ObjectStore bucket=#{@name.inspect}>"
    end

    protected

    # @param name [String] a checked object name
    # @param show_deleted [Boolean] also find a deleted object
    # @param timeout [Numeric, Deadline, nil] the caller's timeout
    # @param followed [Boolean] whether a link led here
    # @return [Reader] a reader over the object, or over what its link points at
    def reader_for(name, show_deleted, timeout, followed: false)
      info = lookup(name, timeout, deleted: show_deleted) or raise missing(name)
      raise Error, "the metadata of #{name} in #{@name} names no chunks" if info.nuid.empty?
      return Reader.new(@stream, info, timeout: timeout) unless info.link?
      raise LinkError, "#{name} in #{@name} is a link to the bucket #{info.link.bucket}" if info.bucket_link?
      raise LinkError, "#{name} in #{@name} is a link that leads to another link" if followed

      target = info.link.bucket == @name ? self : self.class.bind(@jetstream, info.link.bucket, timeout: timeout)
      target.reader_for(check_name(info.link.name), show_deleted, timeout, followed: true)
    end

    private

    def deadline(timeout)
      @jetstream.api.deadline(timeout)
    end

    def check_name(name)
      return name.to_s.encode(Encoding::UTF_8) if (name.is_a?(String) || name.is_a?(Symbol)) && !name.empty?

      raise InvalidObjectNameError, "invalid object name: #{name.inspect} (a non-empty string)"
    rescue EncodingError
      raise InvalidObjectNameError, "invalid object name: #{name.inspect} (not valid text)"
    end

    def check_source(data, chunk_size)
      raise ArgumentError, "chunk_size: must be a positive integer" unless chunk_size.is_a?(Integer) && chunk_size >= 1
      return if data.nil? || data.is_a?(String) || data.respond_to?(:read)

      raise ArgumentError, "the data of an object is a String or something to read from, not #{data.class}"
    end

    def lookup(name, timeout, deleted: false)
      limit = deadline(timeout)
      message = @stream.get(last_for: self.class.metadata_subject(@name, name), timeout: limit)
      info = Info.parse(message.data, modified_at: message.time)
      info unless info.deleted? && !deleted
    rescue JetStream::MessageNotFoundError
      nil
    rescue JetStream::ServiceUnavailableError, JetStream::StreamNotFoundError
      raise if exists?(limit)

      raise gone
    end

    def upload(meta, data, timeout)
      writer = Writer.new(@jetstream, self.class.chunk_subject(@name, meta.nuid), meta.chunk_size, timeout)
      done = false
      result = writer.write(data)
      stored = store(meta.with(size: result.size, chunks: result.chunks, digest: result.digest), timeout)
      done = true
      stored
    ensure
      abandon(writer, meta) unless done
    end

    def store(meta, timeout)
      @jetstream.publish(self.class.metadata_subject(@name, meta.name), meta.to_json, rollup: :subject,
                                                                                      timeout: deadline(timeout))
      meta.with(modified_at: Time.now.utc)
    end

    def abandon(writer, meta)
      limit = Deadline.after(@jetstream.timeout)
      writer.settle(limit)
      @stream.purge(subject: self.class.chunk_subject(@name, meta.nuid), timeout: limit)
    rescue Tether::Error
      nil
    end

    def discard_chunks(previous, timeout)
      @stream.purge(subject: self.class.chunk_subject(@name, previous.nuid), timeout: deadline(timeout))
    rescue Tether::Error
      nil
    end

    def link(name, target, timeout)
      name = check_name(name)
      limit = deadline(timeout)
      writing(limit) do
        taken = lookup(name, limit, deleted: true)
        live = taken && !taken.deleted? && !taken.link?
        raise ObjectExistsError, "an object named #{name} exists in #{@name}" if live

        store(Info.new(name: name, bucket: @name, nuid: Connection::NUID.next, link: target), limit)
      end
    end

    def each_entry(deleted, timeout)
      pull = @stream.ordered_consumer(filter_subjects: self.class.metadata_filter(@name),
                                      deliver_policy: :last_per_subject).messages
      until pull.caught_up?
        message = pull.next_initial(timeout: timeout || @jetstream.timeout) or break
        info = entry(message)
        yield info if info && (deleted || !info.deleted?)
      end
    rescue JetStream::StreamNotFoundError
      raise gone
    ensure
      pull&.stop
    end

    def entry(message)
      Info.parse(message.data, modified_at: message.metadata.time)
    rescue Error
      nil
    end

    def writing(timeout)
      yield
    rescue JetStream::StreamSealedError
      raise BucketSealedError, "bucket #{@name} is sealed"
    rescue JetStream::StreamNotFoundError
      raise gone
    rescue JetStream::NoStreamResponseError, JetStream::ServiceUnavailableError
      raise if exists?(deadline(timeout))

      raise gone
    end

    def exists?(limit)
      @stream.info(timeout: limit.expired? ? @jetstream.timeout : limit)
      true
    rescue JetStream::StreamNotFoundError
      false
    rescue Tether::Error
      true
    end

    def missing(name)
      ObjectNotFoundError.new("no object named #{name} in #{@name}")
    end

    def gone
      BucketNotFoundError.new("bucket #{@name} does not exist")
    end
  end
end

require "tether/object_store/errors"
require "tether/object_store/info"
require "tether/object_store/status"
require "tether/object_store/writer"
require "tether/object_store/reader"
require "tether/object_store/watcher"
