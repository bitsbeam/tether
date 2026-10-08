# frozen_string_literal: true

module Tether
  class KeyValue
    # The options of a bucket, and the stream configuration they stand for.
    #
    # The stream configuration is the one ADR-8 prescribes, so a bucket made
    # here is the same bucket any other NATS client would have made.
    class Config
      MAX_HISTORY = 64
      DUPLICATE_WINDOW = 120
      MIN_LIMIT_MARKER_TTL = 1
      STORAGES = %i[file memory].freeze

      # @return [String] the bucket's name
      attr_reader :bucket

      # @return [String, nil] a description
      attr_reader :description

      # @return [Integer] revisions kept per key, 1 to 64
      attr_reader :history

      # @return [Numeric, nil] seconds a revision lives; nil keeps revisions until they are replaced
      attr_reader :ttl

      # @return [Integer, nil] the largest size of the bucket in bytes; nil for no limit
      attr_reader :max_bytes

      # @return [Integer, nil] the largest size of one value in bytes; nil for no limit
      attr_reader :max_value_size

      # @return [Symbol] :file or :memory
      attr_reader :storage

      # @return [Integer] how many servers hold the bucket
      attr_reader :replicas

      # @return [Boolean] whether the bucket's data is compressed
      attr_reader :compression

      # @return [Numeric, nil] seconds the marker of an expired key lives; setting it enables per-key TTLs
      attr_reader :limit_marker_ttl

      # @return [JetStream::StreamSource, nil] the bucket this one is a read-only copy of
      attr_reader :mirror

      # @return [Array<JetStream::StreamSource>, nil] buckets whose keys are copied into this one
      attr_reader :sources

      # @return [JetStream::Placement, nil] where the bucket's replicas should be placed
      attr_reader :placement

      # @return [JetStream::Republish, nil] a subject every write is also published on
      attr_reader :republish

      # @return [Hash{String => String}, nil] metadata
      attr_reader :metadata

      # Reads the bucket options back from the configuration of a bucket's stream.
      #
      # @param config [JetStream::StreamConfig] the configuration of a `KV_` stream
      # @return [Config] the options
      def self.from_stream(config)
        new(bucket: Naming.bucket_of(config.name), description: config.description,
            history: config.max_messages_per_subject || 1, ttl: limit(config.max_age),
            max_bytes: limit(config.max_bytes), max_value_size: limit(config.max_message_size),
            storage: config.storage || :file, replicas: config.replicas || 1,
            compression: ![nil, :none].include?(config.compression),
            limit_marker_ttl: limit(config.subject_delete_marker_ttl), mirror: config.mirror,
            sources: config.sources, placement: config.placement, republish: config.republish,
            metadata: config.metadata, validate: false)
      end

      # @param value [Numeric, nil] a limit as the server reports it
      # @return [Numeric, nil] the limit, or nil when the value means there is none
      def self.limit(value)
        value&.positive? ? value : nil
      end
      private_class_method :limit

      # @param bucket [String] the bucket's name: letters, digits, `-` and `_`
      # @param history [Integer] revisions kept per key, 1 to 64
      # @param ttl [Numeric, nil] seconds a revision lives
      # @param max_bytes [Integer, nil] the largest size of the bucket in bytes
      # @param max_value_size [Integer, nil] the largest size of one value in bytes
      # @param storage [Symbol] :file or :memory
      # @param replicas [Integer] how many servers hold the bucket
      # @param compression [Boolean] compress the bucket's data
      # @param description [String, nil] a description
      # @param limit_marker_ttl [Numeric, nil] seconds the marker of an expired key lives, at least 1;
      #   enables per-key TTLs and cannot be turned off again
      # @param mirror [JetStream::StreamSource, Hash, nil] the bucket to mirror: `name:` and optionally `external:`
      # @param sources [Array<JetStream::StreamSource, Hash>, nil] buckets to copy keys from
      # @param placement [JetStream::Placement, Hash, nil] where the bucket's replicas should be placed
      # @param republish [JetStream::Republish, Hash, nil] a subject every write is also published on
      # @param metadata [Hash{String => String}, nil] metadata
      # @param validate [Boolean] false accepts what a stream made by another client holds
      # @raise [InvalidBucketNameError] if the name breaks the rules
      # @raise [ArgumentError] if an option is out of range or two options contradict each other
      def initialize(bucket:, history: 1, ttl: nil, max_bytes: nil, max_value_size: nil, storage: :file,
                     replicas: 1, compression: false, description: nil, limit_marker_ttl: nil, mirror: nil,
                     sources: nil, placement: nil, republish: nil, metadata: nil, validate: true)
        @bucket = validate ? Naming.bucket!(bucket) : bucket.to_s
        @history = history
        @ttl = ttl
        @max_bytes = max_bytes
        @max_value_size = max_value_size
        @storage = storage
        @replicas = replicas
        @compression = compression ? true : false
        @description = description
        @limit_marker_ttl = limit_marker_ttl
        @mirror = mirror && JetStream::StreamSource.coerce(mirror)
        @sources = sources&.map { |source| JetStream::StreamSource.coerce(source) }
        @placement = placement && JetStream::Placement.coerce(placement)
        @republish = republish && JetStream::Republish.coerce(republish)
        @metadata = metadata
        check if validate
        freeze
      end

      # @return [JetStream::StreamConfig] the configuration of the stream behind the bucket
      def stream_config
        JetStream::StreamConfig.new(**identity, **limits, **behavior, **copies)
      end

      # @param other [Object] another object
      # @return [Boolean] whether both describe the same stream
      def ==(other)
        other.is_a?(Config) && other.stream_config == stream_config
      end
      alias eql? ==

      # @return [Integer] a hash of the stream configuration
      def hash
        stream_config.hash
      end

      # @return [String] the bucket and its main options
      def inspect
        "#<Tether::KeyValue::Config bucket=#{@bucket.inspect} history=#{@history} ttl=#{@ttl.inspect} " \
          "storage=#{@storage.inspect} replicas=#{@replicas}>"
      end

      private

      def check
        unless @history.is_a?(Integer) && @history.between?(1, MAX_HISTORY)
          raise ArgumentError, "history: must be between 1 and #{MAX_HISTORY}, got #{@history.inspect}"
        end
        raise ArgumentError, "storage: must be :file or :memory" unless STORAGES.include?(@storage)
        raise ArgumentError, "ttl: must not be negative" if @ttl&.negative?

        check_limit_marker
      end

      def check_limit_marker
        return unless @limit_marker_ttl
        raise ArgumentError, "limit_marker_ttl: cannot be used on a mirror" if @mirror
        return if @limit_marker_ttl >= MIN_LIMIT_MARKER_TTL

        raise ArgumentError, "limit_marker_ttl: must be at least #{MIN_LIMIT_MARKER_TTL} second"
      end

      def identity
        { name: Naming.stream(@bucket), description: @description, subjects: (@mirror ? nil : [subjects]),
          storage: @storage, replicas: @replicas || 1, placement: @placement, republish: @republish,
          metadata: @metadata }
      end

      def limits
        { retention: :limits, discard: :new, max_consumers: -1, max_messages: -1,
          max_bytes: unlimited(@max_bytes), max_message_size: unlimited(@max_value_size),
          max_messages_per_subject: @history, max_age: @ttl || 0, duplicate_window: duplicate_window }
      end

      def behavior
        { deny_delete: true, allow_rollup: true, allow_direct: true, compression: @compression ? :s2 : :none,
          allow_message_ttl: (true if @limit_marker_ttl), subject_delete_marker_ttl: @limit_marker_ttl }
      end

      def copies
        return { mirror: @mirror.with(name: prefixed(@mirror.name)), mirror_direct: true } if @mirror

        { sources: @sources&.map { |source| sourced(source) } }
      end

      def subjects
        "#{Naming.prefix(@bucket)}>"
      end

      def unlimited(value)
        value&.positive? ? value : -1
      end

      def duplicate_window
        @ttl&.positive? && @ttl < DUPLICATE_WINDOW ? @ttl : DUPLICATE_WINDOW
      end

      def prefixed(name)
        name.to_s.start_with?(Naming::STREAM_PREFIX) ? name.to_s : Naming.stream(name)
      end

      def sourced(source)
        return source unless source.subject_transforms.nil? || source.subject_transforms.empty?

        name = prefixed(source.name)
        origin = Naming.bucket_of(name)
        return source.with(name: name) if source.external && origin == @bucket

        transform = { source: "#{Naming.prefix(origin)}>", destination: subjects }
        source.with(name: name, subject_transforms: [transform])
      end
    end
  end
end
