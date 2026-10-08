# frozen_string_literal: true

module Tether
  # A JetStream context: a cheap handle holding an API prefix, a default
  # timeout and a reference to the connection.
  class JetStream
    DEFAULT_TIMEOUT = 5
    DEFAULT_PREFIX = "$JS.API"
    NAME = %r{\A[^\s.*>/\\]+\z}

    # @return [Client] the connection this context uses
    attr_reader :client

    # @return [API] request plumbing for the JSON API
    attr_reader :api

    # @param client [Client] the connection
    # @param domain [String, nil] a JetStream domain, which selects `$JS.<domain>.API`
    # @param api_prefix [String, nil] an arbitrary API prefix, for cross-account imports
    # @param timeout [Numeric] the default timeout of every API call, in seconds
    # @param publish_async_max_pending [Integer] asynchronous publishes that may await their acknowledgment
    # @param publish_async_timeout [Numeric, nil] seconds after which an unacknowledged asynchronous publish fails
    # @raise [ArgumentError] if both a domain and a prefix are given, or either cannot be part of a subject
    def initialize(client, domain: nil, api_prefix: nil, timeout: DEFAULT_TIMEOUT,
                   publish_async_max_pending: 4000, publish_async_timeout: nil)
      raise ArgumentError, "domain: and api_prefix: cannot be combined" if domain && api_prefix

      @client = client
      @api = API.new(client, prefix: prefix_for(domain, api_prefix), timeout: timeout)
      @publish_async_max_pending = publish_async_max_pending
      @publish_async_timeout = publish_async_timeout
      @lock = Mutex.new
    end

    # @return [Numeric] the default timeout of every API call, in seconds
    def timeout
      @api.timeout
    end

    # Creates a stream.
    #
    # @param config [StreamConfig, Hash, nil] the configuration, or give its fields as keywords
    # @param timeout [Numeric, Deadline, nil] how long to wait
    # @param fields [Hash{Symbol => Object}] {StreamConfig} fields
    # @return [Stream] a handle to the new stream
    # @raise [StreamNameInUseError] if a stream with this name exists with another configuration
    def create_stream(config = nil, timeout: nil, **fields)
      write_stream("CREATE", config, fields, timeout)
    end

    # Replaces the configuration of an existing stream.
    #
    # @param config [StreamConfig, Hash, nil] the whole configuration, or give its fields as keywords
    # @param timeout [Numeric, Deadline, nil] how long to wait
    # @param fields [Hash{Symbol => Object}] {StreamConfig} fields
    # @return [Stream] a handle to the stream
    # @raise [StreamNotFoundError] if the stream does not exist
    def update_stream(config = nil, timeout: nil, **fields)
      write_stream("UPDATE", config, fields, timeout)
    end

    # Creates a stream, or replaces its configuration when it exists.
    #
    # @param config [StreamConfig, Hash, nil] the configuration, or give its fields as keywords
    # @param timeout [Numeric, Deadline, nil] how long to wait
    # @param fields [Hash{Symbol => Object}] {StreamConfig} fields
    # @return [Stream] a handle to the stream
    def create_or_update_stream(config = nil, timeout: nil, **fields)
      deadline = Deadline.coerce(timeout || self.timeout)
      update_stream(config, timeout: deadline, **fields)
    rescue StreamNotFoundError
      create_stream(config, timeout: deadline, **fields)
    end

    # Looks a stream up.
    #
    # @param name [String] the stream's name
    # @param timeout [Numeric, Deadline, nil] how long to wait
    # @return [Stream] a handle to the stream
    # @raise [StreamNotFoundError] if the stream does not exist
    def stream(name, timeout: nil)
      Stream.new(self, StreamInfo.from_h(@api.request("STREAM.INFO.#{check_name(name)}", timeout: timeout)))
    end

    # @param name [String] the stream's name
    # @param timeout [Numeric, Deadline, nil] how long to wait
    # @return [Boolean] whether the stream exists
    def stream?(name, timeout: nil)
      stream(name, timeout: timeout)
      true
    rescue StreamNotFoundError
      false
    end

    # Deletes a stream and everything in it.
    #
    # @param name [String] the stream's name
    # @param timeout [Numeric, Deadline, nil] how long to wait
    # @return [Boolean] whether the server confirmed the deletion
    def delete_stream(name, timeout: nil)
      @api.request("STREAM.DELETE.#{check_name(name)}", timeout: timeout)["success"] == true
    end

    # @param subject [String, nil] only streams that capture a subject matching this filter
    # @param timeout [Numeric, Deadline, nil] seconds each page request may take, or a deadline for the whole listing
    # @return [Enumerator::Lazy<Stream>] every stream, fetched a page at a time
    def streams(subject: nil, timeout: nil)
      filter = subject ? { "subject" => subject } : {}
      @api.pages("STREAM.LIST", "streams", filter, timeout: timeout).map do |info|
        Stream.new(self, StreamInfo.from_h(info))
      end
    end

    # @param subject [String, nil] only streams that capture a subject matching this filter
    # @param timeout [Numeric, Deadline, nil] seconds each page request may take, or a deadline for the whole listing
    # @return [Enumerator::Lazy<String>] every stream name, fetched a page at a time
    def stream_names(subject: nil, timeout: nil)
      @api.pages("STREAM.NAMES", "streams", subject ? { "subject" => subject } : {}, timeout: timeout)
    end

    # @param subject [String] a subject
    # @param timeout [Numeric, Deadline, nil] how long to wait
    # @return [String, nil] the name of the stream that captures the subject
    def stream_name_for(subject, timeout: nil)
      stream_names(subject: subject, timeout: timeout).first
    end

    # Recreates a stream from a snapshot. The block is given a writer and feeds it the bytes
    # {Stream#snapshot} yielded, in order; each chunk is uploaded once the server took the one before.
    #
    # @param config [StreamConfig, Hash] the configuration the snapshot returned
    # @param state [StreamState, Hash] the state the snapshot returned
    # @param timeout [Numeric, Deadline, nil] seconds each chunk may take, or a deadline for the whole restore
    # @yieldparam writer [RestoreWriter] takes the snapshot's bytes with `write`
    # @return [Stream] a handle to the restored stream
    # @raise [ArgumentError] if no block is given
    # @raise [StreamNameInUseError] if a stream with this name exists
    # @raise [APIError] if the server cannot restore the snapshot
    def restore_stream(config, state, timeout: nil)
      raise ArgumentError, "restore_stream needs a block that writes the snapshot" unless block_given?

      config = StreamConfig.coerce(config)
      request = { "config" => config.to_h, "state" => StreamState.coerce(state).to_h }
      response = @api.request("STREAM.RESTORE.#{check_name(config.name)}", request, timeout: @api.deadline(timeout))
      writer = RestoreWriter.new(@api, response.fetch("deliver_subject"), timeout)
      yield writer
      Stream.new(self, StreamInfo.from_h(writer.finish))
    end

    # Subscribes to the advisories the server publishes about streams, consumers and API calls. They are
    # best effort: the server publishes one only while something is subscribed, and never again.
    #
    # @param kind [Symbol, String, nil] nil for every advisory; :api, :stream, :consumer, :server or :domain
    #   for one family; or the subject after `$JS.EVENT.ADVISORY.`, for example `"STREAM.CREATED.ORDERS"`
    # @yieldparam advisory [Advisory] each advisory, on the subscription's worker thread
    # @return [Connection::Subscription] the subscription; unsubscribe it to stop
    # @raise [ArgumentError] if no block is given or the kind is not known
    def advisories(kind = nil, &block)
      raise ArgumentError, "advisories needs a block to give each advisory to" unless block

      @client.subscribe(Advisory.subject_for(kind)) { |msg| block.call(Advisory.from_msg(msg)) }
    end

    # @param timeout [Numeric, Deadline, nil] how long to wait
    # @return [AccountInfo] JetStream usage and limits of the connection's account
    def account_info(timeout: nil)
      AccountInfo.from_h(@api.request("INFO", timeout: timeout))
    end

    # Publishes a message to a stream and waits for the server to confirm it was stored.
    #
    # @param subject [String] a subject a stream captures
    # @param payload [String, nil] the message body
    # @param headers [Protocol::Headers, Hash, nil] headers to send with the message
    # @param msg_id [String, nil] a deduplication id (`Nats-Msg-Id`)
    # @param expect [Hash{Symbol => Object}, nil] preconditions: `stream:`, `last_sequence:`,
    #   `last_subject_sequence:`, `subject:`, `last_msg_id:`
    # @param ttl [Numeric, Symbol, nil] seconds the message lives, or :never
    # @param rollup [Symbol, nil] :subject or :all, replacing earlier messages
    # @param schedule [Hash{Symbol => Object}, nil] makes the message a schedule on its subject: one of
    #   `at:` (a Time), `every:` (seconds) or `cron:`, the required `target:`, and optionally `source:`,
    #   `ttl:`, `time_zone:` and `rollup:`
    # @param cancel_schedule [String, nil] the subject of a schedule to remove when this message is stored
    # @param timeout [Numeric, Deadline, nil] how long to wait across every attempt
    # @param retries [Integer] extra attempts when no stream answers
    # @param retry_wait [Numeric] seconds between attempts
    # @return [PubAck] the acknowledgment
    # @raise [APIError] the mapped error when the server refuses the message
    # @raise [NoStreamResponseError] if no stream answered any attempt
    def publish(subject, payload = nil, headers: nil, msg_id: nil, expect: nil, ttl: nil, rollup: nil,
                schedule: nil, cancel_schedule: nil, timeout: nil, retries: 2, retry_wait: 0.25)
      deadline = Deadline.coerce(timeout || self.timeout)
      headers = PublishHeaders.build(headers, msg_id: msg_id, expect: expect, ttl: ttl, rollup: rollup,
                                              schedule: schedule, cancel_schedule: cancel_schedule)
      attempt = 0
      begin
        acknowledgment(@client.request(subject, payload, headers: headers, timeout: deadline))
      rescue NoRespondersError
        raise NoStreamResponseError, "no stream answered a publish to #{subject}" if (attempt += 1) > retries

        pause(retry_wait, deadline)
        retry
      end
    end

    # Publishes a message without waiting for the server to confirm it.
    #
    # @param subject [String] a subject a stream captures
    # @param payload [String, nil] the message body
    # @param headers [Protocol::Headers, Hash, nil] headers to send with the message
    # @param stall [Numeric, Deadline] how long to wait for room when `publish_async_max_pending`
    #   publishes are unacknowledged
    # @param options [Hash{Symbol => Object}] the options of {#publish}: `msg_id:`, `expect:`, `ttl:`,
    #   `rollup:`, `schedule:`, `cancel_schedule:`
    # @return [PublishFuture] the pending acknowledgment
    # @raise [TooManyStalledMessagesError] if no room opened up in time; the message was not sent
    def publish_async(subject, payload = nil, headers: nil, stall: Publisher::DEFAULT_STALL, **options)
      publisher.publish(subject, payload, headers: PublishHeaders.build(headers, **options), stall: stall)
    end

    # @return [Integer] asynchronous publishes whose acknowledgment has not arrived
    def pending_publishes
      publisher.pending
    end

    # Waits until every asynchronous publish has been acknowledged or has failed.
    #
    # @param timeout [Numeric, Deadline, nil] how long to wait
    # @return [Boolean] true when none is pending, false if the time ran out first
    def wait_for_publishes(timeout: nil)
      publisher.wait(Deadline.coerce(timeout || self.timeout))
    end

    # Registers a block that receives every asynchronous publish that fails. It replaces an earlier
    # one and runs on a worker thread of its own.
    #
    # @yieldparam future [PublishFuture] the failed future: `future.subject`, `future.error`
    # @return [self] this context
    def on_publish_error(&handler)
      publisher.on_error(&handler)
      self
    end

    # Publishes several messages that the server stores all together or not at all. The batch is
    # committed when the block returns and abandoned when it raises.
    #
    # @param timeout [Numeric, Deadline, nil] how long each exchange with the server may take
    # @yieldparam batch [PublishBatch] publish into it with {PublishBatch#publish}
    # @return [PubAck, nil] the acknowledgment with `batch_id` and `count`; nil when nothing was published
    # @raise [APIError] the error of the first message that failed a check; nothing was stored
    # @raise [IncompleteBatchError] if the server abandoned the batch, which it does after ten idle seconds
    def batch(timeout: nil, &block)
      PublishBatch.open(self, timeout: timeout || self.timeout, &block)
    end

    # Publishes a stream of messages that the server acknowledges a window at a time. Messages are
    # stored as they arrive: the batch is not all or nothing.
    #
    # @param gap [Symbol] :fail ends the batch and raises on the first lost or refused message; :ok
    #   collects them in {FastBatch#gaps} and {FastBatch#errors} and continues
    # @param flow [Integer] the largest number of messages per acknowledgment the server may choose
    # @param windows [Integer] acknowledgment windows the batch may be ahead of the server, 1 to 3
    # @param timeout [Numeric, Deadline, nil] how long each wait for the server may take
    # @yieldparam batch [FastBatch] publish into it with {FastBatch#publish}
    # @return [PubAck, nil] the final acknowledgment: `count` is the last batch sequence the server
    #   accepted; nil when nothing was published
    # @raise [APIError, MessageGapError] what ended the batch early
    def fast_batch(gap: :fail, flow: 100, windows: 2, timeout: nil, &block)
      FastBatch.open(self, gap: gap, flow: flow, windows: windows, timeout: timeout || self.timeout, &block)
    end

    # Returns a handle to a counter, which is one subject of a stream with `allow_message_counter`.
    #
    # @param subject [String] the subject the counter lives on
    # @param stream [String, nil] the stream that captures it; looked up from the subject when nil
    # @return [Counter] the counter
    def counter(subject, stream: nil)
      Counter.new(self, subject, stream: stream)
    end

    # Creates a Key/Value bucket. Creating one that exists with the same options succeeds.
    #
    # @param timeout [Numeric, Deadline, nil] how long to wait
    # @param options [Hash{Symbol => Object}] `bucket:` and the options of {KeyValue::Config#initialize}
    # @return [KeyValue] the bucket
    # @raise [KeyValue::BucketExistsError] if the bucket exists with other options
    # @raise [KeyValue::InvalidBucketNameError] if the name breaks the rules
    def create_key_value(timeout: nil, **options)
      KeyValue.create(self, timeout: timeout, **options)
    end

    # Looks a Key/Value bucket up.
    #
    # @param bucket [String] the bucket's name
    # @param timeout [Numeric, Deadline, nil] how long to wait
    # @return [KeyValue] the bucket
    # @raise [KeyValue::BucketNotFoundError] if the bucket does not exist
    def key_value(bucket, timeout: nil)
      KeyValue.bind(self, bucket, timeout: timeout)
    end

    # Replaces the options of a Key/Value bucket. Options that are not given return to their defaults.
    #
    # @param timeout [Numeric, Deadline, nil] how long to wait
    # @param options [Hash{Symbol => Object}] `bucket:` and the options of {KeyValue::Config#initialize}
    # @return [KeyValue] the bucket
    # @raise [KeyValue::BucketNotFoundError] if the bucket does not exist
    def update_key_value(timeout: nil, **options)
      KeyValue.update(self, timeout: timeout, **options)
    end

    # Replaces the options of a Key/Value bucket, or creates it when it does not exist.
    #
    # @param timeout [Numeric, Deadline, nil] how long to wait
    # @param options [Hash{Symbol => Object}] `bucket:` and the options of {KeyValue::Config#initialize}
    # @return [KeyValue] the bucket
    def create_or_update_key_value(timeout: nil, **options)
      KeyValue.create_or_update(self, timeout: timeout, **options)
    end

    # Deletes a Key/Value bucket and everything in it.
    #
    # @param bucket [String] the bucket's name
    # @param timeout [Numeric, Deadline, nil] how long to wait
    # @return [Boolean] whether the server confirmed the deletion
    # @raise [KeyValue::BucketNotFoundError] if the bucket does not exist
    def delete_key_value(bucket, timeout: nil)
      KeyValue.delete(self, bucket, timeout: timeout)
    end

    # @param timeout [Numeric, Deadline, nil] seconds each page request may take, or a deadline for the whole listing
    # @return [Enumerator::Lazy<String>] the name of every Key/Value bucket, fetched a page at a time
    def key_value_names(timeout: nil)
      KeyValue.names(self, timeout: timeout)
    end

    # @param timeout [Numeric, Deadline, nil] seconds each page request may take, or a deadline for the whole listing
    # @return [Enumerator::Lazy<KeyValue>] every Key/Value bucket, fetched a page at a time
    def key_values(timeout: nil)
      KeyValue.list(self, timeout: timeout)
    end

    # @return [String] the API prefix without the connection
    def inspect
      "#<Tether::JetStream prefix=#{@api.prefix.inspect}>"
    end

    # @api private
    # @param name [String] a stream or consumer name
    # @return [String] the name
    # @raise [ArgumentError] if it cannot be used as a subject token
    def check_name(name)
      text = name.to_s
      return text if text.match?(NAME)

      raise ArgumentError, "invalid name: #{name.inspect} (no whitespace, '.', '*', '>', '/' or '\\')"
    end

    private

    def prefix_for(domain, api_prefix)
      return DEFAULT_PREFIX unless domain || api_prefix
      raise ArgumentError, "invalid domain: #{domain.inspect}" if domain && !domain.to_s.match?(NAME)

      prefix = domain ? "$JS.#{domain}.API" : api_prefix.to_s.delete_suffix(".")
      Protocol::Subject.literal!(prefix)
    rescue InvalidSubjectError
      raise ArgumentError, "invalid api_prefix: #{api_prefix.inspect}"
    end

    def publisher
      @lock.synchronize do
        @publisher ||= Publisher.new(@client, max_pending: @publish_async_max_pending,
                                              ack_timeout: @publish_async_timeout, wait_timeout: timeout)
      end
    end

    def acknowledgment(reply)
      ack = PubAck.from_h(@api.decode(reply.data))
      return ack unless ack.stream.to_s.empty?

      raise Error, "the reply to a publish on #{reply.subject} is not a JetStream acknowledgment"
    end

    def write_stream(action, config, fields, timeout)
      config = config ? StreamConfig.coerce(config).with(**fields) : StreamConfig.new(**fields)
      response = @api.request("STREAM.#{action}.#{check_name(config.name)}", config.to_h,
                              timeout: timeout, level: config.api_level)
      Stream.new(self, StreamInfo.from_h(response))
    end

    def pause(seconds, deadline)
      Thread::Queue.new.pop(timeout: deadline.with_at_most(seconds).remaining)
      deadline.check!("publish")
    end
  end
end

require "tether/jetstream/errors"
require "tether/jetstream/schema"
require "tether/jetstream/stream_config"
require "tether/jetstream/consumer_config"
require "tether/jetstream/api"
require "tether/jetstream/pub_ack"
require "tether/jetstream/schedule"
require "tether/jetstream/publish_future"
require "tether/jetstream/publisher"
require "tether/jetstream/publish_batch"
require "tether/jetstream/fast_batch"
require "tether/jetstream/counter"
require "tether/jetstream/stream"
require "tether/jetstream/stored_message"
require "tether/jetstream/direct_get"
require "tether/jetstream/backup"
require "tether/jetstream/advisory"
require "tether/jetstream/message"
require "tether/jetstream/batch"
require "tether/jetstream/pull_status"
require "tether/jetstream/fetch"
require "tether/jetstream/consumer"
require "tether/jetstream/pull"
require "tether/jetstream/ordered"
require "tether/jetstream/consumers"
