# frozen_string_literal: true

module Tether
  class ObjectStore
    # A live view of a bucket's directory: the current entry of every object,
    # then every change. It is what {ObjectStore#watch} returns.
    #
    # The end of the initial entries is a state, {#ready?}, and an event,
    # {#on_ready}. A watcher reads through an ordered consumer, so it survives
    # reconnects and the loss of its server consumer without missing an entry.
    class Watcher
      POLICIES = { false => :last_per_subject, true => :all }.freeze

      # @api private
      # @param stream [JetStream::Stream] the stream behind the bucket
      # @param bucket [String] the bucket's name
      # @param include_history [Boolean] start from the oldest stored entry instead of the current ones
      # @param ignore_deletes [Boolean] leave the tombstones of deleted objects out
      # @param updates_only [Boolean] skip what exists and deliver only later changes
      # @param on_ready [#call, nil] called once when the initial entries have been delivered
      # @param on_error [#call, nil] receives the watcher's warnings and the error that ends it
      # @yieldparam info [Info] each entry, on the watcher's worker thread
      # @raise [ArgumentError] if `include_history:` is combined with `updates_only:`
      # @raise [BucketNotFoundError] if the bucket does not exist
      def initialize(stream, bucket, include_history: false, ignore_deletes: false, updates_only: false,
                     on_ready: nil, on_error: nil, &block)
        raise ArgumentError, "include_history: and updates_only: cannot be combined" if include_history && updates_only

        @bucket = bucket
        @ignore_deletes = ignore_deletes
        @handler = block
        @lock = Mutex.new
        @became_ready = ConditionVariable.new
        @ready = false
        @ready_handlers = [on_ready].compact
        @pull = start(stream, updates_only ? :new : POLICIES.fetch(include_history == true), on_error)
      end

      # Registers a block for the moment the initial entries have all been delivered. On a watcher that
      # is ready already it runs at once, on the calling thread; otherwise it runs on the thread that
      # delivers the entries.
      #
      # @yield when the watcher is ready
      # @return [self] this watcher
      def on_ready(&handler)
        ready = @lock.synchronize do
          @ready_handlers << handler unless @ready
          @ready
        end
        handler.call if ready
        self
      end

      # Registers a handler for the watcher's warnings and for the error that ends it.
      #
      # @yieldparam error [Exception] the warning or error
      # @return [self] this watcher
      def on_error(&handler)
        @pull.on_error(&handler)
        self
      end

      # @return [Boolean] whether every entry that existed when the watcher started has been delivered
      def ready?
        @ready || @pull.caught_up?
      end

      # Blocks until the initial entries have been delivered. On a watcher without a block that happens
      # while another thread iterates it.
      #
      # @param timeout [Numeric, Deadline, nil] how long to wait; nil waits indefinitely
      # @return [self] this watcher
      # @raise [TimeoutError] if the watcher is not ready in time
      def wait_until_ready(timeout: nil)
        deadline = Deadline.coerce(timeout)
        @lock.synchronize do
          until @ready
            raise TimeoutError, "the watcher on #{@bucket} was not ready within the deadline" if deadline.expired?

            @became_ready.wait(@lock, deadline.remaining)
          end
        end
        self
      end

      # Takes the next entry, for a watcher without a block.
      #
      # @param timeout [Numeric, Deadline, nil] how long to wait; nil waits indefinitely
      # @return [Info] the next entry
      # @raise [TimeoutError] if none arrives in time
      # @raise [StopIteration] if the watcher was stopped
      # @raise [ArgumentError] if the watcher delivers to a block
      def next(timeout: nil)
        deadline = Deadline.coerce(timeout)
        info = nil
        info = entry(@pull.next(timeout: deadline)) until info
        info
      end

      # Yields entries until the watcher is stopped, for a watcher without a block.
      #
      # @yieldparam info [Info] each entry, on the calling thread
      # @return [self, Enumerator<Info>] this watcher, or an enumerator when no block is given
      def each
        return enum_for(:each) unless block_given?

        loop { yield self.next }
        self
      end

      # Stops watching and removes the consumer.
      #
      # @return [self] this watcher
      def stop
        @pull.stop
        self
      end

      # @return [Boolean] whether the watcher has ended
      def stopped?
        @pull.stopped?
      end

      # @return [Exception, nil] the error that ended the watcher
      def error
        @pull.error
      end

      # @return [String] the bucket and state
      def inspect
        "#<Tether::ObjectStore::Watcher bucket=#{@bucket.inspect} ready=#{ready?} stopped=#{stopped?}>"
      end

      private

      def start(stream, policy, on_error)
        ordered = stream.ordered_consumer(filter_subjects: ObjectStore.metadata_filter(@bucket),
                                          deliver_policy: policy)
        caught_up = -> { ready! }
        return ordered.messages(on_caught_up: caught_up, on_error: on_error) unless @handler

        ordered.consume(on_caught_up: caught_up, on_error: on_error) { |message| deliver(message) }
      rescue JetStream::StreamNotFoundError
        raise BucketNotFoundError, "bucket #{@bucket} does not exist"
      end

      def deliver(message)
        info = entry(message)
        @handler.call(info) if info
      end

      def entry(message)
        info = Info.parse(message.data, modified_at: message.metadata.time)
        info unless @ignore_deletes && info.deleted?
      rescue Error
        nil
      end

      def ready!
        handlers = @lock.synchronize do
          next [] if @ready

          @ready = true
          @became_ready.broadcast
          @ready_handlers.dup.tap { @ready_handlers.clear }
        end
        handlers.each(&:call)
      end
    end
  end
end
