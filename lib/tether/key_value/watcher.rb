# frozen_string_literal: true

module Tether
  class KeyValue
    # A stream of entries for the keys matching one or more filters: first
    # what the bucket holds, then every change as it happens.
    #
    # A watcher reads through an ordered consumer, so it carries on after a
    # reconnect or the loss of its server consumer without gaps. The same
    # entry may still be delivered twice; handle entries idempotently by key
    # and revision.
    #
    # The end of the initial values is a state, {#ready?}, and an event,
    # {#on_ready}; nothing is pushed through the entry stream to mark it.
    # With a block, entries and the event are delivered on the watcher's own
    # worker thread. Without one, take entries with {#next} or {#each}; the
    # event then fires on the calling thread, just before {#next} returns the
    # last initial entry.
    class Watcher
      WAKE_INTERVAL = 0.1

      # @return [KeyValue] the bucket being watched
      attr_reader :key_value

      # @return [Array<String>] the key filters
      attr_reader :filters

      # @param key_value [KeyValue] the bucket to watch
      # @param filters [Array<String>] keys, or patterns with `*` and a final `>`; none watches every key
      # @param handler [Proc, nil] called with each entry on the watcher's worker; without one, use {#next}
      # @param include_history [Boolean] start with every stored revision instead of the latest of each key
      # @param updates_only [Boolean] skip the initial values
      # @param ignore_deletes [Boolean] leave tombstones out
      # @param meta_only [Boolean] deliver entries without their values
      # @param resume_from [Integer, nil] start at this revision
      # @param on_ready [#call, nil] called once when the initial values have been delivered
      # @param on_error [#call, nil] receives warnings and the error that ends the watcher
      # @raise [InvalidKeyError] if a filter breaks the rules
      # @raise [ArgumentError] if `include_history:` and `updates_only:` are combined
      # @raise [BucketNotFoundError] if the bucket is gone
      def initialize(key_value, filters, handler = nil, include_history: false, updates_only: false,
                     ignore_deletes: false, meta_only: false, resume_from: nil, on_ready: nil, on_error: nil)
        raise ArgumentError, "include_history: and updates_only: cannot be combined" if include_history && updates_only

        @key_value = key_value
        @filters = (filters.empty? ? [Naming::EVERYTHING] : filters.map { |filter| Naming.filter!(filter) }).freeze
        @handler = handler
        @ignore_deletes = ignore_deletes
        @prefix = key_value.read_prefix
        @lock = Mutex.new
        @changed = ConditionVariable.new
        @ready = false
        @ready_handlers = [on_ready].compact
        @pull = nil
        @pull = start(policy(include_history, updates_only, resume_from), resume_from, meta_only, on_error)
      end

      # Registers a block for the end of the initial values. On a watcher
      # that is ready already, it is called at once on the calling thread.
      #
      # @yield called once, with no arguments
      # @return [self] this watcher
      # @raise [ArgumentError] if no block is given
      def on_ready(&handler)
        raise ArgumentError, "on_ready needs a block" unless handler

        waiting = @lock.synchronize { @ready_handlers << handler unless @ready }
        handler.call unless waiting
        self
      end

      # Registers a handler for this watcher's warnings and for the error that ends it.
      #
      # @yieldparam error [Exception] the warning or error
      # @return [self] this watcher
      def on_error(&handler)
        @pull.on_error(&handler)
        self
      end

      # @return [Boolean] whether everything the bucket held when the watcher started has been delivered
      def ready?
        @ready || @pull&.caught_up? || false
      end

      # Blocks until the initial values have been delivered. A watcher without a block advances only as
      # its entries are taken, so there this answers at once.
      #
      # @param timeout [Numeric, Deadline, nil] how long to wait; nil waits indefinitely
      # @return [Boolean] true once ready; false if the time ran out or the watcher stopped first
      def wait_until_ready(timeout: nil)
        deadline = Deadline.coerce(timeout)
        return ready? unless @handler

        @lock.synchronize do
          until @ready
            return false if stopped? || deadline.expired?

            @changed.wait(@lock, deadline.with_at_most(WAKE_INTERVAL).remaining)
          end
          true
        end
      end

      # Takes the next entry, for a watcher without a block. An entry that
      # did not arrive in time is not lost: the next call returns it.
      #
      # @param timeout [Numeric, Deadline, nil] how long to wait; nil waits indefinitely
      # @return [Entry] the next entry
      # @raise [TimeoutError] if none arrives in time
      # @raise [StopIteration] if the watcher was stopped
      # @raise [ArgumentError] if the watcher delivers to a block
      # @raise [JetStream::Error] the error that ended the watcher
      def next(timeout: nil)
        deadline = Deadline.coerce(timeout)
        entry = nil
        entry = entry_for(@pull.next(timeout: deadline)) until entry
        entry
      end

      # Yields entries until the watcher is stopped.
      #
      # @yieldparam entry [Entry] each entry, on the calling thread
      # @return [self, Enumerator<Entry>] this watcher, or an enumerator when no block is given
      # @raise [JetStream::Error] the error that ended the watcher
      def each
        return enum_for(:each) unless block_given?

        loop { yield self.next }
        self
      end

      # Stops the watcher and removes its server consumer. Entries not yet delivered are dropped.
      #
      # @return [self] this watcher
      def stop
        @pull.stop
        @lock.synchronize { @changed.broadcast }
        self
      end

      # @return [Boolean] whether the watcher has stopped
      def stopped?
        @pull.stopped?
      end

      # Blocks until the watcher has stopped and its block has returned.
      #
      # @param timeout [Numeric, Deadline, nil] how long to wait; nil waits indefinitely
      # @return [self] this watcher
      # @raise [TimeoutError] if it has not stopped in time
      # @raise [JetStream::Error] the error that ended the watcher
      def wait(timeout: nil)
        @pull.wait(timeout: timeout)
        self
      end

      # @return [Exception, nil] the error that ended the watcher
      def error
        @pull.error
      end

      # @return [Integer] the revision of the last entry received; 0 before the first
      def revision
        @pull.stream_sequence
      end

      # Yields the initial values and returns when they have all been taken.
      #
      # @api private
      # @param timeout [Numeric, Deadline, nil] seconds to wait for each entry, or a deadline for all of them
      # @yieldparam entry [Entry] each initial entry
      # @return [void] nothing
      # @raise [TimeoutError] if an entry does not arrive in time
      def each_initial(timeout)
        api = @key_value.jetstream.api
        until @ready
          entry = entry_for(@pull.next(timeout: api.deadline(timeout)))
          yield entry if entry
        end
      end

      # @return [String] the bucket, filters and state
      def inspect
        "#<Tether::KeyValue::Watcher bucket=#{@key_value.bucket.inspect} filters=#{@filters.inspect} " \
          "ready=#{ready?} stopped=#{stopped?}>"
      end

      private

      def policy(include_history, updates_only, resume_from)
        return :by_start_sequence if resume_from
        return :new if updates_only

        include_history ? :all : :last_per_subject
      end

      def start(policy, resume_from, meta_only, on_error)
        ordered = @key_value.stream.ordered_consumer(
          filter_subjects: @filters.map { |filter| "#{@prefix}#{filter}" }, deliver_policy: policy,
          start_sequence: resume_from, headers_only: meta_only
        )
        options = { on_caught_up: method(:announce), on_error: on_error }
        @handler ? ordered.consume(**options) { |message| deliver(message) } : ordered.messages(**options)
      rescue JetStream::StreamNotFoundError
        raise BucketNotFoundError, "bucket #{@key_value.bucket} not found"
      end

      def deliver(message)
        entry = entry_for(message) or return
        @handler.call(entry)
      rescue StandardError => e
        @key_value.jetstream.client.report_handler_error(e, entry || message)
      end

      def entry_for(message)
        subject = message.subject
        return nil unless subject.start_with?(@prefix) && subject.bytesize > @prefix.bytesize

        operation = Entry.operation_of(message.headers)
        return nil if @ignore_deletes && operation != :put

        meta = message.metadata
        Entry.new(bucket: @key_value.bucket, key: subject.byteslice(@prefix.bytesize..), value: message.data,
                  revision: meta.stream_sequence, created_at: meta.time, operation: operation, delta: meta.pending)
      end

      def announce
        handlers = @lock.synchronize do
          next [] if @ready

          @ready = true
          @changed.broadcast
          @ready_handlers.dup.tap { @ready_handlers.clear }
        end
        handlers.each(&:call)
      end
    end
  end
end
