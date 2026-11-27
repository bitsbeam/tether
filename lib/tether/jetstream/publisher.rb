# frozen_string_literal: true

module Tether
  class JetStream
    # Asynchronous publishing for one JetStream context.
    #
    # Every acknowledgment arrives on one resolver subscribed to
    # `<inbox>.*` and is matched to its future by the last subject token, on
    # the reader thread, without decoding it. A bounded window of pending
    # futures keeps a publisher from outrunning the server.
    class Publisher
      DEFAULT_STALL = 0.2
      WORKER = "tether:publish-errors"

      # @param client [Client] the connection
      # @param max_pending [Integer] futures that may await their acknowledgment at once
      # @param ack_timeout [Numeric, nil] seconds after which an unacknowledged publish fails
      # @param wait_timeout [Numeric, nil] how long a future waits by default
      # @raise [ArgumentError] if a limit is not positive
      def initialize(client, max_pending:, ack_timeout: nil, wait_timeout: nil)
        raise ArgumentError, "publish_async_max_pending: must be positive" unless max_pending.to_i.positive?
        raise ArgumentError, "publish_async_timeout: must be positive" if ack_timeout && !ack_timeout.positive?

        @client = client
        @max_pending = max_pending
        @ack_timeout = ack_timeout
        @wait_timeout = wait_timeout
        @lock = Mutex.new
        @released = ConditionVariable.new
        @idle = ConditionVariable.new
        @handler = nil
        reset
      end

      # Publishes a message and returns at once with a future for its acknowledgment.
      #
      # @param subject [String] a subject a stream captures
      # @param payload [String, nil] the message body
      # @param headers [Protocol::Headers, Hash, nil] headers to send with the message
      # @param stall [Numeric, Deadline] how long to wait for room when the window is full
      # @return [PublishFuture] the pending acknowledgment
      # @raise [TooManyStalledMessagesError] if the window stayed full
      def publish(subject, payload, headers: nil, stall: DEFAULT_STALL)
        start
        future, token = register(subject, payload, headers, Deadline.coerce(stall))
        begin
          @client.publish(subject, payload, reply_to: "#{@prefix}.#{token}", headers: headers)
        rescue StandardError
          discard(token)
          raise
        end
        future
      end

      # @return [Integer] publishes whose acknowledgment has not arrived
      def pending
        @lock.synchronize do
          orphan
          @pending.size
        end
      end

      # Waits until no publish is awaiting its acknowledgment.
      #
      # @param deadline [Deadline] when to give up
      # @return [Boolean] whether every publish was resolved in time
      def wait(deadline)
        @lock.synchronize do
          orphan
          until @pending.empty?
            return false if deadline.expired?

            @idle.wait(@lock, deadline.remaining)
          end
        end
        true
      end

      # Registers the handler that receives every future that fails. It runs
      # on a worker thread of its own, never on the reader or timer thread.
      #
      # @yieldparam future [PublishFuture] the failed future
      # @return [void] nothing
      def on_error(&handler)
        @lock.synchronize do
          orphan
          @handler = handler
          work
        end
        nil
      end

      private

      def reset
        @generation = Process.pid
        @pending = {}
        @counter = 0
        @prefix = nil
        @offset = 0
        @resolver = nil
        @listener = nil
        @expiry = nil
        @reports = Thread::Queue.new
        @worker = nil
      end

      def orphan
        return if @generation == Process.pid

        inherited = @pending.values
        reset
        inherited.each { |future| future.complete(ForkedError.new("the publish was made before the fork")) }
        work
      end

      def work
        return unless @handler && !@worker&.alive?

        reports = @reports
        @worker = Thread.new { report(reports) }
      end

      def start
        @lock.synchronize do
          orphan
          next if @resolver

          @prefix = @client.new_inbox
          @offset = @prefix.bytesize + 1
          @resolver = @client.resolver("#{@prefix}.*") { |msg| resolve(msg) }
          @listener = @client.listen { |event, _| interrupted(event) }
        end
      end

      def register(subject, payload, headers, deadline)
        @lock.synchronize do
          make_room(subject, deadline)
          expires_at = @ack_timeout && (Deadline.now + @ack_timeout)
          future = PublishFuture.new(subject, generation: @generation, payload: payload, headers: headers,
                                              timeout: @wait_timeout, expires_at: expires_at)
          token = (@counter += 1).to_s(36)
          @pending[token] = future
          @expiry ||= @client.timer.after(@ack_timeout) { expire } if @ack_timeout
          [future, token]
        end
      end

      def make_room(subject, deadline)
        while @pending.size >= @max_pending
          if deadline.expired?
            raise TooManyStalledMessagesError,
                  "#{@pending.size} publishes await acknowledgment; the publish to #{subject} was not sent"
          end

          @released.wait(@lock, deadline.remaining)
        end
      end

      def discard(token)
        @lock.synchronize { release if @pending.delete(token) }
      end

      def release
        @released.broadcast
        @idle.broadcast if @pending.empty?
      end

      def resolve(msg)
        token = msg.subject.byteslice(@offset, msg.subject.bytesize)
        settle(msg) { [@pending.delete(token)].compact }
      end

      def expire
        error = TimeoutError.new("no acknowledgment within #{@ack_timeout}s")
        settle(error) do
          @expiry&.cancel
          now = Deadline.now
          due = @pending.take_while { |_, future| future.expires_at <= now }
          due.each { |pair| @pending.delete(pair.first) }
          oldest = @pending.each_value.first
          @expiry = oldest && @client.timer.after([oldest.expires_at - now, 0].max) { expire }
          due.map(&:last)
        end
      end

      def interrupted(event)
        case event
        when :disconnect then fail_all(DisconnectedError.new("connection lost before the publish was acknowledged"))
        when :close
          fail_all(ConnectionClosedError.new("connection closed before the publish was acknowledged"))
          @lock.synchronize { @reports }.close
        end
      end

      def fail_all(error)
        settle(error) do
          @expiry&.cancel
          @expiry = nil
          @pending.values.tap { @pending.clear }
        end
      end

      def settle(outcome)
        reports = nil
        futures = @lock.synchronize do
          settled = yield
          settled.each { |future| future.complete(outcome) }
          release unless settled.empty?
          reports = @reports if @handler
          settled
        end
        futures.each { |future| reports.push(future) } if reports && !reports.closed?
      rescue ClosedQueueError
        nil
      end

      def report(reports)
        Thread.current.name = WORKER
        while (future = reports.pop)
          handler = @handler
          next unless handler && future.error

          begin
            handler.call(future)
          rescue StandardError => e
            @client.report_handler_error(e)
          end
        end
      end
    end
  end
end
