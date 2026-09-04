# frozen_string_literal: true

module Tether
  class JetStream
    class Ordered
      # The pull loop of an ordered consumer: what {Ordered#consume} and
      # {Ordered#messages} return.
      #
      # It checks every delivery against the previous one and replaces the
      # server consumer whenever continuity can no longer be assumed.
      class Pull < JetStream::Pull
        BACKOFF = [1, 2, 4, 8, 10].freeze
        ATTEMPT_TIMEOUT = 10
        MAX_HEARTBEAT = 5

        # @param ordered [Ordered] the ordered consumer this loop reads for
        # @param handler [Proc, nil] called with each message on the worker thread
        # @param on_caught_up [#call, nil] called once when everything that existed at creation was delivered
        # @param concurrency [Integer] must be 1, since more workers would give up the ordering
        # @param expires [Numeric] seconds a pull request lives on the server
        # @param heartbeat [Numeric, nil] seconds between the server's idle heartbeats; half of `expires`, at
        #   most 5, by default, since a lost server consumer is usually noticed by their absence
        # @param options [Hash{Symbol => Object}] see {JetStream::Pull#initialize}
        # @raise [ArgumentError] if `concurrency:` is above 1
        def initialize(ordered, handler = nil, on_caught_up: nil, concurrency: 1, expires: DEFAULT_EXPIRES,
                       heartbeat: nil, **options)
          raise ArgumentError, "an ordered consumer delivers on one worker; concurrency: must be 1" if concurrency != 1

          @ordered = ordered
          @on_caught_up = on_caught_up
          @stream_sequence = @consumer_sequence = @received = @attempts = 0
          @retry_at = nil
          @recover = false
          consumer = ordered.create(after: 0)
          @initial = consumer.cached_info.pending_count.to_i + consumer.cached_info.delivered.consumer_sequence.to_i
          @caught_up = @initial.zero?
          super(consumer, handler, expires: expires, heartbeat: heartbeat || [expires / 2.0, MAX_HEARTBEAT].min,
                                   **options)
        end

        # @return [Boolean] whether everything that existed when the consumer was created has been delivered
        def caught_up?
          @caught_up
        end

        # @return [Integer] the stream sequence of the last message handed out; 0 before the first
        attr_reader :stream_sequence

        private

        def started
          @on_caught_up&.call if @caught_up && @handler.nil?
        end

        def before_work
          announce if @caught_up
        end

        def delivered(message)
          caught_up(message) unless @caught_up
        end

        def caught_up(message)
          @received += 1
          return unless @received >= @initial || message.metadata.pending.zero?

          @caught_up = true
          announce
        end

        def announce
          @on_caught_up&.call
        rescue StandardError => e
          @client.report_handler_error(e)
        end

        def admit(msg)
          message = super
          meta = message.metadata
          return nil unless meta.consumer == @consumer.name
          return gap unless meta.consumer_sequence == @consumer_sequence + 1

          @consumer_sequence = meta.consumer_sequence
          @stream_sequence = meta.stream_sequence
          caught_up(message) unless @caught_up || @handler
          message
        end

        def gap
          @recover = true
          nil
        end

        def handle(kind, error, epoch, deadline)
          return super unless (epoch.nil? || epoch == @epoch) && replace?(kind, error)

          report(error)
          @recover = true
        end

        def replace?(kind, error)
          case kind
          when :unavailable, :reconnected then true
          when :silence then !error.nil?
          when :terminal then error.is_a?(ConsumerDeletedError)
          else false
          end
        end

        def prepare(deadline)
          return unless @recover && @buffer.state == :running

          retire
          consumer = create(deadline) or return
          @consumer = consumer
          @consumer_sequence = @attempts = 0
          @retry_at = nil
          @recover = false
          open_session
          pull_again
        end

        def retire
          return unless @resolver

          close_session
          @resolver = nil
          @buffer.clear
          @ordered.discard(@consumer)
        end

        def create(deadline)
          while wait_for_retry(deadline)
            attempt = deadline.with_at_most(ATTEMPT_TIMEOUT)
            begin
              return @ordered.create(after: @stream_sequence, deadline: attempt)
            rescue StreamNotFoundError, ConnectionClosedError => e
              return fail_with(e)
            rescue Tether::Error
              deadline.check!("recreating the ordered consumer on #{@ordered.stream}")
              @retry_at = Deadline.now + BACKOFF.fetch(@attempts, BACKOFF.last)
              @attempts += 1
            end
          end
        end

        def wait_for_retry(deadline)
          return @buffer.state == :running unless @retry_at
          return false unless @buffer.pause(deadline.earliest(Deadline.new(@retry_at)))

          deadline.check!("recreating the ordered consumer on #{@ordered.stream}") if Deadline.now < @retry_at
          true
        end

        def close_session
          super
          @ordered.discard(@consumer) if @finished
        end
      end
    end
  end
end
