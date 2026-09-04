# frozen_string_literal: true

require "time"

module Tether
  class JetStream
    # Builds the headers that turn a published message into a schedule, or
    # that cancel one.
    module Schedule
      PATTERN = "Nats-Schedule"
      TARGET = "Nats-Schedule-Target"
      SOURCE = "Nats-Schedule-Source"
      TTL = "Nats-Schedule-TTL"
      ROLLUP = "Nats-Schedule-Rollup"
      TIME_ZONE = "Nats-Schedule-Time-Zone"
      SCHEDULER = "Nats-Scheduler"
      NEXT = "Nats-Schedule-Next"
      PURGE = "purge"
      KEYS = %i[at every cron target source ttl time_zone rollup].freeze
      TIMES = %i[at every cron].freeze
      MINIMUM_INTERVAL = 1

      class << self
        # Adds the headers of a schedule.
        #
        # @param headers [Protocol::Headers] the headers to add to
        # @param schedule [Hash{Symbol => Object}] one of `at:` (a Time), `every:` (seconds) or `cron:`
        #   (a six-field expression or a predefined name such as `@daily`), the required `target:`, and
        #   optionally `source:`, `ttl:` (seconds or :never), `time_zone:` and `rollup:`
        # @return [Protocol::Headers] the same headers
        # @raise [ArgumentError] if the schedule is incomplete or holds something unusable
        def apply(headers, schedule)
          unknown = schedule.keys - KEYS
          raise ArgumentError, "unknown schedule option: #{unknown.map(&:inspect).join(", ")}" unless unknown.empty?

          headers[PATTERN] = pattern(schedule)
          headers[TARGET] = text(schedule, :target) || raise(ArgumentError, "schedule: needs a target:")
          { source: SOURCE, time_zone: TIME_ZONE }.each do |name, header|
            value = text(schedule, name)
            headers[header] = value if value
          end
          headers[TTL] = ttl(schedule[:ttl]) if schedule[:ttl]
          headers[ROLLUP] = PublishHeaders::ROLLUPS.fetch(:subject) if schedule[:rollup]
          headers
        end

        # Adds the headers that remove a schedule when the message carrying them is stored.
        #
        # @param headers [Protocol::Headers] the headers to add to
        # @param subject [String] the subject the schedule lives on
        # @return [Protocol::Headers] the same headers
        # @raise [ArgumentError] if the subject is empty
        def cancel(headers, subject)
          raise ArgumentError, "cancel_schedule: must be the subject of a schedule" if subject.to_s.empty?

          headers[SCHEDULER] = subject.to_s
          headers[NEXT] = PURGE
          headers
        end

        private

        def pattern(schedule)
          given = TIMES.select { |name| schedule[name] }
          raise ArgumentError, "schedule: needs exactly one of at:, every: or cron:" unless given.size == 1

          case given.first
          when :at then "@at #{instant(schedule[:at])}"
          when :every then "@every #{interval(schedule[:every])}"
          else text(schedule, :cron)
          end
        end

        def instant(time)
          raise ArgumentError, "schedule: at: must be a Time" unless time.respond_to?(:utc)

          time.utc.iso8601(time.subsec.zero? ? 0 : 9)
        end

        def interval(seconds)
          unless seconds.is_a?(Numeric) && seconds >= MINIMUM_INTERVAL
            raise ArgumentError, "schedule: every: must be at least #{MINIMUM_INTERVAL} second"
          end

          PublishHeaders.duration(seconds)
        end

        def ttl(value)
          return "never" if value == :never
          return PublishHeaders.duration(value) if value.is_a?(Numeric) && value.positive?

          raise ArgumentError, "schedule: ttl: must be positive or :never"
        end

        def text(schedule, name)
          return nil unless schedule.key?(name) && !schedule[name].nil?

          value = schedule[name].to_s
          raise ArgumentError, "schedule: #{name}: must not be empty" if value.strip.empty?

          value
        end
      end
    end
  end
end
