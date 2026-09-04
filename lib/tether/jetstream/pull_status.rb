# frozen_string_literal: true

module Tether
  class JetStream
    # What a status frame on a pull's reply subject means for the pull.
    #
    # `kind` is one of:
    #
    # - `:heartbeat`: activity only
    # - `:end`: that pull is over, normally
    # - `:too_large`: that pull is over because the next message exceeds its byte budget
    # - `:warning`: that pull is over for a reason worth reporting
    # - `:reset`: every outstanding pull is void
    # - `:pin`: the pin id sent with the pull is not the pinned one
    # - `:unavailable`: nothing serves the consumer
    # - `:terminal`: no pull on this consumer can succeed
    PullStatus = Data.define(:kind, :code, :error, :pending_messages, :pending_bytes)

    # Classifies status frames. See {PullStatus}.
    class PullStatus
      PENDING_MESSAGES = "Nats-Pending-Messages"
      PENDING_BYTES = "Nats-Pending-Bytes"
      PIN_ID = "Nats-Pin-Id"

      CONFLICTS = [
        [/\Aconsumer deleted/i, :terminal, ConsumerDeletedError],
        [/\Aconsumer is push based/i, :terminal, PushConsumerError],
        [/\Aleadership change/i, :reset, PullError],
        [/\Abatch completed/i, :end, nil],
        [/\Amessage size exceeds maxbytes/i, :too_large, nil]
      ].freeze

      class << self
        # @param msg [Protocol::Msg] a frame that arrived on a pull's reply subject
        # @return [PullStatus, nil] nil when the frame is a user message
        def of(msg)
          return nil unless msg.empty?

          code = msg.status or return nil
          kind, error = classify(code, msg.description.to_s)
          headers = msg.headers
          new(kind: kind, code: code, error: error,
              pending_messages: headers[PENDING_MESSAGES].to_i, pending_bytes: headers[PENDING_BYTES].to_i)
        end

        private

        def classify(code, description)
          case code
          when 100 then [:heartbeat, nil]
          when 404 then [:end, nil]
          when 408 then description.match?(/\Arequests pending/i) ? warning(code, description) : [:end, nil]
          when 409 then conflict(description)
          when 423 then [:pin, PinIdMismatchError.new("#{code} #{description}".strip)]
          when 503 then [:unavailable, ServiceUnavailableError.new("nothing serves the consumer; it may be gone")]
          when 400 then [:terminal, PullError.new(code, description)]
          when 412 then [:terminal, required_level(description)]
          else warning(code, description)
          end
        end

        def conflict(description)
          _, kind, error = CONFLICTS.find { |pattern, _, _| description.match?(pattern) }
          return warning(409, description) unless kind
          return [kind, nil] unless error

          [kind, error == PullError ? PullError.new(409, description) : error.new("409 #{description}")]
        end

        def warning(code, description)
          [:warning, PullError.new(code, description)]
        end

        def required_level(description)
          RequiredApiLevelError.new("code" => 412, "err_code" => 10_185, "description" => description)
        end
      end

      # @return [Boolean] whether the frame only shows that the server still holds the pull
      def heartbeat?
        kind == :heartbeat
      end

      # @return [Boolean] whether a single fetch that receives this ended normally
      def normal_end?
        code == 408 || kind == :end || kind == :too_large
      end
    end
  end
end
