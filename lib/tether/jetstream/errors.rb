# frozen_string_literal: true

module Tether
  class JetStream
    # Base class for every error JetStream raises.
    class Error < Tether::Error; end

    # An error object returned by the JetStream API or in a publish acknowledgment.
    class APIError < Error
      RETRYABLE_CODES = [10_008, 10_009, 10_158, 10_167, 10_210, 10_211].freeze

      # @return [Integer, nil] the HTTP-like status of the error
      attr_reader :code

      # @return [Integer, nil] the server's JetStream error code
      attr_reader :err_code

      # @return [String, nil] the server's description
      attr_reader :description

      # Picks the subclass for an error object.
      #
      # @param error [Hash{String => Object}] the `error` member of a response
      # @return [APIError] the most specific error available
      def self.for(error)
        (CODES[error["err_code"]] || self).new(error)
      end

      # @param error [Hash{String => Object}] the `error` member of a response
      def initialize(error)
        @code = error["code"]
        @err_code = error["err_code"]
        @description = error["description"]
        details = { "code" => @code, "err_code" => @err_code }.compact.map { |name, value| "#{name} #{value}" }
        super("#{@description || "JetStream API error"} (#{details.join(", ")})")
      end

      # @return [Boolean] whether the server reported a transient condition
      def retryable?
        RETRYABLE_CODES.include?(@err_code)
      end
    end

    # The stream named in a call does not exist.
    class StreamNotFoundError < APIError; end

    # A stream with this name exists with a different configuration.
    class StreamNameInUseError < APIError; end

    # The consumer named in a call does not exist.
    class ConsumerNotFoundError < APIError; end

    # A consumer with this name already exists.
    class ConsumerExistsError < APIError; end

    # An expected last sequence, subject sequence or message id did not match.
    class WrongLastSequenceError < APIError; end

    # The publish did not reach the stream named in `expect: { stream: }`.
    class StreamMismatchError < APIError; end

    # No message matches the request.
    class MessageNotFoundError < APIError; end

    # JetStream is not enabled on the server or for the account.
    class JetStreamNotEnabledError < APIError; end

    # The server cannot place or store what was asked.
    class InsufficientResourcesError < APIError; end

    # The server is older than a feature the request uses.
    class RequiredApiLevelError < APIError; end

    # The stream is sealed and cannot be changed.
    class StreamSealedError < APIError; end

    # An account or stream limit was reached.
    class LimitReachedError < APIError; end

    # An atomic batch was abandoned or never completed.
    class IncompleteBatchError < APIError; end

    # An atomic batch holds more messages than the server accepts.
    class BatchTooLargeError < APIError; end

    # The stream does not allow atomic or fast-ingest batches.
    class BatchPublishDisabledError < APIError; end

    # The stream does not allow scheduled messages.
    class SchedulesDisabledError < APIError; end

    # A schedule, its target, source, TTL, rollup or time zone is not acceptable, or what was to be
    # canceled is not a schedule.
    class InvalidScheduleError < APIError; end

    # A message with the same id is being stored right now.
    class DuplicateInProgressError < APIError; end

    # The request itself was malformed.
    class BadRequestError < APIError; end

    class APIError
      CODES = {
        10_002 => LimitReachedError, 10_003 => BadRequestError, 10_013 => ConsumerExistsError,
        10_014 => ConsumerNotFoundError, 10_023 => InsufficientResourcesError, 10_026 => LimitReachedError,
        10_027 => LimitReachedError, 10_037 => MessageNotFoundError, 10_039 => JetStreamNotEnabledError,
        10_043 => MessageNotFoundError, 10_058 => StreamNameInUseError, 10_059 => StreamNotFoundError,
        10_060 => StreamMismatchError, 10_070 => WrongLastSequenceError, 10_071 => WrongLastSequenceError,
        10_076 => JetStreamNotEnabledError, 10_105 => ConsumerExistsError, 10_109 => StreamSealedError,
        10_130 => StreamNameInUseError, 10_148 => ConsumerExistsError, 10_149 => ConsumerNotFoundError,
        10_158 => DuplicateInProgressError, 10_164 => WrongLastSequenceError, 10_176 => IncompleteBatchError,
        10_185 => RequiredApiLevelError, 10_174 => BatchPublishDisabledError, 10_205 => BatchPublishDisabledError,
        10_199 => BatchTooLargeError, 10_188 => SchedulesDisabledError, 10_189 => InvalidScheduleError,
        10_190 => InvalidScheduleError, 10_191 => InvalidScheduleError, 10_192 => InvalidScheduleError,
        10_203 => InvalidScheduleError, 10_212 => InvalidScheduleError, 10_223 => InvalidScheduleError
      }.freeze
    end

    # No stream answered a publish: none captures the subject, or its leader is away.
    class NoStreamResponseError < Error
      # @return [Boolean] true: the stream may answer once it has a leader
      def retryable? = true
    end

    # The JetStream API did not answer at all.
    class ServiceUnavailableError < Error
      # @return [Boolean] true: the API may answer once the cluster has a leader
      def retryable? = true
    end

    # The server did not receive every message of a fast-ingest batch.
    class MessageGapError < Error; end

    # Too many asynchronous publishes are waiting for their acknowledgments.
    class TooManyStalledMessagesError < Error; end

    # A direct get matched more subjects than the server allows in one request.
    class TooManyResultsError < Error; end

    # The consumer was deleted while it was being consumed from.
    class ConsumerDeletedError < Error; end

    # The server stopped sending heartbeats for a pull.
    class NoHeartbeatError < Error; end

    # The pin id sent with a pull is no longer the pinned one.
    class PinIdMismatchError < Error; end

    # The consumer is a push consumer, which tether does not consume from.
    class PushConsumerError < Error; end

    # A pull was refused or ended by the server for a reason carried in the message.
    class PullError < Error
      # @return [Integer, nil] the status code of the frame that ended the pull
      attr_reader :status

      # @param status [Integer, nil] the status code
      # @param description [String, nil] the status description
      def initialize(status, description)
        @status = status
        super("#{status} #{description}".strip)
      end
    end

    # A message was acknowledged terminally more than once.
    class AlreadyAckedError < Error; end

    # An acknowledgment or metadata was asked of a message that did not come from a consumer.
    class NotJetStreamMessageError < Error; end
  end
end
