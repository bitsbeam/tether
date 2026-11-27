# frozen_string_literal: true

module Tether
  class JetStream
    # The pending result of an asynchronous publish.
    #
    # A future is completed exactly once: by the server's acknowledgment, by a
    # timeout, or by the connection going away. Any number of threads may wait
    # on it.
    class PublishFuture
      # @return [String] the subject the message was published to
      attr_reader :subject

      # @return [String, nil] the message body, kept so a failed publish can be sent again
      attr_reader :payload

      # @return [Protocol::Headers, Hash, nil] the headers sent, the message id and expectations included
      attr_reader :headers

      # @api private
      # @return [Float, nil] the monotonic instant the publisher gives up on the acknowledgment
      attr_reader :expires_at

      # @api private
      # @param subject [String] the subject the message was published to
      # @param payload [String, nil] the message body
      # @param headers [Protocol::Headers, Hash, nil] the headers sent with the message
      # @param generation [Integer] the process the publish was made in
      # @param timeout [Numeric, nil] how long {#value} and {#wait} wait by default
      # @param expires_at [Float, nil] the monotonic instant the publisher gives up on the acknowledgment
      def initialize(subject, generation:, payload: nil, headers: nil, timeout: nil, expires_at: nil)
        @subject = subject
        @payload = payload
        @headers = headers
        @generation = generation
        @timeout = timeout
        @expires_at = expires_at
        @lock = Mutex.new
        @completed = ConditionVariable.new
        @outcome = nil
        @decoded = false
        @ack = nil
        @error = nil
      end

      # Waits for the acknowledgment.
      #
      # @param timeout [Numeric, Deadline, nil] how long to wait; defaults to the context's timeout
      # @return [PubAck] the acknowledgment
      # @raise [APIError] the mapped error when the server refused the message
      # @raise [NoStreamResponseError] if no stream captures the subject
      # @raise [DisconnectedError, ConnectionClosedError] if the connection went away first
      # @raise [ForkedError] if the publish was made in a parent process
      # @raise [TimeoutError] if nothing arrived in time; the future stays pending unless the publisher gave up
      def value(timeout: nil)
        raise TimeoutError, "no acknowledgment for #{@subject} within the deadline" unless wait(timeout: timeout)

        failure = error
        raise failure if failure

        @ack
      end

      # Waits until the future is completed, successfully or not.
      #
      # @param timeout [Numeric, Deadline, nil] how long to wait; defaults to the context's timeout
      # @return [Boolean] whether the future completed in time
      def wait(timeout: nil)
        deadline = Deadline.coerce(timeout || @timeout)
        @lock.synchronize do
          orphan
          until @outcome
            return false if deadline.expired?

            @completed.wait(@lock, deadline.remaining)
          end
        end
        true
      end

      # @return [Boolean] whether the outcome is known
      def done?
        @lock.synchronize do
          orphan
          !@outcome.nil?
        end
      end

      # @return [Exception, nil] why the publish failed; nil while pending and after a success
      def error
        @lock.synchronize do
          orphan
          decode
          @error
        end
      end

      # Records the outcome. Only the first call has an effect.
      #
      # @api private
      # @param outcome [Protocol::Msg, Exception] the server's reply, or why there will be none
      # @return [Boolean] whether this call completed the future
      def complete(outcome)
        @lock.synchronize do
          return false if @outcome

          @outcome = outcome
          @completed.broadcast
        end
        true
      end

      # @return [String] the subject and state without the payload
      def inspect
        "#<Tether::JetStream::PublishFuture subject=#{@subject.inspect} done=#{done?}>"
      end

      private

      def orphan
        return if @outcome || @generation == Process.pid

        @outcome = ForkedError.new("the publish to #{@subject} was made before the fork")
        @completed.broadcast
      end

      def decode
        return if @decoded || @outcome.nil?

        @decoded = true
        if @outcome.is_a?(Exception)
          @error = @outcome
        elsif @outcome.no_responders?
          @error = NoStreamResponseError.new("no stream answered a publish to #{@subject}")
        else
          @ack = PubAck.parse(@outcome.data)
        end
      rescue Error => e
        @error = e
      end
    end
  end
end
