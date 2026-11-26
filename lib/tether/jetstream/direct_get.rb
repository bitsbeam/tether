# frozen_string_literal: true

require "json"

module Tether
  class JetStream
    # Reads messages straight from a stream's replicas on `DIRECT.GET`,
    # outside the JSON API: one message, a paged batch, or the last message
    # of each of several subjects.
    #
    # A reply is the stored message with headers that describe it, or a
    # status line. A batch ends with a `204 EOB` status whose headers say
    # whether more is left.
    #
    # @api private
    class DirectGet
      NUM_PENDING = "Nats-Num-Pending"
      LAST_SEQUENCE = "Nats-Last-Sequence"
      UP_TO_SEQUENCE = "Nats-UpTo-Sequence"

      # @param jetstream [JetStream] the context
      # @param stream [String] the stream's name
      def initialize(jetstream, stream)
        @client = jetstream.client
        @api = jetstream.api
        @stream = stream
        @subject = "#{@api.prefix}.DIRECT.GET.#{stream}"
      end

      # Reads one message.
      #
      # @param request [Hash{String => Object}] `seq`, `last_by_subj` or `next_by_subj` with an optional `seq`
      # @param deadline [Deadline] how long to wait
      # @return [StoredMessage] the message
      # @raise [MessageNotFoundError] if no message matches
      # @raise [ServiceUnavailableError] if the stream is missing or does not allow direct access
      def get(request, deadline)
        subject, body = address(request)
        reply = @client.request(subject, body, timeout: deadline)
        raise refusal(reply) if reply.status

        StoredMessage.from_direct(reply)
      rescue NoRespondersError
        raise unavailable
      end

      # Counts the messages a batched request would return, by asking for one and reading how many are left.
      #
      # @param request [Hash{String => Object}] `seq` and `next_by_subj`
      # @param deadline [Deadline] how long to wait
      # @return [Integer] how many messages match
      # @raise [ServiceUnavailableError] if the stream is missing or does not allow direct access
      def count(request, deadline)
        reply = @client.request(@subject, JSON.generate(request.merge("batch" => 1)), timeout: deadline)
        return 0 if reply.status == Protocol::Headers::NOT_FOUND
        raise refusal(reply) if reply.status

        1 + reply.headers[NUM_PENDING].to_i
      rescue NoRespondersError
        raise unavailable
      end

      # Reads a series of messages a page at a time, following the end-of-batch
      # status of each page to the next one.
      #
      # @param request [Hash{String => Object}] a request with `batch` or `multi_last`
      # @param timeout [Numeric, Deadline, nil] seconds each page may take, or a deadline for the whole series
      # @yieldparam message [StoredMessage] each message, on the calling thread
      # @return [void] nothing
      # @raise [TooManyResultsError] if `multi_last` matches more subjects than the server returns
      # @raise [ServiceUnavailableError] if the stream is missing or does not allow direct access
      # @raise [TimeoutError] if a page does not end in time
      def each(request, timeout, &block)
        replies = Thread::Queue.new
        inbox = @client.new_inbox
        resolver = @client.resolver(inbox) { |msg| replies.push(msg) }
        while request
          @client.publish(@subject, JSON.generate(request), reply_to: inbox, flush: true)
          request = following(request, page(replies, @api.deadline(timeout), &block))
        end
      ensure
        resolver&.unsubscribe
      end

      private

      def address(request)
        subject = request["last_by_subj"]
        subject && request.size == 1 ? ["#{@subject}.#{subject}", nil] : [@subject, JSON.generate(request)]
      end

      def page(replies, deadline)
        previous = 0
        loop do
          msg = replies.pop(timeout: deadline.remaining)
          raise TimeoutError, "a direct get from #{@stream} did not finish within the deadline" unless msg
          return ending(msg) if msg.status

          message = StoredMessage.from_direct(msg)
          check_order(msg, previous)
          previous = message.sequence
          yield message
        end
      end

      def ending(msg)
        case msg.status
        when Protocol::Headers::NO_CONTENT then msg.headers
        when Protocol::Headers::NOT_FOUND then nil
        when Protocol::Headers::NO_RESPONDERS then raise unavailable
        else raise refusal(msg)
        end
      end

      def check_order(msg, previous)
        return if msg.headers[LAST_SEQUENCE].to_i == previous

        raise Error, "a direct get from #{@stream} skipped the messages after sequence #{previous}"
      end

      def following(request, eob)
        return nil unless eob && eob[NUM_PENDING].to_i.positive?

        sequence = eob[LAST_SEQUENCE].to_i + 1
        return nil unless sequence > request.fetch("seq", 0)

        request = request.except("start_time").merge("seq" => sequence)
        return request unless request.key?("multi_last") && eob[UP_TO_SEQUENCE]

        request.except("up_to_time").merge("up_to_seq" => eob[UP_TO_SEQUENCE].to_i)
      end

      def refusal(msg)
        error = { "code" => msg.status, "description" => msg.description }
        case msg.status
        when Protocol::Headers::NOT_FOUND then MessageNotFoundError.new(error)
        when Protocol::Headers::REQUEST_TIMEOUT then BadRequestError.new(error)
        when Protocol::Headers::REQUIRED_API_LEVEL then RequiredApiLevelError.new(error)
        when Protocol::Headers::TOO_MANY_RESULTS
          TooManyResultsError.new("the subjects asked of #{@stream} match more messages than one request returns")
        else APIError.new(error)
        end
      end

      def unavailable
        ServiceUnavailableError.new("nothing answered a direct get from #{@stream}: " \
                                    "the stream does not exist or does not allow direct access")
      end
    end
  end
end
