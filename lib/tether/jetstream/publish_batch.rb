# frozen_string_literal: true

module Tether
  class JetStream
    # An atomic batch: messages the server stores all together or not at all.
    #
    # The first message is a request, because the server reports a refused
    # batch on it; the rest are plain publishes; and the commit is an empty
    # end-of-batch marker, so no message has to be held back.
    class PublishBatch
      ID = "Nats-Batch-Id"
      SEQUENCE = "Nats-Batch-Sequence"
      COMMIT = "Nats-Batch-Commit"
      END_OF_BATCH = "eob"
      MAX_SIZE = 1000
      API_LEVEL = 2

      # @return [String] the generated batch id
      attr_reader :id

      # @return [Integer] messages published into the batch so far
      attr_reader :size

      # Runs a block with a new batch, then commits it. The batch is abandoned
      # if the block raises.
      #
      # @param jetstream [JetStream] the context
      # @param timeout [Numeric, Deadline] how long each exchange with the server may take
      # @yieldparam batch [PublishBatch] the open batch
      # @return [PubAck, nil] the acknowledgment of the commit; nil when nothing was published
      def self.open(jetstream, timeout:)
        batch = new(jetstream, timeout: timeout)
        begin
          yield batch
          batch.commit
        ensure
          batch.abandon
        end
      end

      # @param jetstream [JetStream] the context
      # @param timeout [Numeric, Deadline] how long each exchange with the server may take
      def initialize(jetstream, timeout:)
        @client = jetstream.client
        @timeout = timeout
        @id = Connection::NUID.next
        @size = 0
        @subject = nil
        @closed = false
        @lock = Mutex.new
      end

      # Adds a message to the batch. Nothing is stored until the batch commits.
      #
      # @param subject [String] a subject of the stream the batch goes to
      # @param payload [String, nil] the message body
      # @param headers [Protocol::Headers, Hash, nil] headers to send with the message
      # @param options [Hash{Symbol => Object}] the options of {JetStream#publish}: `msg_id:`, `expect:`,
      #   `ttl:`, `rollup:`, `schedule:`, `cancel_schedule:`
      # @return [self] this batch
      # @raise [ArgumentError] if `expect:` holds `last_msg_id:`, which the server rejects in a batch
      # @raise [BatchTooLargeError] if the batch already holds {MAX_SIZE} messages
      # @raise [APIError] the mapped error when the server refuses to open the batch
      # @raise [NoStreamResponseError] if no stream captures the first subject
      def publish(subject, payload = nil, headers: nil, **options)
        if options[:expect]&.key?(:last_msg_id)
          raise ArgumentError, "expect: { last_msg_id: } cannot be used in a batch"
        end

        @lock.synchronize do
          raise Error, "the batch is closed" if @closed
          raise too_large if @size >= MAX_SIZE

          stage(subject, payload, numbered(PublishHeaders.build(headers, **options), @size + 1))
          @size += 1
          @subject = subject
        end
        self
      end

      # Commits the batch with an end-of-batch marker.
      #
      # @api private
      # @return [PubAck, nil] the acknowledgment; nil when nothing was published
      # @raise [APIError] the error of the first message that failed a check; nothing was stored
      # @raise [IncompleteBatchError] if the server abandoned the batch, which it does after ten idle seconds
      # @raise [TimeoutError] if the server did not answer, which leaves the outcome unknown
      def commit
        @lock.synchronize do
          raise Error, "the batch is closed" if @closed

          @closed = true
          next nil if @size.zero?

          headers = numbered(nil, @size + 1)
          headers[COMMIT] = END_OF_BATCH
          confirm(PubAck.parse(request(@subject, nil, headers).data))
        end
      end

      # Makes the server drop what was staged, by breaking the batch's
      # sequence. Does nothing once the batch is committed or abandoned.
      #
      # @api private
      # @return [void] nothing
      def abandon
        @lock.synchronize do
          next if @closed

          @closed = true
          @client.publish(@subject, nil, headers: numbered(nil, @size + 2)) unless @size.zero?
        end
        nil
      rescue Tether::Error
        nil
      end

      # @return [String] the id and size
      def inspect
        "#<Tether::JetStream::PublishBatch id=#{@id.inspect} size=#{@size}>"
      end

      private

      def numbered(headers, sequence)
        headers ||= Protocol::Headers.new
        headers[ID] = @id
        headers[SEQUENCE] = sequence.to_s
        headers[API::LEVEL_HEADER] = API_LEVEL.to_s
        headers
      end

      def stage(subject, payload, headers)
        return @client.publish(subject, payload, headers: headers) unless @size.zero?

        start(subject, payload, headers)
      end

      def start(subject, payload, headers)
        reply = request(subject, payload, headers)
        return if reply.data.empty?

        PubAck.parse(reply.data)
        raise Error, "the server stored the first message instead of opening a batch: it does not support them"
      rescue Tether::Error
        @closed = true
        raise
      end

      def request(subject, payload, headers)
        @client.request(subject, payload, headers: headers, timeout: Deadline.coerce(@timeout))
      rescue NoRespondersError
        raise NoStreamResponseError, "no stream answered a batch publish to #{subject}"
      end

      def too_large
        BatchTooLargeError.new("code" => 400, "err_code" => 10_199,
                               "description" => "atomic publish batch is too large: #{@size + 1}")
      end

      def confirm(ack)
        return ack if ack.batch_id == @id && ack.count == @size

        raise Error, "the commit of batch #{@id} was acknowledged for batch #{ack.batch_id.inspect} " \
                     "with #{ack.count.inspect} of #{@size} messages"
      end
    end
  end
end
