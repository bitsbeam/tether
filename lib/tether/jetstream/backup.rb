# frozen_string_literal: true

module Tether
  class JetStream
    # Downloads a snapshot of a stream: the server sends chunks to a subject
    # and waits for each to be acknowledged before it sends more.
    #
    # @api private
    class Snapshot
      # @param jetstream [JetStream] the context
      # @param stream [String] the stream's name
      def initialize(jetstream, stream)
        @client = jetstream.client
        @api = jetstream.api
        @stream = stream
      end

      # @param options [Hash{String => Object}] `no_consumers` and `chunk_size`
      # @param timeout [Numeric, Deadline, nil] seconds to wait for each chunk, or a deadline for the whole snapshot
      # @yieldparam chunk [String] the next bytes of the snapshot
      # @return [Array(StreamConfig, StreamState)] what a restore needs besides the bytes
      # @raise [Error] if the server ends the snapshot with a failure
      # @raise [TimeoutError] if a chunk does not arrive in time
      def run(options, timeout, &block)
        inbox = @client.new_inbox
        chunks = @client.subscribe(inbox)
        response = @api.request("STREAM.SNAPSHOT.#{@stream}", options.merge("deliver_subject" => inbox),
                                timeout: @api.deadline(timeout))
        receive(chunks, timeout, &block)
        [StreamConfig.from_h(response.fetch("config")), StreamState.from_h(response.fetch("state"))]
      ensure
        chunks&.unsubscribe
      end

      private

      def receive(chunks, timeout)
        loop do
          msg = chunks.next(timeout: @api.deadline(timeout))
          return finish(msg) if msg.empty?

          yield msg.data
          @client.publish(msg.reply_to) if msg.reply_to
        end
      end

      def finish(msg)
        return if msg.status.nil? || msg.status == Protocol::Headers::NO_CONTENT

        raise Error, "the snapshot of #{@stream} failed: #{msg.status} #{msg.description}".strip
      end
    end

    # Receives the bytes of a snapshot from the block given to
    # {JetStream#restore_stream} and uploads them a chunk at a time, waiting
    # for the server to acknowledge each chunk.
    class RestoreWriter
      CHUNK_SIZE = 64 * 1024

      # @api private
      # @param api [API] the request plumbing of the context
      # @param subject [String] the subject the server takes chunks on
      # @param timeout [Numeric, Deadline, nil] seconds each chunk may take, or a deadline for the whole upload
      def initialize(api, subject, timeout)
        @api = api
        @subject = subject
        @timeout = timeout
        @chunk_size = [CHUNK_SIZE, api.client.max_payload || CHUNK_SIZE].min
        @buffer = String.new(encoding: Encoding::BINARY)
        @finished = false
      end

      # Queues bytes of the snapshot and uploads every full chunk.
      #
      # @param bytes [String] the next bytes of the snapshot
      # @return [Integer] how many bytes were taken
      # @raise [APIError] if the server reports that the restore failed
      # @raise [TimeoutError] if the server does not acknowledge a chunk in time
      def write(bytes)
        raise Error, "the restore has already finished" if @finished

        @buffer << bytes.b
        upload(@buffer.slice!(0, @chunk_size)) while @buffer.bytesize >= @chunk_size
        bytes.bytesize
      end
      alias << write

      # Uploads what is left and tells the server the snapshot is complete.
      #
      # @api private
      # @return [Hash{String => Object}] the server's description of the restored stream
      # @raise [APIError] if the server reports that the restore failed
      def finish
        @finished = true
        upload(@buffer.slice!(0, @chunk_size)) until @buffer.empty?
        result = @api.client.request(@subject, "", timeout: @api.deadline(@timeout))
        raise Error, "the server did not report the result of the restore" if result.empty?

        @api.decode(result.data)
      end

      private

      def upload(chunk)
        reply = @api.client.request(@subject, chunk, timeout: @api.deadline(@timeout))
        return if reply.empty?

        @api.decode(reply.data)
        raise Error, "the server ended the restore before the upload was complete"
      end
    end
  end
end
