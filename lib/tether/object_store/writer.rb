# frozen_string_literal: true

require "digest"

module Tether
  class ObjectStore
    # One upload: cuts a source into chunks, publishes them on a fresh chunk
    # subject with a bounded number of acknowledgments outstanding, and hashes
    # the content as it goes.
    #
    # @api private
    class Writer
      WINDOW_BYTES = 8 * 1024 * 1024
      MAX_WINDOW = 256

      # What an upload stored.
      Result = Data.define(:size, :chunks, :digest)

      # @param jetstream [JetStream] the context to publish through
      # @param subject [String] the chunk subject of this upload
      # @param chunk_size [Integer] the most bytes one chunk carries
      # @param timeout [Numeric, Deadline, nil] seconds each wait on the server may take, or a deadline
      #   for the whole upload
      def initialize(jetstream, subject, chunk_size, timeout)
        @jetstream = jetstream
        @subject = subject
        @chunk_size = chunk_size
        @timeout = timeout
        @window = (WINDOW_BYTES / chunk_size).clamp(1, MAX_WINDOW)
        @futures = []
        @digest = Digest::SHA256.new
        @size = @chunks = 0
      end

      # Publishes the whole source and waits for every acknowledgment.
      #
      # @param source [String, #read, nil] the content
      # @return [Result] the size, chunk count and digest of what was stored
      # @raise [JetStream::APIError] if the server refuses a chunk
      # @raise [TimeoutError] if an acknowledgment does not arrive in time
      def write(source)
        pieces(source) { |piece| publish(piece) }
        confirm until @futures.empty?
        Result.new(size: @size, chunks: @chunks, digest: Info.digest_of(@digest.digest))
      end

      # Waits until no chunk of this upload is in flight any more, whatever became of it.
      #
      # @param deadline [Deadline] how long to wait
      # @return [void] nothing
      def settle(deadline)
        @futures.each { |future| future.wait(timeout: deadline) }
        @futures.clear
        nil
      end

      private

      def pieces(source, &block)
        case source
        when nil then nil
        when String then slices(source, &block)
        else stream(source, &block)
        end
      end

      def slices(source)
        offset = 0
        while offset < source.bytesize
          yield source.byteslice(offset, @chunk_size)
          offset += @chunk_size
        end
      end

      def stream(source)
        while (piece = source.read(@chunk_size))
          break if piece.empty?

          yield piece
        end
      end

      def publish(piece)
        confirm while @futures.size >= @window
        @digest.update(piece)
        @futures << @jetstream.publish_async(@subject, piece, stall: deadline)
        @size += piece.bytesize
        @chunks += 1
      end

      def confirm
        @futures.first.value(timeout: deadline)
        @futures.shift
      end

      def deadline
        @jetstream.api.deadline(@timeout)
      end
    end
  end
end
