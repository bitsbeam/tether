# frozen_string_literal: true

require "digest"

module Tether
  class ObjectStore
    # Reads one object's content, a chunk at a time, through an ordered consumer
    # on the object's chunk subject. It is what {ObjectStore#get} returns.
    #
    # Nothing is held in memory but the chunks in flight. The size and the
    # SHA-256 of what was read are checked when the last chunk has been taken,
    # so the call that reaches the end raises {DigestMismatchError} for a
    # corrupt object. Nothing exists on the server until the first read.
    #
    # A reader is meant for one thread. A lost connection or server consumer is
    # recovered from without a gap or a repeated chunk.
    class Reader
      WINDOW_BYTES = 4 * 1024 * 1024
      MAX_WINDOW = 256

      # @return [Info] the metadata of the object being read
      attr_reader :info

      # @api private
      # @param stream [JetStream::Stream] the stream behind the bucket that holds the chunks
      # @param info [Info] the object's metadata
      # @param timeout [Numeric, Deadline, nil] seconds each wait for a chunk may take, or a deadline for
      #   the whole download; nil uses the context's default for each wait
      def initialize(stream, info, timeout: nil)
        @stream = stream
        @info = info
        @total = info.size
        @timeout = timeout || stream.jetstream.timeout
        @digest = Digest::SHA256.new
        @received = 0
        @buffer = nil
        @pull = nil
        @state = :open
        @error = nil
      end

      # Reads bytes, like `IO#read`.
      #
      # @param length [Integer, nil] the most bytes to return; nil reads everything that is left
      # @param buffer [String, nil] a string to fill instead of returning a new one
      # @return [String, nil] the bytes, binary; at the end, an empty string without a length and nil with one
      # @raise [DigestMismatchError] if the object's content does not match its recorded size or digest
      # @raise [TimeoutError] if a chunk does not arrive in time; the reader can be read from again
      # @raise [IOError] if the reader was closed
      def read(length = nil, buffer = nil)
        raise ArgumentError, "negative length #{length} given" if length&.negative?

        data = length ? take(length) : drain
        return data unless buffer

        buffer.replace(data || "")
        data && buffer
      end

      # Yields the content a chunk at a time, as the chunks were stored.
      #
      # @yieldparam chunk [String] the next bytes, binary
      # @return [self, Enumerator<String>] this reader, or an enumerator when no block is given
      # @raise [DigestMismatchError] if the object's content does not match its recorded size or digest
      # @raise [TimeoutError] if a chunk does not arrive in time
      # @raise [IOError] if the reader was closed
      def each
        return enum_for(:each) unless block_given?

        while (chunk = next_chunk)
          yield chunk
        end
        self
      end

      # @return [Boolean] whether everything was read and verified
      def eof?
        @state == :finished && @buffer.nil?
      end

      # Stops reading and removes the consumer. Closing twice is fine.
      #
      # @return [nil] nothing
      def close
        release
        @buffer = nil
        @state = :closed unless @state == :finished
        nil
      end

      # @return [Boolean] whether the reader was closed before it reached the end
      def closed?
        @state == :closed
      end

      # @return [String] the object and how far the reader is
      def inspect
        "#<Tether::ObjectStore::Reader bucket=#{@info.bucket.inspect} name=#{@info.name.inspect} " \
          "read=#{@received}/#{@total}>"
      end

      private

      def take(length)
        data = "".b
        return data if length.zero?

        while data.bytesize < length && (chunk = next_chunk)
          data << chunk
        end
        return nil if data.empty?
        return data if data.bytesize <= length

        @buffer = data.byteslice(length, data.bytesize - length)
        data.byteslice(0, length)
      end

      def drain
        data = "".b
        while (chunk = next_chunk)
          data << chunk
        end
        data
      end

      def next_chunk
        if @buffer
          chunk = @buffer
          @buffer = nil
          return chunk
        end
        raise @error if @error
        raise IOError, "closed stream" if @state == :closed
        return nil if @state == :finished

        fetch
      end

      def fetch
        return finish if @total.zero?

        @pull ||= open
        return finish if @pull.caught_up?

        message = @pull.next_initial(timeout: @timeout) or return verify
        chunk = message.data
        @digest.update(chunk)
        @received += chunk.bytesize
        verify if @pull.caught_up?
        chunk
      rescue StopIteration
        fail_with(Error.new("the download of #{@info.name} from #{@info.bucket} was stopped"))
      rescue JetStream::StreamNotFoundError => e
        fail_with(BucketNotFoundError.new("bucket #{@info.bucket} does not exist"), e)
      end

      def open
        window = (WINDOW_BYTES / [@total.fdiv([@info.chunks, 1].max).ceil, 1].max).clamp(1, MAX_WINDOW)
        @stream.ordered_consumer(filter_subjects: ObjectStore.chunk_subject(@info.bucket, @info.nuid),
                                 deliver_policy: :all).messages(max_messages: window)
      end

      def finish
        verify unless @total.zero?
        @state = :finished
        nil
      end

      def verify
        release
        @state = :finished
        expected = @info.digest_bytes
        subject = "object #{@info.name} in #{@info.bucket}"
        if expected.nil?
          fail_with(DigestMismatchError.new("the digest recorded for #{subject} has an invalid format"))
        elsif @received != @total
          fail_with(DigestMismatchError.new("read #{@received} bytes of #{subject}, which records #{@total}"))
        elsif @digest.digest != expected
          fail_with(DigestMismatchError.new("the content of #{subject} does not match its digest"))
        end
      end

      def fail_with(error, cause = nil)
        release
        @buffer = nil
        @error = error
        raise error, cause: cause || error.cause
      end

      def release
        pull = @pull
        @pull = nil
        pull&.stop
      end
    end
  end
end
