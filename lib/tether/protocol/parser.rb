# frozen_string_literal: true

module Tether
  module Protocol
    # Incremental parser for the server side of the NATS protocol.
    #
    # Bytes are appended to one buffer and consumed by advancing an index.
    # Control lines are read in place: subjects, reply subjects, header blocks
    # and payloads are the only substrings allocated, and integers are scanned
    # straight out of the buffer. The buffer is reset in place once drained and
    # compacted only when the consumed prefix passes {COMPACT_THRESHOLD}.
    #
    # The parser never raises into its caller. The reader thread is the only
    # demultiplexer for the whole connection, so its death would stall every
    # subscription at once; malformed input is reported through
    # `sink.on_protocol_error` and the parser marks itself {#broken?}.
    #
    # The sink must respond to:
    #
    # - `on_msg(subject, sid, reply_to, header_bytes, payload)` where `sid` is
    #   the opaque token this client chose at SUB time, echoed back verbatim,
    #   and `header_bytes` is nil for `MSG` or the raw header block for `HMSG`
    # - `on_ping`, `on_pong`, `on_ok`
    # - `on_info(json)` with the still unparsed JSON text
    # - `on_error(description)` for an `-ERR` line
    # - `on_protocol_error(message)`
    class Parser
      # A consumed prefix this large is dropped even if the remainder is long.
      COMPACT_THRESHOLD = 64 * 1024

      # A leftover at or below this is copied out rather than left in place, so
      # the buffer does not grow across reads and make every later append
      # target a large string.
      SHORT_REMAINDER = 1024
      # How much of an offending control line is quoted back in an error.
      SNIPPET_LIMIT = 64

      # {#state} while the next thing expected is a control line.
      AWAITING_CONTROL_LINE = :control

      # {#state} while a frame's payload is still arriving.
      AWAITING_PAYLOAD = :payload

      # Matched at the read position with \G, so it anchors instead of scanning
      # forward, and read through String#index so no MatchData is allocated.
      MSG_LINE = /\GMSG[ \t]+(\S+)[ \t]+(\S+)[ \t]+(?:(\S+)[ \t]+)?(\d{1,9})[ \t]*\r\n/i
      # As {MSG_LINE}, with the header block's size ahead of the total size.
      HMSG_LINE = /\GHMSG[ \t]+(\S+)[ \t]+(\S+)[ \t]+(?:(\S+)[ \t]+)?(\d{1,9})[ \t]+(\d{1,9})[ \t]*\r\n/i

      # The first byte of the CRLF a payload must end with. The bytes below are
      # what the control line dispatch compares against, in both cases because
      # the protocol is case insensitive.
      CR = 13
      # The second byte of the CRLF a payload must end with.
      LF = 10
      # First byte of a MSG control line.
      UPPER_M = 77
      # First byte of a MSG control line, lower case.
      LOWER_M = 109
      # First byte of an HMSG control line.
      UPPER_H = 72
      # First byte of an HMSG control line, lower case.
      LOWER_H = 104
      # First byte of a PING or PONG line.
      UPPER_P = 80
      # First byte of a PING or PONG line, lower case.
      LOWER_P = 112
      # First byte of an INFO line.
      UPPER_I = 73
      # First byte of an INFO line, lower case.
      LOWER_I = 105
      # First byte of a +OK line.
      PLUS = 43
      # First byte of an -ERR line.
      MINUS = 45
      # Start of the range {#verb?} case folds over.
      UPPER_A = 65
      # End of the range {#verb?} case folds over.
      UPPER_Z = 90
      # Distance from an upper case letter to its lower case form.
      CASE_SHIFT = 32

      # @return [Symbol] :control or :payload
      def state = @awaiting_payload ? AWAITING_PAYLOAD : AWAITING_CONTROL_LINE

      # @return [Integer, nil] largest frame accepted, from the server's INFO
      attr_accessor :max_payload

      # @param sink [Object] receives the parsed frames
      # @param max_control_line [Integer] sanity bound on a single control line
      # @param max_payload [Integer, nil] reject a frame larger than the server
      #   advertised in INFO, rather than buffering toward it
      def initialize(sink, max_control_line: DEFAULT_MAX_CONTROL_LINE, max_payload: nil)
        @sink = sink
        @max_control_line = max_control_line
        @max_payload = max_payload
        @buffer = String.new(capacity: COMPACT_THRESHOLD, encoding: BINARY)
        reset!
      end

      # Appends bytes and consumes every complete frame in the buffer.
      #
      # @param data [String] bytes as read from the socket
      # @return [self] this parser, so appends can be chained
      def <<(data)
        return self if @broken || data.nil? || data.empty?

        @buffer << (data.encoding == BINARY ? data : data.b)
        run
        self
      end
      alias parse <<

      # Discards buffered bytes and returns to the initial state.
      #
      # @return [void] nothing
      def reset!
        @buffer.clear
        @position = 0
        @awaiting_payload = false
        @broken = false
        clear_frame
      end

      # A broken parser consumes nothing further; the client is expected to
      # tear the connection down.
      #
      # @return [Boolean] whether a protocol violation stopped this parser
      def broken?
        @broken
      end

      # @return [Integer] bytes buffered but not yet consumed, which is the tail
      #   of a frame still arriving
      def pending_bytes
        @buffer.bytesize - @position
      end

      private

      def run
        buffer = @buffer
        sink = @sink
        pos = @position
        size = buffer.bytesize

        loop do
          if @awaiting_payload
            needed = @needed
            break if size - pos < needed + CRLF_SIZE

            unless buffer.getbyte(pos + needed) == CR && buffer.getbyte(pos + needed + 1) == LF
              @position = pos
              return fail_protocol!("payload not terminated by CRLF on #{@subject}")
            end

            if (header_size = @header_size)
              sink.on_msg(@subject, @sid, @reply_to, buffer.byteslice(pos, header_size),
                          buffer.byteslice(pos + header_size, needed - header_size))
            else
              sink.on_msg(@subject, @sid, @reply_to, nil, buffer.byteslice(pos, needed))
            end
            pos += needed + CRLF_SIZE
            @awaiting_payload = false
            next
          end

          line_end = buffer.index(CRLF, pos)
          unless line_end
            if size - pos > @max_control_line
              @position = pos
              return fail_protocol!("control line exceeds #{@max_control_line} bytes")
            end
            break
          end

          byte = buffer.getbyte(pos)
          if [UPPER_M, LOWER_M].include?(byte)
            unless buffer.index(MSG_LINE, pos)
              @position = pos
              return malformed(pos, line_end)
            end

            subject = ::Regexp.last_match(1)
            sid = ::Regexp.last_match(2)
            reply_to = ::Regexp.last_match(3)
            header_size = nil
            needed = ::Regexp.last_match(4).to_i
          elsif [UPPER_H, LOWER_H].include?(byte)
            unless buffer.index(HMSG_LINE, pos)
              @position = pos
              return malformed(pos, line_end)
            end

            subject = ::Regexp.last_match(1)
            sid = ::Regexp.last_match(2)
            reply_to = ::Regexp.last_match(3)
            header_size = ::Regexp.last_match(4).to_i
            needed = ::Regexp.last_match(5).to_i
          else
            @position = pos
            return unless dispatch(pos, line_end)

            pos = line_end + CRLF_SIZE
            next
          end

          if (header_size && header_size > needed) || (@max_payload && needed > @max_payload)
            @position = pos
            return reject_sizes(subject, header_size, needed)
          end

          pos = line_end + CRLF_SIZE

          # The payload is usually already buffered, in which case the frame is
          # emitted straight from locals and never touches the object at all.
          if size - pos >= needed + CRLF_SIZE
            unless buffer.getbyte(pos + needed) == CR && buffer.getbyte(pos + needed + 1) == LF
              @position = pos
              return fail_protocol!("payload not terminated by CRLF on #{subject}")
            end

            if header_size
              sink.on_msg(subject, sid, reply_to, buffer.byteslice(pos, header_size),
                          buffer.byteslice(pos + header_size, needed - header_size))
            else
              sink.on_msg(subject, sid, reply_to, nil, buffer.byteslice(pos, needed))
            end
            pos += needed + CRLF_SIZE
            next
          end

          @subject = subject
          @sid = sid
          @reply_to = reply_to
          @header_size = header_size
          @needed = needed
          @awaiting_payload = true
        end

        @position = pos
        compact
      end

      def reject_sizes(subject, header_size, needed)
        return fail_protocol!("header size exceeds total size on #{subject}") if header_size && header_size > needed

        fail_protocol!("frame of #{needed} bytes exceeds max_payload #{@max_payload} on #{subject}")
      end

      def dispatch(start, line_end)
        case @buffer.getbyte(start)
        when UPPER_P, LOWER_P then dispatch_ping_pong(start, line_end)
        when PLUS then dispatch_ok(start, line_end)
        when MINUS then dispatch_err(start, line_end)
        when UPPER_I, LOWER_I then dispatch_info(start, line_end)
        else unknown_operation(start, line_end)
        end
      end

      def dispatch_ping_pong(start, line_end)
        if verb?(start, line_end, "PING")
          @sink.on_ping
          true
        elsif verb?(start, line_end, "PONG")
          @sink.on_pong
          true
        else
          unknown_operation(start, line_end)
        end
      end

      def dispatch_ok(start, line_end)
        return unknown_operation(start, line_end) unless verb?(start, line_end, "+OK")

        @sink.on_ok
        true
      end

      def dispatch_err(start, line_end)
        return unknown_operation(start, line_end) unless verb?(start, line_end, "-ERR")

        description = trimmed(start + 4, line_end)
        @sink.on_error(description.delete_prefix("'").delete_suffix("'"))
        true
      end

      def dispatch_info(start, line_end)
        return unknown_operation(start, line_end) unless verb?(start, line_end, "INFO")

        json = trimmed(start + 4, line_end)
        return fail_protocol!("malformed INFO: #{snippet(start, line_end)}") if json.empty?

        @sink.on_info(json)
        true
      end

      def verb?(start, limit, verb)
        return false if limit - start < verb.bytesize

        verb.bytesize.times do |offset|
          expected = verb.getbyte(offset)
          actual = @buffer.getbyte(start + offset)
          next if actual == expected
          return false unless expected.between?(UPPER_A, UPPER_Z) && actual == expected + CASE_SHIFT
        end
        true
      end

      def trimmed(from, to)
        @buffer.byteslice(from, to - from).strip
      end

      def snippet(start, line_end)
        @buffer.byteslice(start, [line_end - start, SNIPPET_LIMIT].min).inspect
      end

      def clear_frame
        @subject = nil
        @sid = nil
        @reply_to = nil
        @needed = nil
        @header_size = nil
      end

      # Draining the buffer in place is the common case. Otherwise the leftover
      # is copied out whenever it is short, which keeps the append target small
      # instead of letting it grow toward COMPACT_THRESHOLD across many reads;
      # copying a few hundred bytes costs far less than appending into a buffer
      # that has grown to tens of kilobytes. A long remainder, meaning a large
      # frame still arriving, is left alone until the threshold.
      def compact
        remaining = @buffer.bytesize - @position
        if remaining.zero?
          @buffer.clear
          @position = 0
        elsif @position >= COMPACT_THRESHOLD || (@position.positive? && remaining <= SHORT_REMAINDER)
          @buffer = @buffer.byteslice(@position..) || String.new(encoding: BINARY)
          @position = 0
        end
      end

      def malformed(start, line_end)
        fail_protocol!("malformed control line: #{snippet(start, line_end)}")
      end

      def unknown_operation(start, line_end)
        fail_protocol!("unknown protocol operation: #{snippet(start, line_end)}")
      end

      def fail_protocol!(message)
        @broken = true
        @sink.on_protocol_error(message)
        false
      end
    end
  end
end
