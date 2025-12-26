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
      COMPACT_THRESHOLD = 64 * 1024
      SNIPPET_LIMIT = 64

      AWAITING_CONTROL_LINE = :control
      AWAITING_PAYLOAD = :payload

      MSG_LINE = /\GMSG[ \t]+(\S+)[ \t]+(\S+)[ \t]+(?:(\S+)[ \t]+)?(\d{1,9})[ \t]*\r\n/i
      HMSG_LINE = /\GHMSG[ \t]+(\S+)[ \t]+(\S+)[ \t]+(?:(\S+)[ \t]+)?(\d{1,9})[ \t]+(\d{1,9})[ \t]*\r\n/i

      CR = 13
      LF = 10
      UPPER_A = 65
      UPPER_Z = 90
      CASE_SHIFT = 32
      private_constant :MSG_LINE, :HMSG_LINE, :CR, :LF, :UPPER_A, :UPPER_Z, :CASE_SHIFT

      # @return [Symbol] :control or :payload
      attr_reader :state

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
      # @return [self]
      def <<(data)
        return self if @broken || data.nil? || data.empty?

        @buffer << (data.encoding == BINARY ? data : data.b)
        run
        self
      end
      alias parse <<

      # Discards buffered bytes and returns to the initial state.
      #
      # @return [void]
      def reset!
        @buffer.clear
        @position = 0
        @state = AWAITING_CONTROL_LINE
        @broken = false
        clear_frame
      end

      # @return [Boolean] whether a protocol violation stopped this parser
      def broken?
        @broken
      end

      # @return [Integer] bytes buffered but not yet consumed
      def pending_bytes
        @buffer.bytesize - @position
      end

      private

      def run
        loop do
          progressed = @state == AWAITING_CONTROL_LINE ? consume_control_line : consume_payload
          break unless progressed
        end
        compact
      end

      def consume_control_line
        matched = match_message_line
        return matched unless matched.nil?

        line_end = @buffer.index(CRLF, @position)
        unless line_end
          fail_protocol!("control line exceeds #{@max_control_line} bytes") if pending_bytes > @max_control_line
          return false
        end

        start = @position
        @position = line_end + CRLF_SIZE
        dispatch(start, line_end)
      end

      def dispatch(start, line_end)
        case @buffer.getbyte(start)
        when 77, 109, 72, 104 then malformed(start, line_end)
        when 80, 112 then dispatch_ping_pong(start, line_end)
        when 43 then dispatch_ok(start, line_end)
        when 45 then dispatch_err(start, line_end)
        when 73, 105 then dispatch_info(start, line_end)
        else unknown_operation(start, line_end)
        end
      end

      def match_message_line
        byte = @buffer.getbyte(@position)
        pattern = case byte
                  when 77, 109 then MSG_LINE
                  when 72, 104 then HMSG_LINE
                  end
        return nil unless pattern

        match = pattern.match(@buffer, @position)
        return nil unless match

        @position = match.end(0)
        start_payload(match, pattern.equal?(HMSG_LINE))
      end

      def start_payload(match, headers)
        @subject = match[1]
        @sid = match[2]
        @reply_to = match[3]
        @header_size = headers ? match[4].to_i : nil
        @needed = (headers ? match[5] : match[4]).to_i

        return fail_protocol!("header size exceeds total size on #{@subject}") if @header_size && @header_size > @needed
        if @max_payload && @needed > @max_payload
          return fail_protocol!("frame of #{@needed} bytes exceeds max_payload #{@max_payload} on #{@subject}")
        end

        @state = AWAITING_PAYLOAD
        true
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

      def consume_payload
        return false if pending_bytes < @needed + CRLF_SIZE

        return fail_protocol!("payload not terminated by CRLF on #{@subject}") unless payload_terminated?

        emit_message
        @position += @needed + CRLF_SIZE
        @state = AWAITING_CONTROL_LINE
        clear_frame
        true
      end

      def payload_terminated?
        @buffer.getbyte(@position + @needed) == CR && @buffer.getbyte(@position + @needed + 1) == LF
      end

      def emit_message
        if @header_size
          headers = @buffer.byteslice(@position, @header_size)
          payload = @buffer.byteslice(@position + @header_size, @needed - @header_size)
          @sink.on_msg(@subject, @sid, @reply_to, headers, payload)
        else
          @sink.on_msg(@subject, @sid, @reply_to, nil, @buffer.byteslice(@position, @needed))
        end
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

      def compact
        if @position >= @buffer.bytesize
          @buffer.clear
          @position = 0
        elsif @position >= COMPACT_THRESHOLD
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
