# frozen_string_literal: true

require "zlib"
require "digest/sha1"
require "securerandom"

module Tether
  module Connection
    # A transport that carries the NATS byte stream inside WebSocket frames
    # (RFC 6455), for `ws://` and `wss://` servers.
    #
    # Frame boundaries carry no meaning: {#write} wraps whatever it is given in
    # binary messages and {#read} returns the payload of whatever data frames
    # arrived, in any fragmentation.
    #
    # One reader and one writer may use the transport at the same time. The
    # reader never waits on the socket to answer a ping or a close frame: it
    # queues the control frame and writes it only when no data write is in
    # progress and the socket takes it at once. Otherwise the writer that holds
    # the socket sends it ahead of its next message.
    class WebSocket < Transport
      GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
      HTTP_HEADER_LIMIT = 16 * 1024
      HEADER_END = "\r\n\r\n"

      CONTINUATION = 0x0
      TEXT = 0x1
      BINARY = 0x2
      CLOSE = 0x8
      PING = 0x9
      PONG = 0xa

      FIN = 0x80
      RSV1 = 0x40
      RESERVED = 0x30
      MASKED = 0x80
      CONTROL_LIMIT = 125

      NORMAL_CLOSURE = 1000
      PROTOCOL_VIOLATION = 1002

      NO_MASKING_HEADER = "Nats-No-Masking"
      DEFLATE_OFFER = "permessage-deflate; server_no_context_takeover; client_no_context_takeover"
      DEFLATE_TAIL = "\x00\x00\xff\xff".b.freeze

      # Messages at or below this size are sent uncompressed, as the server does
      # for its own, because deflate cannot shrink them.
      COMPRESSION_THRESHOLD = 64

      # The most bytes one outgoing WebSocket message carries.
      #
      # With compression the server refuses a message that inflates to more than
      # its `max_payload`, and a transport is opened before `INFO` says what
      # that is. 32 KiB stays below any `max_payload` a server is plausibly
      # configured with (the default is 1 MiB), equals the deflate window, so
      # splitting costs no compression ratio, and matches the flush size
      # nats-server sees from nats.go. NATS frame boundaries carry no meaning
      # over WebSocket, so a buffer is cut wherever the limit falls.
      MAX_MESSAGE_SIZE = 32 * 1024

      # @return [Integer, nil] the status code of the close frame the server sent, if any
      attr_reader :close_code

      # @return [String, nil] the reason text of the close frame the server sent, if any
      attr_reader :close_reason

      # @param uri [URI::Generic] the server to reach
      # @param options [Options] the client's options
      # @raise [ArgumentError] if the path or a header cannot be put in an HTTP request
      def initialize(uri, options)
        super(uri)
        @options = options
        @settings = options.websocket || Options::WEBSOCKET_DEFAULTS
        @path = request_path
        @headers = request_headers
        @upgraded = false
        @masking = true
        @deflater = nil
        @inflater = nil
        @write_lock = Mutex.new
        @control = Thread::Queue.new
        @backlog = binary
        @close_sent = false
        reset_inbound
      end

      # Opens the socket, encrypts it when the scheme is `wss`, and upgrades the
      # connection to WebSocket.
      #
      # @param deadline [Deadline] covers DNS, TCP, TLS and the HTTP upgrade
      # @return [self] this transport, ready to carry NATS frames
      # @raise [ConnectionError] if the server cannot be reached or refuses the upgrade
      # @raise [TLSError] if the TLS handshake fails
      # @raise [TimeoutError] if the deadline passes first
      def open(deadline)
        super
        encrypt(deadline) if @uri.scheme == "wss"
        upgrade(deadline)
        self
      rescue StandardError
        abandon
        raise
      end

      # Encrypts the socket. Only meaningful before the HTTP upgrade, which
      # {#open} takes care of: TLS on a WebSocket connection is chosen by the
      # `wss` scheme and never negotiated at the NATS level.
      #
      # @param context [OpenSSL::SSL::SSLContext] the TLS settings
      # @param hostname [String] the name to send as SNI and to verify the certificate against
      # @param deadline [Deadline] how long the handshake may take
      # @return [self] this transport, encrypted
      # @raise [TLSError] if the handshake fails or the connection is already upgraded to WebSocket
      def upgrade_to_tls(context, hostname, deadline)
        raise TLSError, "#{label} is a WebSocket connection, use the wss scheme for TLS" if @upgraded

        super
      end

      # Reads the payload of the data frames that arrived, waiting for some when
      # there are none. Ping and close frames are answered on the way.
      #
      # @param deadline [Deadline] how long to wait for bytes
      # @return [String, nil] NATS bytes, or nil once the server closed the connection
      # @raise [TimeoutError] if the deadline passes with nothing readable
      # @raise [ConnectionClosedError] if the socket fails, was closed, or the server breaks the protocol
      def read(deadline = Deadline.infinite)
        loop do
          payload = drain_frames
          return payload if payload
          return nil if @eof

          chunk = super or return nil
          @inbound << chunk
        end
      end

      # Sends the bytes as masked binary messages of at most {MAX_MESSAGE_SIZE}
      # bytes each, compressed when the server agreed to it.
      #
      # @param data [String] the bytes to send
      # @param deadline [Deadline] how long the whole write may take
      # @return [Integer] bytes of data written
      # @raise [TimeoutError] if the socket does not accept the data in time
      # @raise [ConnectionClosedError] if the socket fails or was closed
      def write(data, deadline = Deadline.infinite)
        @write_lock.synchronize do
          out = @backlog
          @backlog = binary
          queued_control(out)
          messages(data, out)
          super(out, deadline)
        end
        flush_control
        data.bytesize
      end

      # Sends a close frame if the socket takes it without waiting, then ends
      # the connection in both directions.
      #
      # @return [void] nothing
      def close
        send_close(NORMAL_CLOSURE)
        super
      end

      private

      def binary
        String.new(encoding: Encoding::BINARY)
      end

      def reset_inbound
        @inbound = binary
        @partial = binary
        @in_message = false
        @compressed = false
        @eof = false
        @close_code = nil
        @close_reason = nil
      end

      def request_path
        path = @settings[:path] || @uri.request_uri
        path = "/" if path.nil? || path.empty?
        raise ArgumentError, "websocket path: #{path.inspect} is not an HTTP request target" if path.match?(/[\s\0]/)

        path
      end

      def request_headers
        (@settings[:headers] || {}).to_h do |name, value|
          name = name.to_s
          value = value.to_s
          if name.empty? || name.match?(/[^!#$%&'*+\-.^_`|~0-9A-Za-z]/) || value.match?(/[\r\n\0]/)
            raise ArgumentError, "websocket header: #{name.inspect} => #{value.inspect} is not an HTTP header"
          end

          [name, value]
        end
      end

      def encrypt(deadline)
        settings = @options.tls || Options::TLS_DEFAULTS
        upgrade_to_tls(TLS.context(settings), settings[:hostname] || @uri.host, deadline)
      end

      def upgrade(deadline)
        key = SecureRandom.base64(16)
        transport_write(upgrade_request(key), deadline)
        status, fields = read_response(deadline)
        refused(status) unless status.match?(%r{\AHTTP/1\.1 101(?: |\z)})

        verify(fields, key)
        negotiate_extensions(fields)
        @upgraded = true
      end

      def transport_write(data, deadline)
        Transport.instance_method(:write).bind_call(self, data, deadline)
      end

      def transport_read(deadline)
        Transport.instance_method(:read).bind_call(self, deadline)
      end

      def upgrade_request(key)
        lines = ["GET #{@path} HTTP/1.1", "Host: #{label}", "Upgrade: websocket", "Connection: Upgrade",
                 "Sec-WebSocket-Key: #{key}", "Sec-WebSocket-Version: 13"]
        lines << "Sec-WebSocket-Extensions: #{DEFLATE_OFFER}" if @settings[:compression]
        @headers.each { |name, value| lines << "#{name}: #{value}" }
        "#{lines.join("\r\n")}#{HEADER_END}"
      end

      def read_response(deadline)
        response = binary
        until (finish = response.index(HEADER_END))
          refuse(response) unless plausible?(response)
          chunk = transport_read(deadline) or refuse(response)
          response << chunk
        end
        @inbound << response.byteslice(finish + HEADER_END.bytesize, response.bytesize)
        status, *lines = response.byteslice(0, finish).split("\r\n")
        [status.to_s, lines.to_h { |line| header_field(line) }]
      end

      def plausible?(response)
        return false if response.bytesize > HTTP_HEADER_LIMIT

        line_end = response.index("\r\n") || response.index("\n")
        line_end ? response.start_with?("HTTP/1.1 101") : "HTTP/1.1 101".start_with?(response.byteslice(0, 12))
      end

      def refuse(response)
        status = response.lines.first.to_s.strip.byteslice(0, 120).to_s.scrub("?")
        refused(status.empty? ? "the server closed the connection" : status)
      end

      def refused(reason)
        raise ConnectionError, "WebSocket upgrade to #{label} failed: #{reason}"
      end

      def header_field(line)
        name, value = line.split(":", 2)
        [name.to_s.strip.downcase, value.to_s.strip]
      end

      def verify(fields, key)
        expected = Digest::SHA1.base64digest(key + GUID)
        refused("the response is not an upgrade to websocket") unless
          fields["upgrade"].to_s.casecmp?("websocket") && fields["connection"].to_s.downcase.include?("upgrade")
        refused("Sec-WebSocket-Accept does not match the key") unless fields["sec-websocket-accept"] == expected
      end

      def negotiate_extensions(fields)
        @masking = !(requested_no_masking? && fields[NO_MASKING_HEADER.downcase] == "true")
        extensions = fields["sec-websocket-extensions"].to_s.downcase.split(/[;,]/).map(&:strip)
        return if extensions.empty?

        refused("the server chose an extension that was not offered") unless
          @settings[:compression] && extensions.include?("permessage-deflate")
        refused("the server did not agree to no context takeover") unless
          extensions.include?("server_no_context_takeover") && extensions.include?("client_no_context_takeover")

        @deflater = Zlib::Deflate.new(Zlib::BEST_SPEED, -Zlib::MAX_WBITS)
        @inflater = Zlib::Inflate.new(-Zlib::MAX_WBITS)
      end

      def requested_no_masking?
        @headers.any? { |name, value| name.casecmp?(NO_MASKING_HEADER) && value == "true" }
      end

      def messages(data, out)
        size = data.bytesize
        return out << frame(BINARY, data, compress: true) if size <= MAX_MESSAGE_SIZE

        offset = 0
        while offset < size
          out << frame(BINARY, data.byteslice(offset, MAX_MESSAGE_SIZE), compress: true)
          offset += MAX_MESSAGE_SIZE
        end
        out
      end

      def frame(opcode, payload, compress: false)
        first = FIN | opcode
        if compress && @deflater && payload.bytesize > COMPRESSION_THRESHOLD
          payload = deflate(payload)
          first |= RSV1
        end
        header = frame_header(first, payload.bytesize)
        return header << payload unless @masking

        key = SecureRandom.random_bytes(4)
        header << key << mask(payload, key)
      end

      def frame_header(first, size)
        flag = @masking ? MASKED : 0
        if size <= CONTROL_LIMIT then [first, flag | size].pack("CC")
        elsif size <= 0xffff then [first, flag | 126, size].pack("CCn")
        else [first, flag | 127, size].pack("CCQ>")
        end
      end

      def mask(payload, key)
        wide = key.unpack1("L")
        masked = payload.unpack("L*").map! { |word| word ^ wide }.pack("L*")
        (payload.bytesize & ~3).upto(payload.bytesize - 1) do |index|
          masked << (payload.getbyte(index) ^ key.getbyte(index & 3))
        end
        masked
      end

      def deflate(payload)
        compressed = @deflater.deflate(payload, Zlib::SYNC_FLUSH)
        @deflater.reset
        compressed.byteslice(0, compressed.bytesize - DEFLATE_TAIL.bytesize)
      end

      def inflate(message)
        @inflater.inflate(message) << @inflater.inflate(DEFLATE_TAIL)
      rescue Zlib::Error => e
        violation("a compressed message could not be inflated: #{e.message}")
      ensure
        @inflater.reset
      end

      def drain_frames
        out = nil
        offset = 0
        until @eof
          first, size, body = parse_header(offset)
          break if first.nil? || @inbound.bytesize < body + size

          data = accept(first, @inbound.byteslice(body, size))
          out = out ? out << data : data if data
          offset = body + size
        end
        consume(offset)
        flush_control
        out unless out.nil? || out.empty?
      end

      def parse_header(offset)
        available = @inbound.bytesize - offset
        return if available < 2

        first = @inbound.getbyte(offset)
        size = @inbound.getbyte(offset + 1)
        violation("a server frame is masked") if size & MASKED != 0
        case size
        when 126 then [first, @inbound.unpack1("n", offset: offset + 2), offset + 4] if available >= 4
        when 127 then [first, @inbound.unpack1("Q>", offset: offset + 2), offset + 10] if available >= 10
        else [first, size, offset + 2]
        end
      end

      def consume(offset)
        return if offset.zero?

        @inbound = offset == @inbound.bytesize ? binary : @inbound.byteslice(offset, @inbound.bytesize - offset)
      end

      def accept(first, payload)
        violation("reserved bits are set") if first & RESERVED != 0
        opcode = first & 0x0f
        return control(first, opcode, payload) if opcode >= CLOSE

        start_message(first, opcode)
        @in_message = first.nobits?(FIN)
        return payload unless @compressed

        @partial << payload
        return if @in_message

        message = @partial
        @partial = binary
        inflate(message)
      end

      def start_message(first, opcode)
        compressed = first & RSV1 != 0
        if opcode == CONTINUATION
          violation("a continuation frame has no message to continue") unless @in_message
          violation("a continuation frame is marked compressed") if compressed
        else
          violation("unknown opcode #{opcode}") if opcode > BINARY
          violation("a message started before the previous one ended") if @in_message
          violation("a compressed frame arrived without compression negotiated") if compressed && !@inflater
          @compressed = compressed
        end
      end

      def control(first, opcode, payload)
        violation("a control frame is fragmented, compressed or too long") if
          first & (FIN | RSV1) != FIN || payload.bytesize > CONTROL_LIMIT

        case opcode
        when CLOSE then closing(payload)
        when PING then @control << frame(PONG, payload)
        when PONG then nil
        else violation("unknown opcode #{opcode}")
        end
        nil
      end

      def closing(payload)
        if payload.bytesize >= 2
          @close_code = payload.unpack1("n")
          @close_reason = payload.byteslice(2, payload.bytesize - 2).force_encoding(Encoding::UTF_8).scrub("?")
        end
        send_close(@close_code || NORMAL_CLOSURE)
        @eof = true
      end

      def violation(message)
        send_close(PROTOCOL_VIOLATION)
        raise ConnectionClosedError, "WebSocket protocol violation by #{label}: #{message}"
      end

      def send_close(code)
        return if @close_sent || !@upgraded || @io.nil?

        @close_sent = true
        @control << frame(CLOSE, [code].pack("n"))
        flush_control
      end

      def queued_control(out)
        while (pending = @control.pop(timeout: 0))
          out << pending
        end
        out
      end

      def flush_control
        until @control.empty?
          return unless @write_lock.try_lock

          begin
            write_backlog(queued_control(@backlog))
          ensure
            @write_lock.unlock
          end
        end
      end

      def write_backlog(backlog)
        io = @io or return backlog.clear
        written = io.write_nonblock(backlog, exception: false)
        return unless written.is_a?(Integer)

        @backlog = backlog.byteslice(written, backlog.bytesize - written)
      rescue IOError, SystemCallError, OpenSSL::SSL::SSLError
        backlog.clear
      end
    end
  end
end
