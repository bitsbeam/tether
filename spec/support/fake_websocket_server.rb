# frozen_string_literal: true

require "socket"
require "digest/sha1"

# A scripted WebSocket endpoint: it answers one HTTP upgrade and then sends
# and receives only the frames a spec tells it to.
class FakeWebSocketServer
  GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
  Frame = Struct.new(:opcode, :payload, :fin, :rsv1, :masked)

  attr_reader :port

  def self.run(**options)
    server = new(**options).start
    yield server
  ensure
    server&.stop
  end

  # @param status [String] the status line of the upgrade response
  # @param accept [String, nil] a Sec-WebSocket-Accept to send instead of the right one
  # @param headers [Hash] extra response headers
  # @param silent [Boolean] whether to leave the upgrade request unanswered
  def initialize(status: "HTTP/1.1 101 Switching Protocols", accept: nil, headers: {}, silent: false)
    @status = status
    @silent = silent
    @accept = accept
    @headers = headers
    @listener = TCPServer.new("127.0.0.1", 0)
    @port = @listener.addr[1]
    @sockets = Thread::Queue.new
    @requests = Thread::Queue.new
    @socket = nil
  end

  def uri
    URI("ws://127.0.0.1:#{@port}")
  end

  def start
    Thread.new { serve }
    self
  end

  def stop
    @listener.close
    @socket&.close
  rescue IOError, SystemCallError
    nil
  end

  # @return [String] the upgrade request the client sent
  def request
    @request ||= @requests.pop(timeout: 5) or raise "no upgrade request arrived"
  end

  def send_frame(opcode, payload = "", fin: true, rsv1: false)
    first = (fin ? 0x80 : 0) | (rsv1 ? 0x40 : 0) | opcode
    size = payload.bytesize
    header = if size <= 125 then [first, size].pack("CC")
             elsif size <= 0xffff then [first, 126, size].pack("CCn")
             else [first, 127, size].pack("CCQ>")
             end
    socket.write(header + payload.b)
  end

  def send_raw(bytes)
    socket.write(bytes)
  end

  def read_frame
    first, second = read_exactly(2).unpack("CC")
    size = second & 0x7f
    size = read_exactly(2).unpack1("n") if size == 126
    size = read_exactly(8).unpack1("Q>") if size == 127
    masked = second.anybits?(0x80)
    key = read_exactly(4).bytes if masked
    payload = read_exactly(size)
    payload = payload.each_byte.with_index.map { |byte, index| byte ^ key[index % 4] }.pack("C*") if masked
    Frame.new(first & 0x0f, payload, first.anybits?(0x80), first.anybits?(0x40), masked)
  end

  # @return [Boolean] whether the client ended the connection
  def eof?
    socket.wait_readable(5) or raise "the client did not close within 5s"
    socket.read_nonblock(1, exception: false).nil?
  rescue SystemCallError
    true
  end

  def drop
    socket.close
  end

  private

  def socket
    @socket ||= @sockets.pop(timeout: 5) or raise "no client connected"
  end

  def read_exactly(size)
    return "".b if size.zero?

    socket.wait_readable(5) or raise "no frame arrived within 5s"
    data = socket.read(size)
    raise "the client closed the connection" if data.nil? || data.bytesize < size

    data
  end

  def serve
    client = @listener.accept
    request = +""
    request << client.readpartial(4096) until request.include?("\r\n\r\n")
    @requests << request
    client.write(response(request)) unless @silent
    @sockets << client
  rescue IOError, SystemCallError
    nil
  end

  def response(request)
    key = request[/^Sec-WebSocket-Key: (.+)\r$/i, 1].to_s
    fields = { "Upgrade" => "websocket", "Connection" => "Upgrade",
               "Sec-WebSocket-Accept" => @accept || Digest::SHA1.base64digest(key + GUID) }.merge(@headers)
    "#{@status}\r\n#{fields.map { |name, value| "#{name}: #{value}\r\n" }.join}\r\n"
  end
end
