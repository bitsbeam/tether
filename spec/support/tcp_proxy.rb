# frozen_string_literal: true

require "socket"

# A TCP proxy between a client and the server, for dropping connections and
# simulating an outage on a fixed port without touching the server.
class TcpProxy
  attr_reader :port

  def self.run(upstream_port)
    proxy = new(upstream_port).start
    yield proxy
  ensure
    proxy&.stop
  end

  def initialize(upstream_port, port: nil)
    @upstream_port = upstream_port
    @port = port || free_port
    @lock = Mutex.new
    @pairs = []
    @listener = nil
  end

  def uri
    "nats://127.0.0.1:#{@port}"
  end

  def start
    @lock.synchronize do
      next if @listener

      @listener = TCPServer.new("127.0.0.1", @port)
      listener = @listener
      Thread.new { accept_loop(listener) }
    end
    self
  end

  # Stops listening and drops every connection.
  def stop
    listener = @lock.synchronize { @listener.tap { @listener = nil } }
    listener&.close
    drop
    self
  end

  # Drops every connection and keeps listening.
  def drop
    pairs = @lock.synchronize { @pairs.dup.tap { @pairs.clear } }
    pairs.flatten.each { |socket| close(socket) }
    self
  end

  def connections
    @lock.synchronize { @pairs.size }
  end

  private

  def free_port
    socket = TCPServer.new("127.0.0.1", 0)
    socket.addr[1].tap { socket.close }
  end

  def accept_loop(listener)
    loop do
      client = listener.accept
      upstream = TCPSocket.new("127.0.0.1", @upstream_port)
      @lock.synchronize { @pairs << [client, upstream] }
      Thread.new { pump(client, upstream) }
      Thread.new { pump(upstream, client) }
    end
  rescue IOError, SystemCallError
    nil
  end

  def pump(from, to)
    loop { to.write(from.readpartial(64 * 1024)) }
  rescue IOError, SystemCallError
    close(from)
    close(to)
    @lock.synchronize { @pairs.reject! { |pair| pair.include?(from) } }
  end

  def close(socket)
    socket.close
  rescue IOError, SystemCallError
    nil
  end
end
