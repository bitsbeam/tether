# frozen_string_literal: true

require "socket"
require "json"

# A scripted stand-in for nats-server: it completes the handshake and then
# says only what a spec tells it to.
class FakeNatsServer
  attr_reader :port

  def self.run(**options)
    server = new(**options).start
    yield server
  ensure
    server&.stop
  end

  # @param info [Hash] fields merged into the INFO it greets with
  # @param pongs [Integer, nil] PINGs it answers before going silent; nil answers all
  # @param greeting [String, nil] raw bytes to greet with in place of INFO
  def initialize(info: {}, pongs: nil, greeting: nil)
    info = { server_id: "FAKE", version: "2.15.0", headers: true, max_payload: 1_048_576, proto: 1 }.merge(info)
    @greeting = greeting || "INFO #{JSON.generate(info)}\r\n"
    @pongs = pongs
    @lock = Mutex.new
    @clients = []
    @received = String.new(encoding: Encoding::BINARY)
    @listener = TCPServer.new("127.0.0.1", 0)
    @port = @listener.addr[1]
  end

  def uri
    "nats://127.0.0.1:#{@port}"
  end

  def start
    Thread.new { accept_loop }
    self
  end

  def stop
    @listener.close
    drop
  rescue IOError
    nil
  end

  def drop
    @lock.synchronize { @clients.dup.tap { @clients.clear } }.each { |client| close(client) }
  end

  # Writes raw protocol bytes to every connected client.
  def push(bytes)
    @lock.synchronize { @clients.dup }.each { |client| client.write(bytes) }
  end

  def received
    @lock.synchronize { @received.dup }
  end

  def connections
    @lock.synchronize { @clients.size }
  end

  private

  def accept_loop
    loop do
      client = @listener.accept
      @lock.synchronize { @clients << client }
      Thread.new { serve(client) }
    end
  rescue IOError, SystemCallError
    nil
  end

  def serve(client)
    client.write(@greeting)
    loop do
      data = client.readpartial(64 * 1024)
      @lock.synchronize { @received << data }
      data.scan("PING\r\n").size.times { client.write("PONG\r\n") if answer? }
    end
  rescue IOError, SystemCallError
    close(client)
  end

  def answer?
    @lock.synchronize do
      next true if @pongs.nil?
      next false if @pongs.zero?

      @pongs -= 1
      true
    end
  end

  def close(client)
    @lock.synchronize { @clients.delete(client) }
    client.close
  rescue IOError, SystemCallError
    nil
  end
end
