# frozen_string_literal: true

require "socket"
require "shellwords"
require "English"

# A nats-server for the specs to run against, in Docker.
#
# Set NATS_URL to point the suite at a server you are already running instead.
class NatsServer
  IMAGE = ENV.fetch("NATS_IMAGE", "nats:alpine")
  CONTAINER_PORT = 4222

  class DockerUnavailable < StandardError; end

  attr_reader :port

  # Yields a running server and removes it afterwards.
  #
  # @param config [String, nil] extra nats.conf directives
  def self.run(config = nil)
    server = new(config: config)
    server.start
    yield server
  ensure
    server&.stop
  end

  def self.available?
    system("docker", "info", out: File::NULL, err: File::NULL)
  end

  def initialize(config: nil)
    @config = config
    @port = free_port
    @name = "tether-spec-#{@port}"
  end

  def start
    raise DockerUnavailable, "docker is not running; start it or set NATS_URL" unless self.class.available?

    @id = `#{command}`.strip
    raise "could not start #{IMAGE}: #{@id}" unless $CHILD_STATUS&.success?

    wait_until_listening
    self
  end

  def stop
    return unless @id

    system("docker", "rm", "-f", @name, out: File::NULL, err: File::NULL)
    @id = nil
  end

  def uri
    "nats://127.0.0.1:#{@port}"
  end

  def log
    `docker logs #{@name} 2>&1`
  end

  private

  def command
    base = ["docker", "run", "-d", "--rm", "--name", @name, "-p", "#{@port}:#{CONTAINER_PORT}"]
    return Shellwords.join(base + [IMAGE]) unless @config

    # Written inside the container so no host path has to be shared with Docker.
    script = "printf '%s' #{Shellwords.escape(config_body)} > /nats.conf && nats-server -c /nats.conf"
    "#{Shellwords.join(base + ["--entrypoint", "sh", IMAGE, "-c", script])} 2>&1"
  end

  def config_body
    "port: #{CONTAINER_PORT}\n#{@config}"
  end

  def free_port
    socket = TCPServer.new("127.0.0.1", 0)
    socket.addr[1].tap { socket.close }
  end

  # Docker's published port accepts a connection before nats-server itself is
  # listening, so a bare TCP connect is not proof of readiness. Wait for a
  # well formed INFO instead.
  def wait_until_listening
    deadline = Tether::Deadline.after(30)
    until deadline.expired?
      begin
        socket = TCPSocket.new("127.0.0.1", @port)
        socket.timeout = 1
        return if socket.readpartial(4096).start_with?("INFO ")
      rescue Errno::ECONNREFUSED, Errno::ECONNRESET, Errno::EADDRNOTAVAIL, EOFError, IO::TimeoutError
        nil
      ensure
        socket&.close
      end
    end
    raise "#{IMAGE} did not serve INFO within 30s:\n#{log}"
  end
end
