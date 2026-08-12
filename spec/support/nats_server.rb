# frozen_string_literal: true

require "socket"
require "openssl"
require "shellwords"
require "English"

# A nats-server for the specs to run against, in Docker.
#
# Set NATS_URL to point the suite at a server you are already running instead.
class NatsServer
  IMAGE = ENV.fetch("NATS_IMAGE", "nats:alpine")
  CONTAINER_PORT = 4222
  CONFIG_PATH = "/nats.conf"
  WEBSOCKET_PORT = 8080

  class DockerUnavailable < StandardError; end

  attr_reader :port, :websocket_port

  # Yields a running server and removes it afterwards.
  #
  # @param config [String, nil] extra nats.conf directives
  # @param options [Hash] see {#initialize}
  def self.run(config = nil, **options)
    server = new(config: config, **options)
    server.start
    yield server
  ensure
    server&.stop
  end

  def self.available?
    system("docker", "info", out: File::NULL, err: File::NULL)
  end

  # Containers are started without `--rm` so a killed server can be started
  # again; whatever is still running when the process ends is removed here.
  def self.running
    @running ||= []
  end

  at_exit { running.dup.each(&:stop) }

  # @param config [String, nil] extra nats.conf directives
  # @param files [Hash{String => String}] files to create inside the container, by absolute path
  # @param tls_first [Boolean] whether the server expects the TLS handshake before INFO
  # @param websocket [Boolean] whether to publish container port 8080 for a `websocket {}` listener
  def initialize(config: nil, files: {}, tls_first: false, websocket: false)
    @config = config
    @files = files
    @tls_first = tls_first
    @port = free_port
    @websocket_port = free_port if websocket
    @name = "tether-spec-#{@port}"
  end

  def start
    raise DockerUnavailable, "docker is not running; start it or set NATS_URL" unless self.class.available?

    @id = `#{command}`.strip
    raise "could not start #{IMAGE}: #{@id}" unless $CHILD_STATUS&.success?

    self.class.running << self
    wait_until_listening
    self
  end

  # Removes the container.
  def stop
    return unless @id

    docker("rm", "-f", @name)
    self.class.running.delete(self)
    @id = nil
  end

  # Kills the server process and keeps the container, so {#resume} brings it
  # back on the same port.
  def halt
    docker("kill", @name)
    self
  end

  def resume
    docker("start", @name)
    wait_until_listening
    self
  end

  def restart
    halt
    resume
  end

  # Freezes the server process without closing its connections, so clients
  # see silence instead of a disconnect, until {#unpause}.
  def pause
    docker("pause", @name)
    self
  end

  def unpause
    docker("unpause", @name)
    self
  end

  # @param name [String] a signal nats-server understands, such as `ldm`
  def signal(name)
    docker("exec", @name, "nats-server", "--signal", "#{name}=1")
    self
  end

  def uri
    "nats://127.0.0.1:#{@port}"
  end

  def tls_uri
    "tls://127.0.0.1:#{@port}"
  end

  # @param scheme [String] `ws` or `wss`
  def websocket_uri(scheme = "ws")
    "#{scheme}://127.0.0.1:#{@websocket_port}"
  end

  def log
    `docker logs #{@name} 2>&1`
  end

  private

  def docker(*arguments)
    system("docker", *arguments, out: File::NULL, err: File::NULL)
  end

  # The configuration is written inside the container so no host path has to
  # be shared with Docker.
  def command
    base = ["docker", "run", "-d", "--name", @name, "-p", "#{@port}:#{CONTAINER_PORT}", "--entrypoint", "sh"]
    base.insert(5, "-p", "#{@websocket_port}:#{WEBSOCKET_PORT}") if @websocket_port
    "#{Shellwords.join(base + [IMAGE, "-c", script])} 2>&1"
  end

  def script
    writes = @files.merge(CONFIG_PATH => config_body).map do |path, content|
      "mkdir -p #{Shellwords.escape(File.dirname(path))} && " \
        "printf '%s' #{Shellwords.escape(content)} > #{Shellwords.escape(path)}"
    end
    "#{writes.join(" && ")} && exec nats-server -c #{CONFIG_PATH}"
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
        return if greeting(socket).start_with?("INFO ")
      rescue SystemCallError, EOFError, IO::TimeoutError, OpenSSL::SSL::SSLError
        nil
      ensure
        socket&.close
      end
    end
    raise "#{IMAGE} did not serve INFO within 30s:\n#{log}"
  end

  def greeting(socket)
    return socket.readpartial(4096) unless @tls_first

    context = OpenSSL::SSL::SSLContext.new
    context.verify_mode = OpenSSL::SSL::VERIFY_NONE
    OpenSSL::SSL::SSLSocket.new(socket, context).connect.readpartial(4096)
  end
end
