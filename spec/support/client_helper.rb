# frozen_string_literal: true

# Steps the feature specs share: clients that are closed after the example,
# a proxy for outages, and queues that collect what arrives.
module ClientHelper
  FAST_RECONNECT = { wait: 0.02, jitter: 0, max_attempts: -1 }.freeze

  def self.included(base)
    base.let(:server) { SharedNatsServer.instance }
    base.let(:topic) { "spec.#{Tether::Connection::NUID.next}" }
    base.let(:gate) { Thread::Queue.new }
    base.after do
      gate.close
      opened_clients.each(&:close)
      @proxy&.stop
      @fake_server&.stop
    end
  end

  def opened_clients
    @opened_clients ||= []
  end

  def connect(url = server.uri, **options)
    Tether::Client.connect(url, **options).tap { |client| opened_clients << client }
  end

  def build(url = server.uri, **options)
    Tether::Client.new(url, **options).tap { |client| opened_clients << client }
  end

  def proxy(upstream = server)
    @proxy ||= TcpProxy.new(upstream.port).start
  end

  def fake_server(**options)
    @fake_server ||= FakeNatsServer.new(**options).start
  end

  def collect(client, subject, **options)
    received = Thread::Queue.new
    client.subscribe(subject, **options) { |msg| received.push(msg) }
    client.flush
    received
  end

  def take(queue, count, timeout: 2)
    Array.new(count) { queue.pop(timeout: timeout) }
  end

  def record(client, *names)
    Thread::Queue.new.tap do |events|
      names.each { |name| client.public_send(:"on_#{name}") { |*args| events.push([name, *args]) } }
    end
  end

  def error_from
    yield
    nil
  rescue Tether::Error => e
    e
  end
end
