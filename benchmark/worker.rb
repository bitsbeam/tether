# frozen_string_literal: true

require "json"
require "socket"

# One benchmark scenario against one client library, in its own process.
#
#   ruby -Ilib benchmark/worker.rb tether|nats-pure SCENARIO NATS_URL
#
# Prints one JSON object. Subscriber scenarios read from a separate raw-socket feeder process (see feeder.rb) so
# the publisher never competes with the client under test for the GVL, and both libraries see identical bytes.
module Bench
  PENDING = 100_000_000
  FEEDER = File.expand_path("feeder.rb", __dir__)

  Adapter = Struct.new(:lib, :url) do
    def tether? = lib == "tether"

    def connect
      if tether?
        require "tether"
        Tether.connect(url)
      else
        require "nats/client"
        NATS.connect(url)
      end
    end

    def publish(conn, subject, payload, headers: nil)
      if tether?
        conn.publish(subject, payload, headers: headers)
      elsif headers
        conn.publish(subject, payload, nil, header: headers)
      else
        conn.publish(subject, payload)
      end
    end

    def subscribe(conn, subject, &block)
      if tether?
        conn.subscribe(subject, max_pending: PENDING, max_pending_bytes: PENDING * 1024, &block)
      else
        conn.subscribe(subject, pending_msgs_limit: PENDING, pending_bytes_limit: PENDING * 1024, &block)
      end
    end

    def next_msg(sub, timeout) = tether? ? sub.next(timeout: timeout) : sub.next_msg(timeout: timeout)

    def request(conn, subject, payload) = conn.request(subject, payload, timeout: 5)

    def respond(msg, payload) = msg.respond(payload)
    def flush(conn) = tether? ? conn.flush(timeout: 30) : conn.flush(30)
    def close(conn) = conn.close
  end

  def self.now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  def self.allocated = GC.stat(:total_allocated_objects)

  def self.measure
    GC.start
    allocs = allocated
    start = now
    yield
    { seconds: now - start, allocs: allocated - allocs }
  end

  def self.rate(count, seconds) = (count / seconds).round
  def self.rss_kb = `ps -o rss= -p #{Process.pid}`.to_i

  def self.percentiles(samples)
    sorted = samples.sort
    pick = ->(q) { sorted[[(sorted.size * q).ceil - 1, 0].max] }
    { p50_us: (pick.call(0.5) * 1e6).round, p99_us: (pick.call(0.99) * 1e6).round, max_us: (sorted.last * 1e6).round }
  end

  def self.publish_run(adapter, count:, size:, headers: nil, threads: 1)
    conn = adapter.connect
    payload = "x" * size
    per_thread = count / threads
    adapter.publish(conn, "bench.warm", payload, headers: headers)
    5_000.times { adapter.publish(conn, "bench.warm", payload, headers: headers) }
    adapter.flush(conn)
    result = measure do
      Array.new(threads) do
        Thread.new do
          per_thread.times do
            adapter.publish(conn, "bench.pub", payload, headers: headers)
          end
        end
      end.each(&:join)
      adapter.flush(conn)
    end
    adapter.close(conn)
    throughput(per_thread * threads, size, result)
  end

  def self.throughput(messages, size, result)
    { messages: messages, msgs_per_s: rate(messages, result[:seconds]),
      mb_per_s: (messages * size / result[:seconds] / 1e6).round(1),
      allocs_per_msg: (result[:allocs].to_f / messages).round(2) }
  end

  def self.start_feeder(url, subjects, count, size)
    feeder = IO.popen(["ruby", FEEDER, url, count.to_s, size.to_s, *subjects], "r+")
    abort "feeder failed" unless feeder.gets&.strip == "ready"
    feeder
  end

  Counter = Struct.new(:expected, :total, :lock, :done) do
    def self.for(expected) = new(expected, 0, Mutex.new, Thread::Queue.new)

    def hit
      done << true if lock.synchronize { (self.total += 1) == expected }
    end

    def value = lock.synchronize { total }
  end

  def self.subscribe_run(adapter, count:, size:, subjects: ["bench.sub"], subscribers: 1)
    conn = adapter.connect
    counter = Counter.for(count * subscribers)
    subjects.each do |subject|
      subscribers.times { adapter.subscribe(conn, subject) { |_msg| counter.hit } }
    end
    adapter.flush(conn)
    feeder = start_feeder(adapter.url, subjects, count, size)
    result = measure do
      feeder.puts("go")
      counter.done.pop(timeout: 120)
    end
    feeder.close
    adapter.close(conn)
    throughput(counter.value, size, result).merge(expected: counter.expected)
  end

  def self.sync_subscribe_run(adapter, count:, size:)
    conn = adapter.connect
    sub = adapter.subscribe(conn, "bench.sync")
    adapter.flush(conn)
    feeder = start_feeder(adapter.url, ["bench.sync"], count, size)
    got = 0
    result = measure do
      feeder.puts("go")
      while got < count
        break unless adapter.next_msg(sub, 10)

        got += 1
      end
    end
    feeder.close
    adapter.close(conn)
    { messages: got, expected: count, msgs_per_s: rate(got, result[:seconds]),
      allocs_per_msg: (result[:allocs].to_f / got).round(2) }
  end

  def self.responder(adapter, subject = "bench.echo")
    conn = adapter.connect
    adapter.subscribe(conn, subject) { |msg| adapter.respond(msg, msg.data) }
    adapter.flush(conn)
    conn
  end

  def self.request_run(adapter, count:, size:, threads: 1)
    responder = responder(adapter)
    conn = adapter.connect
    payload = "x" * size
    200.times { adapter.request(conn, "bench.echo", payload) }
    per_thread = count / threads
    samples = Thread::Queue.new
    result = measure { timed_requests(adapter, conn, payload, threads, per_thread, samples) }
    total = per_thread * threads
    adapter.close(conn)
    adapter.close(responder)
    latencies = percentiles(Array.new(samples.size) { samples.pop })
    { requests: total, req_per_s: rate(total, result[:seconds]),
      allocs_per_req: (result[:allocs].to_f / total).round(1) }.merge(latencies)
  end

  def self.timed_requests(adapter, conn, payload, threads, per_thread, samples)
    Array.new(threads) do
      Thread.new do
        per_thread.times do
          started = now
          adapter.request(conn, "bench.echo", payload)
          samples << (now - started)
        end
      end
    end.each(&:join)
  end

  def self.connect_run(adapter, count:)
    result = measure { count.times { adapter.close(adapter.connect) } }
    { connections: count, ms_per_connect: (result[:seconds] / count * 1000).round(2) }
  end

  def self.idle_run(adapter, subscriptions:)
    conn = adapter.connect
    before = rss_kb
    threads = Thread.list.size
    subscriptions.times { |i| adapter.subscribe(conn, "bench.idle.#{i}") { |_msg| nil } }
    adapter.flush(conn)
    after = rss_kb
    adapter.close(conn)
    { subscriptions: subscriptions, rss_growth_kb: after - before, threads_added: Thread.list.size - threads }
  end

  SCENARIOS = {
    "publish-16b" => ->(a) { publish_run(a, count: 500_000, size: 16) },
    "publish-128b" => ->(a) { publish_run(a, count: 400_000, size: 128) },
    "publish-1kb" => ->(a) { publish_run(a, count: 200_000, size: 1024) },
    "publish-16kb" => ->(a) { publish_run(a, count: 40_000, size: 16 * 1024) },
    "publish-headers" => ->(a) { publish_run(a, count: 200_000, size: 128, headers: { "k1" => "v1", "k2" => "v2" }) },
    "publish-4-threads" => ->(a) { publish_run(a, count: 400_000, size: 128, threads: 4) },
    "subscribe-1-callback" => ->(a) { subscribe_run(a, count: 300_000, size: 128) },
    "subscribe-1-callback-1kb" => ->(a) { subscribe_run(a, count: 150_000, size: 1024) },
    "subscribe-1-sync" => ->(a) { sync_subscribe_run(a, count: 300_000, size: 128) },
    "subscribe-1000-subjects" => lambda { |a|
      subscribe_run(a, count: 300_000, size: 128, subjects: Array.new(1000) { |i| "bench.many.#{i}" })
    },
    "subscribe-fanout-10" => ->(a) { subscribe_run(a, count: 20_000, size: 128, subscribers: 10) },
    "request-sequential" => ->(a) { request_run(a, count: 20_000, size: 128) },
    "request-8-threads" => ->(a) { request_run(a, count: 40_000, size: 128, threads: 8) },
    "connect" => ->(a) { connect_run(a, count: 100) },
    "idle-5000-subscriptions" => ->(a) { idle_run(a, subscriptions: 5_000) }
  }.freeze
end

if ARGV[0] == "list"
  puts Bench::SCENARIOS.keys
  exit
end

lib, scenario, url = ARGV
abort "unknown scenario #{scenario}" unless Bench::SCENARIOS.key?(scenario)
adapter = Bench::Adapter.new(lib, url)
puts JSON.generate(Bench::SCENARIOS.fetch(scenario).call(adapter).merge(lib: lib, scenario: scenario))
$stdout.flush
exit!(0)
