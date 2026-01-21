# Tether

A NATS client for Ruby, written from scratch around one idea: a single connection should be
safe to use from many threads at once.

Every protocol detail in it is derived from the nats-server source rather than from
documentation, and the concurrency behaviour is covered by tests that publish from many threads
at once and assert nothing interleaves.

## Status

Early. The core connection works and is tested against a real server, but this is **not yet
usable in production**:

| | |
|---|---|
| Working | connect, publish, subscribe, queue groups, wildcards, headers, request/reply, backpressure, graceful close |
| Missing | reconnection, TLS, JetStream, KV, Object Store, services, WebSocket, nkeys/JWT auth |

A dropped connection currently stays dropped. Reconnection is the next thing being built.

## Installation

```ruby
gem "tether"
```

Ruby 3.2 or newer. No runtime dependencies.

## Getting started

```ruby
require "tether"

Tether.connect("nats://127.0.0.1:4222") do |nc|
  nc.subscribe("greet.>") { |msg| puts "#{msg.subject}: #{msg.data}" }
  nc.flush

  nc.publish("greet.world", "hello")
  sleep 0.1
end
```

`Tether.connect` yields the client and closes it afterwards. Without a block it returns a
client you close yourself. With no URI it reads `NATS_URL`, then falls back to
`nats://127.0.0.1:4222`.

`flush` round trips a PING, which the server answers only once it has processed everything sent
before it — the way to know a subscription is live before publishing to it.

## Publishing and subscribing

```ruby
nc.publish("orders.created", payload)
nc.publish("orders.created", payload, reply_to: nc.new_inbox)

subscription = nc.subscribe("orders.*") { |msg| handle(msg) }
nc.unsubscribe(subscription)
```

Publishing is fire and forget: a successful return means the frame reached the socket, not that
the server processed it. Follow with `flush` when you need that guarantee.

Each subscription gets its own queue and worker thread, so a slow handler delays only its own
subscription. The reader thread never runs your code.

### Queue groups

Members of a queue group share the work; the server delivers each message to one of them.

```ruby
nc.subscribe("jobs.incoming", queue: "workers") { |msg| process(msg) }
```

### Taking messages yourself

A subscription with no block queues messages instead of dispatching them, and hands them over on
your thread.

```ruby
subscription = nc.subscribe("events")
msg = subscription.next_message(Tether::Deadline.after(5))
```

`next_message` raises `Tether::TimeoutError` when the deadline passes.

## Request and reply

```ruby
nc.subscribe("time.service") { |msg| msg.respond(Time.now.to_s) }
nc.flush

reply = nc.request("time.service", "now", timeout: 2)
puts reply.data
```

Every request on a connection shares one inbox subscription and replies are matched in memory,
so a hundred concurrent requests still cost one subscription. Requests are safe to issue from
many threads at once.

If nothing is subscribed to the subject, the request raises `Tether::NoRespondersError`
immediately rather than waiting out the timeout.

## Headers

Header names are case sensitive and are sent exactly as given — the server matches names like
`Nats-TTL` byte for byte, so nothing is canonicalized.

```ruby
headers = Tether::Headers["Nats-Msg-Id" => "order-1", "Nats-TTL" => "60"]
nc.publish("orders.created", payload, headers: headers)

nc.subscribe("orders.created") do |msg|
  msg.headers["Nats-Msg-Id"]
  msg.status
end
```

A value containing CR or LF raises `ArgumentError`: the server does not validate the header
block it forwards, so an unescaped newline would forge a header line that its own lookups honor.

## Deadlines

Every blocking call takes a deadline rather than a bare number of seconds.

```ruby
deadline = Tether::Deadline.after(5)
deadline.remaining
deadline.expired?
deadline.with_at_most(1)   # narrows, never widens
```

Tether never uses `Timeout.timeout` or `Thread#raise`, which fire at an arbitrary point in
whatever the target thread happens to be executing — mid-frame on a socket, or inside a lock.
It blocks only on primitives that interrupt themselves. **Do not wrap Tether calls in
`Timeout.timeout` yourself**; pass a timeout instead.

That same choice means Tether works unmodified under a `Fiber::Scheduler`. On Falcon or under
`Async`, a blocking call yields to the reactor instead of pinning the thread, with no `Async {}`
wrapping and no special mode.

## Errors

Errors that belong to a call are raised from it:

```ruby
begin
  nc.request("nobody.listening", "hi", timeout: 1)
rescue Tether::NoRespondersError, Tether::TimeoutError => e
  warn e.message
end
```

Errors that arrive on their own — an `-ERR` from the server, a slow consumer, a protocol
violation — are never thrown into an unrelated thread. They go to a handler:

```ruby
nc.on_error { |error| Rails.logger.error(error) }
```

`Tether::ServerError.for` maps the server's text to a class, and `#fatal?` reports whether the
server closes the connection after it. Treat that as advisory: some server errors are sent
without a close, and some closes arrive with no error at all.

## Backpressure

A subscription's queue is bounded. When a handler cannot keep up, messages are dropped and
reported rather than blocking the reader thread and stalling every other subscription.

```ruby
subscription = nc.subscribe("firehose", max_pending: 1024) { |msg| slow(msg) }
subscription.pending
subscription.dropped
```

## Development

```bash
bin/setup
bundle exec rake        # rspec, rubocop, and rbs validate
```

Specs run against `nats:alpine` in Docker, starting a container on a free port. `NATS_IMAGE`
pins the image.

## License

MIT.
