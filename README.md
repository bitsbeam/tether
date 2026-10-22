# Tether

Tether is a NATS client for Ruby: core publish/subscribe and request/reply, JetStream, Key/Value, Object Store and
Services. It is pure Ruby with no runtime dependencies (NKEY signing and WebSocket framing included), and one connection
is safe to share between any number of threads. It reconnects on its own, resubscribes, and buffers what you publish
during an outage. Every call that can block takes a timeout and returns or raises by it, so nothing hangs. Ruby 3.2 or
newer.

## Installation

```ruby
gem "tether"
```

## Quick start

```ruby
require "tether"

nc = Tether.connect("nats://127.0.0.1:4222")

nc.subscribe("greet.*") { |msg| puts "#{msg.subject}: #{msg.data}" }
nc.publish("greet.joe", "hello")

nc.subscribe("time.now") { |msg| msg.respond(Time.now.utc.to_s) }
reply = nc.request("time.now", timeout: 2)
puts reply.data

nc.drain
```

`publish` is fire and forget: it returns once the message is buffered, and a background thread writes it. A block given
to `subscribe` runs on that subscription's own worker thread. `request` returns the first reply or raises
`Tether::NoRespondersError` at once when nobody is subscribed, and `Tether::TimeoutError` when no reply arrives in time.
`drain` unsubscribes, lets the handlers finish what was received, and closes.

## Connecting

```ruby
nc = Tether.connect("nats://a.example.com:4222,nats://b.example.com:4222", name: "billing")

Tether.connect { |nc| nc.publish("greet.joe", "hello") }   # yields the client, closes it, returns the block's value
```

The first argument is one URL, a comma separated list or an array. Without one, `NATS_URL` is read, then
`nats://127.0.0.1:4222`. `Tether.connect` returns a connected client or raises. An unknown option is an `ArgumentError`
that names it, not a silently ignored typo. The options you are most likely to need:

| Option | Default | Meaning |
|---|---|---|
| `name:` | `nil` | connection name shown in server monitoring |
| `connect_timeout:` | `2` | seconds for DNS, TCP, TLS and the handshake of one attempt |
| `reconnect:` | `true` | `false`, or a hash of reconnect settings, see below |
| `retry_on_failed_connect:` | `false` | return a `:reconnecting` client when no server is reachable yet |
| `ping_interval:`, `max_pings_out:` | `120`, `2` | keepalive: seconds between PINGs, and how many may go unanswered |
| `echo:` | `true` | receive your own publishes on your own subscriptions |
| `max_pending:`, `max_pending_bytes:` | `65_536`, `64 MiB` | default per-subscription queue limits |
| `drain_timeout:` | `30` | default timeout of `drain` |
| `logger:` | `nil` | a `Logger` for errors nobody handles; nothing is written without one |

The full list, with TLS and WebSocket settings, is in [docs/design.md](docs/design.md#61-connecting).

### Authentication

Give at most one of these. User and password or a token may also be put in the URL, which wins for that server.

```ruby
Tether.connect(url, user: "app", password: "secret")
Tether.connect(url, token: "s3cr3t")
Tether.connect(url, credentials_file: "/etc/nats/app.creds")        # JWT and NKEY seed; credentials: takes the text
Tether.connect(url, nkey_seed_file: "/etc/nats/app.nk")             # or nkey_seed: "SU..."
Tether.connect(url, jwt: token, nkey_seed: seed)
Tether.connect(url, auth: ->(nonce) { { jwt: fetch_jwt, sig: sign(nonce) } })   # any CONNECT fields
```

`token:`, `user:`, `password:`, `jwt:` and `nkey_seed:` also accept a callable. Callables are invoked and files are read
on every connect, so a reconnect picks up rotated credentials.

### TLS and WebSocket

```ruby
Tether.connect("tls://nats.example.com")                            # TLS, verified against the system CA store
Tether.connect("nats://nats.example.com", tls: { ca_file: "ca.pem", cert_file: "client.pem", key_file: "client.key" })
Tether.connect("nats://nats.example.com", tls: { first: true })     # TLS before INFO, for servers set up that way
Tether.connect("wss://nats.example.com:443", websocket: { compression: true })
```

A `tls://` or `wss://` URL, `tls: true` or a `tls:` hash makes TLS required; otherwise it is used only when the server
demands it. The certificate and hostname are verified unless you pass `verify: false`, and `context:` takes a ready
`OpenSSL::SSL::SSLContext`. `ws://` and `wss://` URLs use the built-in WebSocket transport; `websocket:` also takes
`path:` and `headers:` for servers behind a proxy.

### Events

Things that happen to the connection, and errors that belong to no particular call, are delivered to handlers on a
dedicated thread, one at a time. Several handlers may be registered for one event.

```ruby
nc.on_error      { |error| warn "nats: #{error.class}: #{error.message}" }   # -ERR, slow consumers, handler exceptions
nc.on_disconnect { |error| warn "nats: connection lost (#{error&.message})" }
nc.on_reconnect  { |server| warn "nats: connected to #{server.url}" }
nc.on_close      { warn "nats: closed" }

registration = nc.on_status { |status| puts status }   # every state change
registration.cancel
```

There are also `on_connect`, `on_reconnect_error`, `on_lame_duck` and `on_servers_discovered`. To register handlers
before the first connect, build the client first: `nc = Tether::Client.new(url); nc.on_error { ... }; nc.connect`.

### Reconnecting

When the connection is lost the client becomes `:reconnecting` and tries each known server in turn, including the ones
the cluster advertised, waiting two seconds plus jitter after each full pass. On success it restores every subscription
and sends what was published meanwhile. While it is away:

- `publish` buffers up to 8 MiB, then raises `Tether::ReconnectBufferFullError`. It never blocks.
- A `request` or `flush` that was waiting fails at once with `Tether::DisconnectedError`, which is `retryable?`.
- New subscriptions are remembered and made on the new connection.

A server that fails 60 times in a row is dropped, and the client closes when none is left or when a server refuses the
same credentials twice.

```ruby
Tether.connect(url, reconnect: { max_attempts: -1, wait: 1, jitter: 0.5, buffer_size: 32 * 1024 * 1024 })
Tether.connect(url, reconnect: false)   # a lost connection closes the client
```

`nc.status` is one of `:disconnected`, `:connecting`, `:connected`, `:reconnecting`, `:draining` and `:closed`, with
`connected?`, `reconnecting?`, `draining?` and `closed?` to match. `nc.force_reconnect` drops the socket and reconnects.

`nc.drain(timeout: 30)` is the graceful way out: it unsubscribes everything, waits for handlers to work through what was
already received, flushes pending publishes and closes. `nc.close` closes immediately and is idempotent. A closed client
is never reopened; build a new one.

## Timeouts and `Tether::Deadline`

Every blocking call takes `timeout:`, in seconds, and that is the most time the call takes, however many round trips it
makes inside. Internally the number becomes a `Tether::Deadline`, an absolute point on the monotonic clock that is
passed down to every wait, so three round trips share one budget and cannot each spend the whole timeout.

`timeout:` accepts a `Deadline` as well, which lets you put several calls under one budget:

```ruby
deadline = Tether::Deadline.after(5)

stream   = js.stream("ORDERS", timeout: deadline)
consumer = stream.consumer("billing", timeout: deadline)
batch    = consumer.fetch(10, timeout: deadline)        # gets whatever is left of the five seconds

deadline.remaining          # seconds left, never negative
deadline.expired?
deadline.with_at_most(1)    # a deadline at most a second away; narrows, never widens
```

Two things follow from this design:

- Do not wrap tether calls in `Timeout.timeout`. It raises at an arbitrary point in another thread's work, which can
  leave half a frame on the wire. Pass `timeout:` instead; tether itself never uses `Timeout.timeout` or `Thread#raise`.
- For operations of open-ended length (`each_message`, `snapshot`, listings, batches, Object Store `get` and `put`), a
  number bounds each wait on the server and a `Deadline` bounds the whole operation.

## Subscriptions

```ruby
sub = nc.subscribe("orders.*") { |msg| handle(msg) }                  # a worker thread calls the block
sub = nc.subscribe("jobs", queue: "workers") { |msg| work(msg) }      # queue group: one member gets each message
sub = nc.subscribe("jobs", concurrency: 4) { |msg| work(msg) }        # four workers; gives up ordering
sub = nc.subscribe("events", max: 10) { |msg| handle(msg) }           # ends itself after ten messages

sub = nc.subscribe("events")             # no block: take messages yourself
msg = sub.next(timeout: 5)               # raises Tether::TimeoutError
sub.each(idle_timeout: 1) { |msg| handle(msg) }   # returns when nothing has arrived for a second

sub.unsubscribe                          # stop now, discard what is queued
sub.drain                                # stop intake, handle what is queued, then end
```

A message has `subject`, `data` (always a binary string), `reply_to`, `headers` and `respond`:

```ruby
nc.publish("audit.login", payload, headers: { "Trace-Id" => "abc" })

nc.subscribe("audit.login") do |msg|
  msg.headers["Trace-Id"]      # => "abc"; headers is nil when the message has none
  msg.respond("ok") if msg.reply_to
end
```

Each subscription has its own bounded queue, so a slow handler delays nobody else. When the queue is full
(`max_pending:`, `max_pending_bytes:`) new messages are dropped, `sub.dropped` counts them, and a
`Tether::SlowConsumerError` is reported to `on_error` once per episode. A block that raises does not end the
subscription: the exception is reported to `on_error` as a `Tether::HandlerError` with `cause` and `msg`, and delivery
continues.

To collect several replies to one request, use `request_many`:

```ruby
replies = nc.request_many("search", "ruby", max: 10, stall: 0.25, timeout: 2)   # => Array of messages
```

## JetStream

### Streams

```ruby
js = nc.jetstream                                     # or nc.jetstream(domain: "hub", timeout: 10)

stream = js.create_stream(name: "ORDERS", subjects: ["orders.>"], max_age: 86_400, replicas: 1)
stream = js.stream("ORDERS")                          # raises Tether::JetStream::StreamNotFoundError
stream = js.create_or_update_stream(name: "ORDERS", subjects: ["orders.>", "returns.>"])

stream.info.state.messages                            # fresh info from the server
stream.update(max_age: 3600)                          # merges into the current configuration
stream.get(42)                                        # a stored message by sequence
stream.get(last_for: "orders.created")
stream.purge(subject: "orders.eu.>")                  # => how many were removed
js.stream_names.to_a
js.delete_stream("ORDERS")
```

Durations are seconds and instants are `Time` everywhere; the wire's nanoseconds never reach your code.

### Publishing

```ruby
ack = js.publish("orders.created", payload)           # waits for the server to confirm it stored the message
ack.stream ; ack.sequence

js.publish("orders.created", payload, msg_id: "order-1")                  # deduplicated by id; ack.duplicate?
js.publish("orders.created", payload, expect: { last_sequence: 41 })      # optimistic concurrency
js.publish("orders.created", payload, ttl: 3600)                          # per-message TTL; needs allow_message_ttl
```

A refused publish raises the matching error, for example `Tether::JetStream::WrongLastSequenceError`. When no stream
captures the subject it raises `Tether::JetStream::NoStreamResponseError`, after two retries.

For throughput, publish without waiting and collect the acknowledgments later:

```ruby
future = js.publish_async("orders.created", payload, msg_id: "order-2")
future.value(timeout: 5)                # => the ack, or raises what the server answered

1000.times { |i| js.publish_async("orders.created", "order #{i}") }
js.wait_for_publishes(timeout: 10)      # => true when every one is acknowledged or failed
js.on_publish_error { |failed| warn "#{failed.subject}: #{failed.error.message}" }
```

Tether also covers the newer server features. Each needs the matching option on the stream:

```ruby
js.batch do |batch|                                    # all or nothing; stream option allow_atomic
  batch.publish("orders.created", "a")
  batch.publish("orders.paid", "b")
end

js.fast_batch(flow: 100) do |batch|                    # high-volume ingest, acknowledged in windows; allow_batched
  rows.each { |row| batch.publish("ingest.rows", row) }
end

counter = js.counter("hits.home")                      # a distributed counter; allow_message_counter
counter.increment(5)                                   # => the new total
counter.value

js.publish("schedules.report", payload,                # deliver later or repeatedly; allow_message_schedules
           schedule: { at: Time.now + 3600, target: "reports.daily" })
```

### Consumers

A consumer is the server's cursor into a stream. `durable:` names one that stays until you delete it.

```ruby
consumer = stream.create_consumer(durable: "billing", filter_subjects: ["orders.>"], ack_wait: 30, max_deliver: 5)
consumer = stream.consumer("billing")                 # look up; raises Tether::JetStream::ConsumerNotFoundError
```

There are three ways to read from it. **`fetch`** asks for one bounded batch:

```ruby
batch = consumer.fetch(10, timeout: 5)                # up to ten messages; possibly fewer, possibly none
batch.each do |msg|
  process(msg.data)
  msg.ack
end

msg = consumer.next(timeout: 5)                       # exactly one, or Tether::TimeoutError
```

**`consume`** delivers continuously to a block on a worker thread, keeping a buffer topped up:

```ruby
consuming = consumer.consume(max_messages: 100) do |msg|
  process(msg.data)
  msg.ack
end

consuming.on_error { |error| warn error.message }     # warnings, and the error that ends the loop
consuming.drain                                       # stop pulling, finish what is buffered
consuming.stop                                        # or stop now
```

**`messages`** is the same engine driven by your own thread:

```ruby
messages = consumer.messages(max_messages: 100)
messages.each do |msg|
  process(msg.data)
  msg.ack
end                                                   # runs until messages.stop is called

msg = messages.next(timeout: 5)                       # or take them one at a time; StopIteration once stopped
```

An **ordered consumer** gives strictly ordered, gap-free delivery with no acknowledgments, and recreates itself after
any disruption. Use it to read or replay a stream rather than to share work:

```ruby
ordered = stream.ordered_consumer(filter_subjects: ["orders.>"], deliver_policy: :all)
ordered.consume { |msg| replay(msg) }
```

**Consumers are pull only.** Tether does not create or consume push consumers, where the server sends at its own pace
to a subject and the client can only drop or stall. Pulling does everything pushing did, with the client deciding how
much it holds: `consume` is the replacement for a push subscription, and several processes consuming one durable
consumer replace a deliver group. This is also the direction the NATS documentation and the Go client recommend. A push
consumer created by another client can still be inspected and deleted; reading from it raises `PushConsumerError`.

### Acknowledging

```ruby
msg.ack                      # done
msg.ack_sync(timeout: 5)     # done, and wait for the server to confirm
msg.nak                      # redeliver now
msg.nak(delay: 30)           # redeliver in thirty seconds
msg.in_progress              # still working; restart the ack wait
msg.term(reason: "poison")   # never redeliver

msg.metadata.stream_sequence
msg.metadata.delivered       # 1 on the first delivery
```

A message is acknowledged once; a second `ack`, `nak` or `term` raises `AlreadyAckedError`. On messages from a consumer
with `ack_policy: :none`, ordered consumers included, the ack methods do nothing, so one handler works under either.

## Key/Value

A bucket is a map of keys to values with history, kept in a stream.

```ruby
kv = js.create_key_value(bucket: "config", history: 5)
kv = js.key_value("config")                 # raises Tether::KeyValue::BucketNotFoundError

kv.put("region", "eu")                      # => the new revision
kv["region"]                                # => "eu", or nil when absent or deleted
kv["region"] = "us"

entry = kv.get("region")                    # => an entry, or nil
entry.value ; entry.revision ; entry.created_at

kv.create("lock", "me")                     # raises KeyExistsError unless the key is absent or deleted
kv.update("region", "ap", revision: entry.revision)   # raises RevisionMismatchError if someone wrote in between
kv.delete("lock")                           # history kept
kv.purge("lock")                            # history dropped

kv.keys.to_a                                # live keys; kv.keys("limits.>") filters
kv.history("region")                        # every kept revision, oldest first
kv.each { |entry| puts "#{entry.key}=#{entry.value}" }
```

`watch` delivers the current values and then every change. The end of the initial values is an event, not a `nil` in
the stream of entries:

```ruby
watcher = kv.watch("region", "limits.>") do |entry|
  puts "#{entry.key} #{entry.operation} #{entry.value}"       # operation is :put, :delete or :purge
end
watcher.on_ready { puts "caught up" }
watcher.stop
```

## Object Store

A bucket holds objects of any size, stored as chunks and verified against a SHA-256 digest when read.

```ruby
store = js.create_object_store(bucket: "assets")
store = js.object_store("assets")

info = store.put("logo.png", File.open("logo.png", "rb"), description: "the logo")   # an IO or a String
info.size ; info.chunks ; info.digest

store.get("logo.png") { |chunk| out.write(chunk) }     # streams, a chunk at a time
data = store.get("logo.png").read                      # or everything at once
store.put_file("report.pdf", "/tmp/report.pdf")
store.get_file("report.pdf", "/tmp/copy.pdf")

store.info("logo.png")                                 # metadata, or nil
store.list.map(&:name)
store.delete("logo.png")
store.watch { |info| puts "#{info.name} changed" }
```

Reading a missing object raises `Tether::ObjectStore::ObjectNotFoundError`, and content that does not match its digest
raises `DigestMismatchError`.

## Services

A service is a set of request/reply endpoints that can be discovered and report statistics (the NATS micro services
convention). Instances of one service share a queue group, so each request goes to one of them.

```ruby
service = nc.services.add(name: "calc", version: "1.0.0", description: "arithmetic")

service.endpoint("add") { |request| request.respond((request.data.to_i + 1).to_s) }
service.endpoint("div", subject: "calc.div") do |request|
  next request.respond_error(400, "division by zero") if request.data == "0"

  request.respond_json({ result: 100 / request.data.to_i })
end
service.group("math").endpoint("sqrt") { |request| request.respond(Math.sqrt(request.data.to_f).to_s) }   # math.sqrt

nc.request("add", "41").data            # => "42"

nc.services.ping("calc")                # which instances are out there
nc.services.info("calc")                # their endpoints
nc.services.stats("calc")               # requests, errors and processing time per endpoint

service.stop                            # finish requests in flight, then stop answering
```

An endpoint that raises answers the caller with a 500 service error and keeps serving.

## Errors

Everything tether raises descends from `Tether::Error`, so one `rescue` catches it all and nothing else. Errors that
belong to a call are raised from it; errors that do not (a server `-ERR`, a slow consumer, an exception in a handler) go
to `on_error`.

```ruby
begin
  reply = nc.request("users.get", id, timeout: 2)
rescue Tether::NoRespondersError
  # nobody is subscribed
rescue Tether::TimeoutError
  # no answer in time; the request may or may not have been processed
rescue Tether::Error => e
  retry if e.retryable?
  raise
end
```

`retryable?` is true when repeating the same call may succeed with no change on your side: the connection was lost
(`DisconnectedError`), a stream or the JetStream API had no leader for a moment (`NoStreamResponseError`,
`ServiceUnavailableError`), or the server reported a transient condition. It is false for a timeout, because only you
know whether the call is safe to repeat. JetStream API errors are `Tether::JetStream::APIError` subclasses with `code`,
`err_code` and `description`. The whole hierarchy is in [docs/design.md](docs/design.md#11-errors).

## Threads and fork safety

Tether uses plain threads, all named `tether:*`, and none of them is ever killed or raised into:

| Thread | Count | Does |
|---|---|---|
| reader | 1 per connection | reads and parses; never runs your code |
| flusher | 1 per connection | writes buffered frames |
| timer | 1 per connection | keepalive pings, heartbeat checks, timeouts |
| events | 0 or 1 | runs `on_*` handlers, in order |
| subscription worker | `concurrency` per subscription with a block | runs your block |
| consume worker | `concurrency` per `consume` | runs your block and keeps the buffer full |

A forked child inherits the socket but none of the threads. Tether notices the new process on the first call made in the
child and replaces the inherited connection with a fresh one, without writing to or shutting down the parent's socket.
The child starts with **no subscriptions**, which is the only behavior that cannot process a message twice, so create
subscriptions and consume loops after the fork. A child that never uses the client only keeps an idle copy of the
descriptor until it exits. In a preforking server, do the switch at a known moment rather than inside the first request:

```ruby
# config/puma.rb
on_worker_boot { NATS.after_fork }      # NATS is your Tether::Client
```

## Development

```sh
bin/setup
bundle exec rake                        # specs, RuboCop and rbs validate
```

The suite needs Docker: it starts `nats:alpine` containers on free ports (`NATS_IMAGE` picks another image). Set
`NATS_URL` to run it against a server that is already up; specs that need a particular server configuration still start
their own.

## Further reading

- [docs/design.md](docs/design.md): the complete API, the architecture and the concurrency model, with the reasons.
- [docs/protocol/](docs/protocol/README.md): the NATS protocol as read from the server source.
- [docs/migrating-from-nats-pure.md](docs/migrating-from-nats-pure.md): coming from `nats-pure`.

Released under the [MIT license](LICENSE.txt).
