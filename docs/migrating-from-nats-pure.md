# Migrating from nats-pure

Tether is not a drop-in replacement for `nats-pure`: there is no compatibility layer, and the two
gems can be loaded side by side (`NATS` and `Tether` share nothing), so a codebase can move one
connection at a time. This guide lists what to rename, what to rewrite, and what behaves
differently. The reasons behind the differences are in [design.md](design.md).

The short version: keyword arguments everywhere, and an unknown one raises; a handler block always
receives one argument, the message; every duration is in seconds and every instant a `Time`;
JetStream consumers are pull only, so `js.subscribe` becomes `consumer.consume`; errors are
`Tether::Error` subclasses with `Error` in their names; and no `nkeys` or `websocket` gem is needed.

## Connecting

```ruby
# nats-pure
nc = NATS.connect(servers: ["nats://a:4222", "nats://b:4222"], name: "billing",
                  reconnect_time_wait: 1, max_reconnect_attempts: -1, user_credentials: "app.creds")

# tether
nc = Tether.connect(["nats://a:4222", "nats://b:4222"], name: "billing",
                    reconnect: { wait: 1, max_attempts: -1 }, credentials_file: "app.creds")
```

| nats-pure option | tether |
|---|---|
| `servers: [...]` | the first argument: a URL, a comma separated list or an array |
| `dont_randomize_servers: true` | `randomize: false` |
| `reconnect_time_wait: 2` | `reconnect: { wait: 2 }`, plus `jitter:` |
| `max_reconnect_attempts: 10` | `reconnect: { max_attempts: 60 }`; counted per server, `-1` never gives up. The default is 60, not 10 |
| `max_outstanding_pings:` | `max_pings_out:` |
| `ignore_discovered_urls:` | `ignore_discovered_servers:` |
| `connect_timeout:` | `connect_timeout:`; it now covers DNS, TCP, TLS and the handshake of one attempt |
| `drain_timeout:` | `drain_timeout:`, or `nc.drain(timeout:)` |
| `name:`, `reconnect: false`, `ping_interval:` | the same |
| `user:`, `pass:` | `user:`, `password:` |
| `auth_token:` | `token:` |
| `user_credentials: path` | `credentials_file: path`, or `credentials: text` |
| `nkeys_seed: path` | `nkey_seed_file: path`, or `nkey_seed: text` |
| `user_jwt_cb:`, `user_signature_cb:`, `user_nkey_cb:` | `auth: ->(nonce) { { jwt: ..., sig: ... } }`, or callables for `jwt:` and `nkey_seed:` |
| `tls: { context: ctx }` | `tls: { context: ctx }`, or `ca_file:`, `cert_file:`, `key_file:`, `verify:`, `first:` |
| `custom_inbox_prefix:` | `inbox_prefix:` |
| `verbose:`, `pedantic:`, `no_responders:` | none; verbose and pedantic are always off, no-responders is on whenever the server supports headers |
| `close_timeout:`, `old_style_request:`, `reloader:` | none |
| `NATS_RECONNECT`, `NATS_PING_INTERVAL` and the other `NATS_*` variables | none; only `NATS_URL` is read, and an explicit argument wins |

`NATS::Client.new` connected lazily on first use. `Tether::Client.new(url, **options)` never
connects until you call `connect`; its purpose is registering handlers before the first connect.
Status is a symbol: `nc.status == :connected` instead of `NATS::Status::CONNECTED`. The predicates
`connected?`, `reconnecting?`, `draining?` and `closed?` are the same.

## Callbacks become events

```ruby
# nats-pure: one slot per callback, arity decides the arguments
nc.on_error { |e| log(e) }
nc.on_reconnect { log("reconnected") }

# tether: any number of handlers, each returns a handle with cancel
nc.on_error { |error| log(error) }
nc.on_reconnect { |server| log("reconnected to #{server.url}") }
```

`on_disconnect { |error| }` and `on_close { }` are unchanged. New: `on_connect`,
`on_reconnect_error`, `on_lame_duck`, `on_servers_discovered`, `on_status`. Handlers run on a
dedicated thread, never on the read loop, and one that raises is caught, and logged when a
`logger:` is set.

An exception in a subscription block was passed to `on_error` as it was. Tether wraps it in a
`Tether::HandlerError`, with the original as `cause` and the message as `msg`.

## Publish, subscribe, request

| nats-pure | tether |
|---|---|
| `nc.publish(subject, data, reply, header: h)` | `nc.publish(subject, data, reply_to: reply, headers: h)` |
| `nc.subscribe(s, queue: q) { \|data, reply, subject\| }` | `nc.subscribe(s, queue: q) { \|msg\| }` |
| `pending_msgs_limit:`, `pending_bytes_limit:` | `max_pending:`, `max_pending_bytes:` |
| `processing_concurrency: 4` | `concurrency: 4` |
| a shared pool of 24 callback threads | a shared pool of `worker_threads:` (default 16); `dedicated: true` gives a subscription its own |
| `sub.next_msg(timeout: 1)` | `sub.next(timeout: 1)` |
| `sub.unsubscribe(5)` | `sub.unsubscribe(after: 5)` |
| `nc.request(s, data, timeout: 1, header: h)` | `nc.request(s, data, timeout: 1, headers: h)` |
| `nc.request(s, data) { \|msg\| }`, `nc.old_request`, `publish_msg`, `request_msg` | none; use `request`, or `request_many` for several replies |
| `nc.flush(5)` | `nc.flush(timeout: 5)` |
| `msg.reply`, `msg.header`, `msg.sub` | `msg.reply_to`, `msg.headers`, `msg.subscription` |
| `nc.server_info[:max_payload]`, `nc.discovered_servers` | `nc.max_payload`, `nc.servers.select(&:discovered?)` |

```ruby
# nats-pure
nc.subscribe("orders.*") do |data, reply, subject|
  nc.publish(reply, handle(subject, data)) unless reply.to_s.empty?
end

# tether
nc.subscribe("orders.*") do |msg|
  msg.respond(handle(msg.subject, msg.data)) if msg.reply_to
end
```

Things to check while converting:

- **Block arity.** A block always gets one `Tether::Msg`. A block written as `{ |data, reply| }`
  will not fail loudly: `data` becomes the message and `reply` is `nil`.
- **Default timeouts.** `request` waits 2 seconds, not 0.5. `sub.next` without `timeout:` waits
  until a message arrives, where `next_msg` gave up after half a second.
- **Messages are read-only.** There are no setters on a `Msg`, and `msg.headers` is `nil` when the
  message has none. `msg.respond` no longer copies the request's headers into the reply.
- **Subjects are validated.** An empty token, whitespace, or a wildcard in a publish subject
  raises `Tether::InvalidSubjectError` before anything is written.
- **`drain` blocks.** It returned a thread; now it returns when the client is closed, or raises
  `Tether::TimeoutError`. Subscriptions have their own `sub.drain`.

## JetStream

The context is `nc.jetstream(domain:, api_prefix:, timeout:)`; `prefix:` is now `api_prefix:`.
Management calls return handles (`Stream`, `Consumer`) that carry the info, instead of response
structs, and most per-object calls moved onto the handle.

| nats-pure | tether |
|---|---|
| `js.add_stream(name: "ORDERS", subjects: [...])` | `js.create_stream(name: "ORDERS", subjects: [...])` |
| `js.stream_info("ORDERS")` | `js.stream("ORDERS").info`, or `.cached_info` for what the lookup returned |
| `js.find_stream_name_by_subject(s)` | `js.stream_name_for(s)`, which returns `nil` instead of raising |
| `js.get_msg(stream, seq: 5)` | `js.stream(stream).get(5)` |
| `js.get_last_msg(stream, subject)` | `js.stream(stream).get(last_for: subject)` |
| `js.add_consumer(stream, config)` | `js.create_or_update_consumer(stream, **config)` |
| `js.consumer_info(stream, name)` | `js.consumer(stream, name).info` |
| `js.pause_consumer(stream, name, time)`, `js.resume_consumer(...)` | `consumer.pause_until(time)`, `consumer.resume` |
| `js.unpin_consumer(stream, name, group)`, `js.reset_consumer(stream, name, seq: 5)` | `consumer.unpin(group)`, `consumer.reset(sequence: 5)` |
| `js.publish(s, data, header: h, stream: "ORDERS")` | `js.publish(s, data, headers: h, expect: { stream: "ORDERS" })` |
| `js.pull_subscribe(subject, "billing")` then `psub.fetch(10)` | `js.consumer("ORDERS", "billing").fetch(10)` |
| `js.subscribe(subject, durable: "billing") { }` | `js.consumer("ORDERS", "billing").consume { }` |
| `msg.ack(timeout: 1)` | `msg.ack_sync(timeout: 1)` |
| `msg.nak(delay: 5_000_000_000)` | `msg.nak(delay: 5)` |
| `msg.metadata.sequence.stream`, `.sequence.consumer`, `.num_delivered`, `.num_pending`, `.timestamp` | `msg.metadata.stream_sequence`, `.consumer_sequence`, `.delivered`, `.pending`, `.time` |

Config fields spell out what the wire abbreviates, and durations are seconds in both stream and
consumer configs (`max_age` on a stream was nanoseconds in `nats-pure`):

| nats-pure field | tether keyword |
|---|---|
| `max_msgs`, `max_msgs_per_subject`, `max_msg_size` | `max_messages`, `max_messages_per_subject`, `max_message_size` |
| `num_replicas`, `first_seq` | `replicas`, `first_sequence` |
| `allow_rollup_hdrs`, `allow_msg_ttl`, `allow_msg_counter`, `allow_msg_schedules` | `allow_rollup`, `allow_message_ttl`, `allow_message_counter`, `allow_message_schedules` |
| `durable_name`, `mem_storage`, `sample_freq` | `durable`, `memory_storage`, `sample_frequency` |
| `opt_start_seq`, `opt_start_time` | `start_sequence`, `start_time` |
| `deliver_subject`, `deliver_group`, `flow_control`, `idle_heartbeat`, `rate_limit_bps` | none: these make a push consumer |
| `ack_policy: "explicit"`, `retention: "workqueue"` | symbols: `ack_policy: :explicit`, `retention: :work_queue` |

### Pull subscriptions

```ruby
# nats-pure
psub = js.pull_subscribe("orders.>", "billing", stream: "ORDERS")
begin
  psub.fetch(10, timeout: 5).each { |msg| process(msg); msg.ack }
rescue NATS::Timeout
  # nothing to do
end

# tether
consumer = js.consumer("ORDERS", "billing")     # or stream.create_consumer(durable: "billing", ...)
consumer.fetch(10, timeout: 5).each { |msg| process(msg); msg.ack }
```

`pull_subscribe` created the consumer when it was missing; in tether creating is explicit
(`create_consumer`, `create_or_update_consumer`) and `consumer` only looks up. `fetch` returns a
`Batch`, which is `Enumerable`, and an empty batch when nothing arrived: finding nothing is not an
error. `no_wait: true` is `wait: false`.

### Push subscriptions

Tether does not consume push consumers. A push subscription becomes a pull consumer read with
`consume`, which gives the same shape (a block called for each message on a background thread)
with the client deciding how much it holds.

```ruby
# nats-pure
sub = js.subscribe("orders.>", durable: "billing", queue: "workers") { |msg| process(msg) }

# tether
consumer = js.create_or_update_consumer("ORDERS", durable: "billing", filter_subjects: ["orders.>"])
consuming = consumer.consume do |msg|
  process(msg)
  msg.ack
end
```

To convert a push workflow:

1. **Make a pull consumer.** An existing push consumer cannot be turned into a pull consumer, so
   create a new durable with the same `filter_subjects`, `ack_policy`, `ack_wait` and
   `max_deliver`, and delete the old one once it is drained. To continue where the old one
   stopped, start the new one at the old one's acknowledgment floor:
   `start_sequence: old.info.ack_floor.stream_sequence + 1`.
2. **Acknowledge yourself.** `js.subscribe` acknowledged after the block returned unless
   `manual_ack: true` was given. `consume` never acknowledges for you: call `msg.ack`, and `msg.nak`
   or `msg.term` on failure.
3. **Drop the deliver group.** A queue of push subscribers becomes several processes calling
   `consume` on the same durable consumer; the server hands each message to one of them.
4. **Drop `flow_control` and `idle_heartbeat`.** Pull requests bound what is in flight
   (`max_messages:`, `max_bytes:`), and the loop asks for heartbeats and watches for them itself.
5. **Stop it properly.** `consuming.drain` finishes what is buffered; `consuming.stop` discards it,
   and the server redelivers after `ack_wait`.

For a subscription that only needs to read a stream in order (a cache, a replay, a projection),
use `stream.ordered_consumer(...).consume { }`: nothing to create or acknowledge, and it recovers
from gaps on its own.

## Key/Value

| nats-pure | tether |
|---|---|
| `kv.get(key)` raising `KeyNotFoundError` | `kv.get(key)` returns `nil`; `kv.fetch(key)` raises; `kv[key]` returns the value or `nil` |
| `kv.update(key, value, last: rev)` | `kv.update(key, value, revision: rev)` |
| `kv.delete(key, last: rev)` | `kv.delete(key, revision: rev)` |
| `kv.purge(key)` returning a `PubAck` | returns the revision, like every other write |
| `kv.keys` raising `NoKeysFoundError` when empty | an empty enumerator; `kv.keys("prefix.>")` filters |
| `kv.history(key)` raising `NoKeysFoundError` | an `Array`, empty for a key never written |
| `kv.watch(">")`, `kv.watchall` | `kv.watch`; several filters as `kv.watch("a", "b.>")` |
| `watcher.updates(timeout: 5)` | `watcher.next(timeout: 5)`, or give `watch` a block |
| `entry.created` | `entry.created_at`; `entry.operation` is `:put`, `:delete` or `:purge` |
| `KeyWrongLastSequenceError` | `RevisionMismatchError`, and its subclass `KeyExistsError` from `create` |
| `KeyDeletedError`, `KeyHistoryTooLargeError` | none: a deleted key reads as `nil`, and a bad option is an `ArgumentError` |

```ruby
# nats-pure: nil in the stream of updates marks the end of the initial values
watcher = kv.watch("config.>")
while (entry = watcher.updates(timeout: 5))
  apply(entry)
end
ready!

# tether: the end of the initial values is an event
watcher = kv.watch("config.>", on_ready: -> { ready! }) { |entry| apply(entry) }
```

Keys and bucket names are always validated; there is no `validate_keys:`. New in tether:
`js.key_values`, `update_key_value`, `purge_deletes`, per-key TTLs, mirrors, sources and `kv.each`.

## Services

| nats-pure | tether |
|---|---|
| `service.endpoints.add("add", subject: "calc.add") { \|req\| }` | `service.endpoint("add", subject: "calc.add") { \|req\| }` |
| `service.groups.add("math")` | `service.group("math")` |
| `request.respond_with_error(code: 400, description: "bad")` | `request.respond_error(400, "bad")` |
| `service.on_stats { \|endpoint\| }` | `stats_handler: ->(endpoint) { }` when adding |
| `nc.services.add(name:, version:, ...)`, `service.on_stop { \|error\| }` | the same; `on_stop:` is also accepted when adding |
| `service.reset` | `service.reset_stats` |

A handler that raised a `NATS::Error` stopped the whole service. In tether any exception is
answered with a 500 service error, counted, and the service keeps running. `queue: false` on an
endpoint or group turns the queue group off, and `nc.services.ping`, `info` and `stats` are a
discovery client that `nats-pure` does not have.

## Errors

| nats-pure | tether |
|---|---|
| `NATS::Error`, `NATS::IO::Error` | `Tether::Error` |
| `NATS::Timeout`, `NATS::IO::SocketTimeoutError` | `Tether::TimeoutError` |
| `NATS::IO::NoRespondersError` | `Tether::NoRespondersError` |
| `NATS::IO::BadSubject`, `BadSubscription`, `SlowConsumer` | `Tether::InvalidSubjectError`, `SubscriptionClosedError`, `SlowConsumerError` |
| `NATS::IO::ConnectError`, `AuthError` | `Tether::ConnectionError`; `Tether::AuthorizationError` and the other `Tether::ServerError` subclasses |
| `NATS::IO::DrainTimeoutError`, `ForkDetectedError` | `Tether::TimeoutError`, `Tether::ForkedError` |
| `MaxPayloadError`, `NoServersError`, `StaleConnectionError`, `ConnectionClosedError`, `ConnectionDrainingError` | the same names under `Tether` |
| `JetStream::Error::NoStreamResponse` | `Tether::JetStream::NoStreamResponseError` |
| `JetStream::Error::NotFound`, `StreamNotFound`, `ConsumerNotFound` | `StreamNotFoundError`, `ConsumerNotFoundError`, `MessageNotFoundError` |
| `JetStream::Error::ConsumerAlreadyExists`, `ConsumerDoesNotExist` | `ConsumerExistsError`, `ConsumerNotFoundError` |
| `JetStream::Error::ServiceUnavailable` | `ServiceUnavailableError` |
| `JetStream::Error::MsgAlreadyAckd`, `NotJSMessage` | `AlreadyAckedError`, `NotJetStreamMessageError` |
| `JetStream::API::Error` matched on `err_code` | `JetStream::APIError`, with subclasses such as `WrongLastSequenceError` and `StreamNameInUseError` |

`error.retryable?` says whether repeating the call may succeed without a change on your side.

## Behavior that can surprise

- **A request in flight when the connection drops fails at once** with
  `Tether::DisconnectedError`, which is `retryable?`, where `nats-pure` let it run into its timeout.
  Whether the responder saw it is unknown, so the retry is your decision.
- **Publishing during an outage never blocks.** Messages are buffered up to
  `reconnect: { buffer_size: }` (8 MiB) and the next publish raises
  `Tether::ReconnectBufferFullError`. Code that published in a loop and relied on being slowed
  down needs to rescue it.
- **A server `-ERR` does not drop the connection.** A permissions violation or an invalid subject
  is reported to `on_error` and the connection stays up; only the socket closing starts a
  reconnect.
- **Handler exceptions are events.** They never reach the thread that publishes or the read loop,
  and a raising handler does not end its subscription.
- **Slow subscriptions drop the newest messages**, per subscription, and report
  `SlowConsumerError` once per episode rather than once per message.
- **Fork.** Nothing is installed on `Process`. A forked child gets a fresh connection on its first
  call, or when you call `nc.after_fork`, and starts with no subscriptions; the callbacks of the
  parent's connection do not fire in the child, and nothing is written to the parent's socket.
- **Timeouts are budgets.** A `timeout:` covers the whole call, however many round trips it makes,
  and accepts a `Tether::Deadline` to share one budget between calls. Do not wrap calls in
  `Timeout.timeout`.
- **No extra gems, no Rails hook.** Remove `nkeys` and `websocket` from the Gemfile if only
  `nats-pure` needed them. `nats-pure` wrapped subscription blocks in the Rails reloader; do that
  yourself where you need it: `Rails.application.reloader.wrap { handle(msg) }`.
