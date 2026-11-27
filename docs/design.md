# Tether client design

This document defines what the tether gem is: its public API, its internal architecture, its
concurrency model, and how it was built. Where it and the code disagree, the code is right and this
document has a bug. It is derived from three sources, each studied at the source level rather than
from documentation:

- **The protocol itself**, as implemented by `nats-server` (`2.16.0-dev`, JetStream API level 5).
  The wire contract is written up in [`docs/protocol/`](protocol/README.md); every `Client rule`
  in those chapters is a requirement on this design.
- **`nats.go`** (v1.56.0), the reference client. It defines feature parity and contributes the
  algorithms that are hard to get right: reconnect, the pull-consumer refill loop, the ordered
  consumer, async publish.
- **`nats-pure.rb`** (v2.7.0), the client tether replaces. It defines what Ruby users already
  know, and its defect history shows which mistakes a Ruby client makes when its concurrency model
  is implicit.

Contents:

1. [Goals and non-goals](#1-goals-and-non-goals)
2. [What the two existing clients teach](#2-what-the-two-existing-clients-teach)
3. [Design principles](#3-design-principles)
4. [Architecture](#4-architecture)
5. [The connection](#5-the-connection)
6. [Core API](#6-core-api)
7. [JetStream](#7-jetstream)
8. [Key/Value](#8-keyvalue)
9. [Object Store](#9-object-store)
10. [Services](#10-services)
11. [Errors](#11-errors)
12. [Concurrency model](#12-concurrency-model)
13. [Feature matrix](#13-feature-matrix)
14. [Testing](#14-testing)
15. [How it was built](#15-how-it-was-built)
16. [Decisions and their reasons](#16-decisions-and-their-reasons)

---

## 1. Goals and non-goals

**Goals**

1. Support everything the server offers a client: core pub/sub and request/reply, headers, every
   authentication mechanism, TLS in all its handshake orders, cluster discovery and reconnect,
   WebSocket, and the whole JetStream surface: streams, every way of publishing, pull consumers,
   Key/Value, Object Store and Services.
2. Be Ruby first. A Ruby developer should be able to guess the API. Go's shapes (functional
   options, channels, contexts, error returns, nanosecond integers) are translated, not
   transliterated.
3. One connection is safe to use from any number of threads, with no caller-side locking, and
   works unmodified under a `Fiber::Scheduler`.
4. Fail loudly and promptly. Every blocking call has a deadline; every failure path ends in an
   exception or a callback, never in a silent hang.
5. No runtime dependencies. Ruby 3.2 and its standard library, including OpenSSL, cover everything.

**Non-goals**

- Ractor support. The client relies on shared mutable state guarded by locks.
- A compatibility shim for the `nats-pure` API. The
  [migration guide](migrating-from-nats-pure.md) lists the renames, and that is enough.
- Legacy features the reference client has deprecated: encoded connections, the old per-request
  subscription style, the legacy JetStream API shape (`js.subscribe`, `js.pull_subscribe`).
- Push consumers. The NATS documentation and `nats.go` both steer new code to pull consumers, which
  do everything push consumers do with explicit flow control. Tether consumes only by pulling:
  ordered consumers, Key/Value watchers and Object Store readers are all built on the pull engine.
  A push consumer that already exists on the server can be inspected and deleted, and consuming
  from it raises `PushConsumerError`. `ack_policy: :flow_control`, which exists only for push
  delivery, is not offered either.
- MQTT, leafnode, route and gateway protocols. Tether is a client.
- System-account administration: the `$SYS` server endpoints, JetStream meta-leader operations,
  account purge, stream peer evacuation and move cancellation. They are plain requests that a user
  can issue with `nc.request`; tether adds no wrappers.
- Acting as a trusted proxy (`proxy_sig`). The `auth:` callable can supply any CONNECT field, so
  nothing prevents it, but it is not a designed feature.

---

## 2. What the two existing clients teach

### 2.1 nats-pure.rb

`nats-pure` is maintained again and its recent work is careful, so the case for tether rests on
architecture, not on neglect. It gets several things right that tether must not regress on: user
callbacks never run on the read loop, per-subscription ordering is preserved, slow consumers drop
rather than block, background threads are stopped cooperatively, and library code contains no
`Timeout.timeout` or `Thread#raise`.

Its problems come from one decision. `NATS::Client` is a single 2,211-line class behind one
re-entrant monitor, and every other layer (the parser, subscriptions, JetStream, KV, Services)
reaches into its private methods with `send`. Correctness then lives in hand-written
monitor-and-condition-variable choreography spread across that class, so each fix is local and the
same race reappears at the next wait site. The changelog shows the pattern: lost wakeups in
`fetch`, a wrong waiter woken, accounting drift, teardown hangs. These defects remain, found by
reading the code:

| Area | Defect | What tether does instead |
|---|---|---|
| Error handling | Every server `-ERR` reconnects or closes, so an invalid subject or a permissions violation drops the connection, and with reconnect on, the replayed `SUB` loops | A table derived from the server source says which errors close the connection; reconnect is driven by the socket closing, never by an error's text |
| Server pool | Auth errors are booked against `server_pool.first`, which is the *next* server, not the connected one | The pool tracks the current server explicitly; per-server state attaches to it |
| Request/reply | The requester publishes, then starts waiting; a reply that arrives in between is lost and the request times out | The waiter is registered before the publish and is a queue, so an early reply is simply there when the requester looks |
| Flush | PONGs are matched to waiters last-in-first-out, and every server PING adds a waiter nobody owns | A FIFO of waiters; server PINGs never touch it |
| Write path | No write lock; the flusher, drain, close and reconnect threads all write | One buffer of whole frames behind a buffer lock, and one I/O lock that serializes socket writes (5.4) |
| Encoding | Frames are built by interpolation and joined; a UTF-8 frame and a binary frame in one batch raise `Encoding::CompatibilityError` in the flusher, which reconnects and loses the batch | Every frame is binary before it meets another |
| Back-pressure | The pending buffer is bounded at 32,768 *commands*, not bytes, and publishers block without a timeout during an outage | A byte-bounded reconnect buffer that raises on overflow |
| Reconnect | A failure after the handshake (resubscribe, buffer replay) leaves the client in `RECONNECTING` forever | Reconnect is one supervised loop; every failure either retries or closes with a callback |
| Callbacks | `on_error` runs on the read loop under the client monitor; if it raises, `abort_on_exception` kills the process | Events run on their own thread; an exception in user code is caught and reported |
| Dispatch | One pool of 24 workers serves every subscription *and* request replies, so 24 slow handlers time out every request on the connection | Replies are resolved on the reader thread with no user code, so subscription blocks share a pool without holding up a reply; a subscription can opt out with threads of its own |
| Validation | Subjects, replies, queue names and headers are written into the protocol stream unchecked, which allows CRLF injection | Validated at the API boundary |
| Fork | The child writes the parent's buffered commands to the inherited socket | The child never writes to or shuts down an inherited descriptor |
| JetStream | Legacy API shape built around push subscriptions; `fetch` has no heartbeats; no ordered consumer, no continuous consume, no async publish, no Object Store, no purge or list calls | The modern object model, with the consumer machinery in one reusable place |
| API | Positional arguments and trailing hashes that silently ignore typos, callbacks dispatched by block arity, argument mutation, three unit conventions for durations, closed structs that drop unknown server fields | Keyword arguments, one `Msg` argument, immutable inputs, seconds everywhere, forward-compatible value objects |

### 2.2 nats.go

`nats.go` is the feature checklist and the source of the difficult algorithms. Tether follows it
closely in mechanism:

- **Write coalescing**: frames are appended to a buffer under a lock; a flusher thread, kicked at
  most once, writes them; a full buffer flushes inline.
- **Reconnect**: a server pool with per-server attempt counts, a pause with jitter after each full
  pass over the pool, resubscription with adjusted auto-unsubscribe counts, and a bounded buffer
  for publishes made during the outage.
- **Request mux**: one wildcard inbox subscription per connection, with a token map.
- **Pull consumption**: one inbox subscription per consume loop, overlapping pull requests, a
  refill threshold, heartbeat-based liveness, and status-driven pending accounting.
- **Ordered consumer**: recreate on any gap, from the last delivered stream sequence plus one.

It is not copied where its shape is Go's rather than the problem's, or where the code is known to
be surprising:

| nats.go | Tether |
|---|---|
| `nc.mu` is held during socket writes and the whole handshake, so a stalled write blocks every API call for up to the one minute flusher timeout | Appending to the buffer and writing the socket take different locks, so a stalled write delays nobody until the buffer's hard cap is reached (5.4) |
| Functional options (`nats.Name("x")`, `jetstream.PullMaxMessages(10)`) | Keyword arguments |
| `context.Context` for cancellation and deadlines | `Tether::Deadline`, passed down through every blocking call; `timeout:` accepts seconds or a `Deadline` (section 3) |
| Channels (`MessageBatch.Messages()`, `PubAckFuture.Ok()`, `StatusChanged()`) | `Enumerable`, blocks, and futures with `value`/`wait` |
| Error return values and sentinel errors | Exceptions in a class hierarchy |
| `time.Duration` nanoseconds on the wire and in config structs | Seconds as `Numeric`, `Time` for instants; conversion happens in one place |
| An in-flight request waits out its timeout when the connection drops | It fails at once with a retryable error (section 16) |
| A flush interrupted by a disconnect returns "connection closed" | It raises a disconnected error distinct from closed |
| Unknown `-ERR` closes the connection without calling the error handler | Every `-ERR` reaches the error handler |
| A panic in a subscription callback is not recovered | An exception in a handler is caught, reported, and delivery continues |
| The parser has no control-line length limit | Bounded control line and payload |
| No client for atomic batch publish or counters | Both are implemented from the ADRs and the server source |
| A second `Consume` on a stopped ordered consumer fails | Consumption handles are independent of the consumer handle |

---

## 3. Design principles

**Small objects with contracts.** No object reaches into another's private state. The parser talks
to a dispatcher through seven methods; a subscription knows nothing about sockets; JetStream uses
only the public core API plus the two internal seams named in 4.1. Each piece is testable without a
server.

**The reader thread never runs user code and never blocks.** It parses, looks up, and enqueues. A
slow handler must not delay a PONG, a reply, or another subscription.

**Whole frames, binary, appended under the buffer lock.** A frame is built complete and binary
before it is appended to the write buffer under the writer's buffer lock (5.4). Frames from
different threads cannot interleave, and no string of one encoding ever meets a string of another.

**Every wait is a queue pop with a deadline.** Tether blocks only on `Thread::Queue`,
`Thread::SizedQueue`, `Mutex`, `ConditionVariable#wait` and socket readiness (`IO#wait_readable` for
the reader, `IO#wait_writable` for the writer). These are the primitives a `Fiber::Scheduler`
intercepts, so the client is built to work under Falcon and `async` without a special mode (the
suite does not run under a scheduler yet, section 14). `Timeout.timeout`, `Thread#raise` and
`Thread#kill` are banned from `lib`, and a spec enforces it. A waiter is registered before the
action that will satisfy it is taken, which removes the lost-wakeup class of bug by construction. No
tether thread ever joins or waits on itself: an operation that would, such as draining a
subscription from inside its own handler, is defined to start the work and return.

**One `Deadline` per call, never a bare timeout.** `Tether::Deadline` is an absolute instant on the
monotonic clock. A public method turns its `timeout:` into a `Deadline` once, on entry, with
`Deadline.coerce`, and from there down only the `Deadline` is passed: to the handshake, the writer,
a queue pop, a retry loop. Each blocking primitive is given `deadline.remaining`, so a call that
makes three round trips spends one budget across them and cannot take three times its timeout, which
is what passing seconds from layer to layer produces. Internal methods take `deadline`, not
`timeout`, and no internal method starts a clock of its own. Because `timeout:` also accepts a
`Deadline`, a caller can share one budget across several calls:

```ruby
deadline = Tether::Deadline.after(5)
stream   = js.stream("ORDERS", timeout: deadline)
batch    = stream.consumer("billing", timeout: deadline).fetch(10, timeout: deadline)
```

`deadline.with_at_most(seconds)` narrows a deadline for one step and never widens it, which is how
a per-attempt limit composes with an overall one; `nil` and `Deadline.infinite` mean no limit. A
`Deadline` is only ever consumed by primitives that end themselves; it is never handed to
`Timeout.timeout`.

**Errors that belong to a call are raised from it; errors that do not are events.** Nothing is ever
thrown into an unrelated thread. Asynchronous errors go to handlers on a dedicated thread and are
also remembered so the next call that touches the affected resource can raise.

**Ruby idiom at the boundary.**

- Keyword arguments, validated. An unknown keyword is an `ArgumentError`, not a silently ignored
  typo.
- Blocks for handlers, always called with one argument. No arity inspection.
- `each` where there is a sequence, returning an `Enumerator` without a block. Finite sequences
  (`stream.consumers`, `kv.keys`, a fetched batch) also include `Enumerable`; endless ones (a
  subscription, a consume iterator, a watcher) deliberately do not, because `to_a` or `count` on
  them would never return.
- A handler that reports events for something that has already started can be given when it is
  created (`on_error:`, `on_ready:`) so no early event is lost, and a later `on_ready { }` on
  something already ready fires at once.
- Block-scoped lifetimes where there is a resource: `Tether.connect { |nc| ... }` closes on exit.
- Predicates end in `?`, symbols for enumerations (`ack_policy: :explicit`), `nil` for "absent".
- Seconds as `Integer` or `Float` for every duration, `Time` for every instant. The wire's
  nanoseconds and RFC 3339 strings never reach user code.
- Arguments are never mutated. Returned value objects are frozen.
- Return values are meaningful and consistent: a query returns a value object, a command returns
  `self` or the object it created, nothing returns internal state.

**Forward compatible.** The server gains fields faster than a client can track them. Every decoded
server object keeps the raw hash it came from, unknown keys survive a read-modify-write of a
config, and an unknown enumeration value becomes a symbol rather than an error.

**No global state.** No hooks installed at `require`, no environment variables that override
explicit options, no process-wide registries, no runtime `extend`.

---

## 4. Architecture

### 4.1 Layers

```
            ┌──────────────────────────────────────────────────────────────┐
  users →   │ Tether::KeyValue    Tether::ObjectStore    Tether::Service   │  conventions
            ├──────────────────────────────────────────────────────────────┤
            │ Tether::JetStream   Stream  Consumer  Message  Publisher     │  JetStream
            ├──────────────────────────────────────────────────────────────┤
            │ Tether::Client      publish · subscribe · request · drain    │  core API
            ├──────────────────────────────────────────────────────────────┤
            │ Connection: Lifecycle · ServerPool · Handshake · Writer ·    │  one live
            │   Keepalive · Subscription · Inbox · Events · Timer          │  connection
            ├──────────────────────────────────────────────────────────────┤
            │ Transport: TCP · TLS · WebSocket     Auth: token/user/nkey/jwt│  bytes
            ├──────────────────────────────────────────────────────────────┤
            │ Protocol: Parser · Encoder · Headers · Msg · Subject         │  pure, no I/O
            └──────────────────────────────────────────────────────────────┘
```

Each layer depends only on the ones below it. The protocol layer has no I/O and no threads. The
JetStream layer and everything above it are written against `Tether::Client`'s public methods; the
two exceptions are internal seams, documented as such: `Client#resolver` (5.6), which lets a
library component receive messages on the reader thread without a worker, and `Client#listen`
(5.9), which notifies a library component of connection state changes without going through the
user's event thread.

### 4.2 Source tree


```
lib/tether.rb                            Tether.connect and the public aliases
lib/tether/version.rb
lib/tether/errors.rb                     the hierarchy and the -ERR table
lib/tether/deadline.rb                   the deadline every blocking call carries
lib/tether/client.rb                     the public connection object
lib/tether/options.rb                    validated connect options
lib/tether/server_info.rb                the INFO document as a value object
lib/tether/stats.rb                      the Stats snapshot and the live counters

lib/tether/protocol/parser.rb            bytes to frames, no I/O
lib/tether/protocol/encoder.rb           frames to bytes
lib/tether/protocol/headers.rb
lib/tether/protocol/msg.rb
lib/tether/protocol/dispatcher.rb        what the reader thread does with each frame
lib/tether/protocol/registry.rb          the sid map
lib/tether/protocol/subject.rb           validation

lib/tether/connection/transport.rb       TCP, TLS upgrade, and the base interface
lib/tether/connection/tls.rb             building the SSL context
lib/tether/connection/websocket.rb       RFC 6455 framing as a transport
lib/tether/connection/writer.rb          buffer, flusher, reconnect buffer
lib/tether/connection/server_pool.rb     seeds, discovered servers, rotation
lib/tether/connection/lifecycle.rb       connect, reconnect, drain, close, fork reset
lib/tether/connection/handshake.rb       INFO, TLS, CONNECT, PING, PONG
lib/tether/connection/keepalive.rb       client PING timer
lib/tether/connection/timer.rb           one scheduler thread for all timeouts
lib/tether/connection/events.rb          handler registry and the event thread
lib/tether/connection/subscription.rb    Subscription and Resolver
lib/tether/connection/inbox.rb           request mux
lib/tether/connection/nuid.rb

lib/tether/auth.rb                       credential sources, precedence
lib/tether/auth/nkey.rb                  seed decoding and Ed25519 signing via OpenSSL
lib/tether/auth/credentials.rb           .creds parsing

lib/tether/jetstream.rb                  the context: streams, publish, bucket entry points
lib/tether/jetstream/api.rb              subjects, request and response plumbing, paging
lib/tether/jetstream/errors.rb           APIError and the err_code table
lib/tether/jetstream/schema.rb           declarative field tables, JSON conversion
lib/tether/jetstream/stream_config.rb    StreamConfig, StreamInfo, AccountInfo and their parts
lib/tether/jetstream/consumer_config.rb  ConsumerConfig, ConsumerInfo
lib/tether/jetstream/stream.rb
lib/tether/jetstream/stored_message.rb   a message read back outside of a consumer
lib/tether/jetstream/direct_get.rb       single, batched and multi-last Direct Get
lib/tether/jetstream/backup.rb           snapshot and restore
lib/tether/jetstream/advisory.rb
lib/tether/jetstream/pub_ack.rb          PubAck and the publish headers
lib/tether/jetstream/schedule.rb         the schedule: option of publish
lib/tether/jetstream/publisher.rb        async publish: the ack collector and its window
lib/tether/jetstream/publish_future.rb
lib/tether/jetstream/publish_batch.rb    atomic batches
lib/tether/jetstream/fast_batch.rb       fast-ingest batches
lib/tether/jetstream/counter.rb
lib/tether/jetstream/consumers.rb        consumer management on the context and on a Stream
lib/tether/jetstream/consumer.rb
lib/tether/jetstream/message.rb          ack methods and metadata
lib/tether/jetstream/fetch.rb            one pull request
lib/tether/jetstream/batch.rb            the result of a fetch
lib/tether/jetstream/pull_status.rb      the status classification table
lib/tether/jetstream/pull.rb             the pull engine behind consume and messages; pull/buffer.rb
lib/tether/jetstream/ordered.rb          gap detection and recreation; ordered/pull.rb

lib/tether/key_value.rb                  and key_value/{config,entry,watcher,status,naming,errors}.rb
lib/tether/object_store.rb               and object_store/{info,reader,writer,watcher,status,errors}.rb
lib/tether/service.rb                    and service/{registry,endpoint,group,request,response,info,stats,rules}.rb

sig/                                     RBS for every file above
```

`Tether::Msg`, `Tether::Headers` and `Tether::Subscription` are public aliases for the classes in
`Protocol` and `Connection`, so user code never spells an internal namespace.

---

## 5. The connection

### 5.1 States

```
                 connect                      handshake ok
   :disconnected ────────▶ :connecting ───────────────────────▶ :connected ──── drain ───▶ :draining
          ▲                     │                                 │      ▲                     │
          │ no server reachable │ no server reachable,            │      │ handshake ok        │
          └─────────────────────┤ retry_on_failed_connect         │      │                     │
                                ▼                                 ▼      │                     │
                           :reconnecting ◀──── socket lost, reconnect allowed                  │
                                │                                                              │
                                │ pool exhausted, same auth error twice, or close              ▼
                                └────────────────────────────────────────────────────────▶ :closed
```

- `:disconnected` is the state of a client that has never connected, or whose `connect` failed.
  `connect` may be called again from it.
- `:closed` is terminal. A closed client is never reopened; build a new one.
- A socket lost while `:connected` with `reconnect: false`, or lost while `:draining`, goes
  straight to `:closed`.

`status` returns the symbol, and `connected?`, `reconnecting?`, `draining?`, `closed?` are the
predicates. Transitions are made in exactly one place, `Connection::Lifecycle`, under one lock, and
each transition emits an event (5.9). Nothing else assigns the status.

What each call does in each state:

| Call | `:connected` | `:reconnecting` | `:draining` | `:closed` |
|---|---|---|---|---|
| `publish` | buffered, written | buffered up to the reconnect buffer, then `ReconnectBufferFullError` | allowed until subscriptions are drained | `ConnectionClosedError` |
| `subscribe`, `unsubscribe` | registered and written | registered; replayed on reconnect | `ConnectionDrainingError` | `ConnectionClosedError` |
| `request` | sent | buffered like a publish; the deadline keeps running | allowed | `ConnectionClosedError` |
| `flush`, `rtt` | round trip | `DisconnectedError` at once | round trip | `ConnectionClosedError` |
| `drain` | drains | closes without draining, raises `DisconnectedError` | waits for the drain in progress | no-op |
| `force_reconnect` | drops the socket and reconnects | cuts the current backoff short | `ConnectionDrainingError` | `ConnectionClosedError` |

`force_reconnect` on a client built with `reconnect: false` raises `ConfigurationError`.

Before the first `INFO` has been received (possible with `retry_on_failed_connect`), the server's
`max_payload` and header support are unknown. Publishes are buffered without those checks. One that
turns out to be too large is refused by the server with a `Maximum Payload Violation` `-ERR`, which
arrives as an event, and the server closes the connection, which the client recovers from by
reconnecting.

### 5.2 Handshake

`Connection::Handshake` runs on the connecting thread with one deadline, `connect_timeout`,
covering DNS, TCP connect, TLS and the protocol exchange:

1. Resolve the host and connect with `Socket.tcp`, which tries each address in turn, each with the
   time that remains.
2. If `tls: { first: true }`, perform the TLS handshake now.
3. Read `INFO`. Anything else is a `ProtocolError`.
4. Decide on TLS: upgrade if the client asked for it or the server requires it. If the client
   requires TLS and the server offers none, raise `TLSError`. An `INFO` read before the upgrade is
   unauthenticated, so its `connect_urls` are not added to the pool. A WebSocket transport skips
   this step entirely: its TLS, if any, was established before the HTTP upgrade, and for it
   `tls_required` in `INFO` only describes the listener.
5. Build `CONNECT` from the options and the `INFO` (nonce, `headers`, `proto`), always sending
   `verbose: false`, `pedantic: false` and `protocol: 1` explicitly, because the server treats an
   absent `verbose` or `pedantic` as true.
6. Write `CONNECT` and `PING` in one write. Read until `PONG` (success) or `-ERR` (raise the mapped
   error). The PONG is the proof that authentication succeeded.
7. Hand the parser, with any bytes read past the PONG, to the reader thread.

The post-connect `INFO` with `connect_info: true` arrives on the reader thread and fills in the
account name and the per-user `max_payload`.

### 5.3 Server pool and reconnect

`Connection::ServerPool` holds the seed servers and the ones discovered through `connect_urls`.
Seeds are shuffled once unless `randomize: false`. Discovered servers are added on every `INFO`
received over an authenticated connection and removed when the cluster stops advertising them;
seeds are never removed by discovery. A discovered server inherits the scheme and credentials of
the server that advertised it and, when it is a bare IP, the original hostname for TLS
verification.

Reconnect is one loop on one transient thread, started when the socket is lost while `:connected`:

1. Enter `:reconnecting`. Switch the writer to its reconnect buffer, close the socket, and join
   the reader and flusher. Fail every in-flight request and flush with `DisconnectedError`. Emit
   `:disconnect` with the cause.
2. Take the next server from the pool. A server that has failed `max_attempts` times in a row is
   dropped; when the pool is empty, close with `NoServersError`.
3. After each full pass over the pool, wait `wait` seconds plus a random jitter (`jitter`, or
   `tls_jitter` for a `tls://` server). `reconnect: { delay: ->(attempt) { ... } }` replaces the
   wait entirely. The wait is a timed queue pop, so `close` and `force_reconnect` interrupt it.
4. Run the handshake. On failure emit `:reconnect_error`. If the failure is an authentication
   error (`AuthorizationError`, `AuthenticationExpiredError` or `AuthenticationRevokedError`) and
   this server answered its previous attempt with the same error, close: the credentials will not
   get better by retrying. The count is kept per server and reset by a successful connect.
5. On success, in one critical section under the writer's buffer lock: snapshot the registry;
   put a `SUB` for every live subscription, followed by `UNSUB <sid> <remaining>` for those with an
   auto-unsubscribe limit, in front of the reconnect buffer; switch the writer back to normal mode.
   Then write, start the reader and flusher, enter `:connected`, notify internal listeners, and
   emit `:reconnect`. If the write fails, go back to step 2. There is no state from which the loop
   does not continue or close.

Subscriptions keep their sids across reconnects. A draining subscription is not replayed.

`subscribe` and `unsubscribe` never look at the connection status. They always update the registry
and then hand their frame to the writer, and it is the writer that drops `SUB` and `UNSUB` frames
while in reconnect mode, since step 5 replays the registry. Because the mode switch and the
snapshot share one critical section, a subscription made at any moment is either in the snapshot or
written after it in normal mode. The worst case is a `SUB` sent twice for one sid, which the server
ignores.

Options that shape the pool: `randomize: false` keeps the given order;
`ignore_discovered_servers: true` never adds to it; `reconnect: { server: ->(servers, info) { } }`
picks the next server and returns it; `nc.servers = [...]` replaces the pool at run time;
`reconnect: { max_attempts: -1 }` never gives up on a server; and
`reconnect: { abort_on_auth_error: false }` keeps retrying through repeated authentication errors.

**Lame duck mode.** An `INFO` with `ldm: true` emits `:lame_duck`. The server will close the
connection within its grace period, at which point the normal reconnect runs, already holding the
updated server list. With `reconnect: { on_lame_duck: true }` the client reconnects immediately
instead of waiting to be dropped.

### 5.4 Write path

`Connection::Writer` owns the outbound buffer. It has two locks with different jobs, which is what
keeps a slow socket from stalling the rest of the client:

- The **buffer lock** guards the buffer, the writer's mode and the FIFO of PONG waiters. It is held
  only to append bytes or to swap the buffer out. It is never held across I/O.
- The **I/O lock** serializes socket writes. To write, a thread takes the I/O lock, swaps the
  buffer for an empty one under the buffer lock, releases the buffer lock, and writes the detached
  bytes. Taking the I/O lock before the swap keeps bytes in order.

`write(frame)` appends one complete binary frame under the buffer lock and kicks the flusher, a
thread that writes when kicked; the kick is dropped if one is already pending. When the buffer
passes 128 KB the appending thread writes it itself if the I/O lock is free, which is the common
case and saves a thread handoff. If the I/O lock is busy the appending thread does not wait, until
the buffer reaches a hard cap of 8 MiB; past that, publishers wait for the write in progress,
bounded by the write deadline. That is the only back-pressure on a connected client, and it applies
only when the socket has stopped accepting data.

`write(frame, flush: true)`, used for request publishes, writes before returning when the I/O lock
is free and otherwise kicks. The reader thread and the timer thread never write: a `PONG` or a
keepalive `PING` is an append and a kick, so neither can block on the socket.

Socket writes use `write_nonblock` in a loop with `IO#wait_writable(remaining)` against one
deadline per buffer (`write_timeout`, 10 seconds). `IO#timeout=` is not used, because it is a
single value per socket shared by every thread, and the reader must wait without a limit. A
write that fails or times out part way leaves a truncated frame on the wire, so the writer closes
the socket, which sends the connection into reconnect.

While `:reconnecting` the writer is in reconnect mode: frames accumulate up to
`reconnect: { buffer_size: }` (8 MiB by default) and a publish beyond that raises
`ReconnectBufferFullError` on the publishing thread. `buffer_size: 0` disables buffering, so every
publish and request made during an outage raises `DisconnectedError`. `SUB` and `UNSUB` frames are
dropped in this mode (5.3).

Anything detached for writing when the socket dies is discarded, not replayed: the client cannot
know how much of it the server processed, and replaying could deliver duplicates or half a frame.

Over WebSocket with compression, one WebSocket message must inflate to no more than the server's
`max_payload`, and a transport is opened before `INFO` says what that is. The WebSocket transport
therefore cuts every buffer into messages of at most 32 KiB (`WebSocket::MAX_MESSAGE_SIZE`), which
is below any plausible `max_payload` and equals the deflate window. It cuts wherever the limit
falls; NATS frame boundaries carry no meaning to the server there.

The measured reasons for the buffer size and the kick are recorded in the code.

### 5.5 Read path

One reader thread per live socket loops over `transport.read` and `parser.feed`. The parser calls
the dispatcher, which does the minimum per frame:

- `MSG`/`HMSG`: look up the sid, build a `Msg` holding the unparsed header bytes, hand it to the
  registry entry. Headers are parsed lazily on whichever thread first asks for them.
- `PING`: append `PONG` to the write buffer and kick the flusher.
- `PONG`: pop the oldest entry of the PONG FIFO and, if it is a waiter rather than a keepalive
  placeholder, release it. Reset the outstanding-ping count.
- `INFO`: replace the server info, update the pool, emit `:lame_duck` or `:servers_discovered`.
- `-ERR`: map to a `ServerError` subclass and emit `:error`. A permissions violation that names a
  subscription is also recorded on that subscription (5.6). The reader does not otherwise act on the
  text; if the server closes the socket, the read returns EOF and reconnect starts.

A parse failure marks the parser broken and ends the loop the same way as EOF.

### 5.6 Subscriptions and resolvers

A registry entry is anything that responds to `deliver(msg)` and never blocks. There are two kinds.

**`Connection::Subscription`** is what `subscribe` returns. It has a bounded queue and, when given
a block, a worker thread that calls the block. Limits are `max_pending:` messages (65,536) and
`max_pending_bytes:` (64 MiB), readable and writable on the subscription later
(`sub.max_pending = 1_000`); lowering a limit keeps what already waits. When either is exceeded the message is dropped, `dropped` is
incremented, and a `SlowConsumerError` event is emitted once per episode, not once per message.
An episode ends only when the queue has drained below half of its limit, so a subscription that
stays near full does not flap. A subscription whose `SUB` the server refused with a permissions
violation gets that error as an event and from its next `next` call, and stays registered so a
later reconnect with new permissions can succeed.

**A resolver** is a registry entry with no queue and no worker, used by library components that must
not be starved by user handlers: the request mux, `request_many`, the async publish ack collector,
the single-shot `fetch`, and the pull engine's consume loop. Its `deliver` runs on the reader thread
and only does a hash lookup and a queue push or counter update.
`Client#resolver(subject) { |msg| ... }` creates one; it is internal API, documented as such, and
the block must obey the reader-thread rule. Appending a frame to the write buffer is allowed from a resolver, since it
cannot block (5.4).

This split is what keeps reply latency independent of user code. A request's reply goes reader
thread → waiter queue → requester, with one thread handoff.

### 5.7 Keepalive

`Connection::Keepalive` sends a `PING` every `ping_interval` (120 seconds). If `max_pings_out` (2)
PINGs are outstanding when the timer fires, the connection is stale: the timer reports the socket as
lost to `Lifecycle`, which records the transition and hands the rest to the reconnect thread (12.2).
Any PONG resets the count. Each keepalive `PING` puts a placeholder in the PONG FIFO in the same
critical section as the append, so its PONG is never mistaken for the answer to a caller's `flush`.
The timer is an entry in the connection's `Timer` (5.10), not a thread of its own, and its callback
only appends and kicks.

### 5.8 Flush, drain, close

- **`flush(timeout: 10)`** registers a waiter in the PONG FIFO and appends a `PING` in one critical
  section under the buffer lock, writes, then pops the waiter. The server answers a PING only
  after processing everything sent before it.
- **`Subscription#drain(timeout:)`** sends `UNSUB`, round trips a flush so no more messages are
  in flight, lets the worker empty the queue, then closes the subscription. It blocks until done
  and raises `TimeoutError` if the deadline passes, after closing anyway. The default timeout is
  the client's `drain_timeout` (30 seconds).
- **`Client#drain(timeout:)`** enters `:draining`, sends `UNSUB` for every subscription, flushes,
  waits for each subscription's worker to empty its queue, then flushes pending publishes and
  closes. New subscriptions are refused while draining; publishing stays allowed until the
  subscriptions are done, so handlers can still reply. A second `drain` while one is in progress
  waits for the first to finish, bounded by its own timeout, and returns. A `drain` without a live
  connection closes the client and raises `DisconnectedError`.
- **`close`** is immediate: it writes what is in the buffer without waiting for the server, shuts
  the socket down in both directions and closes it, joins the reader and flusher, releases every
  waiter with `ConnectionClosedError`, closes subscriptions, and emits `:close`. It is idempotent.
  The shutdown matters after a fork: it ends the TCP connection even when a child process still
  holds a copy of the descriptor.

**Calling these from a handler.** A thread never waits for itself. `Subscription#drain` called from
that subscription's own block stops intake, returns at once, and the subscription closes after the
block returns and the queue is empty. `Client#drain` called from any tether thread (a subscription
block, an event handler) starts the work on a helper thread and returns; completion is observable
through `on_close`. `close` runs on the calling thread and skips joining it.

### 5.9 Events

`Connection::Events` holds the handlers. They are registered with a block, several may be
registered for the same event, and each registration returns a handle whose `cancel` removes it:

```ruby
nc.on_error            { |error| ... }      # async errors: -ERR, slow consumer, handler exceptions
nc.on_connect          { |server| ... }     # the first successful connect
nc.on_disconnect       { |error| ... }      # error is nil for a requested disconnect
nc.on_reconnect        { |server| ... }
nc.on_reconnect_error  { |error| ... }      # each failed attempt
nc.on_close            { ... }
nc.on_lame_duck        { |server| ... }
nc.on_servers_discovered { |servers| ... }
nc.on_status           { |status| ... }     # every state transition

registration = nc.on_error { |error| ... }
registration.cancel
```

Events are queued and delivered one at a time, in order, on a dedicated event thread, outside
every internal lock. The thread starts with the first registration and, at close, exits only after
delivering `:close`. A handler may call back into the client freely. A handler that raises is
caught; the exception is written to the client's `logger` and delivery continues. The queue is
bounded at 10,000 entries (`Events::QUEUE_LIMIT`) and an event that finds it full is dropped, so a
handler that raises on every message, or a publisher hitting a permissions violation in a loop,
cannot grow the queue without bound. An event nobody registered for is not queued at all; an error
without an `on_error` handler goes to the `logger`. `nc.last_error` returns the most recent
asynchronous error, whether or not it was delivered.

Handlers that belong to something narrower than the connection do not run on the event thread: a
consume loop's `on_error` and a watcher's `on_ready` run on that loop's worker, and a
subscription's `on_close` and a service's `on_stop` run on the thread that ends them. That keeps
their ordering relative to the messages they concern.

**Internal listeners.** A consume loop must issue a fresh pull after a reconnect, and an ordered
consumer must recreate itself. If they used `on_reconnect`, a slow user handler ahead of them in
the queue would delay recovery, and every loop would leave a handler behind. They use
`Client#listen { |event, detail| ... }` instead, an internal seam that returns a cancellable handle
and is invoked on the thread making the transition (`:connect`, `:disconnect`, `:reconnect`,
`:reconnect_error`, `:close`). A listener must not block: it posts to its
owner's control queue and returns.

### 5.10 Timer

Heartbeat monitors, the ping interval, async publish ack timeouts and ordered consumer backoff all
need "call this in N seconds". `Connection::Timer` is one thread per connection, started with the
first entry, that keeps the entries by due time and sleeps in `ConditionVariable#wait` until the
earliest. A callback may append a frame to the write buffer or post to a queue, and nothing else;
neither can block. Anything more is done by the worker that owns the queue. This avoids both a
thread per timeout and a dependency on an external scheduler.

### 5.11 Transports, TLS and authentication

`Connection::Transport` is the interface: `open(deadline)`, `read(deadline)`,
`write(data, deadline)`, `close`, `closed?`, `tls?`, `upgrade_to_tls(context, hostname, deadline)`
and `abandon`, which drops an inherited descriptor after a fork (5.12). There are two
implementations.

- **TCP** with `TCP_NODELAY`, non-blocking reads and writes, and `IO#wait_readable` and
  `IO#wait_writable` against the deadline.
- **WebSocket** for `ws://` and `wss://` (default ports 80 and 443): TLS first when the scheme is
  `wss`, then the HTTP upgrade, masked binary client frames, reassembly of server frames into the
  byte stream the parser expects, answering ping frames, and sending and honoring close frames.
  Masking is dropped only when the caller asks for it with
  `headers: { "Nats-No-Masking" => "true" }` and the server answers with the same header. It is
  implemented from RFC 6455 with the standard library only (`Zlib` for compression, `pack("m0")` for
  base64). TCP and WebSocket URLs cannot be mixed in one pool. Options live under `websocket:`:
  `compression: true` requests `permessage-deflate` and requires the server to agree to no context
  takeover in both directions; `path:` overrides the URL path, for servers behind a proxy;
  `headers:` adds headers to the upgrade request, which is also how cookie-based authentication is
  supplied. NATS-level TLS negotiation does not apply: TLS on a WebSocket connection is chosen by
  the `wss` scheme alone.

**TLS** options live under `tls:`:

```ruby
Tether.connect("tls://nats.example.com",
  tls: { ca_file: "ca.pem", ca_path: nil, cert_file: "client.pem", key_file: "client.key",
         verify: true, first: false, min_version: :TLS1_2, hostname: nil, context: nil })
```

`tls: true` is the same as `tls: {}`: TLS with the defaults. `tls: nil` or `false` means TLS only
if a URL scheme or the server demands it. A `tls://` or `wss://` scheme, or a `tls:` hash, makes
TLS required. Verification is on by
default and checks the hostname; `hostname:` overrides the name sent as SNI and verified, and
`context:` accepts a ready `OpenSSL::SSL::SSLContext` for anything unusual. `min_version:` is left
to OpenSSL's default unless given. The TLS handshake runs with the connect deadline.

**Authentication** sources, at most one of which may be given beyond URL userinfo; a second one
is an `ArgumentError` when the client is built:

| Option | Mechanism |
|---|---|
| `nats://user:pass@host`, `user:`, `password:` | user and password |
| `nats://token@host`, `token:` | token |
| `nkey_seed:` or `nkey_seed_file:` | NKEY: signs the server nonce |
| `credentials:` (the text) or `credentials_file:` | user JWT plus NKEY seed, `.creds` format |
| `jwt:` with `nkey_seed:` or `nkey_seed_file:` | the same, supplied separately |
| `jwt:` alone | a bearer JWT, which the server accepts without a signature |
| `auth: ->(nonce) { { jwt:, sig: } }` | a callable returning CONNECT fields, for anything else |

A literal value and a file are separate options, so nothing guesses whether a string is a path.
Each of `token:`, `user:`, `password:`, `jwt:` and `nkey_seed:` also accepts a callable, invoked on
every connect. Files are re-read on each connect too, so rotated credentials are picked up by a
reconnect.
Userinfo in a server's URL takes precedence for that server. A credential that cannot be read or
decoded raises `CredentialsError`. NKEY support needs no gem: a seed is
base32 decoded and CRC checked, and the Ed25519 signature is produced by OpenSSL. The signature
covers the nonce's text exactly as it appears in `INFO` and is sent base64url encoded without
padding.

### 5.12 Fork safety

A forked child inherits the socket's file descriptor but none of the threads. Tether keeps a
generation on the client, made of the process id recorded at connect. Every public object that
belongs to a connection (the client, subscriptions, JetStream handles, futures, watchers) compares
it with `Process.pid` on entry to its public methods. No hook is installed on `Process`.

In the child, the first such call resets the client under a lock of its own, so two child threads
cannot both do it:

1. Close the child's copy of the raw descriptor without writing to it, without `shutdown`, and
   without a TLS `close_notify`, so the parent's connection is untouched.
2. Discard the inherited write buffer, waiters and subscriptions. Inherited `Subscription`,
   consume and watcher objects report closed from then on, and inherited futures fail with
   `ForkedError`. No user callbacks fire for the inherited connection.
3. With reconnect enabled, connect afresh as if `connect` had just been called. The call that
   triggered the reset therefore can take up to `connect_timeout` and can raise a connect error.
   With `reconnect: false` the child's client is closed and the call raises
   `ConnectionClosedError`.

The child gets a working client with no subscriptions, which is the only behavior that cannot
duplicate message processing. Code that wants subscriptions in a forked worker creates them after
the fork.

Two things are left to the application, and the README says so. A child that never touches the
client keeps its copy of the descriptor until it exits; that costs nothing while the parent is
alive, and the parent's `close` ends the TCP connection regardless because it shuts the socket
down (5.8). And a preforking server that wants the reset to happen at a known moment, not inside
the first request, calls `nc.after_fork` from its worker-boot hook, which performs the steps above
eagerly.

### 5.13 Statistics

`nc.stats` returns a frozen snapshot: `messages_in`, `messages_out`, `bytes_in`, `bytes_out`,
`reconnects`. Byte counts are of everything read from and written to the socket, as ADR-40
specifies, which is also the cheapest thing to count. `nc.rtt` measures a flush round trip.

---

## 6. Core API

### 6.1 Connecting

```ruby
require "tether"

nc = Tether.connect("nats://a.example.com,nats://b.example.com",
  name: "billing",
  connect_timeout: 2,
  reconnect: { max_attempts: 60, wait: 2, jitter: 0.1, tls_jitter: 1, buffer_size: 8 * 1024 * 1024 },
  ping_interval: 120, max_pings_out: 2,
  echo: true, inbox_prefix: "_INBOX",
  token: nil, user: nil, password: nil, nkey_seed: nil, credentials: nil,
  tls: nil, logger: nil)

Tether.connect { |nc| nc.publish("greet", "hello") }   # yields, closes, returns the block's value
```

- The first argument is one URL, a comma-separated list, or an array. With none, `NATS_URL` is
  read, then `nats://127.0.0.1:4222`. This is the only environment variable tether reads, and an
  explicit argument always wins.
- `Tether.connect` returns a connected client or raises the last failure after trying every server
  once. `reconnect: false` disables reconnect.
  `retry_on_failed_connect: true` returns a client in `:reconnecting` when no server is reachable
  yet, which suits a process that starts before its broker.
- Options are validated once into a frozen `Tether::Options`. An unknown key raises
  `ArgumentError` naming it.
- `Tether::Client.new(...)` builds without connecting, for callers that register handlers first:

  ```ruby
  nc = Tether::Client.new(url, name: "worker")
  nc.on_disconnect { |error| log.warn("nats down: #{error}") }
  nc.connect
  ```

Every option, with its default:

| Option | Default | Meaning |
|---|---|---|
| `name:` | `nil` | connection name shown in server monitoring |
| `connect_timeout:` | `2` | seconds for DNS, TCP, TLS and the handshake of one attempt |
| `write_timeout:` | `10` | seconds one buffer may take to reach the socket |
| `reconnect:` | `true` | `false`, or a hash: `max_attempts: 60` (per server, `-1` unlimited), `wait: 2`, `jitter: 0.1`, `tls_jitter: 1`, `delay: nil` (callable), `server: nil` (callable), `buffer_size: 8 MiB`, `on_lame_duck: false`, `abort_on_auth_error: true` |
| `retry_on_failed_connect:` | `false` | return a `:reconnecting` client when no server is reachable |
| `randomize:` | `true` | shuffle the seed servers |
| `ignore_discovered_servers:` | `false` | never add `connect_urls` to the pool |
| `ping_interval:` | `120` | seconds between keepalive PINGs; `nil` disables |
| `max_pings_out:` | `2` | unanswered PINGs that mark the connection stale |
| `echo:` | `true` | receive your own publishes on your own subscriptions |
| `inbox_prefix:` | `"_INBOX"` | prefix for reply subjects |
| `max_pending:`, `max_pending_bytes:` | `65_536`, `64 MiB` | default per-subscription limits |
| `worker_threads:` | `16` | most threads running subscription blocks at once |
| `drain_timeout:` | `30` | default for `drain` |
| `validate_subjects:` | `true` | check publish subjects for wildcards and empty tokens |
| `token:`, `user:`, `password:`, `jwt:`, `nkey_seed:`, `nkey_seed_file:`, `credentials:`, `credentials_file:`, `auth:` | `nil` | see 5.11 |
| `tls:` | `nil` | see 5.11 |
| `websocket:` | `nil` | see 5.11 |
| `logger:` | `nil` | a `Logger` for internal diagnostics; nothing is written without one |

### 6.2 Publishing

```ruby
nc.publish("orders.created", payload)
nc.publish("orders.created", payload, headers: { "Trace-Id" => "abc" }, reply_to: inbox)
nc.publish("orders.created", payload, flush: true)
```

`publish` returns `self`. The payload is a `String`, sent as bytes whatever its encoding, or `nil`
for an empty message. `headers:` takes a `Hash` (values may be arrays for repeated keys) or a
`Tether::Headers`. The subject and reply are always checked before anything is written: non-empty,
no whitespace, no control characters, since any of those would corrupt the protocol stream. With
`validate_subjects: true`, the default, a publish subject must also be a literal subject in the
server's own sense: no empty token and no token that is exactly `*` or `>`. The server accepts
both silently. A subject with an empty token matches nothing, and one with a wildcard token is not
expanded; it reaches only subscriptions that have a wildcard at that position. Either is almost
always a bug in the caller, and this is the same check the server applies in pedantic mode.

Raises `MaxPayloadError` when headers plus payload exceed the server's limit, which is the quantity
the server compares,
`HeadersNotSupportedError` when the server lacks header support, `ReconnectBufferFullError` or
`DisconnectedError` during an outage as described in 5.4, and `ConnectionClosedError` after close.

### 6.3 Subscribing

```ruby
sub = nc.subscribe("orders.*") { |msg| handle(msg) }               # runs on the client's worker pool
sub = nc.subscribe("jobs", queue: "workers") { |msg| work(msg) }   # queue group
sub = nc.subscribe("events")                                       # no block: pull messages yourself

msg = sub.next(timeout: 5)               # raises Tether::TimeoutError; without timeout: waits indefinitely
sub.each { |msg| ... }                   # blocks until the subscription ends
sub.each(idle_timeout: 1) { |msg| ... }  # returns when nothing has arrived for a second
```

Options: `queue:`, `max_pending:`, `max_pending_bytes:`, `max:` (auto-unsubscribe after that many
messages), `concurrency:` (workers that may run this subscription's block at once, default 1, which preserves
order), `dedicated:` (run the block on threads of its own instead of the client's pool).

`Subscription` methods: `unsubscribe`, `unsubscribe(after: n)`, `drain(timeout:)`, `subject`,
`queue`, `sid`, `pending`, `pending_bytes`, `delivered`, `dropped`, `closed?`, `draining?`,
`on_close { }`. `closed?` becomes true when the subscription ends for any reason, including the
connection closing. `timeout:` on a single-message call raises when it expires; `idle_timeout:` on
an iteration ends it. `next` on a subscription that has ended and has nothing left raises
`SubscriptionClosedError`. A subscription has `each` but is not `Enumerable`, because it has no
end.

`Msg` has readers only: `subject`, `data`, `reply_to`, `headers` (parsed lazily, `nil` when the
message has none), `status`, `description`, `no_responders?`, `empty?`, `subscription`,
`respond(payload = nil, headers: nil)`, `size`. `respond` never copies the request's headers into
the reply, and raises `Tether::Error` when the message has no reply subject.

A block that raises does not end the subscription. The exception becomes a `HandlerError` event
carrying the message and the cause, and the next message is delivered.

### 6.4 Request and reply

```ruby
reply = nc.request("time.now", timeout: 2)                 # => Msg, or raises
reply = nc.request("users.get", id, headers: { "X" => "1" }, timeout: Tether::Deadline.after(2))

replies = nc.request_many("search", query, max: 10, stall: 0.25, timeout: 2)   # => Array<Msg>
nc.request_many("search", query, timeout: 2) { |msg| ... }                     # yields as they arrive

inbox = nc.new_inbox                                        # "_INBOX.<22 chars>"
```

`request` raises `NoRespondersError` immediately when nobody subscribes to the subject,
`TimeoutError` when the deadline passes, and `DisconnectedError` when the connection is lost while
waiting. The default timeout of both calls is 2 seconds. `request_many` publishes when called and
collects replies until `max` have arrived, no reply has arrived for `stall` seconds, the deadline
passes, or the `sentinel:` callable returns true for a reply (which is not included), following the
request-many conventions in ADR-47. `stall:` applies only between replies: the first reply has the
whole deadline, because a responder's time to first answer says nothing about the gap between
answers. It returns the replies as an array, or yields each to a block as it arrives and returns
how many there were. Running out of time is the normal way for it to end, so it
does not raise `TimeoutError`; a no-responders status ends it and raises `NoRespondersError` only
if nothing was received. It uses a dedicated resolver rather than the mux, so many replies do not
flow through a single-slot waiter.

All single requests share one resolver on `<inbox_prefix>.<nuid>.*`, created on first use. A
connection lost while requests are waiting releases every one of them with `DisconnectedError`.

### 6.5 Introspection and control

```ruby
nc.status            # :connected
nc.server            # the Server connected to, or the last one; server.url carries no credentials
nc.server_info       # Tether::ServerInfo: id, name, version, host, port, cluster, max_payload,
                     #   headers?, jetstream?, api_level, domain, client_id, client_ip, account, ...
nc.local_address ; nc.remote_address   # Addrinfo of each end of the socket, nil without one
nc.tls_state         # Connection::TLS::State: version, cipher, peer_certificate(_chain); nil unencrypted
nc.servers           # the pool: [#<Server url: discovered?: attempts:>]; nc.servers = [...] replaces it
nc.user_info         # a Hash of who the server says this connection is: account, user, permissions, expiry
nc.max_payload
nc.stats             # Tether::Stats
nc.subscription_count # subscriptions held on the server, internal ones included
nc.buffered_bytes    # bytes published but not yet written, including the reconnect buffer
nc.rtt               # seconds
nc.last_error
nc.flush(timeout: 10)
nc.force_reconnect
nc.drain(timeout: 30)
nc.close
nc.after_fork        # replace an inherited connection now (5.12)
```

---

## 7. JetStream

### 7.1 Object model

```ruby
js = nc.jetstream                         # or nc.jetstream(domain: "hub", api_prefix: nil, timeout: 5)
```

```
JetStream ──▶ Stream ──▶ Consumer ──▶ Message
    │            │           ├── fetch / next        one request, a bounded batch
    │            │           ├── consume { }         continuous, callback
    │            │           └── messages            continuous, iterator
    │            └── ordered_consumer
    ├── publish / publish_async / batch / fast_batch / counter
    ├── key_value / object_store
    └── account_info
```

A `JetStream` is a cheap handle: an API prefix, a default timeout, and a reference to the
connection. `Stream` and `Consumer` are handles to server-side objects that carry the last known
info. None of them owns a thread until consumption starts.

`domain:` selects `$JS.<domain>.API`; `api_prefix:` selects an arbitrary prefix for cross-account
imports. Every API call takes `timeout:` and defaults to the context's. The context also carries
the async publish settings (7.4): `publish_async_max_pending:`, `publish_async_timeout:`.

**API levels.** The server advertises its JetStream API level in `INFO` (`nc.server_info.api_level`)
and honors a `Nats-Required-Api-Level` header on API requests, pull requests, direct gets and batch
messages, refusing those it is too old for. Each schema field and each operation in tether records
the level that introduced it. When a request uses anything above level 0, tether sends the header
with the highest level it used, so an older server answers with a clear `RequiredApiLevelError`
where it would otherwise ignore the unknown field and behave differently from what was asked. An API
request that gets no answer at all, because JetStream is not enabled or has no leader, raises
`ServiceUnavailableError`, which is `retryable?`. `nats.go` has no such check; this is one place
tether does more.

### 7.2 Configuration and info objects

The server defines about forty stream fields and thirty consumer fields, and adds more with each
release. Hand-writing a class per struct is how a client falls behind, so tether declares each
struct once as a table of fields and derives everything else from it:

```ruby
class StreamConfig < Schema
  field :name,        :string
  field :subjects,    [:string]
  field :retention,   :enum,     values: { limits: "limits", interest: "interest", work_queue: "workqueue" }
  field :max_age,     :duration                       # seconds in Ruby, nanoseconds on the wire
  field :storage,     :enum,     values: { file: "file", memory: "memory" }
  field :replicas,    :integer,  key: "num_replicas"
  field :sources,     [StreamSource]
  ...
end
```

From the table a `Schema` class gets: a keyword initializer that rejects unknown keys; readers;
`to_h` and `to_json` that emit only fields that were set, in wire form, so older servers never see
a field they would reject; `from_h` that converts durations to seconds, timestamps to `Time`, enum
strings to symbols and nested structs to their classes; `coerce`, which accepts an instance or a
hash of keywords; `with(**changes)` returning a modified copy; `==`;
and `raw`, the decoded hash with every key the server sent, including ones tether does not know.
`with` and `to_json` carry unknown keys through, so reading a config from a newer server and
writing it back does not turn features off. The `_nats.*` keys the server adds to `metadata` are
kept out of `==` and are not sent back on update.

Ruby names spell words out where the wire abbreviates them: `max_messages` for `max_msgs`,
`max_message_size`, `max_messages_per_subject`, `allow_message_ttl`, `allow_message_counter`,
`allow_message_schedules`, `replicas` for `num_replicas`, `durable` for `durable_name`.

Methods that take a config accept keywords directly, so the class rarely needs naming:

```ruby
js.create_stream(name: "ORDERS", subjects: ["orders.>"], max_age: 86_400, replicas: 3)
```

### 7.3 Streams

```ruby
stream = js.create_stream(name: "ORDERS", subjects: ["orders.>"])
stream = js.update_stream(name: "ORDERS", subjects: ["orders.>", "returns.>"])
stream = js.create_or_update_stream(name: "ORDERS", subjects: ["orders.>"])
stream = js.stream("ORDERS")                       # raises StreamNotFoundError
stream = js.stream("ORDERS", lookup: false)        # bound by name, no round trip
js.stream?("ORDERS")
js.delete_stream("ORDERS")
js.streams(subject: "orders.*")                    # lazy Enumerator of Stream, paged underneath
js.stream_names(subject: nil)                      # lazy Enumerator of String
js.stream_name_for("orders.created")               # => String or nil
js.account_info                                    # AccountInfo: memory, storage, streams, consumers, limits, api, tiers

stream.name
stream.info(deleted_details: false, subjects: "orders.>")   # fresh StreamInfo; subject paging handled
stream.cached_info
stream.config ; stream.state                        # from the cached info
stream.update(max_age: 3600)                        # reads the current config, merges, writes it back
stream.purge(subject: "orders.eu.>", keep: 10)      # or sequence:; returns the purged count
stream.get(42)                                      # StoredMessage by sequence
stream.get(last_for: "orders.eu.1")
stream.get(next_for: "orders.>", from: 100)
stream.each_message(from: 1, subject: "orders.>", start_time: nil, batch: 100, max_bytes: nil) { |m| }
stream.last_messages(["orders.eu.>", "orders.us.>"], up_to_sequence: nil, up_to_time: nil)
stream.count(subject: "orders.eu.>", from: 1)      # => Integer, counted by the server
stream.delete_message(42, erase: true)
stream.delete
stream.step_down(placement: nil) ; stream.remove_peer(name)
config, state = stream.snapshot(no_consumers: false, chunk_size: nil) { |chunk| io.write(chunk) }
js.restore_stream(config, state) { |writer| writer.write(io.read(64 * 1024)) until io.eof? }
```

`get` uses Direct Get when the stream allows it and the JSON API otherwise; `direct: true` or
`false` forces one. A `StoredMessage` has `subject`, `sequence`, `data`, `headers`, `time`.
Listing methods return lazy enumerators, so `js.streams.first(3)` fetches one page.

`lookup: false` skips the `STREAM.INFO` request, for code that already knows the stream exists and
only purges, deletes or reads. Its `cached_info` holds the name alone until `info` is called, so
`get` reads through the JSON API unless it is given `direct:`, and a missing stream surfaces as
the error of the first call that needs it.

`each_message` is the batched Direct Get: it asks for `batch` messages at a time, reads until the
`204 EOB` status, and uses the `Nats-Num-Pending` and `Nats-UpTo-Sequence` headers of that status
to request the next page against a consistent view. `last_messages` is the multi-subject form
(`multi_last`), returning the last message for each subject that matches, and raises
`TooManyResultsError` when the server answers `413` because the filters match more than its limit
of subjects. `count` is a Direct Get of one message: the server sends it with how many more
match in `Nats-Num-Pending`, so counting costs one small request however many messages there are,
and a `404` is a count of zero. All three need `allow_direct` on the stream and raise
`ServiceUnavailableError` without it, since nothing answers a Direct Get there.

`delete_message` erases by default, overwriting the message's bytes on disk, as the server does
when not told otherwise; `erase: false` only marks it deleted, which is faster.

`snapshot` and `restore_stream` implement the chunked, flow-controlled backup protocols: `snapshot`
yields chunks, acknowledging each so the server sends the next, and returns the stream's config and
state when the server signals the end; `restore_stream` sends what the block writes in chunks,
waiting for each acknowledgement.

**Timeouts of streaming operations.** `each_message`, `snapshot`, `restore_stream`, the listing
enumerators, the batches of 7.4 and Object Store `get` and `put` make an open-ended number of round
trips, so one number of seconds cannot sensibly bound all of them. For these, a numeric `timeout:`
bounds each wait on the server (one page, one chunk, one acknowledgment), and a `Deadline` bounds
the whole operation. Everywhere else a number and a `Deadline` mean the same thing.

### 7.4 Publishing

```ruby
ack = js.publish("orders.created", payload,
  headers: nil,
  msg_id: "order-1",                         # Nats-Msg-Id
  expect: { stream: "ORDERS",                # Nats-Expected-Stream
            last_sequence: 41,               # Nats-Expected-Last-Sequence
            last_subject_sequence: 7,        # Nats-Expected-Last-Subject-Sequence
            subject: "orders.*",             # ...-Subject: which subject that sequence is checked on
            last_msg_id: "order-0" },        # Nats-Expected-Last-Msg-Id
  ttl: 3600,                                 # Nats-TTL, seconds or :never
  rollup: :subject,                          # Nats-Rollup: sub, or :all
  timeout: 5, retries: 2, retry_wait: 0.25)
ack.stream ; ack.sequence ; ack.duplicate? ; ack.domain ; ack.value
```

A synchronous publish is a request. A server error in the ack raises the mapped `APIError`
subclass, for example `WrongLastSequenceError`. No responders means no stream captures the subject
or its leader is momentarily absent, so the publish is sent again up to `retries` more times,
`retry_wait` apart (three attempts in all by default), before raising `NoStreamResponseError`.
`timeout:` covers every attempt together.

**Asynchronous publish** for throughput:

```ruby
future = js.publish_async("orders.created", payload, msg_id: "order-2")
future.value(timeout: 5)        # => PubAck, or raises what the server answered
future.wait(timeout: 5)         # => true/false, never raises
future.done? ; future.error ; future.subject

js.pending_publishes                    # how many acks are outstanding
js.wait_for_publishes(timeout: 10)      # => true when all are resolved, false on timeout
js.on_publish_error { |future| ... }    # a failed future: future.subject, future.error
```

All async acks arrive on one resolver subscription and are matched to futures by reply token. At
most `publish_async_max_pending` (4,000) are in flight; beyond that `publish_async` blocks for up
to `stall: 0.2` seconds and then raises `TooManyStalledMessagesError` without sending. A future
that gets no ack within `publish_async_timeout` (off by default) fails with `TimeoutError`.
`value` and `wait` default to the context's timeout, and a `value` that times out leaves the
future pending. `on_publish_error` holds one handler, which a later call replaces, and runs it on
a worker thread of its own.

An asynchronous publish is never sent twice. When nothing answers it the future fails with
`NoStreamResponseError` instead of retrying as `publish` does, and when the connection drops,
in-flight futures fail with `DisconnectedError`: a resend could reorder messages. A caller that
wants retries supplies `msg_id:` and republishes.

**Atomic batches**, for streams with `allow_atomic`:

```ruby
ack = js.batch do |batch|
  batch.publish("orders.created", a)
  batch.publish("orders.paid", b, expect: { last_subject_sequence: 3 })
end                                       # commits on block exit; raises and stores nothing on failure
ack.batch_id ; ack.count                  # ack is nil when the block published nothing
```

The block is given a `JetStream::PublishBatch` (`Batch` is the result of a fetch, 7.5), and the
block form guarantees a batch is always committed or abandoned. An exception inside the block
abandons it. The batch id is generated. The first message is sent as a request, because the server
reports a refused batch on it; the rest are plain publishes; and the commit is the empty `eob`
form, so the batch never has to hold a message back to learn that it is the last. `batch.publish`
takes the same options as `js.publish` except `expect: { last_msg_id: }`, which the server rejects
inside a batch and tether rejects first. `batch.size` is checked against the server's limit of
1,000 messages, and the publish that would exceed it raises `BatchTooLargeError`. The server
abandons a batch idle for ten seconds, so a block that stalls fails at commit with
`IncompleteBatchError`. `timeout:` bounds each exchange with the server, not the block.

**Fast-ingest batches**, for streams with `allow_batched` (API level 4), trade the all-or-nothing
guarantee for throughput:

```ruby
ack = js.fast_batch(gap: :fail, flow: 100, windows: 2) do |batch|
  rows.each { |row| batch.publish("ingest.rows", row) }
  batch.acknowledged             # the highest batch sequence the server has acknowledged
  batch.errors                   # per-message failures: [#<Failure sequence:, error:>], with gap: :ok
  batch.gaps                     # ranges of batch sequences the server did not receive, with gap: :ok
end
ack.count ; ack.sequence         # the last batch sequence accepted, and its stream sequence
```

All control information travels in the reply subject of each publish, on a dedicated inbox. The
block is given a `JetStream::FastBatch`. Tether waits for the server's first flow acknowledgement
before sending more, then lets the batch run at most `windows:` acknowledgement windows ahead of
the server (1 to 3, 2 by default, as ADR-50 recommends), pausing `batch.publish` when it gets
further: a message is sent only while its batch sequence is at most the last acknowledged sequence
plus `windows` times the server's current cadence. `flow:` is the largest cadence the server may
choose. When an expected acknowledgment does not come within a second the batch pings the server,
and `timeout:` bounds each such wait. With `gap: :fail` the first lost message raises
`MessageGapError` and the first refused one raises its `APIError`, ending the batch; with
`gap: :ok` they are collected and the batch continues. Messages are stored as they arrive, so a
batch that fails part way leaves what it already stored in the stream.

**Counters**, for streams with `allow_message_counter`:

```ruby
counter = js.counter("hits.home")               # stream: is looked up from the subject and cached
counter.increment(5)                            # => Integer, the new total; the delta may be negative
counter.value                                   # => Integer, or nil when never incremented
counter.sources                                 # per-source totals on an aggregating stream
```

Totals are arbitrary-precision on the server and travel as strings; Ruby integers are too, so
nothing is lost.

**Scheduled messages**, for streams with `allow_message_schedules`:

```ruby
js.publish("schedules.report", payload,
  schedule: { at: Time.now + 3600,          # or every: 60, or cron: "0 0 9 * * *"
              target: "reports.daily",      # required: where the produced message goes
              source: nil,                  # take the body from the last message on this subject
              ttl: nil,                     # TTL of produced messages, seconds or :never
              time_zone: nil,               # for cron schedules
              rollup: false })              # produced messages replace earlier ones on the target
js.publish("reports.daily", payload, cancel_schedule: "schedules.report")   # publish and cancel atomically
```

A schedule lives on its subject, one per subject, so publishing to the same subject replaces it.
Purging that subject (`stream.purge(subject:)`) also cancels it.

### 7.5 Consumers

```ruby
consumer = stream.create_consumer(durable: "billing", ack_policy: :explicit,
                                  filter_subjects: ["orders.eu.>"], max_deliver: 5, ack_wait: 30)
consumer = stream.create_or_update_consumer(durable: "billing", ...)
consumer = stream.update_consumer(durable: "billing", ...)        # the whole config, not a merge
consumer = stream.consumer("billing")              # looks up; raises ConsumerNotFoundError
stream.consumer?("billing")
consumer = stream.create_consumer(filter_subjects: ["orders.>"])   # no name: ephemeral, name generated
stream.consumers ; stream.consumer_names           # lazy enumerators
stream.delete_consumer("billing")

consumer.name ; consumer.stream
consumer.info ; consumer.cached_info ; consumer.config
consumer.pause_until(Time.now + 600) ; consumer.resume
consumer.reset(sequence: nil) ; consumer.unpin(group) ; consumer.step_down
consumer.delete
```

`stream.consumer` only ever reads. Anything that creates server state has `create` in its name.

`durable:` names a consumer that stays until it is deleted. `name:` alone names an ephemeral one,
which the server removes after its `inactive_threshold` without pulls, and no name at all
generates one. When both are given they must be equal. An update replaces the whole
configuration, so `update_consumer` on a durable consumer has to say `durable:` again: with
`name:` alone the configuration sent no longer carries the durable name. Defaults filled in when
omitted are `ack_policy: :explicit`, `replay_policy: :instant` and a `deliver_policy` derived from
`start_sequence:` or `start_time:`, otherwise `:all`.

`js.create_consumer("ORDERS", ...)` and the other stream-level calls also exist on the context,
taking the stream name first, for code that does not hold a `Stream`.

Tether creates and consumes pull consumers only (section 1). `ConsumerConfig` has no
`deliver_subject`, `deliver_group`, `flow_control`, `idle_heartbeat` or `rate_limit` keyword, so a
push consumer cannot be created by accident. A push consumer made by another client still decodes:
`consumer.info` and `consumer.config.raw` show it, `consumer.push?` is true, it can be deleted, and
`fetch`, `consume` and `messages` on it raise `PushConsumerError` before anything is sent.

**Fetch**: one pull request, a bounded result.

```ruby
batch = consumer.fetch(10, timeout: 5)                  # => Batch, possibly fewer, possibly empty; 30 s default
batch = consumer.fetch(10, timeout: 5, max_bytes: 1_048_576)
batch = consumer.fetch(10, wait: false)                 # no_wait: whatever is available now
batch = consumer.fetch(10, wait: false, timeout: 5)     # no_wait with expires: wait only if empty
consumer.fetch(100, timeout: 5) { |msg| msg.ack }       # block form yields as messages arrive
message = consumer.next(timeout: 5)                     # one message, or raises TimeoutError

batch.each { |msg| ... } ; batch.size ; batch.empty? ; batch.to_a
batch.error                                             # why the pull ended early, or nil
```

Each call uses its own resolver on a fresh inbox, so any number of threads may fetch from one
`Consumer` at once without seeing each other's messages or statuses. A `Batch` is `Enumerable`.
`fetch` returns what it has when the batch is complete, the server ends the pull, or the deadline
passes. Finding nothing is not an error, so an empty batch is returned, not raised; a status that
ends the pull abnormally (`ConsumerDeletedError`, a `PullError` carrying the server's status, a
`NoHeartbeatError` when the pull went silent) raises when no message was received and is otherwise
left on `batch.error`, so messages already delivered are never thrown away.

The pull's `expires` is the caller's timeout less a margin of ten percent, at most one second, so
the server's `408` normally arrives before the client's own deadline, which is `timeout` itself.
With an `expires` of ten seconds or more, a five second idle heartbeat detects a dead pull early;
`heartbeat:` sets another interval, at most half of the timeout. The
pin id of a pinned-client priority group belongs to the `Consumer` handle, so fetches from several
threads share it.

**Consume**: continuous delivery to a block.

```ruby
consuming = consumer.consume(max_messages: 500, expires: 30, heartbeat: nil, threshold: nil,
                             concurrency: 1, on_error: ->(error) { log.warn(error) }) do |msg|
  process(msg)
  msg.ack
end

consuming.on_error { |error| ... }     # warnings and terminal errors for this loop
consuming.stop                         # stop now; buffered messages are discarded
consuming.drain(timeout: 30)           # stop pulling; finish buffered messages
consuming.wait(timeout: nil)           # block until it ends; raises if it ended with an error
consuming.stopped? ; consuming.error
```

Without an `on_error` handler, a loop's warnings and errors go to the connection's `on_error`. An
exception raised by the block is reported there too, as a `HandlerError` carrying the message, and
the loop continues.

**Messages**: the same engine, driven by the caller.

```ruby
consumer.messages(max_messages: 100).each { |msg| process(msg); msg.ack }

iterator = consumer.messages
msg = iterator.next(timeout: 5)        # TimeoutError when none arrives; StopIteration once stopped
iterator.stop
```

`consume` and `messages` both return a `JetStream::Pull`, one class with or without a handler. It
has `each` but is not `Enumerable`. It implements the algorithm from
[`03-consumers.md`](protocol/03-consumers.md): one inbox for the loop; a pull request for
`max_messages` (500) with `expires` (30 seconds, at least 1) and an idle heartbeat of half that,
kept between half a second and 30 seconds; a new pull
for the difference whenever the outstanding count falls below `threshold` (half of `max_messages`);
pending counts corrected from the `Nats-Pending-Messages` and `Nats-Pending-Bytes` headers of
terminating statuses; a heartbeat monitor; and a fresh pull after a reconnect. `max_bytes:`
switches the accounting to bytes, with a message count of 1,000,000 unless `max_messages:` is given
too. `group:`, `min_pending:`, `min_ack_pending:` and `priority:` pass
through for priority groups, and a pin id from the server is remembered and sent with subsequent
pulls until a pin mismatch clears it.

The engine is split across two threads so that a slow handler cannot distort its view of the
server:

- **On the reader thread**, the loop's resolver looks at each arriving frame. A frame with a status
  line is protocol: it updates the pending counts, and if it ends the pull or the consumer it posts
  that to the loop's control queue. Every frame, status or message, stamps the time of last
  activity. A user message is pushed to the loop's bounded message queue. None of this blocks.
- **On the loop's worker**, messages are taken from the queue and given to the block. Taking a
  message decrements the outstanding count, and when that falls below the threshold the worker
  publishes the next pull. The worker also serves the control queue: terminal statuses, the
  reconnect notice, `stop` and `drain`.

The heartbeat monitor is a timer entry that compares the time of last activity with now. Because
activity is stamped on arrival, not on processing, a handler that takes a minute does not look like
a dead server. After two missed heartbeats the monitor posts to the control queue; the worker
resets the outstanding count, pulls again, and reports `NoHeartbeatError` as a warning.

The message queue is sized from the pull: `max_messages` entries, or `max_bytes`. The outstanding
count is decremented when the worker takes a message, not when the handler returns, so one slow
handler does not hold back the next pull by a whole batch, while the number of messages held in
memory stays bounded by `max_messages`. With `concurrency:` above one, several workers take from
the same queue; protocol handling is unaffected because it is not on those workers.

Every status the server can send is classified in one table:

| Status | Effect |
|---|---|
| `100 Idle Heartbeat` | activity only |
| `404 No Messages`, `408 Request Timeout`, `409 Batch Completed`, `409 Message Size Exceeds MaxBytes` | ends that pull; pending counts corrected from its headers |
| `408 Requests Pending`, `409 Exceeded MaxRequestBatch` and its siblings, `409 Exceeded MaxWaiting`, `409 Server Shutdown`, any other `409` | ends that pull; reported as a warning (`PullError`) |
| `409 Leadership Change` | every outstanding pull is void; reported, and pulled again |
| `423` pin mismatch | clears the stored pin id; reported as `PinIdMismatchError`; pull again |
| `503` no responders | the consumer or its leader is unreachable. The loop asks for the consumer's info: if it is gone the loop ends with `ConsumerDeletedError`; otherwise `ServiceUnavailableError` is reported and the next heartbeat miss retries |
| `409 Consumer Deleted`, `409 Consumer is push based`, `400 Bad Request`, `412 Required Api Level` | ends the loop with `ConsumerDeletedError`, `PushConsumerError`, `PullError` or `RequiredApiLevelError` |

Loop-ending statuses surface through `on_error`, `wait` and the iterator. Warnings go to `on_error`
only. An unrecognized status is a warning, so a newer server cannot stop an older client.

**Ordered consumers** give strictly ordered, gap-free delivery without acknowledgements:

```ruby
ordered = stream.ordered_consumer(filter_subjects: ["orders.>"], deliver_policy: :all,
                                  start_sequence: nil, start_time: nil, headers_only: false,
                                  replay_policy: nil, inactive_threshold: 300, name_prefix: nil)
ordered.consume(on_caught_up: -> { ... }) { |msg| ... }
ordered.messages.each { |msg| ... }
ordered.fetch(100)               # continues after the last message an earlier fetch returned
ordered.close                    # deletes the server consumer kept between fetches
```

`JetStream::Ordered` wraps the pull engine with an ephemeral, memory-backed, `ack_policy: :none`
consumer. It tracks the last stream and consumer sequence delivered. A consumer sequence that is not
the previous plus one, a missed heartbeat, a deleted consumer, a no-responders status on a pull (how
a vanished memory consumer is usually noticed first), or a reconnect makes it create a new consumer
starting at the last stream sequence plus one, with backoff between attempts, and delete the old one
in the background so recovery does not wait for it. The user sees an uninterrupted ordered stream.
Nothing exists on the server until `consume`, `messages` or `fetch` is called, and each loop has its
own server consumer, which it removes when it stops. `on_caught_up:` is called once, when everything
that existed when the loop's consumer was created has been handed out, which is what a watcher's
`on_ready` is built on. `concurrency:` above one is refused, since it would defeat the ordering.
Key/Value watchers and Object Store readers are built on this class, so the recovery logic exists
once.

### 7.6 Messages and acknowledgement

```ruby
msg.subject ; msg.data ; msg.headers ; msg.reply_to
msg.ack                          # +ACK, fire and forget
msg.ack_sync(timeout: 5)         # waits for the server to confirm
msg.nak                          # redeliver now
msg.nak(delay: 30)               # redeliver after 30 seconds
msg.in_progress                  # +WPI, reset the ack wait timer
msg.term                         # +TERM, never redeliver
msg.term(reason: "poison")       # +TERM with a reason
msg.ack(next: 5)                 # +NXT: acknowledge and request more in one message; consume and messages only
msg.acked?
msg.marker? ; msg.marker_reason  # a server delete marker (Nats-Marker-Reason), not user data

meta = msg.metadata
meta.stream ; meta.consumer ; meta.domain
meta.stream_sequence ; meta.consumer_sequence
meta.delivered                   # delivery attempt, 1 on first delivery
meta.pending                     # messages remaining for this consumer
meta.time                        # Time the message was stored
```

Metadata is parsed from the `$JS.ACK` reply subject and accepts both the nine-token layout and the
eleven-or-more-token layout current servers emit. A message may be acknowledged terminally once; a
second `ack`, `nak` or `term` raises `AlreadyAckedError`, guarded atomically. `in_progress` may be
sent any number of times. An `ack_sync` that times out leaves the message open, so it may be
acknowledged again. The ack methods return the message. On a message from a consumer with
`ack_policy: :none`, which includes ordered consumers, the ack methods do nothing, so the same
handler works under either policy.

Advisories are ordinary messages on `$JS.EVENT.ADVISORY.>`. Tether does not wrap them beyond
`js.advisories(kind = nil) { |advisory| ... }`, which subscribes, decodes the JSON into a value
object with the advisory's `type`, and returns the `Subscription`. `kind` is `nil` for all of them,
one of `:api`, `:stream`, `:consumer`, `:server` and `:domain` for a family, or the subject after
`$JS.EVENT.ADVISORY.`.

---

## 8. Key/Value

A bucket is the stream `KV_<bucket>`, and every operation is a publish, a direct get, or an ordered
consumer, as specified in [`04-kv-object-services.md`](protocol/04-kv-object-services.md).

```ruby
kv = js.create_key_value(bucket: "config", history: 5, ttl: nil, max_bytes: nil, max_value_size: nil,
                         storage: :file, replicas: 1, compression: false, description: nil,
                         limit_marker_ttl: nil, allow_message_ttl: false, mirror: nil, sources: nil,
                         placement: nil,
                         republish: nil, metadata: nil)   # history defaults to 1
kv = js.key_value("config")                 # raises BucketNotFoundError, or BadBucketError
kv = js.update_key_value(bucket: "config", history: 10)   # options not given return to their defaults
kv = js.create_or_update_key_value(bucket: "config", history: 10)
js.key_value_names ; js.key_values ; js.delete_key_value("config")
kv.bucket ; kv.config ; kv.stream

entry = kv.get("region")                    # => Entry or nil; a deleted key is nil
entry.bucket ; entry.key ; entry.value ; entry.revision ; entry.created_at
entry.operation                             # :put, :delete or :purge; entry.put?, entry.tombstone?
entry.delta                                 # revisions a watcher still had to deliver; 0 from a get
kv.get("region", revision: 3)               # nil when that revision belongs to another key
kv["region"]                                # => value String or nil
kv["region"] = "eu"                         # put
kv.fetch("region")                          # raises KeyNotFoundError; kv.fetch("region") { default }

kv.put("region", "eu")                      # => revision Integer
kv.create("region", "eu", ttl: nil)         # raises KeyExistsError unless absent or deleted
kv.update("region", "us", revision: 4)      # raises RevisionMismatchError on conflict
kv.delete("region", revision: nil)          # tombstone, history kept; => revision of the tombstone
kv.purge("region", revision: nil, ttl: nil) # tombstone, history dropped; => revision of the tombstone
kv.purge_deletes(older_than: 1800)          # removes tombstones of that age; => how many

kv.keys                                     # lazy Enumerator of live keys
kv.keys("config.>")                         # with subject filters
kv.history("region")                        # Array<Entry>, oldest first; [] for a key never written
kv.each { |entry| ... }                     # current entries; Enumerable
kv.status                                   # Status: bucket, values, history, ttl, limit_marker_ttl, bytes,
                                            #   compressed?, backing_store, metadata, config, stream_info
```

Creating a bucket that exists with the same options succeeds; with other options it raises
`BucketExistsError`. Binding to a stream that is named like a bucket but does not keep at least one
message per subject raises `BadBucketError`.

Watching:

```ruby
watcher = kv.watch("region", "limits.>", include_history: false, updates_only: false,
                   ignore_deletes: false, meta_only: false, resume_from: nil,
                   on_ready: -> { puts "initial values delivered" },
                   on_error: ->(error) { log.warn(error) }) do |entry|
  apply(entry)
end
watcher.on_ready { ... }                    # fires at once if already ready
watcher.on_error { |error| ... }
watcher.ready? ; watcher.wait_until_ready(timeout: 5)   # => true, or false when time ran out
watcher.revision                            # the revision of the last entry received
watcher.stop ; watcher.stopped? ; watcher.wait ; watcher.error

kv.watch.each { |entry| ... }               # without a block, iterate; ends when stopped
kv.watch.next(timeout: 5)                   # or take one entry; StopIteration once stopped
```

The end of the initial values is a state (`ready?`) and an event (`on_ready`), not a `nil` pushed
through the entry stream. A watcher on an empty bucket is ready as soon as it is created. A watcher
is an ordered consumer, so it survives reconnects and consumer loss without gaps. `keys`,
`history` and `each` stop their consumer when enumeration ends, completes or not, using `ensure`,
so abandoning an enumerator leaks nothing. Their `timeout:` follows the streaming rule of 7.3: a
number bounds the wait for each entry, a `Deadline` the whole listing.

Reads are answered by any replica or mirror of the bucket, so a `get` made right after a write may
still see the previous revision. Where the value just written matters, use the revision the write
returned.

A revision conflict in `create`, `update`, `delete(revision:)` or `purge(revision:)` raises
`RevisionMismatchError` (`KeyExistsError`, its subclass, from `create`). The server reports the
conflict as error 10071 on a single-server stream and as 10164 on a replicated one when another
write to the same key is in flight; both are translated.

Keys and bucket names are validated against the character sets of ADR-6 and ADR-8, raising
`InvalidKeyError` and `InvalidBucketNameError`. Per-key TTLs (`create(ttl:)`, `purge(ttl:)`) need a
bucket with `limit_marker_ttl:` or `allow_message_ttl: true`, either of which needs a server at
JetStream API level 1. With `limit_marker_ttl:` an expired key leaves a marker that lives that many
seconds; with `allow_message_ttl: true` alone it disappears, as if it had never been written.
Under a JetStream domain or API prefix, the bucket's subjects are prefixed as chapter 4 specifies,
and a bucket that is a mirror reads through its own stream and writes through the origin's
subjects.

---

## 9. Object Store

A bucket is the stream `OBJ_<bucket>` holding chunk messages and one metadata message per object.

```ruby
store = js.create_object_store(bucket: "assets", ttl: nil, max_bytes: nil, storage: :file,
                               replicas: 1, compression: false, description: nil,
                               placement: nil, metadata: nil)
store = js.object_store("assets")                                # raises ObjectStore::BucketNotFoundError
store = js.update_object_store(bucket: "assets", max_bytes: 10_000_000) # options not given return to their defaults
store = js.create_or_update_object_store(bucket: "assets", ttl: 86_400)
js.object_store_names ; js.object_stores ; js.delete_object_store("assets")
store.name ; store.stream

info = store.put("logo.png", File.open("logo.png", "rb"), description: nil, headers: nil,
                 metadata: nil, chunk_size: 128 * 1024)          # any IO or String
info.name ; info.size ; info.chunks ; info.digest ; info.modified_at ; info.deleted? ; info.link?

store.get("logo.png") { |chunk| out.write(chunk) }               # streams; verifies the digest at the end
store.get("logo.png", into: io)                                  # or writes into an IO; both return the Info
data = store.get("logo.png").read                                # an IO-like Reader: read, each, eof?, close
store.get_file("logo.png", "/tmp/logo.png")
store.put_file("logo.png", "/tmp/logo.png")

store.info("logo.png")                # => ObjectStore::Info or nil, like kv.get; show_deleted: true
store.exist?("logo.png")
store.delete("logo.png")
store.update_meta("logo.png", name: "brand.png", description: "v2")
store.add_link("latest", info) ; store.add_bucket_link("other", other_store)
store.list                            # lazy Enumerator of Info; deleted: true includes tombstones
watcher = store.watch(include_history: false, ignore_deletes: false, updates_only: false,
                      on_ready: nil, on_error: nil) { |info| ... }
store.seal ; store.status             # Status: bucket, size, sealed?, storage, replicas, ttl, max_bytes, ...
```

`put` reads the source in `chunk_size` pieces, publishes each chunk with an async publish window
(8 MiB of chunks, at most 256), computes SHA-256 as it goes, then writes the metadata as a
per-subject rollup and purges the previous version's chunks, which stay readable until then. If
anything fails, the chunks already written are purged. `get` reads chunks through an ordered
consumer and raises `DigestMismatchError` if the data does not match the recorded size or digest;
`get` of a missing or deleted object raises `ObjectNotFoundError`, and `get` follows a link to
another object once, raising `LinkError` for a link to a bucket or to another link. A reader that
is not read to the end must be closed, which removes its server consumer. `update_meta` to a name
a live object already has, and a link under such a name, raise `ObjectExistsError`; any write to a
sealed bucket raises `BucketSealedError`. The watcher is the Key/Value watcher's sibling:
`on_ready`, `ready?`, `wait_until_ready`, `next`, `each`, `stop`. The `timeout:` of `put`, `get`
and `list` follows the streaming rule of 7.3.

Object subjects are `$O.<bucket>.C.<id>` and `$O.<bucket>.M.<name>`, published as plain subjects.
Unlike Key/Value they are not rewritten under an API prefix, which is how `nats.go` behaves and
therefore what interoperates today; chapter 4 records the disagreement with ADR-19.

---

## 10. Services

The Services API (ADR-32) is request/reply plus discovery: three verbs (`PING`, `INFO`, `STATS`),
each on three subjects under `$SRV` (all services, by name, by name and id), so nine plain
subscriptions per service.

```ruby
services = nc.services                            # a Service::Registry; nc.services(prefix: "$SRV")
service = services.add(name: "calc", version: "1.2.0", description: "arithmetic",
                       metadata: {}, queue: "q",
                       stats_handler: ->(endpoint) { { "cache" => "warm" } },   # becomes `data` in STATS
                       on_stop: ->(error) { log.info("stopped: #{error.inspect}") },
                       on_error: ->(error, endpoint) { log.warn(error.message) })

service.endpoint("add") { |request| request.respond((request.data.to_i + 1).to_s) }
service.endpoint("div", subject: "calc.div", queue: false, metadata: {}) do |request|
  next request.respond_error(400, "division by zero") if request.data == "0"
  request.respond_json({ result: 100 / request.data.to_i })
end

math = service.group("math", queue: "math-q")
math.endpoint("sqrt") { |request| ... }          # subject "math.sqrt"

service.name ; service.id ; service.version ; service.endpoints
service.info ; service.stats ; service.reset_stats
service.on_stop { |error| }                       # error is what stop was given, or nil
service.on_error { |error, endpoint| }            # the server refused a subscription; endpoint nil for $SRV
service.stop(error = nil, timeout: nil)           # drains endpoints, then answers no more discovery
service.stopped?

services.ping("calc")                             # => [Service::Ping], one per instance that answered
services.info("calc") ; services.stats("calc")    # => [Service::Info], [Service::Stats]
services.stats("calc", id: service.id)            # one instance; also max:, stall:, timeout:
```

Each endpoint is an ordinary queue subscription whose worker times the handler and updates the
stats. `queue:` on an endpoint or group is a name, `nil` to inherit from the parent (the service
default is `"q"`), or `false` for no queue group, so every instance receives every request. An
exception in a handler is answered with a 500 error response carrying `Nats-Service-Error` and
`Nats-Service-Error-Code`, counted, and recorded as the endpoint's last error; if the handler had
already responded, the error is only counted. When the server refuses one of the service's
subscriptions with a permissions violation, the service keeps running, as in nats.go: the refusal
is counted and recorded as that endpoint's last error, and `on_error` is called with the error and
the endpoint, or `nil` when a discovery subject was refused. The service learns of the refusal from
the connection's `on_error`, so its handlers run on the event thread. A `Service::Request` is a `Msg` with `respond`,
`respond_json`, `respond_error(code, description, data = nil, headers: nil)` and `responded?`.

`nc.services` returns a `Service::Registry`: `add` starts a service on the connection, and `ping`,
`info` and `stats` are the discovery client, each a `request_many` on the control subject for
every service, one service by name, or one instance by name and `id:`. They return value objects
(`Ping`, `Info`, `Stats`, with `EndpointInfo` and `EndpointStats` inside) that keep the raw
document; processing times are in seconds. `nc.services(prefix:)` selects another discovery
prefix, for use across account imports. `stop` waits for requests in flight up to `timeout:`
(the client's `drain_timeout` by default), raises `TimeoutError` if some remain, and is stopped
either way.

---

## 11. Errors

One hierarchy, rooted at `Tether::Error`, so `rescue Tether::Error` catches everything the gem
raises and nothing else.

```
Tether::Error
├── TimeoutError
├── ProtocolError
├── ConfigurationError                 (an option tether cannot act on, such as force_reconnect without reconnect)
├── InvalidSubjectError · MaxPayloadError · HeadersNotSupportedError
├── NoRespondersError
├── SlowConsumerError · HandlerError   (events; HandlerError carries #msg and #cause)
├── SubscriptionClosedError · ForkedError
├── ConnectionError
│   ├── ConnectionClosedError · DisconnectedError · ConnectionDrainingError
│   ├── NoServersError · ReconnectBufferFullError
│   ├── TLSError
│   └── CredentialsError               (a credential cannot be read or decoded)
├── ServerError                        (-ERR lines; #fatal? says whether the server closes)
│   ├── AuthorizationError · AuthenticationExpiredError · AuthenticationRevokedError
│   ├── AuthenticationTimeoutError · PermissionsViolationError · StaleConnectionError
│   ├── MaxConnectionsError · MaxAccountConnectionsError · MaxSubscriptionsError
│   └── ...                            (the table in errors.rb, derived from the server source)
├── JetStream::Error
│   ├── APIError                       (#code, #err_code, #description)
│   │   ├── StreamNotFoundError · StreamNameInUseError · StreamMismatchError · StreamSealedError
│   │   ├── ConsumerNotFoundError · ConsumerExistsError · MessageNotFoundError
│   │   ├── WrongLastSequenceError · DuplicateInProgressError · LimitReachedError
│   │   ├── JetStreamNotEnabledError · InsufficientResourcesError · RequiredApiLevelError
│   │   ├── BadRequestError · IncompleteBatchError · BatchTooLargeError · BatchPublishDisabledError
│   │   └── SchedulesDisabledError · InvalidScheduleError
│   ├── ServiceUnavailableError · NoStreamResponseError
│   ├── TooManyStalledMessagesError · MessageGapError · TooManyResultsError
│   ├── ConsumerDeletedError · NoHeartbeatError · PinIdMismatchError · PushConsumerError
│   ├── PullError                      (#status; a pull the server refused or ended)
│   └── AlreadyAckedError · NotJetStreamMessageError
├── KeyValue::Error
│   ├── BucketNotFoundError · BucketExistsError · BadBucketError
│   ├── KeyNotFoundError · InvalidKeyError · InvalidBucketNameError
│   └── RevisionMismatchError ── KeyExistsError
├── ObjectStore::Error
│   ├── BucketNotFoundError · BucketExistsError · BucketSealedError · InvalidBucketNameError
│   ├── ObjectNotFoundError · ObjectExistsError · InvalidObjectNameError
│   └── DigestMismatchError · LinkError
└── Service::Error
```

An unknown option or keyword is a plain `ArgumentError`, not a `Tether::Error`: it is a bug in the
calling code, not a condition to rescue.

`APIError.for(error)` picks the subclass from the `err_code`, falling back to `APIError` itself
for codes without a dedicated class, so a new server error is still raised with its code and text.
`KeyValue::KeyExistsError` and the like are raised by translating the specific `APIError` at the
call that knows what it means; the original is available as `cause`.

Which exceptions are worth retrying is a method, not folklore. `error.retryable?` is true where
repeating the same call may succeed with no change by the caller: `DisconnectedError`,
`JetStream::NoStreamResponseError`, `JetStream::ServiceUnavailableError`, and an `APIError` whose
code is one of the server's transient ones (JetStream temporarily unavailable or leaderless, a
duplicate still in progress, too many batches in flight). It is false for everything else,
`TimeoutError` included: a timeout says nothing about whether the call took effect, so repeating it
is the caller's decision.

---

## 12. Concurrency model

### 12.1 Threads

| Thread | Count | Lifetime | Does |
|---|---|---|---|
| reader | 1 per live socket | until the socket closes | read, parse, dispatch; never user code, never blocks on anything but the socket read |
| flusher | 1 per client | first connect until close | writes the buffer when kicked |
| timer | 0 or 1 per client | first timer entry (normally the keepalive, at connect) until close | fires pings, heartbeat monitors, ack timeouts, backoff; only appends and posts |
| events | 0 or 1 per client | first handler registered until `:close` is delivered | runs connection event handlers in order |
| reconnect | 0 or 1 | one outage | the reconnect loop |
| close | 0 or 1 | transient | closes the client when the socket is lost and reconnect is not allowed |
| worker | up to `worker_threads` per client, started when a block subscription has messages | until idle for 30 s, or close | runs subscription blocks, a batch of 64 messages per subscription per turn |
| dedicated worker | up to `concurrency` per subscription with `dedicated: true` | until idle for 30 s, or the subscription ends | runs that subscription's block |
| consume worker | `concurrency` per `consume` | until stopped | runs the block, issues pulls, serves the control queue; a `messages` iterator has no worker, the thread calling `next` does this work |
| publish errors | 0 or 1 per JetStream context | from `on_publish_error` on | runs the handler for failed asynchronous publishes |

Key/Value and Object Store watchers with a block are consume loops and run on a consume worker;
endpoints of a service are subscriptions. Threads are named `tether:reader`, `tether:flusher`,
`tether:timer`, `tether:events`, `tether:reconnect`, `tether:close`, `tether:worker`, `tether:sub:<subject>`,
`tether:consume:<consumer>` and `tether:publish-errors`, and none sets `abort_on_exception`. The
name is how `Client#drain` knows it was called from a tether thread. A connected client with no
handlers and no block subscriptions runs three threads. Threads end because the thing they block on
is closed (a socket, a queue), never because another thread killed them.

### 12.2 Locks

| Lock | Guards | Held across |
|---|---|---|
| lifecycle | status, the current transport, the reconnect thread, the fork generation | state transitions only; never I/O |
| writer I/O | the right to write to the socket | one socket write, bounded by the write deadline |
| writer buffer | the outbound buffer, the writer's mode, the PONG FIFO | appending and swapping; never I/O |
| registry | the sid map | lookups and edits |
| inbox | the token map of the request mux | lookups and edits |
| per object | a subscription's counters, a pull engine's pending counts, a future's value | field updates |

Ordering, to exclude deadlock: `lifecycle → writer I/O → writer buffer → registry`. The reconnect
replay is the one place that takes the registry lock under the buffer lock, to snapshot it. `inbox`
and per-object locks are leaves that are never held while taking another lock. The reader and timer
threads take only the buffer lock, the registry lock, leaf locks and, to report a lost socket, the
lifecycle lock. None of these is ever held across I/O, so neither thread can be stalled by a slow
socket. Reporting a lost socket only records the transition and starts the reconnect thread; closing
the old socket, joining threads and connecting again all happen there. The fork reset (5.12) has its
own lock for the same reason: the fresh connect it ends with is I/O. User code is never called with
a lock held.

### 12.3 Guarantees

- Publishes from one thread reach the server in the order they were made. Publishes from different
  threads are interleaved at frame boundaries only.
- A subscription is scheduled on the pool as one job, never one per message, and at most `concurrency`
  workers hold it at once; a worker handles up to 64 of its messages and then puts it at the back of the
  line. A block that blocks holds one worker until it returns, so once every worker is held the other
  subscriptions wait; `dedicated: true` is for blocks that wait on slow I/O.
- Messages on one subscription are delivered in the order received. With `concurrency: 1` the
  block never runs concurrently with itself.
- A reply is never delayed by a user handler.
- Every blocking call returns or raises by its deadline, and at once if the connection closes.
- An exception in user code never kills a tether thread and never propagates to another thread.

---

## 13. Feature matrix

| Feature | Server | nats.go | nats-pure | tether |
|---|---|---|---|---|
| Pub/sub, queue groups, headers, no-responders | yes | yes | yes | yes |
| Auto-unsubscribe restored across reconnect | n/a | yes | no | yes |
| Request mux; request-many | n/a | mux only | mux only | both |
| No echo | yes | yes | no | yes |
| Token, user/password, NKEY, JWT/creds | yes | yes | yes, via `nkeys` gem | yes, no gem |
| TLS; TLS first; client certificates | yes | yes | TLS only via context | yes |
| Cluster discovery; lame duck notification | yes | yes | discovery only | yes |
| Reconnect with jitter and byte-bounded buffer | n/a | yes | fixed delay; buffer bounded by command count, then publishers block | yes |
| Per-subscription and connection drain | n/a | yes | connection only | yes |
| WebSocket transport, with compression | yes | yes | yes, via `websocket` gem, no compression | yes, no gem |
| Fork safety | n/a | n/a | yes, with hazards | yes |
| Fiber scheduler compatible | n/a | n/a | not designed for | by construction: only scheduler-aware primitives block; the suite does not run under one yet |
| Streams: create, update, info, list, purge, delete, message get and delete | yes | yes | no purge, message delete or lists | yes |
| Direct get; batch and multi-last direct get | yes | single | single | yes |
| Stream snapshot and restore | yes | no | no | yes |
| `Nats-Required-Api-Level` on requests | yes | no | no | yes |
| Publish with dedupe and expectations; async publish | yes | yes | sync only | yes |
| Per-message TTL; schedules | yes | yes | yes | yes |
| Atomic batch publish; fast-ingest batches; counters | yes | no | acks parsed only | yes (`batch`, `fast_batch`, `counter`) |
| Pull: fetch, continuous consume, iterator | yes | yes | fetch only | yes (`fetch`, `consume`, `messages`) |
| Push consumers | yes | yes, legacy | KV watch only | no, by design: inspect and delete only |
| Ordered consumers | yes | yes | no | yes |
| Multiple filter subjects; pause; reset; priority groups and pinning | yes | yes | yes | yes |
| Key/Value, with watch, per-key TTL, limit markers, key filters, mirrors, purge-deletes | conventions | yes | watch only; none of the rest | yes |
| Object Store, with links, watch and seal | conventions | yes | no | yes |
| Services, with discovery client | conventions | yes | service side only | yes |
| Advisories as decoded value objects | yes | no | no | yes |

---

## 14. Testing

`bundle exec rake` runs RSpec, RuboCop and `rbs validate`. Feature specs run against a real server
in Docker (`nats:alpine`, or `NATS_IMAGE`): one shared container on a free port, plus a container
of its own for every spec that needs a particular server configuration. Setting `NATS_URL` points
the suite at a server that is already running. Unit specs, which need no server, live in
`spec/unit/`.

- **Protocol layer** is tested without sockets. The parser keeps its byte-split invariance spec
  (identical events at every split point and one byte at a time).
- **Behavior over implementation.** Specs go through the public API and never read instance
  variables.
- **Concurrency suite**, the part that distinguishes tether: N threads publishing checksummed
  payloads with no interleaving; concurrent requests; concurrent flush; a stuck subscriber that
  delays nothing else; concurrent `fetch` on one consumer; reconnect under publish load with no
  lost subscriptions; requests failed promptly on disconnect; a request racing a fast reply.
- **Fault injection.** A small TCP proxy in `spec/support` sits between the client and the server to
  drop connections and to simulate an outage on a fixed port, and scripted fake servers
  (`fake_nats_server.rb`, `fake_websocket_server.rb`) say exactly what a spec needs a server to say:
  an `-ERR`, a lame duck `INFO`, advertised cluster URLs, silence. That is how reconnect,
  stale-connection detection and discovery are exercised deterministically.
- **Not there yet:** specs against a real three-server cluster (failover and leader changes are
  covered only through the fake server and the status table), and a second run of the suite under
  a `Fiber::Scheduler`.
- **Regression specs** for every `nats-pure` defect in section 2.1: mixed encodings in one flush,
  CRLF in subjects and headers, a permissions violation that must not drop the connection, a
  handler that raises, a KV watcher on an empty bucket, an abandoned `keys` enumerator.
- **A static spec** (`spec/unit/tether_spec.rb`) fails the build if `Timeout.timeout`,
  `Thread#raise` or `Thread#kill` appears in `lib`.

Features that need a newer server than the test image provides are tagged with their API level and
skipped with a message when the server reports a lower one.

---

## 15. How it was built

The gem was built bottom up, each step ending with the suite green and one or more atomic commits.
Later steps added files and did not reshape earlier ones. RBS signatures and YARD were written
with each file, not at the end.

1. **Protocol layer and a first connection.** Parser, encoder, headers, the request mux and a
   single-socket client, written before this design existed.
2. **Connection foundation and resilience.** `Options`, `ServerInfo`, `Subject`, `Events`, `Timer`,
   the `Writer` with its two locks, `Handshake`, and `Lifecycle` with the state machine; `Client`
   shrank to delegation. `ServerPool`, reconnect with resubscribe and the reconnect buffer,
   keepalive, lame duck, `force_reconnect`, fork detection, and the fault-injection proxy.
3. **Core completeness and security.** Auto-unsubscribe, `concurrency:`, byte limits, subscription
   and connection drain, `request_many`, resolvers, stats, `rtt`, no echo, subject validation; TLS
   in all orders, client certificates, NKEY and JWT via OpenSSL, credentials files.
4. **WebSocket transport**, with compression.
5. **Services.**
6. **JetStream.** `Schema`, the config and info classes, the API transport with error mapping and
   paging, stream management; then publishing (sync with retries, async with the bounded window,
   TTL, rollups, schedules, atomic and fast batches, counters); then consuming (consumer
   management, messages and acks, `fetch`, the pull engine with `consume` and `messages`, priority
   groups, the ordered consumer).
7. **Key/Value**, then **Object Store**, both on the ordered consumer.
8. **Documentation.** The README, the [migration guide](migrating-from-nats-pure.md), and this
   document brought back in line with the code.

Still open: a run of the suite under a `Fiber::Scheduler`, and specs against a real cluster
(section 14).

---

## 16. Decisions and their reasons

These settle the open questions recorded in earlier notes.

**Consumers are pull only.** Push delivery is the legacy model: the server sends at its own pace
and the client can only drop or stall. A pull consumer covers every use, including ordered delivery
and watching, with the client deciding how much it holds. Supporting both would double the consumer
machinery to carry a model the server's own documentation advises against.

**`Deadline`, not timeouts, inside the library.** Seconds are a duration and lose their starting
point each time they cross a method boundary. Every blocking call carries one `Deadline` instead
(section 3), so a timeout a caller gives is the time the call takes at most, however many round
trips it makes.

**A request in flight when the connection drops fails immediately.** `nats.go` lets it wait out its
timeout, since the shared inbox is resubscribed and a responder that answers after the reconnect
would still be heard. But a reply sent during the gap is lost, and the caller cannot tell which
case it is in until the timeout expires. Failing with `DisconnectedError`, which is `retryable?`,
reports the uncertainty when it arises and leaves the retry decision to the caller, who knows
whether the request is safe to repeat. Requests made *while* reconnecting are buffered and answered
normally.

**Slow consumers drop the newest message and report once per episode.** Blocking the reader would
stall the whole connection; dropping the oldest would reorder what the handler sees relative to
what it already took. Limits are per subscription, in messages and bytes.

**Publishing during an outage buffers up to a byte limit, then raises.** Blocking the publisher
hides the outage, and an unbounded buffer turns an outage into memory exhaustion.

**A consume loop runs on its own worker, not a shared pool.** A pool shared between consumers lets
one slow handler starve the rest, which is the `nats-pure` defect in a new place. Parallelism is
opt-in per loop with `concurrency:`.

**Reconnect backoff follows `nats.go`**: a fixed wait plus jitter after each pass over the pool,
with a callable for anything else. Exponential backoff by default would make a brief failover take
longer to recover from, and the jitter is what prevents a thundering herd.

**Fork detection by pid comparison, not a `Process._fork` hook.** A library should not patch
`Process` at `require`. `Process.pid` measures at about 60 nanoseconds per call, which is noise
next to building a frame.

**No runtime dependencies, including for NKEY and WebSocket.** Ed25519 signing is in OpenSSL, base32
and CRC16 are a few lines each, and RFC 6455 framing for a client is small. Each avoided dependency
is one less version constraint imposed on every application that uses the gem.

**Seconds everywhere.** The wire uses nanoseconds, the Go client exposes them, and `nats-pure`
mixes three conventions. Tether converts at the schema boundary, so `ack_wait: 30` means thirty
seconds in every method.

**`nil` is never an in-band signal.** A missing key returns `nil` from `get` because absence is a
value there, but a watcher's "initial values done" is a state and an event, and a fetch that finds
nothing returns an empty batch.

**Renames from `nats-pure`.** The most common ones are below; the
[migration guide](migrating-from-nats-pure.md) has the full list, with options, errors and
behavior differences.

| nats-pure | tether |
|---|---|
| `NATS.connect(url, opts)` | `Tether.connect(url, **opts)` |
| `nc.publish(subject, data, reply, header: h)` | `nc.publish(subject, data, reply_to: reply, headers: h)` |
| `nc.subscribe(subject, queue: q) { \|data, reply, subject\| }` | `nc.subscribe(subject, queue: q) { \|msg\| }` |
| `sub.next_msg(timeout: 1)` | `sub.next(timeout: 1)` |
| `nc.flush(5)` | `nc.flush(timeout: 5)` |
| `msg.reply`, `msg.header` | `msg.reply_to`, `msg.headers` |
| `js.add_stream(name: ...)` | `js.create_stream(name: ...)` |
| `js.pull_subscribe(subject, durable)` then `psub.fetch(n)` | `js.consumer(stream, durable).fetch(n)` |
| `js.subscribe(subject) { }` | `js.consumer(stream, durable).consume { }` |
| `msg.ack_sync`, `msg.nak(delay: ns)` | `msg.ack_sync`, `msg.nak(delay: seconds)` |
| `kv.watch` yielding `nil` after initial values | `watcher.on_ready { }` |
| `NATS::IO::Timeout`, `NATS::IO::NoRespondersError` | `Tether::TimeoutError`, `Tether::NoRespondersError` |
