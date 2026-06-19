# The NATS protocol

A reference for the protocol a NATS client speaks, written so that the design of the server can be
understood from it and a client can be implemented from it. It is the specification tether is
built against; the client's own design is in [`../design.md`](../design.md).

## The shape of the system

NATS is one small protocol with everything else layered on top of it as convention.

```
  ┌───────────────────────────────────────────────────────────────────────────────┐
  │ Key/Value            Object Store             Services                        │
  │ a stream named       a stream named           request/reply plus discovery    │
  │ KV_<bucket>          OBJ_<bucket>             subjects under $SRV             │
  │                                                                  chapter 4    │
  ├───────────────────────────────────────────────────────────────────────────────┤
  │ Consumers: server-side cursors over a stream. Pull requests, push delivery,   │
  │ acknowledgements carried in reply subjects.                      chapter 3    │
  ├───────────────────────────────────────────────────────────────────────────────┤
  │ JetStream: streams that capture published messages. Managed by JSON           │
  │ request/reply on $JS.API.>; a publish is acknowledged on its reply subject.   │
  │                                                                  chapter 2    │
  ├───────────────────────────────────────────────────────────────────────────────┤
  │ Core NATS: one TCP connection, text control lines, length-prefixed payloads.  │
  │ PUB, SUB, MSG, headers, request/reply by convention.             chapter 1    │
  └───────────────────────────────────────────────────────────────────────────────┘
```

The server speaks only the bottom layer to a client. It has no "create stream" verb and no "get
key" verb. JetStream is a set of internal subscribers inside the server that answer requests on
well-known subjects; a consumer delivers by publishing ordinary messages; Key/Value and Object
Store exist only as agreements between client libraries about stream names, subject layouts and
headers. Three consequences follow, and they explain most of what a client has to do:

1. **A client is a core client first.** Every higher feature is built from publish, subscribe and
   request. If the connection layer is correct under concurrency and across reconnects, the rest is
   surface area.
2. **Control information travels in-band.** Errors and control signals arrive as messages: a status
   line in a header block (`NATS/1.0 503`), a JSON error object in a reply body, metadata encoded
   in a reply subject (`$JS.ACK...`). A client must classify every inbound message before treating
   it as user data.
3. **Interoperability lives in the client.** Two libraries interoperate on a Key/Value bucket only
   if they create the same stream configuration and use the same subjects and headers, byte for
   byte. Those conventions are specified here as exactly as the wire protocol.

## Chapters

| Chapter | Covers |
|---|---|
| [1. Core](01-core.md) | The connection lifecycle, every protocol verb, subjects, headers, request/reply, authentication, TLS, keepalive, errors, cluster behavior, WebSocket |
| [2. JetStream](02-jetstream.md) | The API envelope, streams and their configuration, publishing and its guarantees, direct get, batches, counters, schedules, the error code table |
| [3. Consumers](03-consumers.md) | Consumer configuration, the pull and push protocols, acknowledgements, ordered consumers, priority groups |
| [4. Key/Value, Object Store, Services](04-kv-object-services.md) | The three conventions, operation by operation |

Read them in order the first time. Each chapter opens with a mental model, then gives the exact
reference, then worked scenarios as byte-exact transcripts, and ends with a checklist of everything
a client must implement.

## Conventions

- Wire transcripts prefix each line with `C:` for bytes the client sends and `S:` for bytes the
  server sends, and show the line terminator as `␍␊`. Byte counts in examples are real.
- A blockquote beginning **Client rule:** states a requirement on a client implementation. The
  rules are collected at the end of each chapter.
- Exact strings (JSON keys, header names, subjects, status descriptions) are in `code font` and
  spelled as they appear on the wire. Header names are case sensitive.
- Durations in JSON are integer nanoseconds unless stated otherwise; timestamps are RFC 3339.

## Sources and versions

The facts here were read from source, not from documentation or memory:

| Source | Version |
|---|---|
| `nats-server` | commit `a0385c9af`, `v2.15.0-573`, reporting `2.16.0-dev`; JetStream API level 5 |
| `nats.go`, for client-side conventions | commit `792e440`, v1.56.0 |
| NATS architecture and design records (ADRs) | as of November 2026 |

The transcripts were also spot-checked against a running `nats-server` 2.15.3 (the `nats:alpine`
image the specs use): the handshake and the server's first PING, the no-responders frame, publish
acknowledgments including duplicates and failed expectations, direct get, and the pull statuses
with their header blocks all matched byte for byte. That server emits the 9-token acknowledgment
subject; 2.16 switches to the 11-token form, and chapter 3 covers both.

Where an ADR and the server disagree, the chapter says so and describes what the server does.
Anything that could not be confirmed in source is marked as unverified where it appears. When the
minimum supported server version moves, the version-dependent statements should be checked again;
features gated by API level are labeled with the level that introduced them.
