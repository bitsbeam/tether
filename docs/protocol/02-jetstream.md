# 2. JetStream: Streams, the API and Publishing

This chapter describes JetStream as a client sees it on the wire: the request/reply API under
`$JS.API.>`, the stream configuration and state objects, every stream-level operation, and the
publish path with its acknowledgments, headers and failure modes. It is written from the server's
point of view (what the server accepts, checks and emits) so that a client can be derived from it.
Facts are taken from the nats-server source at `v2.15.0-573` (`VERSION` `2.16.0-dev`, JetStream API
level 5), from the Go client `nats.go` v1.56.0 where an algorithm lives only in clients, and from
the architecture decision records (ADRs). The transport underneath (PUB, HPUB, MSG, HMSG, headers,
request/reply, "no responders") is defined in [01-core.md](01-core.md). Reading messages back
through consumers is covered in [03-consumers.md](03-consumers.md); key-value, object store and
services in [04-kv-object-services.md](04-kv-object-services.md).

Notation: `␍␊` is the two bytes CR LF. In transcripts, `C:` marks bytes sent by the client and `S:`
bytes sent by the server. "Clustered" means a server running JetStream in clustered mode (a Raft
*meta group* manages assets; each replicated stream has its own Raft group). "Standalone" means a
single JetStream server. Durations in JSON are **integer nanoseconds** unless stated otherwise.
Times are RFC 3339 strings in UTC with up to nanosecond precision. A JSON key marked *omit* is
left out of server output when it has its zero value; a key not marked *omit* is always present.

Contents

1. [Mental model](#1-mental-model)
2. [The API envelope](#2-the-api-envelope)
3. [Stream configuration](#3-stream-configuration)
4. [Stream info and state](#4-stream-info-and-state)
5. [Stream operations](#5-stream-operations)
6. [Publishing](#6-publishing)
7. [Mirrors, sources, subject transforms and republish](#7-mirrors-sources-subject-transforms-and-republish)
8. [Cluster-visible behavior](#8-cluster-visible-behavior)
9. [Advisories](#9-advisories)
10. [Errors every client must special-case](#10-errors-every-client-must-special-case)
11. [Scenarios](#11-scenarios)
12. [ADR versus server discrepancies](#12-adr-versus-server-discrepancies)
13. [Client implementation checklist](#13-client-implementation-checklist)
14. [Unverified items](#14-unverified-items)
- [Appendix A. Complete error code table](#appendix-a-complete-error-code-table)
- [Appendix B. Status lines used outside the JSON envelope](#appendix-b-status-lines-used-outside-the-json-envelope)

---

## 1. Mental model

### 1.1 A persistence layer built from core NATS

JetStream adds storage to NATS without adding anything to the wire protocol. There is no new
protocol verb. Every JetStream interaction is one of the core operations from
[01-core.md](01-core.md), used in a fixed convention:

| What the application does | What actually goes on the wire |
|---|---|
| Manage a stream (create, inspect, purge, ...) | A core request (`PUB` with a reply subject) to a subject under `$JS.API.>` with a JSON body, or an empty body. The server replies with one JSON message. |
| Publish a message into a stream | An ordinary `PUB`/`HPUB` to an ordinary subject. If a stream has that subject in its configuration, the server stores the message. If the publish carried a reply subject, the server sends a small JSON *publish acknowledgment* (PubAck) to it. |
| Control how a publish is stored | `Nats-*` headers on that `HPUB` (deduplication id, expected sequence, TTL, ...). |
| Read one message by sequence or subject | A request to `$JS.API.STREAM.MSG.GET.<stream>` (JSON reply) or `$JS.API.DIRECT.GET.<stream>` (the stored message itself comes back, with metadata in headers). |
| Read messages continuously | Through a *consumer*, a server-side cursor over a stream ([03-consumers.md](03-consumers.md)). |
| Observe what the system is doing | Subscribe to *advisories*, ordinary messages on `$JS.EVENT.ADVISORY.>`. |

The central object is the **stream**: a named, ordered, append-only log owned by one account. A
stream has a list of subjects (wildcards allowed). The server holds an internal subscription for
each of those subjects; whatever is published to a matching subject by any client of the account is
appended to the log and assigned the next **sequence number** (a 64-bit integer starting at 1, never
reused) and a timestamp. The publisher does not address the stream. It publishes to a subject, and
the stream captures the message. A published message is captured by at most one stream because the
subjects of different streams in one account may not overlap.

A stored message consists of: stream sequence, subject, optional header block, payload, timestamp.
Nothing else. All metadata a client can attach travels in headers.

### 1.2 What each piece guarantees

**Retention** decides when a stored message is removed (`retention`):

| Policy | A message is removed when |
|---|---|
| `limits` (default) | A limit is exceeded: `max_msgs`, `max_bytes`, `max_age`, `max_msgs_per_subject`, or a per-message TTL. Messages are retained whether or not anyone reads them. |
| `interest` | As for `limits`, and also as soon as every consumer that is interested in the message has acknowledged it. A message published while no consumer is interested is not stored at all (it still consumes a sequence number, [section 6.14](#614-retention-rules-visible-to-publishers)). |
| `workqueue` | As for `limits`, and also as soon as the one consumer whose filter matches it acknowledges it. Consumers of a work queue may not overlap. |

**Limits** bound the log. What happens when a limit is reached is the **discard policy**: `old`
(default) removes the oldest messages to make room, so a publish always succeeds; `new` rejects the
publish with an error acknowledgment and leaves the log unchanged
([section 6.13](#613-size-limits-full-streams-and-overload)).

**Replication** (`num_replicas`, 1 to 5) places the log on that many servers of a cluster. The
replicas form a Raft group with one **leader**. Only the leader subscribes to the stream's subjects,
validates publishes, proposes them to the group and sends the acknowledgment, which is sent after
the message is committed (applied by a quorum). With one replica there is no Raft group and the
single server stores and acknowledges.

**Delivery guarantees** follow from the acknowledgment being optional and from the transport being
at-most-once:

- A publish without a reply subject is *fire and forget*: stored if all checks pass, silently
  dropped otherwise, and the publisher learns nothing either way.
- A publish with a reply subject that receives a success PubAck is durably stored (subject to
  `persist_mode`, [section 3.5](#35-storage-and-replication)). A publish whose acknowledgment does
  not arrive is in an unknown state: the message or the acknowledgment may have been lost. The
  publisher must retry, and therefore the stream may contain the message twice. This is
  **at-least-once** publishing.
- The **deduplication window** turns at-least-once into effectively-once for retries: a publish
  carrying `Nats-Msg-Id` whose id was already seen within `duplicate_window` (default 2 minutes) is
  not stored again; the publisher receives the original sequence with `"duplicate": true`.
- **Optimistic concurrency**: a publish may carry an expectation about the current state
  (`Nats-Expected-Last-Sequence`, `Nats-Expected-Last-Subject-Sequence`,
  `Nats-Expected-Last-Msg-Id`, `Nats-Expected-Stream`). The leader checks it atomically with the
  append and rejects the publish if it does not hold. This is compare-and-set on the log, or on one
  subject within it, and is the primitive under key-value revisions
  ([04-kv-object-services.md](04-kv-object-services.md)).

Read-side guarantees (redelivery, acknowledgment of consumed messages, ordering) belong to
consumers and are in [03-consumers.md](03-consumers.md).

### 1.3 How the layering shapes the server and therefore the client

Because JetStream is implemented as internal subscribers inside the server, its behavior inherits
the properties of core NATS, and several of them are surprising if one expects an RPC system.

1. **The responder is whichever server holds the role, and everyone else is silent.** A JetStream
   API request is delivered, like any message, to every server that has interest. In a cluster each
   endpoint is answered by one role: the meta leader, the stream leader, the consumer leader, or any
   replica. Servers that do not hold the role drop the request without replying. A client never
   needs to find the leader; it sends the request and the right server answers. The price is that
   during a leader election **nobody** answers, and the client sees a timeout, not an error
   ([section 8](#8-cluster-visible-behavior)).
2. **Absence of interest is reported by core NATS, not by JetStream.** If no server subscribes to
   the request subject (JetStream is not enabled for the account, or a publish targets a subject no
   stream leader currently listens on), the core "no responders" status 503 comes back immediately.
   It has no JSON body and is not a JetStream error.
3. **Errors are data.** An API failure is a normal reply whose JSON contains an `error` object. A
   failed publish is a normal reply whose JSON contains an `error` object. A failed Direct Get is a
   reply with a status line in the header block and no body. The connection is never closed and no
   `-ERR` is sent because of a JetStream-level failure.
4. **The server protects itself by dropping.** The API has a bounded request queue; when it
   overflows, queued requests are discarded without a reply. A stream's ingest queue is bounded;
   when it overflows, publishes are dropped (with a 429 reply if they asked for one).
5. **The account is implied by the connection.** API subjects carry no account. The server
   attaches the caller's identity to the request internally (the `Nats-Request-Info` header added
   when the request crosses the service import of `$JS.API.>` into the system account) and resolves
   the account from that. Clients never set this header.
6. **Everything is a subject, so everything is subject to permissions and mappings.** The API
   prefix can be remapped (JetStream domains, cross-account imports), Direct Get puts the looked-up
   subject into the request subject so that permissions can be applied to it, and so on.

For a client these properties mean: every JetStream call is "send a request, wait for exactly one
reply or a timeout, then classify the reply" (JSON success, JSON error, status-only message, no
responders). The rest of this chapter defines the subjects, the bodies and the classification.

> **Client rule:** Build JetStream entirely on the core request/reply primitive. Each API call and
> each acknowledged publish has four possible outcomes that must be distinguished: (a) a reply with
> a JSON body and no `error` key, (b) a reply with a JSON body containing `error`, (c) a reply with
> an empty body and a status line in its headers (503 no responders, or a Direct Get status), (d) no
> reply before the deadline. Outcome (d) carries no information about whether the operation
> happened.

---

## 2. The API envelope

### 2.1 Transport model

The whole management API is request/reply on subjects under `$JS.API.>`. The server exports
`$JS.API.>` from its system account as a service, and every JetStream-enabled account receives an
import of it. One internal catch-all subscription dispatches each request to a handler chosen by
subject pattern.

Rules that hold for every endpoint:

- The request is a core `PUB` (or `HPUB`) with a reply subject. If the request has no reply subject
  the handler still runs and performs the operation, but sends nothing.
- The reply is exactly one message on the reply subject, sent as a plain `MSG` (no headers), whose
  payload is a JSON object. Exceptions: Direct Get ([section 5.7](#57-direct-get)), the snapshot
  chunk stream ([section 5.9](#59-snapshot)), `$JS.API.META.RESCUE` (one reply per server) and
  consumer pull requests ([03-consumers.md](03-consumers.md)).
- Each answered call increments the account's `api.total` counter (and `api.errors` for error
  replies) and emits an audit advisory ([section 9](#9-advisories)). Successful
  `STREAM.MSG.GET` replies and all Direct Gets bypass this layer: they are neither counted nor
  audited.

### 2.2 Request and response bodies

**Request body.** A JSON object, or nothing. The server treats a request as *empty* when the payload
has zero length, is exactly `{}`, or is JSON that decodes to an empty object. Several handlers parse
the body only if its first non-whitespace byte is `{` or `[`; any other payload (for example a bare
string) is silently treated as "no options".

Decoding is strict by default: the server decodes with unknown fields disallowed. An unknown key, a
wrong type, or an unknown enum string fails the request with error 10025 (`invalid JSON: ...`). A
server started with JetStream strict mode disabled falls back to lenient decoding (unknown fields
ignored). Direct Get is the exception: its request body is always decoded leniently.

> **Client rule:** Send only keys the target server knows. Omit optional keys that have their
> default value instead of sending them, so that a request built for a newer server still decodes
> on an older one whenever the feature is not actually used. Never send a key the server does not
> define: under the default strict mode it turns a valid request into error 10025.

**Response body.** Every JSON API response has a `type` key and, on failure, an `error` key. On
success the fields of the result object appear at the top level next to `type` (the server embeds
the result struct):

```json
{"type":"io.nats.jetstream.api.v1.stream_delete_response","success":true}
```

```json
{"type":"io.nats.jetstream.api.v1.stream_create_response","error":{"code":404,"err_code":10059,"description":"stream not found"}}
```

The `type` string identifies the response schema. It is `io.nats.jetstream.api.v1.` followed by a
name such as `stream_create_response`; the full list is in
[section 2.9](#29-complete-api-subject-table). The type is set before the handler runs, so an error
response normally carries the type of the endpoint that was called. Two cases return a different
type: a request rejected by the dispatcher carries `io.nats.jetstream.api.v1.system_response`, and
some operations answer with another endpoint's type (restore completes with a
`stream_create_response`; a system-account stream move answers with `stream_update_response`).

> **Client rule:** Do not validate `type` against the endpoint called and do not dispatch on it.
> Decide success or failure only by the presence of `error`.

**The error object.**

| JSON key | Type | Meaning |
|---|---|---|
| `code` | int | HTTP-like class of the failure. Values in use: 400, 404, 409, 412, 429, 500, 503. |
| `err_code` | uint16, *omit* | The JetStream error code, 10002 to 10228 in this server version. Each code identifies one error constant ([Appendix A](#appendix-a-complete-error-code-table)). |
| `description` | string, *omit* | Human-readable text. Many descriptions are templates with interpolated values (`wrong last sequence: 5`). |

`code` and `err_code` are different things. `code` is coarse and ambiguous: 171 of the 227 defined
errors have `code` 400, and several generic errors use `code` 500 with a free-form description
(error 10052 carries every stream configuration validation message). `err_code` is the stable
identifier. The reference Go client compares errors by `err_code` only.

> **Client rule:** Represent a JetStream error as (`code`, `err_code`, `description`). Match on
> `err_code`. Never match on `description`. Use `code` only as a fallback class (404 not found,
> 503 unavailable, 429 throttled) when the `err_code` is unknown, because newer servers add codes.
> Expose all three values to the application.

Go's standard JSON encoder is used for API responses, so the characters `<`, `>` and `&` inside
strings are emitted as `<`, `>` and `&`. A stream subject `orders.>` therefore
appears on the wire as `"orders.>"`. Any conforming JSON parser decodes this transparently.
Success publish acknowledgments are an exception; they are assembled by hand
([section 6.1](#61-mechanics-and-the-puback)).

Some response structs contain non-*omit* fields beside `error`. An error reply can therefore carry
zero-valued result fields, for example `"total":0,"offset":0,"limit":0` on a failed
`STREAM.INFO`, or `"purged":0` on a failed purge. They mean nothing.

### 2.3 Silence, system errors and no responders

Not every failure produces an error reply. The dispatcher and handlers behave as follows.

| Situation | What the client sees |
|---|---|
| Nothing is subscribed or imported for the subject: the account never had JetStream enabled (no `$JS.API.>` import), or a wrong API prefix is used | Core **no responders**: a reply with status `503`, no body, immediately ([01-core.md](01-core.md)). |
| Server reachable, JetStream disabled on that server | Each handler returns without replying: **timeout** (unless another server answers). |
| Account exists but has no JetStream | Error 10039 (`code` 503, `JetStream not enabled for account`). **Exception:** if the account has leaf node connections the server stays silent and lets the other side answer: timeout or the remote answer. |
| Subject under `$JS.API.>` matches no endpoint | `type` `io.nats.jetstream.api.v1.system_response`, error 10003 (`bad request`). |
| Subject matches more than one handler | Same `system_response` with error 10003. |
| Request lacks the internal `Nats-Request-Info` header (possible only for requests injected inside the system account) | Dropped silently; for the system account itself: `system_response` with error 10039, except for `$JS.API.INFO`. |
| API request queue over its limit | The request is **dropped with no reply**: timeout ([section 2.4](#24-the-request-queue-and-dropped-requests)). |
| Clustered; this server does not hold the role for the endpoint | This server is silent; the role holder answers. |
| Clustered; no meta leader | Silence (timeout) at first. Once the meta group has been leaderless for more than 10 seconds, servers answer with error 10008 (`code` 503, `JetStream system temporarily unavailable`). |
| Clustered; the stream's Raft group has no leader | A group member that knows the group is leaderless answers 10008; otherwise timeout. |
| Stream is offline or unsupported by the server | Error 10118 or 10194, delayed by 500 ms ([section 8.3](#83-offline-and-unsupported-streams)). `STREAM.MSG.GET` on an offline stream is deliberately not answered: timeout. |
| `Nats-Required-Api-Level` not satisfied | Error 10185 (`code` 412) ([section 2.7](#27-api-levels-and-_nats-metadata)). |
| Body does not decode | Error 10025 (`code` 400, `invalid JSON: ...`). |

The no-responders message produced by this server version has the header block
`NATS/1.0 503␍␊Nats-Subject: <request subject>␍␊␍␊` and an empty payload
([scenario 11.7](#117-publish-with-no-stream-on-the-subject)). The reference client recognizes it
by an empty payload and status `503` alone.

A constant for a second system type, `io.nats.jetstream.api.v1.system_overloaded`, is declared in
the server but never sent by this version.

> **Client rule:** For every API call apply this classification, in this order: (1) empty payload
> and header status 503: raise a distinct "no responders" error (for API calls this means
> "JetStream not enabled or not reachable with this prefix"; the Go client maps it to "JetStream
> not enabled" only for the account-info call and returns the raw no-responders error elsewhere);
> (2) no reply within the deadline: raise a timeout error; (3) payload is not a JSON object: raise
> an "invalid response" error; (4) `error` present: raise an API error built from it; (5) otherwise
> return the decoded result.

> **Client rule:** Treat a timeout on a JetStream API call as "unknown outcome". The request may
> have been dropped by the queue limit, may be waiting for a leader election, or may have been
> executed with the reply lost. Mutating calls that are idempotent on the server (create with an
> identical configuration, delete, purge) are safe to repeat; a client library should not retry
> automatically, but should make it possible for the application to do so.

### 2.4 The request queue and dropped requests

API requests are copied onto in-process queues and served by a pool of worker goroutines (the
smaller of the CPU count and 16). There are two queues. The "info" endpoints (`$JS.API.INFO`,
`STREAM.NAMES`, `STREAM.LIST`, `STREAM.INFO.*`, `CONSUMER.NAMES.*`, `CONSUMER.LIST.*`,
`CONSUMER.INFO.*.*`) go to a separate queue that is served at lower priority, so that monitoring
traffic cannot starve mutating requests.

Each queue has a limit of 10,000 pending requests by default (server options `request_queue_limit`
and `info_queue_limit`). When a request arrives and the queue is at its limit, the **entire queue
is drained and discarded**, not only the new request. None of the discarded requesters receives a
reply. The server logs a warning and publishes the advisory
`$JS.EVENT.ADVISORY.API.LIMIT_REACHED` to the system account with the number of dropped requests.

There is no JetStream-specific request size limit; the bound is the connection's `max_payload`.
Content limits enforced by validation are: asset name at most 255 bytes, description at most 4096
bytes, metadata at most 128 KiB summed over all keys and values.

> **Client rule:** Do not poll info endpoints in tight loops and do not fan out thousands of
> concurrent API requests; a burst can cause the server to discard every queued request of every
> client. Give every API call a deadline (the Go client's default is 5 seconds, applied only when
> the caller supplied none), and bound the number of concurrent API requests issued by helper code
> such as listers.

### 2.5 Subject prefix: domains and custom prefixes

All API subjects in this chapter are written with the default prefix `$JS.API`. Two mechanisms
replace it.

**Domains.** A server (typically a leaf node, or a separate cluster reached through one) can be
configured with a JetStream *domain* name. Its API is then also reachable as
`$JS.<domain>.API.>`; the server installs a subject mapping from that form to `$JS.API.>`. The
unprefixed form always addresses the JetStream of the server the client is connected to. For
bridging, the mapping is installed per sub-tree:

| `$JS.<domain>.API.` followed by | maps to |
|---|---|
| `INFO` | `$JS.API.INFO` |
| `STREAM.>` | `$JS.API.STREAM.>` |
| `CONSUMER.>` | `$JS.API.CONSUMER.>` |
| `DIRECT.>` | `$JS.API.DIRECT.>` |
| `META.>` | `$JS.API.META.>` |
| `SERVER.>` | `$JS.API.SERVER.>` |
| `ACCOUNT.>` | `$JS.API.ACCOUNT.>` |
| `$KV.>` | `$KV.>` |
| `$OBJ.>` | `$OBJ.>` |

The last two rows mean that key-value and object store *publish* subjects in another domain are
addressed as `$JS.<domain>.API.$KV.>` and `$JS.<domain>.API.$OBJ.>`
([04-kv-object-services.md](04-kv-object-services.md)). A domain name is valid if
`$JS.<domain>.API.>` is a valid subject and the name is not reserved.

**Custom prefix.** An account can import another account's JetStream API under an arbitrary
prefix, for example `acme.js.>` mapped to `$JS.API.>` of the exporting account. The client then
uses that prefix in place of `$JS.API`.

In both cases the substitution applies to **every** API subject, including `DIRECT.GET` and the
consumer pull subject `CONSUMER.MSG.NEXT`. Subjects that are not API calls (the subjects messages
are published to, acknowledgment reply subjects, advisories) are not affected by the client-side
prefix.

The server reports its domain in responses: `domain` in a PubAck, in stream info, in account info
and in most advisories.

> **Client rule:** Hold one API prefix per JetStream context: `$JS.API` by default,
> `$JS.<domain>.API` when a domain is configured, or a user-supplied prefix. Build every API
> subject as `<prefix>.<rest>`. Reject an empty domain and an empty prefix, and strip a trailing
> `.` from a user-supplied prefix before joining. Constructing the context performs no network
> round trip. When a stream configuration names a mirror or source in another domain, express it as
> `external.api` = `$JS.<domain>.API` ([section 3.9](#39-mirror-and-sources)).

### 2.6 Paging

List-type responses are paged by the server. The page size is fixed by the server and cannot be
requested.

Request fragment: `{"offset": <int>}` (other keys depend on the endpoint). Response fragment, always
present on paged endpoints:

| JSON key | Type | Meaning |
|---|---|---|
| `total` | int | Number of items that match in total. |
| `offset` | int | Offset of the first item in this page (the request offset clamped to `[0, total]`; negative offsets are treated as 0). |
| `limit` | int | The server's page size. |

| Endpoint | Page size | Items | Order |
|---|---|---|---|
| `STREAM.NAMES` | 1024 | `streams`: array of names | Ascending by name. |
| `STREAM.LIST` | 256 | `streams`: array of stream info | By name. |
| `STREAM.INFO` with `subjects_filter` | 100,000 | `state.subjects`: map of subject to count | Subjects sorted, slice `[offset, offset+100000)`. |
| `CONSUMER.NAMES` / `CONSUMER.LIST` | 1024 / 256 | see [03-consumers.md](03-consumers.md) | |

For `STREAM.INFO`, `total`, `offset` and `limit` are populated only when `subjects_filter` matched
at least one subject; otherwise all three are 0.

> **Client rule:** Page with this loop: start with `offset = 0`; send the request; append the
> returned items; add the number of items **received** to `offset`; stop when `offset >= total`.
> Also stop when a page arrives with zero items while `offset < total` (the reference client would
> spin in that case). For stream-info subject paging, merge each page's `state.subjects` into one
> map, set `offset` to the size of the merged map, and stop when `total == 0` or `total <= offset`.
> One deadline should cover the whole iteration.

### 2.7 API levels and `_nats.*` metadata

The server implements an integer **API level** (5 in this version) that increases when features are
added. It is advertised in account info as `api.level` and in asset metadata.

**Request header `Nats-Required-Api-Level: <int>`.** A client may add this header to an API request
to require a minimum level. If the header is present and its value is not an integer, or the
server's level is lower than the value, the server rejects the request:

| Where | Result |
|---|---|
| JSON API endpoints | Error 10185, `code` 412, `JetStream minimum api level required`. Checked after the leader checks, before the body is decoded. |
| Direct Get | Status line `NATS/1.0 412 Required Api Level`, no body. |
| Atomic and fast batch publishes | Checked on every message; failure abandons the batch (advisory reason `unsupported`) and returns 10185. The header is always removed before the message is stored. |

The header exists so that a request relying on a newer feature fails loudly on an older server
instead of being half-understood. The reference Go client does not send it; it detects missing
features after the fact by comparing the configuration it sent with the configuration the server
echoed back.

**Metadata keys.** The server reserves the metadata key prefix `_nats.` on stream (and consumer)
configurations:

| Key | Stored | Meaning |
|---|---|---|
| `_nats.req.level` | yes | Minimum API level required by the features this configuration uses. Always written by the server on create and update; `"0"` if no leveled feature is used. |
| `_nats.ver` | no (added to responses) | Version string of the server that produced the response. |
| `_nats.level` | no (added to responses) | API level of the server that produced the response. |

The two dynamic keys are stripped from what is stored and added to create, update and info
responses. (The standalone `STREAM.LIST` path returns configurations without the dynamic keys.)
Consequently a configuration read back from the server always has a non-empty `metadata` map even
if the client sent none. The server ignores the `_nats.*` keys when it compares configurations for
create idempotency.

Feature to required level:

| Configuration uses | `_nats.req.level` | Introduced (per source comments) |
|---|---|---|
| `allow_msg_ttl` or `subject_delete_marker_ttl > 0` | 1 | v2.11 |
| `allow_msg_counter` | 2 | v2.12 |
| `allow_atomic` | 2 | v2.12 |
| `allow_msg_schedules` | 2 | v2.12 |
| `persist_mode: "async"` | 2 | v2.12 |
| `allow_batched` | 4 | v2.14 |

An asset whose stored `_nats.req.level` is higher than the level of the server that loads it, or
whose stored configuration does not decode strictly (unknown field or enum value), is loaded as
**unsupported**; it is offline on that server and API calls touching it fail with error 10194
([section 8.3](#83-offline-and-unsupported-streams)).

> **Client rule:** Treat metadata keys starting with `_nats.` as server-owned: never set them, and
> ignore them when comparing a desired configuration with one read back. To learn what a server
> supports, read `api.level` from account info or `_nats.level` from any stream response; do not
> parse version strings. Sending `Nats-Required-Api-Level` is optional; a client that sends it must
> map error 10185 and Direct Get status 412 to a distinct "server too old for this feature" error.

### 2.8 Handler check order

Every JSON API handler performs its checks in the same order. Knowing the order explains which
error wins when several conditions hold.

1. JetStream enabled on this server; otherwise silent.
2. Parse the internal request info; otherwise silent.
3. Clustered: no cluster state: silent. Meta group leaderless: error 10008. This server not the
   responsible leader: silent.
4. `Nats-Required-Api-Level`: error 10185.
5. Account has JetStream: error 10039 (or silent with leaf nodes).
6. Decode the body: error 10025.
7. Endpoint-specific validation and execution.

### 2.9 Complete API subject table

Every response `type` is `io.nats.jetstream.api.v1.` followed by the name in the "Response type"
column. "Answered by": **ML** meta leader (the only server when standalone); **SL** stream leader;
**CL** consumer leader; **any** any replica; **SYS** callable from the system account only.
"Body": the request JSON object; "empty" means the payload must be or may be empty; "optional" means
an empty payload is valid.

| Subject (`$JS.API.` +) | Request body | Response type | Answered by | Section |
|---|---|---|---|---|
| `INFO` | ignored | `account_info_response` | ML | [5.14](#514-account-info) |
| `STREAM.CREATE.<stream>` | stream configuration, plus `pedantic` | `stream_create_response` | ML | [5.1](#51-create) |
| `STREAM.UPDATE.<stream>` | stream configuration, plus `pedantic` | `stream_update_response` | ML | [5.2](#52-update) |
| `STREAM.NAMES` | optional `{offset, subject}` | `stream_names_response` | ML | [4.6](#46-stream-names) |
| `STREAM.LIST` | optional `{offset, subject}` | `stream_list_response` | ML (gathers from stream leaders) | [4.7](#47-stream-list) |
| `STREAM.INFO.<stream>` | optional `{offset, deleted_details, subjects_filter}` | `stream_info_response` | SL | [4.5](#45-stream-info-request) |
| `STREAM.DELETE.<stream>` | must be empty | `stream_delete_response` | ML | [5.4](#54-delete) |
| `STREAM.PURGE.<stream>` | optional `{filter, seq, keep}` | `stream_purge_response` | SL | [5.5](#55-purge) |
| `STREAM.MSG.GET.<stream>` | required, message get request | `stream_msg_get_response` | SL | [5.6](#56-message-get) |
| `STREAM.MSG.DELETE.<stream>` | required `{seq, no_erase}` | `stream_msg_delete_response` | SL | [5.8](#58-message-delete) |
| `DIRECT.GET.<stream>` | required, message get request | none: raw message(s) with headers | any replica and participating mirrors (queue group) | [5.7](#57-direct-get) |
| `DIRECT.GET.<stream>.<subject>` | empty or `{"no_hdr":true}` | none: raw message with headers | any replica and participating mirrors | [5.7](#57-direct-get) |
| `STREAM.SNAPSHOT.<stream>` | required, snapshot request | `stream_snapshot_response`, then a chunk stream | SL | [5.9](#59-snapshot) |
| `STREAM.RESTORE.<stream>` | required `{config, state}` | `stream_restore_response`; the final result is a `stream_create_response` | ML | [5.10](#510-restore) |
| `STREAM.LEADER.STEPDOWN.<stream>` | optional `{placement}` | `stream_leader_stepdown_response` | SL | [5.11](#511-leader-stepdown) |
| `STREAM.PEER.REMOVE.<stream>` | required `{peer}` | `stream_remove_peer_response` | ML | [5.12](#512-peer-remove-evacuate-and-cancel-move) |
| `STREAM.PEER.EVACUATE.<stream>` | required `{peer}` | `stream_evacuate_peer_response` | ML | [5.12](#512-peer-remove-evacuate-and-cancel-move) |
| `STREAM.CANCEL_MOVE.<stream>` | empty | `stream_cancel_move_response` | ML | [5.12](#512-peer-remove-evacuate-and-cancel-move) |
| `CONSUMER.CREATE.<stream>` | consumer create request | `consumer_create_response` | ML | [03-consumers.md](03-consumers.md) |
| `CONSUMER.CREATE.<stream>.<consumer>` and `CONSUMER.CREATE.<stream>.<consumer>.<filter>` | consumer create request | `consumer_create_response` | ML | 03 |
| `CONSUMER.DURABLE.CREATE.<stream>.<consumer>` | consumer create request (legacy) | `consumer_create_response` | ML | 03 |
| `CONSUMER.NAMES.<stream>` | optional `{offset}` | `consumer_names_response` | ML | 03 |
| `CONSUMER.LIST.<stream>` | optional `{offset}` | `consumer_list_response` | ML | 03 |
| `CONSUMER.INFO.<stream>.<consumer>` | empty | `consumer_info_response` | CL | 03 |
| `CONSUMER.DELETE.<stream>.<consumer>` | empty | `consumer_delete_response` | ML | 03 |
| `CONSUMER.PAUSE.<stream>.<consumer>` | pause request | `consumer_pause_response` | ML | 03 |
| `CONSUMER.RESET.<stream>.<consumer>` | reset request | `consumer_reset_response` | see 03 | 03 |
| `CONSUMER.UNPIN.<stream>.<consumer>` | unpin request | `consumer_unpin_response` | CL | 03 |
| `CONSUMER.MSG.NEXT.<stream>.<consumer>` | pull request | none: messages and status messages | CL | 03 |
| `CONSUMER.LEADER.STEPDOWN.<stream>.<consumer>` | optional `{placement}` | `consumer_leader_stepdown_response` | CL | 03 |
| `CONSUMER.PEER.REMOVE.<stream>.<consumer>` | `{peer}` | `consumer_remove_peer_response` | ML | 03 |
| `CONSUMER.PEER.EVACUATE.<stream>.<consumer>` | `{peer}` | `consumer_evacuate_peer_response` | ML | 03 |
| `META.LEADER.STEPDOWN` | optional `{placement}` | `meta_leader_stepdown_response` | SYS, ML | [5.13](#513-system-account-endpoints) |
| `SERVER.REMOVE` | required `{peer, peer_id}` | `meta_server_remove_response` | SYS, ML | [5.13](#513-system-account-endpoints) |
| `SERVER.EVACUATE` | required `{peer, peer_id}` | `meta_server_evacuate_response` | SYS, ML | [5.13](#513-system-account-endpoints) |
| `META.RESCUE` | required `{quorum_needed}` | `meta_rescue_response`, one per online server | SYS, every server | [5.13](#513-system-account-endpoints) |
| `ACCOUNT.PURGE.<account>` | empty | `account_purge_response` | SYS | [5.13](#513-system-account-endpoints) |
| `ACCOUNT.STREAM.MOVE.<account>.<stream>` | `{server, cluster, domain, tags}` | `stream_update_response` | SYS or the named account, ML | [5.13](#513-system-account-endpoints) |
| `ACCOUNT.STREAM.CANCEL_MOVE.<account>.<stream>` | empty | `stream_cancel_move_response` | SYS or the named account, ML | [5.13](#513-system-account-endpoints) |

The `STREAM.TEMPLATE.*` endpoints of old servers do not exist in this version; only their error
constants remain.

Other JetStream subject spaces a client meets (for subscribe and publish permissions):

| Subject | Purpose |
|---|---|
| `$JS.ACK.<stream>.<consumer>...` and `$JS.ACK.<domain>.<account hash>.<stream>.<consumer>...` | Acknowledgment reply subjects of delivered messages ([03-consumers.md](03-consumers.md)). |
| `$JS.FC.<stream>.<consumer>.*` | Push consumer flow control replies. |
| `$JS.SNAPSHOT.ACK.<stream>.<id>` (+ `.<chunk length>.<index>` on each chunk) | Reply subjects of snapshot chunks ([section 5.9](#59-snapshot)). |
| `$JS.SNAPSHOT.RESTORE.<stream>.<id>` | Upload subject of a restore ([section 5.10](#510-restore)). |
| `$JS.EVENT.ADVISORY.>`, `$JS.EVENT.METRIC.>` | Advisories and metrics ([section 9](#9-advisories)). |

The prefix `$JS.` is reserved: the server never accepts a subject starting with `$JS.` as a
user-provided reply or delivery destination. Leaf node isolation between JetStream domains is done
by denying `$JS.API.>`, `$KV.>` and `$OBJ.>` across the leaf connection.

### 2.10 Timeouts and error mapping in the reference client

The Go client issues every API call as: subject = prefix + rest; one core request with the
caller's deadline, or 5 seconds if the caller gave none; decode JSON; if `error` is present map a
few `err_code` values to named errors and return every other error as a generic API error carrying
the three fields. It adds no headers and performs no retries at this layer. Listers apply one
deadline to the whole paged iteration.

> **Client rule:** Provide a single internal `api_request(subject_suffix, body, deadline)` that
> implements the classification of [section 2.3](#23-silence-system-errors-and-no-responders), and
> build every management call on it. Give API errors one class with the fields `code`, `err_code`,
> `description`, plus named subclasses or predicates for the codes listed in
> [section 10](#10-errors-every-client-must-special-case).

---

## 3. Stream configuration

A stream is defined entirely by its configuration object. The same object is the body of create and
update requests, is echoed in every response that describes the stream, and is what a client
compares when it decides whether an existing stream is "the same". This section lists every field.

### 3.1 Conventions and pedantic mode

- Create and update requests are the configuration object with one extra, request-scoped key:
  `pedantic` (bool, *omit*, default `false`).
- The server **normalizes** a configuration before storing it: it fills defaults (`-1` for
  unlimited, a default `duplicate_window`, ...) and in some cases overrides contradictory settings.
  The response contains the normalized configuration. With `"pedantic": true` the server refuses to
  adjust anything silently and instead fails with error 10157 (`code` 400,
  `pedantic mode: <reason>`). The cases where pedantic mode changes the outcome are marked
  "(pedantic: error)" below.
- Most validation failures surface as error **10052** with `code` **500** and the validation message
  as the description (the message texts are quoted below so they can be recognized in logs, but a
  client must not match on them). Failures that have a dedicated code are listed with that code.
- "Level" is the `_nats.req.level` the field's use requires
  ([section 2.7](#27-api-levels-and-_nats-metadata)); blank means 0.
- "Update" states what an update request may do with the field. "Mutable" means any valid value.

> **Client rule:** Model the configuration as a plain value object with one attribute per key
> below, serialize durations as integer nanoseconds, and omit keys that are unset. Parse responses
> leniently (ignore unknown keys) so that configurations written by newer servers can still be
> read. Do not validate configuration contents client-side beyond the stream name; the server is
> the authority and its rules change between versions.

### 3.2 Identity and metadata

| JSON key | Type | Default | Validation and semantics | Update | Level |
|---|---|---|---|---|---|
| `name` | string | required | Non-empty; at most 255 bytes; must not contain space, TAB, CR, LF, form feed, `.`, `*`, `>`, `\` or `/`. Violation: 10052 `stream name is required and can not contain '.', '*', '>', '\\', '/'`; path separators are also caught earlier as 10128. Must equal the `<stream>` token of the request subject, else 10056. | Immutable (`stream configuration name must match original`). | |
| `description` | string, *omit* | none | At most 4096 bytes. | Mutable. | |
| `metadata` | map of string to string, *omit* | server adds `_nats.req.level` | Sum of key and value lengths at most 128 KiB (`stream metadata exceeds maximum size of 131072 bytes`). Keys starting with `_nats.` are server-owned ([section 2.7](#27-api-levels-and-_nats-metadata)). | Mutable. | |
| `consumer_limits` | object, always present (possibly `{}`) | `{}` | Defaults and caps applied to consumers of this stream: `inactive_threshold` (duration, *omit*) and `max_ack_pending` (int, *omit*). | Mutable; the update fails if an existing consumer exceeds a new limit (`change to limits violates consumers: ...`, as 10052 standalone or 10069 clustered). | |

> **Client rule:** Validate the stream name before any request: non-empty and free of `>`, `*`,
> `.`, space, `/`, `\`, TAB, CR, LF. An invalid name would otherwise produce a malformed API
> subject. This is the only client-side validation the reference client performs on streams.

### 3.3 Subjects

| JSON key | Type | Default | Validation and semantics | Update | Level |
|---|---|---|---|---|---|
| `subjects` | array of string, *omit* | `[<name>]` if empty and the stream has no mirror and no sources | Each entry a valid subject (wildcards allowed). No duplicates, and no two entries may overlap each other. Must not overlap the subjects of another stream in the account: 10065. Not allowed together with `mirror`: 10034. A stream needs at least one subject, a source, or a mirror. Special cases follow. | Mutable; the server changes its subscriptions live. | |

Restrictions on what a stream may capture:

- The full wildcard `>` requires `no_ack: true` and `num_replicas: 1`.
- A subject that overlaps `$JS.>`, `$JSC.>` or `$NRG.>` requires `no_ack: true`. Exception: subjects
  that are subsets of `$JS.EVENT.>` (a stream may capture advisories and still acknowledge).
- A subject that overlaps `$SYS.>` requires `no_ack: true`. Exception: subsets of `$SYS.ACCOUNT.>`.

The reason is that a stream which acknowledges everything it captures would otherwise reply to
internal protocol messages (including its own acknowledgments).

### 3.4 Retention and limits

| JSON key | Type | Default | Validation and semantics | Update | Level |
|---|---|---|---|---|---|
| `retention` | string enum | `"limits"` | `"limits"`, `"interest"`, `"workqueue"` ([section 1.2](#12-what-each-piece-guarantees)). | `limits` and `interest` may be exchanged; any change to or from `workqueue` is rejected (`can not change retention policy to/from workqueue`). | |
| `max_consumers` | int | `-1` | `-1` unlimited. `0` or a value below `-1` becomes `-1` (pedantic: error if below `-1`). | Mutable. | |
| `max_msgs` | int64 | `-1` | Maximum number of messages; `-1` unlimited. | Mutable. | |
| `max_bytes` | int64 | `-1` | Maximum total size; `-1` unlimited. An account may require it (10113 if the account sets `max_bytes_required` and the value is not positive) and may cap it (10122). A positive value is a storage **reservation** against account and server limits: 10047, 10028, or 10053 when it cannot be reserved. | Mutable; only a positive increase is newly reserved. | |
| `max_age` | duration | `0` (unlimited) | Negative: `max age can not be negative`. Non-zero below 100 ms: `max age needs to be >= 100ms`. | Mutable; forced to 0 when sealing. | |
| `max_msgs_per_subject` | int64 | `-1` | Maximum number of messages kept per distinct subject; `-1` unlimited. | Mutable. | |
| `max_msg_size` | int32, *omit* | `-1` | Maximum size of one message, measured as header block plus payload ([section 6.13](#613-size-limits-full-streams-and-overload)); `-1` unlimited. For a mirror or source whose origin is visible locally and both have limits, this limit must be at least the origin's (10030, 10046). | Mutable. | |
| `discard` | string enum | `"old"` | `"old"` or `"new"`. Decoded case-insensitively. `"new"` is forbidden with `allow_msg_counter` and with `allow_msg_schedules`. | Mutable; forced to `"new"` when sealing. | |
| `discard_new_per_subject` | bool, *omit* | `false` | Makes `max_msgs_per_subject` reject new messages instead of dropping the oldest for that subject. Requires `discard: "new"` and `max_msgs_per_subject > 0`. | Mutable (re-validated). | |

### 3.5 Storage and replication

| JSON key | Type | Default | Validation and semantics | Update | Level |
|---|---|---|---|---|---|
| `storage` | string enum | `"file"` | `"file"` or `"memory"`. | Immutable (`can not change storage type`). | |
| `num_replicas` | int | `1` (`0` becomes `1`) | 1 to 5 (`maximum replicas is 5`). Negative: 10133. Greater than 1 on a standalone server: 10074. | Mutable when clustered (scale up or down); may not be combined with a move in one update (10123). | |
| `placement` | object, *omit* | none; `{}` is normalized to absent | `cluster` (string) and `tags` (array of string) select the servers that may host the stream. `preferred` is rejected here (`preferred server not permitted in placement`); it is valid only in stepdown requests. Unsatisfiable placement: 10005. | Changing it is a **move request** (clustered). | |
| `compression` | string enum | `"none"` | `"none"` or `"s2"` (file storage block compression). | No update rule exists in the server's update check; the effect of changing it is unverified. | |
| `first_seq` | uint64, *omit* | `0` | Sequence number the first stored message will receive (the stream starts as if purged to it). Forbidden with `mirror`: 10143. | No update rule exists in the server's update check; the effect of changing it is unverified. | |
| `persist_mode` | string enum, *omit* | `"default"` | `"default"`: the server flushes to storage before acknowledging. `"async"`: the acknowledgment may be sent before the write is persisted. `""` decodes as default. `"async"` requires file storage, `num_replicas` 1, and not `allow_atomic`. | Immutable (`can not change persist mode`). | 2 for `"async"` |

### 3.6 Deduplication and acknowledgment

| JSON key | Type | Default | Validation and semantics | Update | Level |
|---|---|---|---|---|---|
| `duplicate_window` | duration, *omit* | If 0 and the stream has no mirror and no sources: the smallest of 2 minutes, the server's `max_duplicate_window`, and `max_age` when set. Streams with a mirror or sources keep 0, which **disables** deduplication. | Negative: error. Larger than `max_age` when that is set: `duplicates window can not be larger then max age`. Larger than the server limit: error. Non-zero below 100 ms: `duplicates window needs to be >= 100ms`. | Mutable. | |
| `no_ack` | bool, *omit* | `false` | The stream never sends publish acknowledgments and never reports publish errors ([section 6.1](#61-mechanics-and-the-puback)). | Mutable. | |

### 3.7 Protection flags

| JSON key | Type | Default | Validation and semantics | Update | Level |
|---|---|---|---|---|---|
| `sealed` | bool | `false` | A sealed stream accepts no publishes, deletes or purges. **May not be true on create** (`stream configuration for create can not be sealed`). Sealing by update also forces `max_age = 0`, `discard = "new"`, `deny_delete = true`, `deny_purge = true`, `allow_rollup_hdrs = false`. | One way, false to true (`can not unseal a sealed stream`). | |
| `deny_delete` | bool | `false` | Blocks `STREAM.MSG.DELETE`. | One way, false to true (`can not cancel deny message deletes`). | |
| `deny_purge` | bool | `false` | Blocks `STREAM.PURGE`. Incompatible with `allow_rollup_hdrs` (`roll-ups require the purge permission`). | One way, false to true (`can not cancel deny purge`). | |
| `allow_rollup_hdrs` | bool | `false`; forced to `true` (and `deny_purge` to `false`) when `subject_delete_marker_ttl > 0` or `allow_msg_schedules` is set (pedantic: error) | Enables the `Nats-Rollup` publish header ([section 6.7](#67-rollups)). | Mutable; forced false when sealing. | |

### 3.8 Direct access, subject transform and republish

| JSON key | Type | Default | Validation and semantics | Update | Level |
|---|---|---|---|---|---|
| `allow_direct` | bool | `false` | Enables Direct Get on `$JS.API.DIRECT.GET.<stream>` and `$JS.API.DIRECT.GET.<stream>.<subject>` ([section 5.7](#57-direct-get)). | Mutable. | |
| `mirror_direct` | bool | For a mirror whose origin is visible locally: forced to the origin's `allow_direct` (pedantic: error if they differ) | Makes a mirror answer Direct Get requests addressed to the **origin** stream's name. `true` without `mirror`: 10052 `stream has no mirror but does have mirror direct`. | Mutable. | |
| `subject_transform` | object `{src, dest}`, *omit* | none | Rewrites the subject of each incoming message before anything else happens ([section 7.3](#73-stream-subject-transform)). `src` must be a valid subject if non-empty (10155); the mapping must be valid (10156). Subjects that do not match `src` are left unchanged. | Mutable. | |
| `republish` | object `{src, dest, headers_only}`, *omit* | none; empty `src` becomes `">"` (pedantic: error) | After a message is stored, publish a copy to a core subject ([section 7.4](#74-republish)). `src` (*omit*) filters which stored subjects are republished, `dest` is the destination mapping, `headers_only` (bool, *omit*) suppresses the payload. `dest` must not overlap any stream subject (`republish destination forms a cycle`); `src` to `dest` must form a valid transform. Special case: `src = ">"`, `dest = ">"` together with a `subject_transform` whose `src` is the stream's only subject is rewritten to the transform destination (pedantic: error). | Mutable. | |

### 3.9 Mirror and sources

A **mirror** is a read-only copy of exactly one other stream, at the same sequence numbers. A
stream with **sources** aggregates copies of messages from one or more other streams into its own
sequence space, next to messages published to its own subjects. Behavior is in
[section 7](#7-mirrors-sources-subject-transforms-and-republish).

| JSON key | Type | Default | Validation and semantics | Update | Level |
|---|---|---|---|---|---|
| `mirror` | stream source object, *omit* | none | See below. | Cannot be changed. It can only be **removed**, which promotes the mirror to a normal stream; anything else: 10055. | |
| `sources` | array of stream source objects, *omit* | none | See below. Duplicate sources (same name, external, consumer, and filter or transform set): 10140. | Mutable. | |

The stream source object:

| JSON key | Type | Meaning and validation |
|---|---|---|
| `name` | string | Origin stream name. Must be a valid stream name (10141 for a source; 10142 for a mirror, checked only when `external` is absent). |
| `opt_start_seq` | uint64, *omit* | Start copying at this origin sequence. |
| `opt_start_time` | RFC 3339 time, *omit* | Start copying at the first message at or after this time. |
| `filter_subject` | string, *omit* | Copy only matching subjects. Mutually exclusive with `subject_transforms` (10144, 10150). |
| `subject_transforms` | array of `{src, dest}`, *omit* | Filter and rewrite subjects while copying. The filters of one stream's sources must not overlap (10147, 10152). Invalid `src`: 10145, 10151. Invalid mapping: 10146, 10154. |
| `external` | object `{api, deliver}`, *omit* | The origin lives in another account or domain. `api` is the API prefix to reach it (for a domain: `$JS.<domain>.API`); it must be a valid publish subject without wildcards and must not collide with `$JS.API` (10021). `deliver` is the delivery subject prefix for the copying consumer (10024, 10022). |
| `consumer` | object `{name, deliver_subject}`, both *omit*; *omit* | Use a pre-created durable consumer on the origin for copying (ADR-60). `name` must be a valid name and `deliver_subject` a literal subject. Incompatible with `opt_start_seq`, `opt_start_time` and `filter_subject` (10215, 10213). The same consumer used twice: 10216. |

A mirror may not have: `subjects` (10034), `sources` (10031), `first_seq` (10143),
`allow_msg_counter` (10173), `allow_atomic` (10198), `allow_batched` (10209),
`allow_msg_schedules` (10186), or `subject_delete_marker_ttl` (`subject delete markers forbidden on
mirrors`). Cycles across mirrors and sources are rejected (10052 `detected cycle`).

### 3.10 Per-message features

Each of these flags enables a publish-side feature that is driven by headers on individual
messages ([section 6](#6-publishing)).

| JSON key | Type | Default | Validation and semantics | Update | Level |
|---|---|---|---|---|---|
| `allow_msg_ttl` | bool | `false`; forced to `true` when `subject_delete_marker_ttl > 0` (pedantic: error) | Enables the `Nats-TTL` header ([section 6.8](#68-per-message-ttl-and-delete-markers)). Incompatible with `allow_msg_counter`. | One way, false to true (`message TTL status can not be disabled`). | 1 |
| `subject_delete_marker_ttl` | duration, *omit* | `0` | When greater than 0, the server writes a *delete marker* when the last message of a subject is removed by age, and the marker itself lives this long. Negative: error. Non-zero below 1 s: `subject delete marker TTL must be at least 1 second`. Forbidden on mirrors. | Mutable. | 1 |
| `allow_msg_counter` | bool, *omit* | `false` | Makes the stream a counter stream ([section 6.9](#69-counters)): every message is an increment. Forbids `discard: "new"`, `allow_msg_ttl`, `allow_msg_schedules` and any retention other than `limits`. Forbidden on mirrors (10173). | **Immutable in both directions** (`can not change message counter setting`). | 2 |
| `allow_atomic` | bool, *omit* | `false` | Enables atomic batch publishing ([section 6.10](#610-atomic-batch-publishing)). Forbidden on mirrors (10198). Incompatible with `persist_mode: "async"`. | Mutable. ADR-50 states that disabling it abandons batches in flight. | 2 |
| `allow_batched` | bool, *omit* | `false` | Enables fast-ingest batch publishing ([section 6.11](#611-fast-ingest-batch-publishing)). Forbidden on mirrors (10209). | Mutable. | 4 |
| `allow_msg_schedules` | bool, *omit* | `false` | Enables message scheduling ([section 6.12](#612-message-scheduling)). Forbids `discard: "new"`; forces `allow_rollup_hdrs` (pedantic: error). Forbidden on mirrors (10186) and on streams with sources (10187). Incompatible with counters. | One way, false to true (`message schedules can not be disabled`). | 2 |

### 3.11 Enum strings

| Field | Exact wire values | Notes |
|---|---|---|
| `retention` | `limits`, `interest`, `workqueue` | |
| `discard` | `old`, `new` | Decoded case-insensitively; emitted in lower case. |
| `storage` | `file`, `memory` | |
| `compression` | `none`, `s2` | |
| `persist_mode` | `default`, `async` | `""` is accepted on input; the key is omitted on output when default. |

An unknown enum string fails JSON decoding: error 10025 with a description such as
`invalid JSON: can not unmarshal "<value>"`.

### 3.12 Create and update rules that span fields

- **Create is idempotent.** If a stream with that name exists and the normalized configuration is
  identical, ignoring the `_nats.*` metadata keys, the server returns the existing stream as a
  success. If the configuration differs: error **10058** (`code` 400, `stream name already in use
  with a different configuration`).
- `did_create` (bool, *omit*) on the create response is not a reliable "newly created" signal. The
  standalone server sets it to `true` on every successful create, including the idempotent case. In
  a cluster it is set when the assignment is actually processed, and an identical re-create is
  answered with a stream-info reply relabeled as a create response, without `did_create` (inferred
  from two code paths, not exercised).
- Account limits on create: 10027 (maximum number of streams), 10120 (no applicable limit tier),
  and the storage reservation errors of `max_bytes`.
- Wrapping: a create failure that is not already an API error is wrapped as 10049; an update
  failure as 10069 or 10052. All three have `code` 500 and a free-form description.
- **Update takes a full configuration**, not a patch. It is validated like a create and then checked
  against the "Update" rules above. Updating a stream that does not exist: 10059.

> **Client rule:** Offer three operations and implement them the way the reference client does.
> `create`: send `STREAM.CREATE`; map 10058 to "name already in use". `update`: send
> `STREAM.UPDATE`; map 10059 to "stream not found". `create_or_update`: send `STREAM.UPDATE` first
> and, only if it fails with 10059, send `STREAM.CREATE`. (Update-first makes the common "already
> exists" case one round trip and never trips 10058.) Do not use `did_create`.

> **Client rule:** An update must send the complete desired configuration. To change one setting,
> read the current configuration (stream info), drop the `_nats.*` metadata keys, change the
> setting and send the whole object back. Sending a partial object resets every omitted field to
> its default, which either changes the stream or fails on an immutable field.

> **Client rule:** After create or update, the echoed `config` is the truth. Because a server that
> does not know a field either rejects it (strict mode) or drops it (lenient mode), the reference
> client compares request and response for the features it cares about and raises a "not supported
> by server" error when a requested `subject_transform` is missing from the response, when the
> number of `sources` differs, or when a source's `subject_transforms` count differs. Note that the
> stream has already been created or changed when this check fails.

### 3.13 Examples

A minimal request is valid: `{"name":"ORDERS"}` creates a file-backed, single-replica,
limits-retention stream whose only subject is `ORDERS`.

A request that states the common fields explicitly:

```json
{
  "name": "ORDERS",
  "description": "order events",
  "subjects": ["orders.>"],
  "retention": "limits",
  "max_consumers": -1,
  "max_msgs": -1,
  "max_bytes": -1,
  "max_age": 86400000000000,
  "max_msgs_per_subject": -1,
  "max_msg_size": 1048576,
  "discard": "old",
  "storage": "file",
  "num_replicas": 3,
  "duplicate_window": 120000000000,
  "placement": {"cluster": "east", "tags": ["ssd"]},
  "compression": "s2",
  "allow_direct": true,
  "allow_rollup_hdrs": true,
  "allow_msg_ttl": true,
  "subject_delete_marker_ttl": 60000000000,
  "republish": {"src": "orders.>", "dest": "audit.orders.>", "headers_only": true},
  "metadata": {"owner": "billing"}
}
```

A stream that aggregates two origins, one of them in another domain:

```json
{
  "name": "ALL_ORDERS",
  "storage": "file",
  "sources": [
    {"name": "ORDERS_EU", "filter_subject": "orders.eu.>"},
    {"name": "ORDERS_US",
     "subject_transforms": [{"src": "orders.us.>", "dest": "orders.na.>"}],
     "external": {"api": "$JS.us-west.API", "deliver": "sync.us-west"}}
  ]
}
```

A mirror that serves Direct Get for its origin:

```json
{"name": "ORDERS_REPLICA", "storage": "file", "mirror": {"name": "ORDERS"}, "mirror_direct": true}
```

The configuration as the server echoes it (key order and always-present keys as emitted by the
server; this is the `config` member of [scenario 11.1](#111-create-a-stream)):

```json
{
  "name": "ORDERS",
  "subjects": ["orders.>"],
  "retention": "limits",
  "max_consumers": -1,
  "max_msgs": -1,
  "max_bytes": -1,
  "max_age": 0,
  "max_msgs_per_subject": -1,
  "max_msg_size": -1,
  "discard": "old",
  "storage": "file",
  "num_replicas": 1,
  "duplicate_window": 120000000000,
  "compression": "none",
  "allow_direct": true,
  "mirror_direct": false,
  "sealed": false,
  "deny_delete": false,
  "deny_purge": false,
  "allow_rollup_hdrs": false,
  "consumer_limits": {},
  "allow_msg_ttl": false,
  "metadata": {"_nats.level": "5", "_nats.req.level": "0", "_nats.ver": "2.16.0-dev"}
}
```

---

## 4. Stream info and state

### 4.1 The stream info object

Returned by create, update, info, list, cancel-move and restore.

| JSON key | Type | Meaning |
|---|---|---|
| `config` | stream configuration | The normalized configuration, with the dynamic `_nats.*` metadata on create, update and info responses. |
| `created` | time | Creation time. |
| `state` | stream state | [Section 4.2](#42-stream-state). |
| `domain` | string, *omit* | JetStream domain of the answering server. |
| `cluster` | cluster info, *omit* | [Section 4.3](#43-cluster-info). Present in `STREAM.INFO` responses and clustered create responses; not included per stream in `STREAM.LIST`. |
| `mirror` | source info, *omit* | Progress of the mirror ([section 4.4](#44-mirror-and-source-info)). |
| `sources` | array of source info, *omit* | Progress of each source. |
| `alternates` | array of `{name, domain (omit), cluster}`, *omit* | Only on `STREAM.INFO`, only when clustered, and only when the stream has mirrors: this stream and its mirrors, ordered by preference for the requester's cluster, so that a client can choose a closer copy for reads. |
| `ts` | time | When this info was gathered. |

### 4.2 Stream state

| JSON key | Type | Meaning |
|---|---|---|
| `messages` | uint64 | Number of messages currently stored. |
| `bytes` | uint64 | Bytes currently stored. |
| `first_seq` | uint64 | Sequence of the oldest stored message. |
| `first_ts` | time | Its timestamp (`0001-01-01T00:00:00Z` when the stream is empty). |
| `last_seq` | uint64 | Sequence of the newest message ever stored. It never decreases, not even after a purge. |
| `last_ts` | time | Its timestamp. |
| `num_subjects` | int, *omit* | Number of distinct subjects stored. |
| `subjects` | map of subject to uint64, *omit* | Message count per subject. Only when `subjects_filter` was requested; paged ([section 2.6](#26-paging)). |
| `num_deleted` | int, *omit* | Number of sequences between `first_seq` and `last_seq` that hold no message (interior deletes). |
| `deleted` | array of uint64, *omit* | Those sequences. Only with `deleted_details: true`. |
| `lost` | object `{msgs: [uint64], bytes: uint64}`, *omit* | Messages the store detected as lost (corruption). |
| `consumer_count` | int | Number of consumers. In a cluster this is the cluster-wide assignment count when it is greater than 0. |

Because sequences are never reused and messages can be removed from anywhere in the log, a client
must not assume `messages == last_seq - first_seq + 1`; the difference is `num_deleted`.

### 4.3 Cluster info

| JSON key | Type | Meaning |
|---|---|---|
| `name` | string, *omit* | Cluster name. |
| `raft_group` | string, *omit* | Name of the stream's Raft group. |
| `leader` | string, *omit* | Server name of the current leader. Absent while there is none. |
| `leader_since` | time, *omit* | When it became leader. |
| `system_account` | bool, *omit* | The asset belongs to the system account. |
| `traffic_account` | string, *omit* | Account used for replication traffic. |
| `replicas` | array of peer info, *omit* | The other members of the group. The answering leader does not list itself. |
| `desired` | desired cluster info, *omit* | Present while a reconfiguration (move, scale, retention change) is in progress. |

Peer info:

| JSON key | Type | Meaning |
|---|---|---|
| `name` | string | Server name. |
| `current` | bool | The peer is caught up. |
| `offline` | bool, *omit* | The peer is considered offline. |
| `active` | duration | Nanoseconds since the peer was last seen. |
| `lag` | uint64, *omit* | Number of operations the peer is behind. |
| `peer` | string | Unique peer id. |
| `pending` | bool, *omit* | Part of the assignment but not yet a member of the Raft group, or being removed. |

Desired cluster info: `created` (time), `name` (*omit*), `replicas` (array of
`{name, offline (omit), peer}`), `origin` (*omit*; `{replicas: int, placement (omit), retention
(omit)}`, the state being moved away from), `status` (*omit*; `{description: string, type: string,
err: string (omit)}`). The `status.type` values are `meta`, `membership`, `snapshot`, `catchup`,
`quorum`, `blocked`, `unavailable`.

### 4.4 Mirror and source info

Used for `mirror` and for each element of `sources`.

| JSON key | Type | Meaning |
|---|---|---|
| `name` | string | Origin stream name. |
| `external` | object `{api, deliver}`, *omit* | As configured. |
| `lag` | uint64 | Messages still to be copied, as last reported by the copying consumer. |
| `seq` | uint64, *omit* | Declared in the schema; this server version does not set it. |
| `active` | duration | Nanoseconds since the last message or heartbeat from the origin; **`-1` if nothing was ever received**. |
| `error` | error object, *omit* | The last error while setting up the copying consumer. |
| `filter_subject` | string, *omit* | Active filter. |
| `subject_transforms` | array of `{src, dest}`, *omit* | Active transforms. |

### 4.5 Stream info request

`$JS.API.STREAM.INFO.<stream>`. The body is optional and is parsed only if it starts with `{` or
`[`.

| JSON key | Type | Effect |
|---|---|---|
| `offset` | int | Page offset into the sorted subject list. Meaningful only with `subjects_filter`. |
| `deleted_details` | bool, *omit* | Include `state.deleted`. |
| `subjects_filter` | string, *omit* | Include `state.subjects` for the matching subjects. Wildcards allowed; `>` means all subjects. |

The response is the stream info object plus `type` and the three paging keys, which are always
serialized (0 when subject paging is not in effect):

```json
{
  "type": "io.nats.jetstream.api.v1.stream_info_response",
  "total": 2, "offset": 0, "limit": 100000,
  "config": {"name": "ORDERS", "subjects": ["orders.>"], "retention": "limits", "...": "...",
             "metadata": {"_nats.level": "5", "_nats.req.level": "0", "_nats.ver": "2.16.0-dev"}},
  "created": "2026-01-01T00:00:00Z",
  "state": {"messages": 3, "bytes": 123, "first_seq": 1, "first_ts": "2026-01-01T00:00:01Z",
            "last_seq": 3, "last_ts": "2026-01-01T00:00:03Z",
            "num_subjects": 2, "subjects": {"orders.a": 2, "orders.b": 1}, "consumer_count": 0},
  "cluster": {"name": "C1", "raft_group": "S-R3F-abc", "leader": "n1",
              "replicas": [{"name": "n2", "current": true, "active": 1234567, "peer": "Xy12"},
                           {"name": "n3", "current": true, "active": 2345678, "peer": "Zq34"}]},
  "ts": "2026-01-01T00:00:04Z"
}
```

Errors: 10059 stream not found (`code` 404); 10118 stream offline (all peers offline); 10194 stream
unsupported or offline with a reason; 10008 meta group leaderless. The last three are delayed by
500 ms to give a real leader the chance to answer first. In a cluster the stream leader answers; if
the stream's group has no leader, another group member answers so that info stays available.

> **Client rule:** Send stream info with no body when no options are set. With `subjects_filter`,
> page as described in [section 2.6](#26-paging). If the client caches stream info (the reference
> client caches the last info per stream handle and uses it to choose between Direct Get and
> `MSG.GET`), do not keep the `state.subjects` map in the cache.

### 4.6 Stream names

`$JS.API.STREAM.NAMES`. Optional body `{"offset": <int>, "subject": "<filter>"}` (`subject` is
*omit*). An empty request means all streams. With `subject`, the server returns the streams that
have at least one configured subject overlapping the filter (wildcards on either side); in a
cluster, streams without subjects are matched by name.

```json
{"type":"io.nats.jetstream.api.v1.stream_names_response","total":2,"offset":0,"limit":1024,"streams":["A","B"]}
```

`streams` is not *omit* and is `null` when there are no streams.

> **Client rule:** Accept `null` as well as an array for `streams`. To find the stream that
> captures a subject, request `STREAM.NAMES` with `{"subject": "<subject>"}` and take the single
> name returned; zero names means no stream captures it.

### 4.7 Stream list

`$JS.API.STREAM.LIST`. Same optional request as names. The response contains full stream info
objects:

| JSON key | Type | Meaning |
|---|---|---|
| `total`, `offset`, `limit` | int | Paging; `limit` is 256. |
| `streams` | array of stream info | Never `null` (an empty array when there are none). |
| `missing` | array of string, *omit* | Names of streams that should have been listed but whose info could not be obtained. |
| `offline` | map of stream name to reason, *omit* | Streams that are offline on the answering side, with the reason text. |

Standalone, an offline stream appears in both `offline` and `missing` and is left out of
`streams`. In a cluster the meta leader gathers info from the stream leaders with an inactivity
timeout of 4 seconds and reports streams that did not answer in `missing`.

> **Client rule:** A list result is complete only if `missing` is absent or empty. Surface
> `missing` and `offline` to the caller instead of silently returning a short list.

---

## 5. Stream operations

Unless stated otherwise each operation is one request and one JSON reply, with the envelope rules of
[section 2](#2-the-api-envelope). Every operation can additionally fail with the generic outcomes
of [section 2.3](#23-silence-system-errors-and-no-responders); only operation-specific errors are
listed here.

### 5.1 Create

`$JS.API.STREAM.CREATE.<stream>`. Body: the stream configuration
([section 3](#3-stream-configuration)) plus optional `pedantic`. Answered by the meta leader.

Response: `type` `io.nats.jetstream.api.v1.stream_create_response`, the stream info keys at the top
level, and `did_create` (bool, *omit*). A byte-exact example is
[scenario 11.1](#111-create-a-stream).

Errors: 10056 name in the body differs from the subject; 10128 name contains path separators;
10058 name in use with a different configuration; 10052 configuration invalid; 10157 pedantic
violation; 10065 subject overlap; 10027, 10120, 10113, 10122, 10047, 10028, 10053 account limits
and reservations; 10074 replicas on a standalone server; 10005 no suitable peers; 10023
insufficient resources; 10049 any other failure. The advisory
`$JS.EVENT.ADVISORY.STREAM.CREATED.<stream>` is published.

### 5.2 Update

`$JS.API.STREAM.UPDATE.<stream>`. Body: a **full** stream configuration plus optional `pedantic`.
Answered by the meta leader. Response: `type` `io.nats.jetstream.api.v1.stream_update_response`
and the stream info keys.

Errors: 10059 stream not found; 10194 stream offline or unsupported (delayed 500 ms); the
validation errors of create; the update-rule violations of [section 3](#3-stream-configuration)
(as 10052 or 10069; 10055 for a mirror change). In a cluster additionally:

| Condition | Error |
|---|---|
| `placement` changed together with `num_replicas` or `retention` | 10123 `can not move and scale a stream in a single update` |
| A move is requested while one is in progress | 10124 `stream move already in progress` |
| A move is requested while another reconfiguration is pending | 10226 `stream reconfiguration already in progress` |
| No peers satisfy the new placement or replica count | 10005 |
| Not enough resources on the selected peers | 10023 |

The advisory `$JS.EVENT.ADVISORY.STREAM.UPDATED.<stream>` is published. Client-side rules for
create, update and create-or-update are in
[section 3.12](#312-create-and-update-rules-that-span-fields).

### 5.3 Info, names and list

See [section 4.5](#45-stream-info-request), [4.6](#46-stream-names) and [4.7](#47-stream-list).

### 5.4 Delete

`$JS.API.STREAM.DELETE.<stream>`. The body must be empty; a non-empty body fails with 10038
(`expected an empty request payload`). Answered by the meta leader.

```json
{"type":"io.nats.jetstream.api.v1.stream_delete_response","success":true}
```

Errors: 10059 not found; 10050 deletion failed. The advisory
`$JS.EVENT.ADVISORY.STREAM.DELETED.<stream>` is published.

### 5.5 Purge

`$JS.API.STREAM.PURGE.<stream>`. Answered by the stream leader. The body is optional:

| JSON key | Type | Meaning |
|---|---|---|
| `filter` | string, *omit* | Restrict the purge to messages whose subject matches (wildcards allowed). |
| `seq` | uint64, *omit* | Remove messages with a sequence **lower than** this value (the message at `seq` is kept). |
| `keep` | uint64, *omit* | Keep this many of the newest (matching) messages and remove the rest. |

All combinations:

| Body | Effect |
|---|---|
| empty, or `{}` | Remove every message. |
| `{"filter":"a.>"}` | Remove every message whose subject matches `a.>`. |
| `{"seq":N}` | Remove every message with sequence below N. `{"seq":1}` removes nothing. |
| `{"keep":K}` | Keep the newest K messages. |
| `{"filter":"a.>","seq":N}` | Remove matching messages with sequence below N. |
| `{"filter":"a.>","keep":K}` | Keep the newest K matching messages; remove the other matching messages. |
| `{"seq":N,"keep":K}` with both greater than 0 | **Rejected**: 10003 `bad request`. |

```json
{"type":"io.nats.jetstream.api.v1.stream_purge_response","success":true,"purged":42}
```

`purged` is the number of messages removed and is always present. Errors: 10109 stream is sealed;
10110 with description `stream purge not permitted` when `deny_purge` is set; 10051 other failure.

A purge does not reset sequence numbers: `last_seq` is unchanged and the next message gets
`last_seq + 1`. Consumers are moved past the purged messages.

> **Client rule:** Reject a purge request that sets both `seq` and `keep` before sending it (the
> reference client does). Return the `purged` count to the caller. The reference client sends `{}`
> when no option is set; an empty payload is equivalent.

### 5.6 Message get

`$JS.API.STREAM.MSG.GET.<stream>`. Answered by the **stream leader** only, so the result is
consistent with all acknowledged publishes. A body is required.

| JSON key | Type | Meaning |
|---|---|---|
| `seq` | uint64 | The message at exactly this sequence; or, together with `next_by_subj`, the lowest sequence to consider. |
| `last_by_subj` | string | The newest message on this literal subject. |
| `next_by_subj` | string | The first message at or after `seq` (or `start_time`) whose subject matches; wildcards allowed. |
| `start_time` | RFC 3339 time | The first message at or after this time. |
| `no_hdr` | bool | Leave `hdrs` out of the response. |

Valid forms: `{"seq":N}`, `{"last_by_subj":S}`, `{"seq":N,"next_by_subj":S}`, `{"next_by_subj":S}`,
`{"start_time":T}`, `{"start_time":T,"next_by_subj":S}`, each optionally with `no_hdr`. The request
is rejected with 10003 when: the body is empty; none of `seq`, `last_by_subj`, `next_by_subj`,
`start_time` is set; `seq > 0` with `last_by_subj`; `seq > 0` with `start_time`; `start_time` with
`last_by_subj`; `last_by_subj` with `next_by_subj`; or `batch` or `max_bytes` is greater than 0 (this
endpoint does not support batches). The keys `multi_last`, `up_to_seq` and `up_to_time` are accepted
by the decoder and ignored.

```json
{"type":"io.nats.jetstream.api.v1.stream_msg_get_response",
 "message":{"subject":"orders.a","seq":3,"hdrs":"TkFUUy8xLjANCk5hdHMtTXNnLUlkOiB4DQoNCg==","data":"aGVsbG8=","time":"2026-01-01T00:00:00.123456789Z"}}
```

| `message` key | Type | Meaning |
|---|---|---|
| `subject` | string | Subject the message is stored under. |
| `seq` | uint64 | Stream sequence. |
| `hdrs` | base64 string, *omit* | The raw header block exactly as stored: `NATS/1.0␍␊`, header lines, and the terminating empty line. The example decodes to `NATS/1.0␍␊Nats-Msg-Id: x␍␊␍␊`. |
| `data` | base64 string, *omit* | The payload. Absent for an empty payload. |
| `time` | time | Timestamp assigned when the message was stored. |

Errors: 10037 (`code` 404, `no message found`) for a missing message or any store error; 10059
stream not found. If the stream is offline the server does not answer at all.

> **Client rule:** Decode `hdrs` with the same header-block parser used for `HMSG`
> ([01-core.md](01-core.md)); decode `data` as standard base64; treat a missing `data` as an empty
> payload and a missing `hdrs` as no headers.

### 5.7 Direct Get

Direct Get reads a message without going through the JSON API layer and without requiring the
leader. It exists for high-volume point reads (key-value `get` is built on it). It is available
only if the stream has `allow_direct: true`. All replicas of the stream, and all mirrors configured
with `mirror_direct: true`, subscribe to the Direct Get subjects **of the origin stream's name** in
one shared queue group, so the server picks one of them per request.

Consequences of this design:

- A read is served by any replica or mirror and can be **stale**: a replica that has not yet applied
  the latest acknowledged publish answers with older data. Read-your-writes is not guaranteed.
- It keeps working while the stream has no leader.
- The response is not JSON. On success it is the stored message itself, as an `HMSG`, with the
  message metadata in headers. On failure it is an `HMSG` with a status line and an empty body.
- If the stream does not exist, or does not allow direct access, nothing is subscribed and the
  client receives **no responders** (or a timeout if no-responders is not in effect).
- A request without a reply subject is ignored.
- The call is not counted in API statistics and produces no audit advisory.

**Form A: `$JS.API.DIRECT.GET.<stream>`** with a JSON body. The body is decoded leniently (unknown
keys are ignored).

| JSON key | Type | Meaning |
|---|---|---|
| `seq` | uint64 | Exact sequence; with `batch` or `next_by_subj` it is the starting point (first message at or after it). |
| `last_by_subj` | string | Newest message on a subject. |
| `next_by_subj` | string | First message at or after `seq` or `start_time` whose subject matches (wildcards allowed). |
| `start_time` | RFC 3339 time | Start at the first message at or after this time. |
| `batch` | int | Return up to this many messages, then an end-of-batch marker. |
| `max_bytes` | int | Stop a batch after this many bytes (subject plus headers plus payload per message). Default: the server's `max_pending` (64 MiB by default). |
| `multi_last` | array of string | For each subject matching any of these filters (wildcards allowed), return its newest message. |
| `up_to_seq` | uint64 | With `multi_last`: consider only messages at or below this sequence. |
| `up_to_time` | RFC 3339 time | With `multi_last`: consider only messages before this time. |
| `no_hdr` | bool | Return only the payload. This suppresses **all** headers, including the `Nats-*` metadata headers below, so the reply is a plain `MSG`. |

**Form B: `$JS.API.DIRECT.GET.<stream>.<subject>`**. Everything after the stream token is the
subject, and the request means "newest message on this subject". The body may be empty. If a JSON
body is present it may contain only `no_hdr` (and `max_bytes`, which is not checked and has no
effect); any of `seq`, `last_by_subj`, `next_by_subj`, `multi_last`, `batch`, `start_time`,
`up_to_seq`, `up_to_time` is rejected. This form needs no JSON encoding and puts the subject where
publish permissions can constrain it.

**Success headers.** The reply's header block is the stored message's own header block with these
headers appended (if the stored message had no headers, a new block is created):

| Header | Value | Present |
|---|---|---|
| `Nats-Stream` | Stream name. | always |
| `Nats-Subject` | Subject the message is stored under. | always |
| `Nats-Sequence` | Stream sequence, decimal. | always |
| `Nats-Time-Stamp` | Storage time, RFC 3339 with nanoseconds, UTC (trailing zeros of the fraction are trimmed). | always |
| `Nats-Num-Pending` | Number of matching messages remaining after this one. | batch and multi only |
| `Nats-Last-Sequence` | Sequence of the previous message in this reply series; `0` on the first. | batch and multi only |

The payload is the stored payload, unmodified. The reply is sent to the request's reply subject
with no reply subject of its own. The subject of the `HMSG` is therefore the inbox, and the
message's real subject is only in `Nats-Subject`.

**Failure statuses** (header block is the status line only; body empty):

| Condition | Status line |
|---|---|
| `Nats-Required-Api-Level` not satisfied | `NATS/1.0 412 Required Api Level` |
| Form A: empty body | `NATS/1.0 408 Empty Request` |
| Form A: body is not valid JSON | `NATS/1.0 408 Malformed Request` |
| Form A: none of `seq`, `last_by_subj`, `next_by_subj`, `multi_last`, `start_time` set | `NATS/1.0 408 Empty Request` |
| Conflicting options: `seq` + `last_by_subj`; `seq` + `start_time`; `start_time` + `last_by_subj`; `last_by_subj` + `next_by_subj`; `last_by_subj` + `batch`; `last_by_subj` + `multi_last`; `next_by_subj` + `multi_last`; `up_to_seq` + `up_to_time` | `NATS/1.0 408 Bad Request` |
| Form B: empty subject, or a forbidden key in the body | `NATS/1.0 408 Bad Request` |
| Message not found (single get, or the first message of a batch) | `NATS/1.0 404 Message Not Found` |
| `multi_last`: nothing matched, or `up_to_time` is before the first message | `NATS/1.0 404 No Results` |
| `multi_last`: more than 1024 subjects would be returned | `NATS/1.0 413 Too Many Results` |
| `multi_last`: a resolved message could not be loaded | `NATS/1.0 404 Message Not Found` |
| `multi_last`: other store error | `NATS/1.0 500 <error text>` |

Note that 408 is used here for malformed requests, not for timeouts.

**Batch and multi replies.** When the request has `batch` (even `batch: 1`) or `multi_last`, the
server sends a series of messages to the reply subject and ends it with an **end-of-batch (EOB)**
message with an empty body:

- Batch: `NATS/1.0 204 EOB␍␊Nats-Num-Pending: <n>␍␊Nats-Last-Sequence: <sequence of the last message sent>␍␊␍␊`
- Multi: the same plus `Nats-UpTo-Sequence: <sequence>`, the upper bound the server used. A client
  that pages through a large multi-get repeats the request with `up_to_seq` set to this value, so
  that all pages see the same snapshot, and with `seq` set to `Nats-Last-Sequence` + 1 to advance
  (ADR-31); paging ends when the EOB reports `Nats-Num-Pending: 0`.

Details:

- A request without `batch` and without `multi_last` receives exactly one message and **no** EOB.
- A plain `seq` batch skips deleted sequences. If a later message of the batch cannot be loaded,
  the series ends early and the EOB is still sent. If the **first** message cannot be found the
  reply is a single `404 Message Not Found` and no EOB (with `batch: 1` a miss is also a bare 404).
- When `max_bytes` is reached, the message that crossed the limit is still sent, then the EOB
  follows. The client detects that more data exists from `Nats-Num-Pending` greater than 0 in the
  EOB, and continues with `seq` = `Nats-Last-Sequence` + 1.
- Gaps in delivery can be detected with `Nats-Last-Sequence`: each message names its predecessor in
  the series.

Reads through `MSG.GET` and Direct Get are isolated from writes in progress: they never observe
part of an atomic batch, a half-applied rollup or a half-applied purge.

> **Client rule:** Choose the read path per stream: if the stream's configuration has
> `allow_direct: true`, use Direct Get (Form B for "last by subject" with an empty body, Form A
> otherwise); else use `STREAM.MSG.GET`. The reference client decides from its cached stream info.
> Document for users that Direct Get may return stale data.

> **Client rule:** Decode a single Direct Get reply as follows. Empty payload and a status in the
> header block: `404` is "message not found"; `503` is no responders (stream missing or direct
> access not enabled); any other status is an error carrying the status description. Otherwise
> require `Nats-Stream`, `Nats-Subject`, `Nats-Sequence` (unsigned integer) and `Nats-Time-Stamp`
> (RFC 3339 with nanoseconds); a missing or unparsable one is an "invalid response" error. Build
> the message from `Nats-Subject`, `Nats-Sequence`, `Nats-Time-Stamp`, the full header block and
> the payload. A status check must come first and must not rely on the payload being empty alone:
> a stored message may legitimately have an empty payload, in which case it has no status code.

> **Client rule:** A batch or multi Direct Get needs a dedicated subscription (or a request
> primitive that accepts multiple replies): subscribe to a fresh inbox, publish the request with
> that inbox as reply, collect messages until one arrives with status `204` (EOB) or an error
> status, or until the deadline; then unsubscribe. Verify continuity with `Nats-Last-Sequence`.
> Read `Nats-Num-Pending` from the EOB to decide whether to request the next page.

### 5.8 Message delete

`$JS.API.STREAM.MSG.DELETE.<stream>`. Answered by the stream leader. A body is required:

| JSON key | Type | Meaning |
|---|---|---|
| `seq` | uint64 | Sequence of the message to remove. |
| `no_erase` | bool, *omit* | `false` (default): **secure erase**; the stored bytes are overwritten with random data before removal. `true`: the message is only marked removed, which is faster. |

```json
{"type":"io.nats.jetstream.api.v1.stream_msg_delete_response","success":true}
```

Errors: 10003 empty body; 10109 stream sealed; 10057 with description `message delete not
permitted` when `deny_delete` is set; 10043 `sequence <n> not found`; 10057 other store error.

> **Client rule:** Expose both variants explicitly: a plain delete that sends
> `{"seq":N,"no_erase":true}` and a secure delete that sends `{"seq":N}`. If a reply has neither
> `error` nor `"success":true`, report a generic "delete unsuccessful" error; do not assume an
> `error` object is present.

### 5.9 Snapshot

`$JS.API.STREAM.SNAPSHOT.<stream>` produces a backup of a stream as a stream of chunks delivered to
a subject of the client's choosing. Answered by the stream leader. A body is required.

| JSON key | Type | Meaning |
|---|---|---|
| `deliver_subject` | string, required | Where chunks are sent. Must be a valid publish subject and not a reserved destination (anything under `$JS.`, a service reply subject, or a gateway-routed reply): otherwise 10015. |
| `no_consumers` | bool, *omit* | Leave consumer state out of the snapshot. |
| `chunk_size` | int, *omit* | Chunk size in bytes. Default 128 KiB; clamped to 1 KiB .. 1 MiB. |
| `window_size` | int, *omit* | Flow control window in bytes. Default 8 MiB; clamped to 1 KiB .. 32 MiB and at least the chunk size. |
| `jsck` | bool, *omit* | Verify all message checksums before snapshotting. |

The immediate JSON reply has `type` `io.nats.jetstream.api.v1.stream_snapshot_response` and two
members the client must keep for a later restore: `config` (stream configuration) and `state`
(stream state). Errors: 10003 empty body; 10059; 10015; 10064 `snapshot failed: <reason>`.

The chunk protocol:

1. The client subscribes to `deliver_subject` **before** sending the request. The server waits up
   to about 2 seconds for interest on that subject.
2. Each chunk arrives as a message on `deliver_subject` with the reply subject
   `$JS.SNAPSHOT.ACK.<stream>.<id>.<chunk length>.<chunk index>`. The client must publish an empty
   message to that reply subject for every chunk. These acknowledgments release flow control
   credit: the server keeps at most `window_size / chunk_size` chunks unacknowledged.
3. The end of the snapshot is a message with an **empty body** and the header status line
   `NATS/1.0 204`.
4. A failure ends the stream with an empty-body message carrying one of: `NATS/1.0 408 No Interest`
   (the subscriber went away), `NATS/1.0 408 No Flow Response` (no chunk acknowledgment for 5
   seconds), `NATS/1.0 500 <error text>`.
5. The snapshot is the concatenation of all chunk payloads in arrival order. For file storage it is
   an S2-compressed tar stream. The client treats it as opaque bytes.

Advisories: `$JS.EVENT.ADVISORY.STREAM.SNAPSHOT_CREATE.<stream>` and
`...SNAPSHOT_COMPLETE.<stream>`.

> **Client rule:** For snapshot: subscribe, send the request, store `config` and `state` from the
> reply, then for each message on the delivery subscription: if it has a status header and an empty
> body, finish (204 success, anything else failure); otherwise append the payload to the output and
> publish an empty message to the message's reply subject. Apply an inactivity timeout between
> chunks.

### 5.10 Restore

`$JS.API.STREAM.RESTORE.<stream>` recreates a stream from a snapshot. Answered by the meta leader.

Request body: `{"config": <stream configuration>, "state": <stream state>}`, normally the two
objects saved from the snapshot reply. If `config.name` is empty the name is taken from the
subject; a different name fails with 10056. The configuration is validated and account limits are
checked as for create. The stream must not exist: 10130 `stream name already in use, cannot
restore`.

```json
{"type":"io.nats.jetstream.api.v1.stream_restore_response","deliver_subject":"$JS.SNAPSHOT.RESTORE.ORDERS.Ky9Pf34qY6Nb3wWD25RQ4F"}
```

The upload protocol:

1. The client sends each chunk of the snapshot as a **request** to `deliver_subject`. A reply
   subject is mandatory on every chunk. The server answers each chunk with an empty message; that
   reply is the flow control, so the client sends the next chunk after receiving it.
2. The client signals the end with a **zero-length** request.
3. The reply to that final request is the result of the restore: a JSON object with `type`
   `io.nats.jetstream.api.v1.stream_create_response` carrying the stream info on success, or
   `error` 10062 `restore failed: <reason>`. A write failure in the middle of the upload is
   reported the same way, as the reply to the chunk that failed.
4. If no chunk arrives for 5 seconds the server aborts the restore as stalled.
5. The server detects the snapshot format itself (a newer archive format, ADR-63, or the legacy
   format).

Advisories: `$JS.EVENT.ADVISORY.STREAM.RESTORE_CREATE.<stream>` and
`...RESTORE_COMPLETE.<stream>`.

> **Client rule:** During a restore upload, inspect every chunk reply: an empty payload means
> "continue"; a non-empty payload is a JSON `stream_create_response` and, if it contains `error`,
> the restore has failed and the upload must stop. Keep each chunk well below `max_payload`.

### 5.11 Leader stepdown

`$JS.API.STREAM.LEADER.STEPDOWN.<stream>` asks the current stream leader to give up leadership.
Answered by the stream leader. The body is optional:

```json
{"placement": {"cluster": "east", "tags": ["ssd"], "preferred": "n2"}}
```

`preferred` names the server that should become leader and takes precedence; otherwise the
candidates are the peers in `cluster` that carry **all** of `tags`.

```json
{"type":"io.nats.jetstream.api.v1.stream_leader_stepdown_response","success":true}
```

Errors: 10010 (`code` 503) on a standalone server; 10008 meta or stream group leaderless; 10059;
10005 with one of the descriptions `preferred server "<name>" is already leader`, `preferred server
"<name>" not known`, `no replacement peer connected`; 10041 Raft error.

### 5.12 Peer remove, evacuate and cancel move

| Subject | Body | Response type | Effect |
|---|---|---|---|
| `$JS.API.STREAM.PEER.REMOVE.<stream>` | `{"peer":"<server name or peer id>"}`, required | `stream_remove_peer_response` with `success` | Remove one replica from the stream's peer set. |
| `$JS.API.STREAM.PEER.EVACUATE.<stream>` | `{"peer":"<server name or peer id>"}`, required | `stream_evacuate_peer_response` with `success` | Move the stream's replica away from that peer. |
| `$JS.API.STREAM.CANCEL_MOVE.<stream>` | empty | `stream_cancel_move_response` with the stream info keys | Roll back a reconfiguration in progress (move, scale, retention change). |

All three are answered by the meta leader. Errors: 10003 empty or missing body (peer operations);
10010 not clustered; 10008 leaderless; 10059; 10040 `peer not a member`; 10075 `peer remap failed`;
for cancel move also 10194 and 10129 `stream reconfiguration not in progress`.

### 5.13 System-account endpoints

These are administrative and are accepted only from the system account (the two
`ACCOUNT.STREAM.*` subjects are also accepted from the named account). A general-purpose client
needs them only if it implements cluster administration.

| Subject | Body | Response | Errors and notes |
|---|---|---|---|
| `$JS.API.META.LEADER.STEPDOWN` | optional `{"placement":{...}}` | `meta_leader_stepdown_response` `{success}` | 10005, 10041. |
| `$JS.API.SERVER.REMOVE` | `{"peer":"<server name>","peer_id":"<id>"}` (`peer_id` *omit*, takes precedence) | `meta_server_remove_response` `{success}` | 10003 empty body; 10044 not a member; 10202 another membership change in flight. Advisory `...SERVER.REMOVED`. |
| `$JS.API.SERVER.EVACUATE` | `{"peer":"...","peer_id":"..."}` | `meta_server_evacuate_response` `{success}` | 10003; 10044. |
| `$JS.API.META.RESCUE` | `{"quorum_needed": <int>}` | `meta_rescue_response` `{server, server_id, prev_quorum (omit), new_quorum (omit)}`; **one reply per online server** | 10224. Advisory `...SERVER.META_RESCUE`. ADR-61. |
| `$JS.API.ACCOUNT.PURGE.<account>` | empty | `account_purge_response` `{"initiated":true}` | 10051 for a name with path separators; 10008 during recovery. |
| `$JS.API.ACCOUNT.STREAM.MOVE.<account>.<stream>` | `{"server","cluster","domain","tags":[...]}` (all *omit*) | `stream_update_response` with stream info | 10035, 10059, 10194, 10044, 10040, 10005. |
| `$JS.API.ACCOUNT.STREAM.CANCEL_MOVE.<account>.<stream>` | empty | `stream_cancel_move_response` | 10035, 10059, 10194, 10129. |

### 5.14 Account info

`$JS.API.INFO`. The body is ignored. Answered by the meta leader. This is the usual probe for "is
JetStream available to me": no responders means no, a reply with error 10039 means the account has
no JetStream, and a success reply carries usage, limits and the API level.

Response `type` `io.nats.jetstream.api.v1.account_info_response` with these keys at the top level:

| JSON key | Type | Meaning |
|---|---|---|
| `memory` | uint64 | Bytes used by memory-storage streams. |
| `storage` | uint64 | Bytes used by file-storage streams. |
| `reserved_memory` | uint64 | Bytes reserved through `max_bytes` of memory streams. |
| `reserved_storage` | uint64 | Bytes reserved through `max_bytes` of file streams. |
| `streams` | int | Number of streams. |
| `consumers` | int | Number of consumers. |
| `limits` | account limits | Populated when the account uses a single untiered limit; zero values when tiers are used. |
| `domain` | string, *omit* | JetStream domain. |
| `api` | API stats | See below. |
| `tiers` | map of tier name to tier, *omit* | Present with tiered limits. The key is `R<replicas>` (`R1`, `R3`); each value has `memory`, `storage`, `reserved_memory`, `reserved_storage`, `streams`, `consumers`, `limits`. |

Account limits (all always present):

| JSON key | Type | Meaning |
|---|---|---|
| `max_memory` | int64 | Maximum bytes of memory storage. |
| `max_storage` | int64 | Maximum bytes of file storage. |
| `max_streams` | int | Maximum number of streams. |
| `max_consumers` | int | Maximum number of consumers. |
| `max_ack_pending` | int | Cap on a consumer's `max_ack_pending`. |
| `memory_max_stream_bytes` | int64 | Cap on one memory stream's `max_bytes`. |
| `storage_max_stream_bytes` | int64 | Cap on one file stream's `max_bytes`. |
| `max_bytes_required` | bool | Every stream must set `max_bytes` (else 10113). |

The server's default limits for accounts with dynamically enabled JetStream use `-1` in every
numeric field, meaning unlimited.

API stats:

| JSON key | Type | Meaning |
|---|---|---|
| `level` | int | API level implemented by the answering server (5 in this version). |
| `total` | uint64 | API requests answered for this account since the server started. |
| `errors` | uint64 | How many of them were error replies. |
| `inflight` | uint64, *omit* | Requests currently being served. Set only in server-level statistics; expect it to be absent here. |

```json
{
  "type": "io.nats.jetstream.api.v1.account_info_response",
  "memory": 0, "storage": 1024, "reserved_memory": 0, "reserved_storage": 0,
  "streams": 1, "consumers": 0,
  "limits": {"max_memory": -1, "max_storage": -1, "max_streams": -1, "max_consumers": -1,
             "max_ack_pending": -1, "memory_max_stream_bytes": -1, "storage_max_stream_bytes": -1,
             "max_bytes_required": false},
  "domain": "hub",
  "api": {"level": 5, "total": 12, "errors": 1}
}
```

> **Client rule:** Implement account info as the JetStream availability probe and map its outcomes:
> no responders to "JetStream not enabled" (the reference client does this mapping for this call
> only); error 10039 to "JetStream not enabled for account"; error 10076 to "JetStream not
> enabled". Do not call it implicitly when a context is constructed.

---

## 6. Publishing

### 6.1 Mechanics and the PubAck

A publish into a stream is an ordinary `PUB` or `HPUB` to a subject that a stream captures. There
is no "publish to stream" API subject (the domain-prefixed `$KV`/`$OBJ` mappings of
[section 2.5](#25-subject-prefix-domains-and-custom-prefixes) are the only prefixed publish
subjects). The stream leader's internal subscription receives the message, copies it, removes any
status line from its header block, and puts it on the stream's ingest queue. A single ingest loop
per stream processes the queue in order, so the sequence a message receives is the order in which
the leader dequeued it.

The server sends an acknowledgment only if **all** of these hold: the stream's `no_ack` is false,
the message had a reply subject, and this server is the leader. The same condition governs error
reports. Therefore:

- A publish without a reply subject is stored if it passes all checks and is otherwise dropped
  silently. No error is reported anywhere.
- A stream with `no_ack: true` never acknowledges and never reports publish errors.

The acknowledgment is a JSON object sent as a plain `MSG` to the reply subject. It has **no `type`
key**.

| JSON key | Type | Meaning |
|---|---|---|
| `stream` | string | Name of the stream that stored the message. |
| `seq` | uint64 | Sequence assigned to the message. For a duplicate: the sequence of the original. |
| `domain` | string, *omit* | JetStream domain of the server. |
| `duplicate` | bool, *omit* | `true` when the message was recognized as a duplicate by `Nats-Msg-Id` and not stored again. |
| `val` | string, *omit* | Counter streams: the counter's new total as a decimal string. |
| `batch` | string, *omit* | Batch id, on the acknowledgment that commits an atomic or fast batch. |
| `count` | uint64, *omit* | Number of messages in the committed batch. |
| `error` | error object, *omit* | Present instead of a successful result ([section 2.2](#22-request-and-response-bodies)). |

The success acknowledgment is assembled from a pre-built byte template, not by a JSON encoder, so
its exact bytes are fixed:

```
{"stream":"ORDERS","seq":22}
{"stream":"ORDERS","domain":"hub","seq":22}
{"stream":"ORDERS","seq":22,"duplicate": true}
{"stream":"COUNTERS","seq":7,"val":"101"}
{"stream":"ORDERS","seq":30,"batch":"b7Qm2xLp","count":10}
```

Note that `domain` precedes `seq`, and that the duplicate form has a space after the colon. Error
acknowledgments are produced by a JSON encoder from a structure that always contains the stream
name, so they carry `"stream"` and `"seq":0` next to `error`:

```
{"error":{"code":400,"err_code":10071,"description":"wrong last sequence: 5"},"stream":"ORDERS","seq":0}
```

> **Client rule:** Parse a PubAck with a real JSON parser; never compare bytes. Check `error`
> first: if present, the publish failed regardless of the other keys (`stream` is set and `seq` is
> 0 in error acks). Without `error`, a missing or empty `stream` means the reply is not a PubAck
> and must be reported as an "invalid acknowledgment" error; this catches the case where the
> subject is not captured by a stream but some other service replied.

> **Client rule:** A publish that must not be lost has to be sent as a request (with a reply
> subject) and its acknowledgment awaited. Offer fire-and-forget publishing only as plain core
> publish; do not present it as a JetStream publish.

In a cluster the leader validates the message, proposes it to the Raft group, and sends the
acknowledgment when the entry is applied. With `persist_mode: "async"` the acknowledgment may be
sent before the write has been persisted.

### 6.2 When a publish gets no acknowledgment

A publisher that asked for an acknowledgment receives **nothing from JetStream** in these cases:

| Case | What the publisher sees |
|---|---|
| No stream captures the subject | Core no responders (status 503, empty body), immediately. |
| The stream exists but currently has no leader (election in progress, quorum lost) | No responders, because the stream's subject subscriptions exist only on the leader and are removed on a leadership change. A timeout is also possible if the message was routed toward a server that lost leadership in the meantime. |
| `no_ack: true` | No responders is **not** sent (the stream's subscription counts as a responder); the request times out. |
| The message reached a server that is no longer leader | Nothing: timeout. |
| The store failed with an I/O error | Nothing, deliberately: timeout. |
| The stream was closed while the message was queued | Nothing: timeout. |
| The ingest queue was full | A reply with status 429 and a JSON error body ([section 6.13](#613-size-limits-full-streams-and-overload)). |

No responders on a publish is therefore ambiguous between "this subject is not bound to any stream"
(a configuration error) and "the stream is between leaders" (transient). ADR-22 resolves this by
retrying a small number of times before reporting the failure.

### 6.3 Order of server checks

The order matters because the first failing check determines the error. For a normal (non-batch)
publish on a standalone server the ingest loop does the following (`server/stream.go`,
`processJetStreamMsgWithBatch`). Clustered streams run the same rules on the leader before the
proposal, with the differences in [section 6.15](#615-clustered-path-differences).

1. Apply the stream's `subject_transform` to the subject. Everything below uses the result.
2. File storage hard record size limit: 10077.
3. Stream is sealed: 10109.
4. Remove the internal `Nats-Request-Info` header.
5. `max_msg_size` (headers plus payload): 10054.
6. Header block larger than 65535 bytes: 10097.
7. `Nats-Incr` checks: 10168, 10170, 10171 ([section 6.9](#69-counters)).
8. `Nats-Expected-Stream`: 10060.
9. `Nats-TTL` present but the stream lacks `allow_msg_ttl`: 10166.
10. `Nats-Expected-Last-Subject-Sequence` (and its `-Subject` companion): 10071, 10193.
11. `Nats-Expected-Last-Sequence`: 10071.
12. Scheduling headers: 10188 to 10192, 10203, 10212, 10223
    ([section 6.12](#612-message-scheduling)).
13. `Nats-Stream-Source` set by a client: 10227.
14. `Nats-Msg-Id` duplicate detection: success acknowledgment with `"duplicate": true`; processing
    stops.
15. `Nats-Expected-Last-Msg-Id`: 10070.
16. `Nats-Rollup`: 10111.
17. Counter stream but no `Nats-Incr`: 10169.
18. Server resource limits: 10023.
19. Interest retention with no interested consumer: the message is skipped
    ([section 6.14](#614-retention-rules-visible-to-publishers)).
20. Account limits: 10002 (or the specific storage or memory error).
21. `Nats-TTL` value parse: 10165.
22. Store the message. A discard-new limit fails here: 10077.
23. Execute the rollup purge, emit the republish copy, send the acknowledgment, signal consumers.

One consequence is worth knowing. On this standalone path the expected-sequence checks (10, 11)
run **before** duplicate detection (14). A retried publish that carries both `Nats-Msg-Id` and an
expected sequence therefore fails with 10071 on the retry instead of being reported as a
duplicate, because the first attempt already advanced the sequence. On the clustered path the
order is the other way round ([section 6.15](#615-clustered-path-differences)) and the retry gets
the duplicate acknowledgment.

> **Client rule:** When a publish that carried both a message id and an expected-sequence header is
> retried after a lost acknowledgment and fails with 10071 or 10164, the first attempt may have
> succeeded. The client cannot tell from the error alone; document this, and let the application
> decide (for example by reading the last message for the subject).

### 6.4 Publish header reference

Header names are matched exactly as written here (case-sensitive lookup). All of them are ordinary
NATS headers in the `HPUB` header block ([01-core.md](01-core.md)).

| Header | Value | Semantics | On violation |
|---|---|---|---|
| `Nats-Msg-Id` | Opaque string | Deduplication id ([section 6.5](#65-deduplication)). | Not an error: duplicate ack. Clustered only: 10158 (`code` 409) if the original is still in flight. |
| `Nats-Expected-Stream` | Stream name | Must equal the name of the capturing stream. Guards against publishing to a subject that is bound to an unexpected stream. | 10060 `expected stream does not match` |
| `Nats-Expected-Last-Sequence` | Unsigned decimal | Must equal the stream's current last sequence. `0` is a real check: "the stream has never stored a message". | 10071 `wrong last sequence: <actual>` |
| `Nats-Expected-Last-Subject-Sequence` | Unsigned decimal | Must equal the sequence of the newest message on this message's subject (after the stream's subject transform). `0` means "no message on this subject". | 10071 with the actual per-subject sequence; clustered with a concurrent publish on the same subject: 10164 `wrong last sequence` (no number) |
| `Nats-Expected-Last-Subject-Sequence-Subject` | Subject, wildcards allowed | Replaces the subject used for the per-subject check above: the check then applies to the newest message matching this subject. | Present without `Nats-Expected-Last-Subject-Sequence`: 10193 |
| `Nats-Expected-Last-Msg-Id` | String | Must equal the `Nats-Msg-Id` of the last message stored in the stream (empty if that message had none). | 10070 `wrong last msg ID: <actual>`. Not allowed in atomic batches (10177). |
| `Nats-Rollup` | `sub` or `all` (compared in lower case) | After this message is stored, remove older messages ([section 6.7](#67-rollups)). | 10111 with description `rollup not permitted` or `rollup value invalid: "<value>"` |
| `Nats-TTL` | Go duration string (`90s`, `1h`), integer seconds (`3600`), or `never` (any case) | Per-message time to live ([section 6.8](#68-per-message-ttl-and-delete-markers)). | 10166 if the stream lacks `allow_msg_ttl`; 10165 if invalid |
| `Nats-Incr` | Signed base-10 integer of any size (`+1`, `-5`, `10`) | Counter increment ([section 6.9](#69-counters)). | 10168, 10169, 10170, 10171, 10172 |
| `Nats-Batch-Id`, `Nats-Batch-Sequence`, `Nats-Batch-Commit` | Id of at most 64 characters; unsigned integer from 1; `1` or `eob` | Atomic batch ([section 6.10](#610-atomic-batch-publishing)). | 10174 to 10177, 10179, 10199 to 10201, 10210 |
| `Nats-Schedule`, `Nats-Schedule-Target`, `Nats-Schedule-Source`, `Nats-Schedule-TTL`, `Nats-Schedule-Rollup`, `Nats-Schedule-Time-Zone` | See [section 6.12](#612-message-scheduling) | Message scheduling. | 10188 to 10192, 10203, 10223 |
| `Nats-Scheduler` with `Nats-Schedule-Next: purge` | Subject; literal `purge` | Publish a message and cancel a schedule atomically ([section 6.12](#612-message-scheduling)). | 10212 |
| `Nats-Required-Api-Level` | Integer | Honored on batch publishes, per message; always stripped before storing. | 10185 |
| `Nats-Stream-Source` | Server-generated | Clients may **not** set it. | 10227 |
| `Nats-Trace-Dest`, `Nats-Trace-Only`, `Nats-Trace-Hop`, `Nats-Trace-Origin-Account`, `traceparent` | See ADR-41 | Message tracing. When a traced message is stored, its `Nats-Trace-Dest` is overwritten with the literal `trace disabled` so that later delivery does not trace again. With `Nats-Trace-Only` the message is **not stored**; the checks still run and a trace event is emitted. A trace-only message inside a batch makes the batch fail. | none |
| `Nats-Request-Info` | Server-generated on cross-account imports | Removed before storing. Clients never set it. | none |
| `Nats-Marker-Reason` | `MaxAge`, `Purge`, `Remove` | Written **by the server** on delete markers ([section 6.8](#68-per-message-ttl-and-delete-markers)). Not validated on client publishes. | none |
| `KV-Operation` | `PURGE` | Recognized by the server as equivalent to a delete marker for marker bookkeeping ([04-kv-object-services.md](04-kv-object-services.md)). | none |

All other headers are stored and delivered untouched.

> **Client rule:** Provide publish options that set exactly these headers: message id; expected
> stream; expected last sequence; expected last subject sequence, optionally with an explicit
> subject; expected last message id; TTL; rollup. For the two expected-sequence options the value
> 0 must be **sent** (it is a meaningful assertion), so "unset" has to be represented separately
> from 0. Reject an empty explicit subject client-side. Write header values in the canonical
> forms shown (decimal integers without sign, lower-case `sub`/`all`).

### 6.5 Deduplication

The stream keeps a table of the `Nats-Msg-Id` values it has stored, each with its sequence and
timestamp, for `duplicate_window`.

- If a publish carries an id that is in the table, the message is **not stored** and the
  acknowledgment is `{"stream":...,"seq":<original sequence>,"duplicate": true}`. This is a
  success, not an error.
- Entries are removed once their age reaches the window (checked by a timer with a granularity of
  at least 50 ms). After a restart the table is rebuilt from the stored messages.
- `duplicate_window: 0` disables deduplication. This is the default for streams with a mirror or
  sources. **Mirrors never deduplicate.**
- On interest-retention streams the id is recorded even when the message is skipped for lack of
  interest.
- In a cluster, a second publish with the same id that arrives while the first is still being
  replicated (its sequence is not yet known) is rejected with **10158** (`code` 409, `duplicate
  message id is in process`). The second publisher should retry shortly; it will then get the
  duplicate acknowledgment.

> **Client rule:** Treat `duplicate: true` as success and expose the flag. Treat 10158 as
> retryable. Generate message ids in the application (they must be stable across retries of the
> same logical message); a client library must not generate a fresh id per attempt.

### 6.6 Optimistic concurrency

The four `Nats-Expected-*` headers make a publish conditional. They are evaluated by the leader
against the state the message would be appended to, in one step with the append, so two
conflicting conditional publishes cannot both succeed.

- `Nats-Expected-Last-Sequence: N` serializes all writers of a stream.
- `Nats-Expected-Last-Subject-Sequence: N` serializes writers of one subject and leaves other
  subjects free. This is how a key-value "update if revision is N" and "create if absent" (N = 0)
  are implemented.
- With `Nats-Expected-Last-Subject-Sequence-Subject: <pattern>` the condition covers a set of
  subjects: the publish succeeds only if the newest message matching the pattern has sequence N.
- The error description of 10071 contains the **actual** value, which a client may show but must
  not parse.

In a cluster these checks run against the last sequence *including proposals in flight*, and a
per-subject check that races with an uncommitted publish on the same subject fails with **10164**,
whose meaning is the same as 10071.

> **Client rule:** Map both 10071 and 10164 to one "wrong last sequence" error. Higher layers
> (key-value) translate it to "key exists" or "revision mismatch".

### 6.7 Rollups

`Nats-Rollup` lets a publisher replace history with a single message, atomically with the publish.
It requires `allow_rollup_hdrs: true` and `deny_purge: false`.

| Value | Effect, after the message is stored |
|---|---|
| `sub` | Remove all earlier messages **on the same subject**; the new message is the only one left for the subject. |
| `all` | Remove **every** earlier message in the stream; the new message is the only one left. |

On an interest-retention stream where the message itself is skipped for lack of interest, the
rollup is still executed (and removes everything it covers). A message carrying `Nats-Rollup` is
also exempt from the `discard_new_per_subject` limit, since it is about to replace the messages
that fill the subject.

### 6.8 Per-message TTL and delete markers

**`Nats-TTL`** gives one message its own expiry, independent of `max_age`. It requires
`allow_msg_ttl: true`.

| Value | Parsed as |
|---|---|
| `never` (any letter case) | The message never expires by TTL; per ADR-43 it is also exempt from `max_age`. |
| A Go duration string of at least one second (`1s`, `90s`, `5m`, `1h30m`) | That duration, truncated to whole seconds. |
| A Go duration below one second (`500ms`, `0s`, `0`) | Invalid: 10165. |
| A non-negative decimal integer (`3600`) | That many seconds. |
| Anything else, or a negative integer | Invalid: 10165. |

The minimum TTL is one second. If the stream has `subject_delete_marker_ttl > 0` and
`max_msgs_per_subject` is not 1, a TTL shorter than the marker TTL is **raised** to the marker TTL
and the stored `Nats-TTL` header is rewritten as integer seconds. The stored message can therefore
carry a different `Nats-TTL` than the one published.

**Subject delete markers.** With `subject_delete_marker_ttl > 0`, when the **last** remaining
message of a subject is removed because of age (`max_age` or a per-message TTL), the server stores
a new message on that subject with an empty payload and this header block:

```
NATS/1.0␍␊
Nats-Marker-Reason: MaxAge␍␊
Nats-TTL: 1m0s␍␊
Nats-Rollup: sub␍␊
␍␊
```

`Nats-TTL` on the marker is the configured marker TTL formatted as a Go duration (`1m0s` for 60
seconds), so the marker itself disappears after that time. The marker gets a normal sequence number
and is delivered to consumers like any message. It tells readers that are following the stream
(key-value watchers, caches) that the subject has become empty, which they could not otherwise
learn, because expiry produces no message.

Constants for the reasons `Purge` and `Remove` exist in the server and ADR-43 describes markers for
deletes and purges, but this server version only ever emits `MaxAge`.

> **Client rule:** Treat any message that has a `Nats-Marker-Reason` header as a tombstone for its
> subject, whatever the reason value, and never as application data. Accept the TTL in all three
> input forms when parsing headers of received messages; emit a Go duration string or integer
> seconds when publishing (the reference client emits a Go duration string such as `1m0s`).

### 6.9 Counters

A stream with `allow_msg_counter: true` is a set of distributed counters, one per subject
(ADR-49). Clients do not publish values; they publish increments, and the server stores the
running total.

Rules for every client publish to a counter stream:

| Rule | Error |
|---|---|
| The message must carry `Nats-Incr` | 10169 `message counter increment is missing` |
| The payload must be empty | 10170 `message counter has payload` |
| The value must parse as a base-10 integer | 10171 `message counter increment is invalid` |
| `Nats-Incr` may not be combined with `Nats-Rollup`, `Nats-Expected-Stream`, `Nats-Expected-Last-Msg-Id`, `Nats-Expected-Last-Sequence`, `Nats-Expected-Last-Subject-Sequence` or `Nats-Expected-Last-Subject-Sequence-Subject` | 10171 |
| `Nats-Incr` on a stream that is not a counter stream | 10168 `message counters is disabled` |
| The previous message on the subject is not a valid counter | 10172 `message counter is broken` |

What the server does: it loads the newest message on the subject, parses its body
`{"val":"<decimal>"}`, adds the increment with arbitrary precision, and **stores a new message**
whose body is `{"val":"<new total>"}` and which keeps the publisher's `Nats-Incr` header. The first
increment on a subject starts from zero. The resulting message is checked again against the
server's `max_payload` and the stream's `max_msg_size`.

The acknowledgment is `{"stream":"<name>","seq":<n>,"val":"<new total>"}`. `val` is a **string**
because totals are arbitrary-precision integers.

Reading a counter means reading the newest message on its subject (Direct Get form B is the natural
choice) and parsing `val` from the body; the `Nats-Incr` header of that message is the last
increment applied.

On a stream that *sources* other counter streams, the server maintains a `Nats-Counter-Sources`
header whose value is a JSON object `{"<origin stream>":{"<origin subject>":"<last value seen>"}}`.
For each sourced counter message it records the origin's latest total, rewrites `Nats-Incr` to the
difference from the previously recorded total, and applies that difference. This lets several
regional counters be summed into a global one without double counting.

> **Client rule:** A counter API is three operations: `add(subject, delta)` publishes an `HPUB`
> with `Nats-Incr: <delta>` and an empty payload and returns `val` from the acknowledgment;
> `get(subject)` reads the last message for the subject and returns `val` from its JSON body;
> both must handle integers of arbitrary size (parse `val` into a big integer, never a float or a
> fixed 64-bit integer). The reference Go client has no counter API; this rule is derived from the
> server and ADR-49.

### 6.10 Atomic batch publishing

With `allow_atomic: true` a client can publish several messages that are stored either all or not
at all, at consecutive sequences, with all conditions checked together at commit (ADR-50). The
server routes a publish to the atomic path when it has a `Nats-Batch-Id` header and its reply
subject does not end in `.$FI`.

Protocol:

1. The client chooses a batch id (at most 64 characters, unique among its open batches).
2. **First message**: headers `Nats-Batch-Id: <id>` and `Nats-Batch-Sequence: 1`, sent **as a
   request**. The reply is a **zero-length message** if the batch was opened and the message
   staged, or an error PubAck. The client must wait for this reply before sending more; it is the
   only confirmation that the stream supports batches.
3. **Following messages**: the same id and a sequence that increases by exactly 1. A reply subject
   is optional. If present, each staged message is answered with a zero-length message.
4. **Commit**: the final message additionally carries `Nats-Batch-Commit`:
   - `1`: this message is stored as the last message of the batch.
   - `eob`: this message is **not** stored; it only ends the batch, and the previous message
     becomes the last (the server adds `Nats-Batch-Commit: 1` to it).
   Any other value: 10200. The commit is sent as a request so that its result can be observed.
5. **Commit reply**: a PubAck with `batch` and `count`:
   `{"stream":"ORDERS","seq":6,"batch":"b7Qm2xLp","count":3}`. `seq` is the stream sequence of the
   last message of the batch; the others occupy the sequences immediately before it. With `eob`,
   `count` does not include the end marker.

All per-message checks (expected-sequence headers, TTL, counters, schedules, rollups, discard-new
limits, account limits) are evaluated **at commit**, under one lock, for the whole batch. The error
of the first failing message is returned as the commit reply and nothing is stored. Readers using
`MSG.GET` or Direct Get never observe part of a batch.

Rules and limits:

| Rule | Result |
|---|---|
| Stream lacks `allow_atomic` | 10174 `atomic publish is disabled` |
| Batch id longer than 64 characters | 10179 |
| `Nats-Batch-Sequence` missing or not a number | 10175 |
| Unknown batch id and sequence not 1 | 10176 `atomic publish batch is incomplete` (10199 if the sequence already exceeds the maximum size) |
| A gap or repeat in the sequence, the batch timed out, or the first message is an `eob` | Batch abandoned; 10176; advisory reason `incomplete` |
| More messages than the maximum batch size (default 1000, server option `max_batch_size`; the `eob` marker is not counted) | 10199 `atomic publish batch is too large: <size>`; advisory reason `large` |
| Too many open batches: more than 50 per stream (`max_batch_inflight_per_stream`) or 1000 per server (`max_batch_inflight_total`) | 10210 (`code` 429) |
| No message for 10 seconds (`max_batch_timeout`) | Batch abandoned **silently**; advisory reason `timeout`. The next message of that batch gets 10176. |
| `Nats-Expected-Last-Msg-Id` on any message | 10177 `atomic publish unsupported header used: <header>` (at commit) |
| A `Nats-Msg-Id` that repeats within the batch or is already in the deduplication window | The whole batch is rejected: 10201 |
| `Nats-Expected-Last-Sequence` on a message other than the first | 10164 |
| `Nats-Expected-Last-Subject-Sequence` for a subject already written earlier in the batch | 10164 |
| `Nats-Rollup: sub` for a subject already written in the batch; `Nats-Rollup: all` on a message other than the first | 10111 |
| `Nats-Required-Api-Level` not satisfied on any message | Batch abandoned; 10185; advisory reason `unsupported` |
| A trace-only message | Dropped, so the batch later fails on the gap |
| Stream sealed, server out of resources, account limits, `max_msg_size` | 10109, 10023, 10002, 10054 on the offending message |

Abandoned batches are announced on `$JS.EVENT.ADVISORY.STREAM.BATCH_ABANDONED.<stream>`
([section 9](#9-advisories)). When messages of a batch are copied into another stream by a mirror
or source, the `Nats-Batch-*` headers are removed.

> **Client rule:** Implement an atomic batch as an object with `add(message)` and
> `commit(message)` (and optionally `commit_eob`). Number messages from 1. Send the first message
> as a request and require a zero-length success reply before continuing; send intermediate
> messages without a reply subject (or with one, treating any non-empty reply as an error PubAck);
> send the commit as a request and decode the PubAck, requiring `batch` to equal the batch id and
> `count` to equal the number of data messages sent. Any error reply, at any step, ends the batch:
> the server has discarded it. A timeout on the commit leaves the outcome unknown. Enforce the
> 64-character id limit and refuse `Nats-Expected-Last-Msg-Id` client-side. The reference Go client
> does not implement this; the rule is derived from the server and ADR-50.

### 6.11 Fast-ingest batch publishing

With `allow_batched: true` (API level 4) a client can stream a large number of messages with
flow control and periodic acknowledgments instead of one acknowledgment per message. Unlike an
atomic batch it is **not** all-or-nothing: messages are stored as they arrive. All control
information travels in the **reply subject**; no batch headers are used. The server routes a
publish to this path when its reply subject ends in `.$FI`.

Reply subject format:

```
<prefix>.<batch id>.<initial flow>.<gap mode>.<batch sequence>.<operation>.$FI
```

| Token | Values | Rules |
|---|---|---|
| `<prefix>` | The client's inbox prefix for this batch | The client subscribes to `<prefix>.<batch id>.>`. |
| `<batch id>` | At most 64 characters | Longer: 10207. |
| `<initial flow>` | Positive integer | The largest "acknowledge every N messages" the client accepts. Zero or negative is treated as 10; values above 65535 as 65535. |
| `<gap mode>` | `ok` or `fail` | `fail`: a lost message ends the batch. `ok`: gaps are reported and the batch continues. |
| `<batch sequence>` | Unsigned integer from 1 | Position of this message in the batch. |
| `<operation>` | `0` start, `1` append, `2` commit (store this message), `3` commit without storing (end of batch), `4` ping | A start must have sequence 1; an append must have a sequence above 1; operation `3` cannot have sequence 1. |

A reply subject that ends in `.$FI` but does not parse is answered with 10206 (`batch publish
pattern is invalid`). When the publish crosses an account import, the server recovers the original
reply subject from the request info it attached.

Messages from the server arrive on the reply subject of the publish that triggered them, hence the
wildcard subscription. They are distinguished by the JSON `type` key; the final PubAck has none.

| Message | JSON | Meaning |
|---|---|---|
| Flow acknowledgment | `{"type":"ack","seq":<n>,"msgs":<m>}` | Sent in reply to the first message with `seq` 0, announcing the acknowledgment cadence `msgs`, and thereafter each time another `msgs` messages have been persisted. In `fail` mode, every message up to and including batch sequence `seq` is persisted. In `ok` mode, some of them may be missing. `msgs` can change from one acknowledgment to the next: the server lowers it when several publishers compete and raises it again, never above the client's `<initial flow>`. |
| Gap report | `{"type":"gap","last_seq":<expected>,"seq":<received>}` | The server expected batch sequence `last_seq` and received `seq`. If `seq` is greater, the messages from `last_seq` up to but not including `seq` were lost. If `seq` is lower or equal, a message was duplicated or reordered and the batch is ended. Informational. |
| Error report | `{"type":"err","seq":<n>,"error":{...}}` | The message with batch sequence `seq` was rejected (for example by an expected-sequence check). Informational; in `fail` mode it ends the batch. |
| PubAck | `{"stream":"...","seq":<stream sequence>,"batch":"<id>","count":<n>}` | Final. Sent when the batch is committed, or when the server ends it because of a gap or error. `count` is the last batch sequence that was accepted; `seq` is the stream sequence of the last stored message. |

Flow acknowledgments, gap reports and error reports can be lost in transit. A client that expects
an acknowledgment and does not receive one sends a **ping**: operation `4` with the highest batch
sequence it has sent (a ping does not consume a sequence). The server resends the latest flow
acknowledgment unchanged. If the server has not received everything up to the ping's sequence it
first sends a gap report whose `seq` is the ping's sequence plus one. A ping also keeps an idle
batch alive.

Other rules:

| Rule | Result |
|---|---|
| Stream lacks `allow_batched` | 10205 `batch publish is disabled` |
| Unknown batch id with a sequence other than 1, or a ping for an unknown batch | 10208 `batch publish ID unknown` |
| Too many open batches: 1000 per stream, 50,000 per server (same server options as atomic batches) | 10211 (`code` 429) |
| No message for 10 seconds | The batch is abandoned without any reply. Messages already stored stay in the stream. A later message, ping or commit for it gets 10208. |
| `Nats-Required-Api-Level` not satisfied | Batch removed; 10185 |
| `max_msg_size` exceeded, server out of resources | 10054, 10023 |

There is no maximum batch size. Ordinary publish headers (`Nats-Msg-Id`, `Nats-Expected-*`,
`Nats-TTL`, ...) work per message; a duplicate is skipped and counts as persisted.

An illustrative exchange (one publisher, initial flow 100, `fail` mode, 101 messages):

```
C: SUB _INBOX.Ky9Pf34qY6Nb3wWD25RQ6H.> 3␍␊
C: PUB orders.eu.20 _INBOX.Ky9Pf34qY6Nb3wWD25RQ6H.fb01.100.fail.1.0.$FI 2␍␊m1␍␊
S: MSG _INBOX.Ky9Pf34qY6Nb3wWD25RQ6H.fb01.100.fail.1.0.$FI 3 33␍␊{"type":"ack","seq":0,"msgs":100}␍␊
C: PUB orders.eu.20 _INBOX.Ky9Pf34qY6Nb3wWD25RQ6H.fb01.100.fail.2.1.$FI 2␍␊m2␍␊
   ... messages 3 to 100 with operation 1 ...
S: MSG _INBOX.Ky9Pf34qY6Nb3wWD25RQ6H.fb01.100.fail.100.1.$FI 3 35␍␊{"type":"ack","seq":100,"msgs":100}␍␊
C: PUB orders.eu.20 _INBOX.Ky9Pf34qY6Nb3wWD25RQ6H.fb01.100.fail.101.2.$FI 4␍␊m101␍␊
S: MSG _INBOX.Ky9Pf34qY6Nb3wWD25RQ6H.fb01.100.fail.101.2.$FI 3 56␍␊{"stream":"ORDERS","seq":107,"batch":"fb01","count":101}␍␊
```

> **Client rule:** A fast-ingest publisher needs its own subscription on
> `<inbox>.<batch id>.>`; the shared request/reply mux cannot be used because replies arrive on
> many subjects and more than one reply can arrive per publish. Send the first message (operation
> `0`) and **wait for its reply**: a flow acknowledgment with `seq` 0 gives the cadence; an error
> PubAck means the batch was refused. Then publish freely, tracking the highest acknowledged
> batch sequence. Before each publish, wait if
> `last_ack.seq + last_ack.msgs * max_outstanding_acks <= next_batch_sequence`, where
> `max_outstanding_acks` is 1, 2 (recommended default) or at most 3 (ADR-50: clients must not allow
> more than 3). An acknowledgment for sequence N implies all earlier ones. Adopt a changed `msgs`
> value from each flow acknowledgment, but never from a gap report. On silence, ping. Classify
> every incoming message by `type`: `ack`, `gap`, `err`, or (no `type`) the final PubAck, which
> ends the batch. The reference Go client does not implement this; the rule is derived from the
> server and ADR-50.

### 6.12 Message scheduling

With `allow_msg_schedules: true` (ADR-51) a stored message can act as a **schedule**: at the
scheduled time the server publishes a derived message to a target subject **in the same stream**.
A schedule is created by publishing a message with `Nats-Schedule` to some subject of the stream;
that subject identifies the schedule.

| Header on the schedule message | Format | Rules (error on violation) |
|---|---|---|
| `Nats-Schedule` | `@at <RFC 3339 time>` for one firing; `@every <Go duration of at least 1s>` for repetition; a six-field cron expression `second minute hour day-of-month month day-of-week`; or a predefined name: `@yearly`/`@annually` (`0 0 0 1 1 *`), `@monthly` (`0 0 0 1 * *`), `@weekly` (`0 0 0 * * 0`), `@daily`/`@midnight` (`0 0 0 * * *`), `@hourly` (`0 0 * * * *`) | Invalid: 10189 |
| `Nats-Schedule-Target` | Publish subject | Required. Must be a valid publish subject, differ from the schedule's own subject, and be captured by the stream: 10190 |
| `Nats-Schedule-Source` | Literal subject | Optional. When the schedule fires, the payload and headers are taken from the **newest message on this subject** (or from the schedule message itself if there is none). Must be valid, differ from the target and from the schedule subject, and be within the stream's subjects: 10203 |
| `Nats-Schedule-TTL` | Same formats as `Nats-TTL` | Becomes `Nats-TTL` on produced messages. Invalid: 10191. Requires `allow_msg_ttl`: 10166 |
| `Nats-Schedule-Rollup` | Only `sub` | Becomes `Nats-Rollup` on produced messages. Other values: 10192 |
| `Nats-Schedule-Time-Zone` | A time zone name known to the server (IANA name, `UTC`, `Local`) | Cron schedules only; with `@at` or `@every` the pattern is rejected (10189). Empty or unknown: 10223 |

Behavior:

- A stream without `allow_msg_schedules` rejects any schedule header with 10188.
- The schedule message is stored as a per-subject rollup: the server adds `Nats-Rollup: sub` if it
  is absent, and an explicit `Nats-Rollup` other than `sub` is rejected with 10192. Hence there is
  **one schedule per subject**, and publishing a new schedule to the same subject replaces the old
  one.
- The produced message is the source content with all `Nats-Schedule*` headers, all
  `Nats-Expected-*` headers, `Nats-Msg-Id`, `Nats-TTL` and `Nats-Rollup` removed, and with these
  added: `Nats-Scheduler: <schedule subject>`; `Nats-Schedule-Next: purge` for a one-shot schedule
  (the schedule message is then removed) or `Nats-Schedule-Next: <RFC 3339 time of the next
  firing>` for a repeating one; and `Nats-TTL` / `Nats-Rollup` from the schedule's TTL and rollup
  headers.
- Firings missed while the server was down fire once, not once per missed interval.
- **Canceling atomically**: a client publish carrying `Nats-Schedule-Next: purge` and
  `Nats-Scheduler: <schedule subject>` is stored and removes the schedule on that subject in the
  same step. From clients, `Nats-Schedule-Next` must be exactly `purge`; `Nats-Scheduler` must be a
  non-empty valid publish subject different from the publish subject; `Nats-Scheduler` without
  `Nats-Schedule-Next` is rejected; and if a message exists on the scheduler subject it must really
  be a schedule. Violations: 10212.
- A schedule can also be stopped by deleting the schedule message by sequence or purging its
  subject (ADR-51).
- Under interest retention a schedule published while no consumer is interested in its subject is
  not stored and never fires (ADR-51).
- Not available on mirrors or on streams with sources.

> **Client rule:** Expose scheduling as publish options that set `Nats-Schedule` (`@at` with a UTC
> RFC 3339 time, `@every` with a duration of at least one second, or a cron/predefined string),
> the required `Nats-Schedule-Target`, and the optional source, TTL (duration or `never`), time
> zone and rollup (`sub`). Validate client-side only what the reference client validates: a
> non-empty target, source, time zone and cron string, an `@every` interval of at least 1 s, and a
> positive TTL. Map 10188 to 10192 and 10203 to named errors.

### 6.13 Size limits, full streams and overload

Size limits, in the order a message meets them:

1. The connection's `max_payload` (headers plus payload), enforced by the core protocol before
   JetStream sees the message ([01-core.md](01-core.md)).
2. The stream's `max_msg_size`, compared against header block plus payload (not the subject):
   10054 (`code` 400).
3. The header block alone may not exceed 65535 bytes: 10097.
4. File storage has an absolute per-record limit: 10077.

When a stream is full:

| Limit reached | `discard: "old"` (default) | `discard: "new"` |
|---|---|---|
| `max_msgs` or `max_bytes` | The oldest messages are removed; the publish succeeds. | The publish is rejected: **10077** (`code` 503) with description `maximum messages exceeded` or `maximum bytes exceeded`. |
| `max_msgs_per_subject` | The oldest message **of that subject** is removed; the publish succeeds. | Same as for `old`, unless `discard_new_per_subject` is set. |
| `max_msgs_per_subject` with `discard_new_per_subject: true` | not applicable (requires `discard: "new"`) | Rejected: 10077 with description `maximum messages per subject exceeded`. A message carrying `Nats-Rollup` is let through. |
| `max_age`, per-message TTL | Expiry is time-driven and independent of the discard policy. | Same. |

10077 is the generic "store failed" error; with `discard: "new"` its description is the only thing
that distinguishes "stream full" from other store failures, and its `code` is 503 although the
condition is not transient in the usual sense.

Other overload signals on publish:

- **Ingest queue full.** Each stream buffers at most 100,000 messages or 128 MiB awaiting
  processing (server options `max_buffered_msgs`, `max_buffered_size`). A message that does not fit
  is **dropped**. If it had a reply subject, the reply has both a status line and a JSON body:

  ```
  S: HMSG _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.21 1 34 133␍␊NATS/1.0 429 Too Many Requests␍␊␍␊{"error":{"code":429,"err_code":10167,"description":"too many requests"},"stream":"ORDERS","seq":0}␍␊
  ```

- Account storage or memory limit: 10002 (`code` 400, `resource limits exceeded for account`).
- Server resources exhausted: 10023 (`code` 503, `insufficient resources`). In a cluster the leader
  also steps down.
- Sealed stream: 10109.

> **Client rule:** The 429 ingest reply is the one PubAck that arrives as an `HMSG` with a status
> **and** a body. Parse the body like any PubAck (it contains `error` 10167); do not classify a
> publish reply as "status only" merely because a status line is present. Treat 10167, 10210 and
> 10211 (all `code` 429) as "back off and retry later". Treat 10077 as "the stream rejected the
> message"; it is not safe to retry blindly because with `discard: "new"` it will keep failing
> until space is freed.

### 6.14 Retention rules visible to publishers

- `limits`: nothing special.
- `interest`: if no consumer is interested in the message's subject, the message is **not stored**
  but still consumes a sequence number and is acknowledged normally. Its `Nats-Msg-Id` is still
  recorded for deduplication and a rollup header is still executed. No republish copy is emitted
  for such a message.
- `workqueue`: no publish-side difference; the constraints are on consumers.

> **Client rule:** A success PubAck means "accepted", not "retrievable": on an interest stream
> with no consumers the acknowledged sequence may hold no message. Do not follow a publish with a
> get-by-sequence as a correctness check.

### 6.15 Clustered-path differences

For a replicated stream (or any stream backed by a Raft group) the leader performs the checks of
[section 6.3](#63-order-of-server-checks) before proposing, with these differences:

- The order of the header checks is different (`server/jetstream_batching.go`,
  `checkMsgHeadersPreClusteredProposal`): header size, `Nats-Incr`, `Nats-Expected-Stream`,
  `Nats-TTL` (both "disabled" 10166 and "invalid" 10165), `Nats-Msg-Id` duplicate detection,
  `Nats-Stream-Source`, counter processing, `Nats-Expected-Last-Sequence`,
  `Nats-Expected-Last-Subject-Sequence`, scheduling headers, `Nats-Rollup`, discard-new limits. In
  particular duplicate detection comes **before** the expected-sequence checks.
- Consistency checks run against the last sequence **including proposals not yet committed**, so
  `Nats-Expected-Last-Sequence` takes in-flight messages into account.
- Discard-new limits are pre-checked on the leader with the same 10077 descriptions.
- A duplicate whose original is still in flight yields 10158; a per-subject expectation that races
  with an in-flight publish yields 10164 ([sections 6.5](#65-deduplication),
  [6.6](#66-optimistic-concurrency)).
- For a sealed stream the error acknowledgment is published without the usual `no_ack` guard.
- 10063 (`code` 503, `expected stream sequence does not match`) can be returned when the
  replicated sequence does not line up when the entry is applied.
- When server resources are exhausted the leader replies 10023 and steps down.
- A publish that reaches a server which is no longer the leader gets no reply.

### 6.16 Synchronous publish: client algorithm

The reference client's synchronous publish (ADR-22, `nats.go` `jetstream/publish.go`) is:

1. If the caller gave no deadline, apply the default (5 seconds).
2. Write the option headers into the message ([section 6.4](#64-publish-header-reference)).
3. Send the message as a core request and wait for one reply.
4. **If the reply is no responders, retry**: wait `retry_wait` (default 250 ms), send the same
   request again, at most `retry_attempts` additional times (default 2, so three requests in
   total). Only no responders is retried. A timeout or cancellation is returned immediately and is
   never retried.
5. If the last attempt still got no responders, fail with a distinct "no response from stream"
   error.
6. Decode the reply as a PubAck: invalid JSON is an "invalid acknowledgment" error; `error`
   present is an API error; a missing ack or an empty `stream` is an "invalid acknowledgment"
   error; otherwise return the ack.

Why only no responders is retried: it proves the message was **not** delivered to any stream, so
resending cannot create a duplicate. After a timeout the message may have been stored, so a blind
resend is only safe if the application supplied a `Nats-Msg-Id`; that decision belongs to the
application.

> **Client rule:** Implement synchronous publish exactly as steps 1 to 6, with configurable
> `retry_wait` (must be positive) and `retry_attempts` (must not be negative). The wait between
> attempts must respect the caller's deadline. The headers and the reply subject must be the same
> on every attempt, except that each attempt uses a fresh reply token if the request/reply mux
> requires one.

### 6.17 Asynchronous publish: client algorithm

Waiting for each acknowledgment limits throughput to one message per round trip. Asynchronous
publish sends messages without waiting and matches acknowledgments to messages afterwards. It keeps
at-least-once semantics by bounding the number of unacknowledged messages. The reference client's
design:

**State** (one set per JetStream context, guarded by one lock): a reply prefix, one subscription,
a table of pending messages keyed by reply token, a "stalled" signal, and a "all done" signal.

**Reply subjects.** On the first asynchronous publish the client creates the reply prefix
`<inbox prefix><6 random base-62 characters>.` (for example `_INBOX.Xk2P9a.`) and subscribes once
to `<reply prefix>*`. Each message gets a random 6-character base-62 token that is not currently in
use; its reply subject is `<reply prefix><token>`. The token is recovered from an incoming
acknowledgment by cutting the known prefix length off the subject. One subscription therefore
serves every pending publish.

**Publishing a message:**

1. Reject a message that already has a reply subject.
2. **Bounded window.** If the number of pending messages is at the limit (default 4000), block
   until an acknowledgment frees a slot or until `stall_wait` (default 200 ms) elapses. On expiry
   fail with a "stalled with too many outstanding async published messages" error; the message is
   not sent. The single `stall_wait` budget covers the whole wait, however often the publisher is
   woken.
3. Allocate the token, record the pending entry (the message, retry counters, and a result slot
   that a future or promise exposes), optionally start a per-message acknowledgment timer.
4. Publish the message with the reply subject using a plain core publish. If the publish itself
   fails, remove the entry and return the error.
5. Return the future to the caller.

**Handling a reply** (on the subscription's delivery thread):

1. Look up the pending entry by token; ignore the reply if there is none (a late acknowledgment).
2. Stop the per-message timer.
3. **No responders** (empty payload, status 503): if the entry has retries left, increment its
   retry count and re-publish the same message with the **same reply subject** after `retry_wait`
   (default 250 ms, default 2 retries); the entry stays pending and keeps counting against the
   window. Otherwise remove the entry and complete it with the "no response from stream" error.
4. Otherwise remove the entry, wake stalled publishers, and decode the PubAck exactly as in the
   synchronous case. Complete the future with the ack, or with the error.
5. If the table became empty, fire the "all done" signal.
6. Invoke the user's ack or error callback, if configured, outside the lock.

**Per-message timeout** (optional, disabled by default): when it fires and the entry is still
pending, remove it and complete it with an "ack timeout" error.

**Connection loss.** When the connection enters the reconnecting state, **every pending entry is
failed immediately** with a "disconnected" error and removed; stalled publishers are woken; the "all
done" signal fires. Nothing is re-published automatically, because the client cannot know which of
the messages the server stored before the connection dropped; that is the application's decision
(with message ids, resending everything is safe). Acknowledgments that arrive after the reconnect
find no entry and are dropped. On a final close the reference client does not fail pending
entries.

**Completion and cleanup.** The client exposes the number of pending messages and a signal that
fires when it reaches zero (stalled publishers are not counted). An explicit cleanup operation
unsubscribes the reply subscription and fails all pending entries with a "publisher closed" error.

> **Client rule:** Implement asynchronous publish with: one wildcard reply subscription per
> context, created lazily; a pending table keyed by reply token; a hard bound on pending entries
> (default 4000) with a bounded blocking wait (default 200 ms) that fails with a distinct "stalled"
> error; no-responders retry that keeps the entry pending; failure of all pending entries with a
> "disconnected" error as soon as the connection is lost; and an awaitable "all acknowledged"
> signal. Complete each future exactly once. Run user callbacks outside the publisher lock, and
> document that callbacks run on the reply delivery thread and must not block.

> **Client rule:** Never resubscribe-and-resend pending asynchronous publishes after a reconnect
> on the application's behalf. Fail them and let the application resend with the same
> `Nats-Msg-Id`.

---

## 7. Mirrors, sources, subject transforms and republish

These four features move or rewrite messages on the server. A client configures them through the
stream configuration ([section 3](#3-stream-configuration)); this section describes what a client
then observes in the messages.

### 7.1 Sources

A stream with `sources` runs, for each source, an internal consumer on the origin stream and stores
every message it receives into itself.

- Sourced messages get **new sequence numbers** in the destination stream and are interleaved with
  directly published messages in arrival order.
- Every sourced message carries the header **`Nats-Stream-Source`**. A pre-existing
  `Nats-Stream-Source` (from an earlier hop in a chain of sourcing streams) is replaced, so the
  header always describes the immediate upstream. Its value is a space-separated list:

  ```
  <source name> <origin sequence> <filter> <transform destination> <original subject>[ <stream identity>]
  ```

  | Field | Content |
  |---|---|
  | 1 | Origin stream name, with the suffix `:<hash of the external API prefix>` when the source is `external`, and `:C=<hash of the consumer name>` when a durable sourcing consumer is configured. |
  | 2 | Sequence of the message in the origin stream. |
  | 3 | The source's subject filter, `>` if none. Several transforms are joined with the form feed character (0x0C). |
  | 4 | The transform destination(s), `>` if none, joined the same way. |
  | 5 | The message's subject in the origin stream, before any transform. |
  | 6 | Optional: an identity of the origin stream. Present only with newer servers. |

  Example (from ADR-59): `Nats-Stream-Source: ORDERS 42 > > orders.us.new`. Older servers wrote
  shorter forms (a bare `$JS.ACK...` subject, or `<stream> <sequence>`), which this server still
  parses.
- `Nats-Expected-*` and `Nats-Batch-*` headers are removed from sourced messages. Other headers,
  including `Nats-Msg-Id`, `Nats-TTL`, `Nats-Incr` and `Nats-Rollup`, pass through.
- Deduplication by `Nats-Msg-Id` applies in the destination stream if its `duplicate_window` is
  non-zero (the default for a stream with sources is 0).
- Sourced messages bypass the "feature enabled" checks of the destination: a `Nats-TTL` is honored
  even if `allow_msg_ttl` is false, a rollup is executed even without `allow_rollup_hdrs`, a
  `Nats-Incr` is stored verbatim on a non-counter stream, and scheduling headers are not
  re-validated.
- A client may not publish `Nats-Stream-Source` itself: 10227.

> **Client rule:** When exposing the origin of a sourced message, split `Nats-Stream-Source` on
> spaces and accept five or six fields (and the legacy two-field form); take the origin stream name
> from field 1 up to the first `:` and the origin sequence from field 2. Never set this header on
> a publish.

### 7.2 Mirrors

- A mirror stores each message at the **same sequence number and timestamp** as in the origin, and
  fills gaps left by deleted origin messages with deleted sequences of its own. No
  `Nats-Stream-Source` header is added.
- `Nats-Expected-*` and `Nats-Batch-*` headers are removed on receipt. A mirror does not
  deduplicate.
- A mirror has no subjects, so clients cannot publish into it.
- A mirror is **promoted** to a normal stream by an update that removes `mirror` from its
  configuration.
- With `mirror_direct: true` the mirror answers Direct Get requests addressed to the origin
  stream's name ([section 5.7](#57-direct-get)), and the origin's stream info lists it in
  `alternates`.
- Progress and health are reported in stream info under `mirror` and `sources`
  ([section 4.4](#44-mirror-and-source-info)): `lag`, `active` (`-1` when the origin was never
  heard from) and `error`.

### 7.3 Stream subject transform

`subject_transform` is applied to the subject of each inbound message **before** every check and
before storing; a subject that does not match the transform's `src` is left unchanged. Everything
downstream works on the transformed subject: the stored `subject`, per-subject limits,
`Nats-Expected-Last-Subject-Sequence` (unless overridden by the `-Subject` header), rollups and
consumer filters. Inside atomic batches the transform is applied for the commit-time checks as
well.

> **Client rule:** The subject a message is read back under can differ from the subject it was
> published to. Never assume they are equal; use the subject returned with the stored message.

### 7.4 Republish

With `republish` configured, after a message is stored the **leader** publishes a copy to the
destination subject as a plain core NATS message (at-most-once, no acknowledgment, no replay).
Destinations that are invalid or reserved (`$JS.`, service replies) are suppressed. Subscribers
receive the original headers followed by:

| Header | Value |
|---|---|
| `Nats-Stream` | Stream name. |
| `Nats-Subject` | The subject the message is stored under. |
| `Nats-Sequence` | Stream sequence. |
| `Nats-Time-Stamp` | Storage time, RFC 3339 with nanoseconds, UTC. |
| `Nats-Last-Sequence` | Sequence of the previous message **on the same subject**, `0` if there is none. |
| `Nats-Msg-Size` | Only with `headers_only: true`: the payload length. The payload itself is omitted. |

`Nats-Last-Sequence` lets a subscriber that follows one subject detect a missed message without a
consumer: if it does not equal the `Nats-Sequence` of the last message the subscriber saw for that
subject, something was lost and can be fetched with a Direct Get. No copy is published for messages
that are skipped on interest-retention streams.

> **Client rule:** Republished messages need no special client support: they are ordinary `HMSG`
> deliveries on a core subscription. A helper that follows a republished subject should track
> `Nats-Sequence` per `Nats-Subject`, compare it with the next message's `Nats-Last-Sequence`, and
> recover gaps through Direct Get.

---

## 8. Cluster-visible behavior

### 8.1 Who answers, and what a leader gap looks like

The "Answered by" column of [section 2.9](#29-complete-api-subject-table) assigns each endpoint to
a role. Servers without the role stay silent. The observable consequences:

| Situation | Endpoints affected | What the client sees |
|---|---|---|
| No meta leader (election in progress) | Create, update, delete, names, list, account info, restore, peer operations | **Timeout** while the gap is shorter than 10 seconds. After the meta group has been leaderless for more than 10 seconds, every server answers 10008 (`code` 503). |
| No stream leader | Info, purge, message get, message delete, stepdown, snapshot | A group member that knows the group is leaderless answers 10008; otherwise timeout. Stream info is special: with the group leaderless, a non-leader member answers with its own view. |
| No stream leader | Publishes | No responders (503) or a timeout ([section 6.2](#62-when-a-publish-gets-no-acknowledgment)). Messages published without a reply subject during the gap are lost. |
| No stream leader | Direct Get | Still served, by any replica or mirror, possibly stale. |
| Leader exists but is slow, or the API queue overflowed | Any | Timeout. |

Error replies for 10008, 10118 and 10194 on stream info and update are delayed by 500 ms, so that
a healthy leader's answer arrives first if one exists.

### 8.2 Timeouts a client should plan for

| Interval | Value | Meaning |
|---|---|---|
| Meta leaderless before 10008 is sent | 10 s | Shorter gaps produce timeouts, not errors. |
| Delay of offline/leaderless error replies | 500 ms | Minimum latency of those errors. |
| `STREAM.LIST` gather in a cluster | 4 s of inactivity | Streams that have not answered by then are reported in `missing`. |
| Snapshot: wait for interest on the delivery subject | about 2 s | Then the snapshot fails. |
| Snapshot: chunk acknowledgment | 5 s | Then `408 No Flow Response`. |
| Restore: chunk inactivity | 5 s | Then the restore fails as stalled. |
| Atomic and fast batch idle time | 10 s (server option `max_batch_timeout`) | Then the batch is abandoned. |
| Reference client default API deadline | 5 s | Applied when the caller supplies none. |
| Reference client publish retry | 250 ms wait, 2 retries | On no responders only. |

> **Client rule:** Default every API call and every acknowledged publish to a deadline of 5
> seconds, make it configurable, and document that a leader election surfaces as a timeout or as
> 10008. Expose a predicate that classifies an error as transient cluster unavailability (timeout,
> no responders on publish, 10008, 10158, 10167) so that applications can write retry loops; the
> library itself retries only no responders on publish.

### 8.3 Offline and unsupported streams

A stream can exist in the cluster's metadata but be unusable on the servers that host it:

| Condition | Error |
|---|---|
| All peers of the stream are offline | 10118 (`code` 500, `stream is offline`) |
| The stream requires a higher API level than the server implements, or its stored configuration cannot be decoded by this server (unknown field or enum value) | 10194 (`code` 500, `stream is offline: <reason>`), with a reason such as `unsupported - config error: ...` |

Such streams appear in `STREAM.LIST` under `offline` (and `missing`). `STREAM.MSG.GET` on an
offline stream is not answered. The usual cause of 10194 is a mixed-version cluster: a stream
created through a newer server uses a feature an older server does not know.

### 8.4 Replicas and placement

- `num_replicas` is 1 to 5. More than 1 requires clustered JetStream (10074 otherwise).
- `placement.cluster` and `placement.tags` choose the peer set when the stream is created; if they
  cannot be satisfied the error is 10005 with a descriptive text.
- Changing `placement` in an update is a **move**; changing `num_replicas` is a **scale**; the two
  may not be combined (10123). While a reconfiguration runs, stream info shows `cluster.desired`
  with `origin` and `status` ([section 4.3](#43-cluster-info)); `STREAM.CANCEL_MOVE` rolls it back.
- Account limit tiers are keyed by replica count (`R1`, `R3`, ...), so the applicable limits depend
  on `num_replicas`.
- A server-side limit, `max_ha_assets`, caps the number of streams and consumers with more than one
  replica per server; it is enforced by the meta leader.
- A stream that captures `>` must have one replica and `no_ack`.

### 8.5 Server limits relevant to clients

| Server option | Meaning | Default in source |
|---|---|---|
| `request_queue_limit`, `info_queue_limit` | API queue lengths ([section 2.4](#24-the-request-queue-and-dropped-requests)) | 10,000 |
| `max_buffered_msgs`, `max_buffered_size` | Per-stream ingest queue | 100,000 messages, 128 MiB |
| `max_duplicate_window` | Upper bound for `duplicate_window` | none stated |
| `max_batch_inflight_per_stream` | Open batches per stream | atomic 50, fast 1000 |
| `max_batch_inflight_total` | Open batches per server | atomic 1000, fast 50,000 |
| `max_batch_size` | Messages per atomic batch | 1000 |
| `max_batch_timeout` | Idle time before a batch is abandoned | 10 s |
| `max_ha_assets` | Replicated assets per server | none stated |
| `default_max_consumers` | Default cap on consumers per stream | 1000 |
| `max_request_batch`, `max_ack_pending` | Consumer limits ([03-consumers.md](03-consumers.md)) | none stated |

### 8.6 Consistency notes

- `STREAM.MSG.GET` (leader) and Direct Get are isolated from partially applied atomic batches,
  rollups and purges. Other read paths (consumer delivery, mirrors and sources, stream info) may
  observe a prefix of a batch that is being applied.
- `state.consumer_count` in a cluster is the cluster-wide number of assigned consumers.
- `Nats-Request-Info` is added by the server when a request crosses the `$JS.API.>` service
  import; the API requires it to identify the caller's account, which is why raw `$JS.API` requests
  injected inside the system account (other than `$JS.API.INFO`) are rejected. It is removed from
  stored messages.

---

## 9. Advisories

Advisories are JSON events the server publishes as ordinary messages on `$JS.EVENT.ADVISORY.>`. A
client receives them with a plain subscription (or by capturing them in a stream: subjects under
`$JS.EVENT.>` may be captured without `no_ack`). An advisory is published only if something has
interest in its subject at that moment. They are best-effort notifications, not a reliable log.

Every event has three common keys: `type` (the schema name), `id` (a unique id) and `timestamp`
(RFC 3339, UTC). Where a `client` key is listed it is an object describing the connection that
caused the event, with the keys `start`, `host`, `id`, `acc`, `svc`, `user`, `name`, `lang`, `ver`,
`rtt`, `server`, `cluster`, `stop`, `issuer_key`, `name_tag`, `tags`, `kind`, `client_type`,
`client_id`, `nonce`, `reply` (all *omit*; `jwt` and alternates are blanked). `domain` is *omit*
everywhere.

### 9.1 API advisories

| Subject | `type` | Additional keys | Published to |
|---|---|---|---|
| `$JS.EVENT.ADVISORY.API` | `io.nats.jetstream.advisory.v1.api_audit` | `server` (string); `client`; `subject` (the API subject called); `request` (string, *omit*: the raw request body); `response` (string: the raw JSON response); `domain` | The caller's account |
| `$JS.EVENT.ADVISORY.API.LIMIT_REACHED` | `io.nats.jetstream.advisory.v1.api_limit_reached` | `server`; `domain`; `dropped` (int64: number of requests discarded from the queue) | System account |

The audit advisory is emitted for every answered API call except successful `STREAM.MSG.GET`
replies and Direct Gets.

### 9.2 Stream advisories

| Subject | `type` | Additional keys | Published to |
|---|---|---|---|
| `$JS.EVENT.ADVISORY.STREAM.CREATED.<stream>` | `io.nats.jetstream.advisory.v1.stream_action` | `stream`; `action`: `"create"`; `domain` | Stream's account |
| `$JS.EVENT.ADVISORY.STREAM.DELETED.<stream>` | `io.nats.jetstream.advisory.v1.stream_action` | `stream`; `action`: `"delete"`; `domain` | Stream's account |
| `$JS.EVENT.ADVISORY.STREAM.UPDATED.<stream>` | `io.nats.jetstream.advisory.v1.stream_action` | `stream`; `action`: `"modify"`; `domain` | Stream's account |
| `$JS.EVENT.ADVISORY.STREAM.SNAPSHOT_CREATE.<stream>` | `io.nats.jetstream.advisory.v1.snapshot_create` | `stream`; `state` (stream state); `client`; `domain` | Stream's account |
| `$JS.EVENT.ADVISORY.STREAM.SNAPSHOT_COMPLETE.<stream>` | `io.nats.jetstream.advisory.v1.snapshot_complete` | `stream`; `start`; `end` (times); `client`; `domain`; `error` (string, *omit*) | Stream's account |
| `$JS.EVENT.ADVISORY.STREAM.RESTORE_CREATE.<stream>` | `io.nats.jetstream.advisory.v1.restore_create` | `stream`; `client`; `domain` | Account |
| `$JS.EVENT.ADVISORY.STREAM.RESTORE_COMPLETE.<stream>` | `io.nats.jetstream.advisory.v1.restore_complete` | `stream`; `start`; `end`; `bytes` (int64); `client`; `domain` | Account |
| `$JS.EVENT.ADVISORY.STREAM.LEADER_ELECTED.<stream>` | `io.nats.jetstream.advisory.v1.stream_leader_elected` | `account` (*omit*); `stream`; `leader` (server name); `replicas` (array of peer info); `domain` | Stream's account, and the system account with `account` set |
| `$JS.EVENT.ADVISORY.STREAM.QUORUM_LOST.<stream>` | `io.nats.jetstream.advisory.v1.stream_quorum_lost` | `account` (*omit*); `stream`; `replicas`; `domain` | Stream's account, and the system account with `account` set |
| `$JS.EVENT.ADVISORY.STREAM.BATCH_ABANDONED.<stream>` | `io.nats.jetstream.advisory.v1.stream_batch_abandoned` | `account` (*omit*); `stream`; `domain`; `batch` (batch id); `reason`: `"timeout"`, `"large"`, `"incomplete"` or `"unsupported"` | Stream's account, and the system account with `account` set |

The `action` values are exactly `create`, `delete`, `modify`.

### 9.3 Server and domain advisories (system account)

| Subject | `type` | Additional keys |
|---|---|---|
| `$JS.EVENT.ADVISORY.DOMAIN.LEADER_ELECTED` | `io.nats.jetstream.advisory.v1.domain_leader_elected` | `leader`; `replicas` (array of peer info); `cluster`; `domain` |
| `$JS.EVENT.ADVISORY.SERVER.OUT_OF_STORAGE` | `io.nats.jetstream.advisory.v1.server_out_of_space` | `server`; `server_id`; `stream` (*omit*); `cluster`; `domain` |
| `$JS.EVENT.ADVISORY.SERVER.REMOVED` | `io.nats.jetstream.advisory.v1.server_removed` | `server`; `server_id`; `cluster`; `domain` |
| `$JS.EVENT.ADVISORY.SERVER.META_RESCUE` | `io.nats.jetstream.advisory.v1.meta_rescue` | `server`; `server_id`; `prev_quorum` (int); `new_quorum` (int); `cluster`; `domain` |

Two names do not match their subjects: the subject `SERVER.OUT_OF_STORAGE` carries the type
`...server_out_of_space`, and the subject `SNAPSHOT_CREATE` carries `...snapshot_create` (not
"created").

### 9.4 Consumer advisories and metrics

Listed for completeness; payloads are in [03-consumers.md](03-consumers.md).

| Subject prefix | `type` |
|---|---|
| `$JS.EVENT.METRIC.CONSUMER.ACK.<stream>.<consumer>` | `io.nats.jetstream.metric.v1.consumer_ack` |
| `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES` | `io.nats.jetstream.advisory.v1.max_deliver` |
| `$JS.EVENT.ADVISORY.CONSUMER.MSG_NAKED` | `io.nats.jetstream.advisory.v1.nak` |
| `$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED` | `io.nats.jetstream.advisory.v1.terminated` |
| `$JS.EVENT.ADVISORY.CONSUMER.CREATED`, `.DELETED` | `io.nats.jetstream.advisory.v1.consumer_action` |
| `$JS.EVENT.ADVISORY.CONSUMER.PAUSE` | `io.nats.jetstream.advisory.v1.consumer_pause` |
| `$JS.EVENT.ADVISORY.CONSUMER.PINNED`, `.UNPINNED` | `io.nats.jetstream.advisory.v1.consumer_group_pinned`, `...consumer_group_unpinned` |
| `$JS.EVENT.ADVISORY.CONSUMER.LEADER_ELECTED`, `.QUORUM_LOST` | `io.nats.jetstream.advisory.v1.consumer_leader_elected`, `...consumer_quorum_lost` |

The consumer acknowledgment metric is the only subject under `$JS.EVENT.METRIC.>`; there is no
stream-level metric.

> **Client rule:** Advisories need no dedicated client machinery. If typed access is offered,
> decode by the `type` key, ignore unknown keys and unknown types, and never depend on advisories
> for correctness: they are published only while a subscriber exists and are not retried.

---

## 10. Errors every client must special-case

The complete table is in [Appendix A](#appendix-a-complete-error-code-table). These are the codes
a client library should recognize by `err_code` and turn into distinct, documented errors, because
library logic or common application logic branches on them.

| `err_code` | `code` | Meaning | Why it needs special handling |
|---|---|---|---|
| 10039 | 503 | JetStream not enabled for account | Availability probe; distinct from a transient failure. |
| 10076 | 503 | JetStream not enabled | Same class as 10039, server-wide. |
| 10008 | 503 | JetStream system temporarily unavailable | Leaderless meta or stream group. Transient; retry with backoff. |
| 10059 | 404 | Stream not found | Drives create-or-update (update first, create on 10059) and "exists?" checks. |
| 10058 | 400 | Stream name already in use with a different configuration | Create collided with a different existing stream. |
| 10037 | 404 | No message found | `STREAM.MSG.GET` miss. Must map to the same "message not found" error as Direct Get status 404. |
| 10071 | 400 | Wrong last sequence (with the actual value) | Optimistic concurrency failure; applications retry their read-modify-write. |
| 10164 | 400 | Wrong last sequence (no value) | Same meaning as 10071 on clustered streams; map to the same error. |
| 10070 | 400 | Wrong last message id | Optimistic concurrency by message id. |
| 10060 | 400 | Expected stream does not match | The subject is bound to a different stream than the publisher assumed. |
| 10158 | 409 | Duplicate message id is in process | Clustered duplicate race; retry shortly. |
| 10077 | 503 | Store failed (free-form description) | "Stream full" under `discard: "new"`, among others. Not blindly retryable. |
| 10054 | 400 | Message size exceeds maximum allowed | Per-stream `max_msg_size`; permanent for that message. |
| 10167 | 429 | Too many requests | Ingest queue overflow; back off and retry. Arrives with a status line and a body. |
| 10210, 10211 | 429 | Too many batches in flight | Back off before opening another batch. |
| 10003 | 400 | Bad request | Also returned (with `type` `system_response`) for an unknown API subject, which usually means a client/server version mismatch. |
| 10025 | 400 | Invalid JSON | Under strict decoding this includes "unknown field": the server is older than the feature the client used. |
| 10185 | 412 | Minimum API level required | Server too old for the request. |
| 10052 | 500 | Stream configuration invalid (free-form description) | Despite `code` 500 this is a caller error; do not retry. |
| 10157 | 400 | Pedantic mode violation | Only when the client sets `pedantic`. |
| 10118, 10194 | 500 | Stream offline / offline with reason | Not retryable in the short term; 10194 usually means a version mismatch inside the cluster. |
| 10109 | 400 | Invalid operation on sealed stream | Publishes, purges and deletes on a sealed stream. |
| 10023 | 503 | Insufficient resources | Server out of storage or memory. |
| 10002 | 400 | Resource limits exceeded for account | Account quota reached. |
| 10010 | 503 | Clustering support required | A cluster-only operation was sent to a standalone server. |
| 10165, 10166 | 400 | Per-message TTL invalid / disabled | Feature errors for `Nats-TTL`. |
| 10168 to 10172 | 400 | Counter errors | Feature errors for `Nats-Incr`. |
| 10174 to 10177, 10179, 10199 to 10201 | 400 | Atomic batch errors | Any of them means the batch is gone. |
| 10205 to 10208 | 400 | Fast batch errors | 10205 disabled, 10206 bad reply subject, 10207 bad id, 10208 unknown batch. |
| 10188 to 10192, 10203, 10212, 10223 | 400 | Scheduling errors | Feature errors for `Nats-Schedule*`. |

Three rules of thumb follow from the table. First, `code` alone is unreliable for deciding whether
to retry: 10077 is 503 but may be permanent, and 10052 is 500 but is a caller error. Second, a
library should retry none of these errors on its own; its only automatic retry is for no
responders on publish ([section 6.16](#616-synchronous-publish-client-algorithm)). Third, the
consumer-related codes are handled in [03-consumers.md](03-consumers.md).

---

## 11. Scenarios

The transcripts show the bytes on one client connection after `CONNECT` with `headers` and
`no_responders` enabled ([01-core.md](01-core.md)). The server is a single standalone server
without a JetStream domain. The client uses one multiplexed reply subscription for all requests:

```
C: SUB _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.* 1␍␊
```

Each request uses a fresh reply token under that prefix. Byte counts on control lines are exact.
For `HPUB`/`HMSG` the first number is the size of the header block (including its terminating
empty line) and the second is header block plus payload. Timestamps, the server version and the
initial values of an empty stream's state are illustrative; everything else is what this server
version emits for the given input. Each transcript line is one protocol operation and is not
wrapped.

### 11.1 Create a stream

```
C: PUB $JS.API.STREAM.CREATE.ORDERS _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.1 78␍␊{"name":"ORDERS","subjects":["orders.>"],"storage":"file","allow_direct":true}␍␊
S: MSG _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.1 1 827␍␊{"type":"io.nats.jetstream.api.v1.stream_create_response","config":{"name":"ORDERS","subjects":["orders.\u003e"],"retention":"limits","max_consumers":-1,"max_msgs":-1,"max_bytes":-1,"max_age":0,"max_msgs_per_subject":-1,"max_msg_size":-1,"discard":"old","storage":"file","num_replicas":1,"duplicate_window":120000000000,"compression":"none","allow_direct":true,"mirror_direct":false,"sealed":false,"deny_delete":false,"deny_purge":false,"allow_rollup_hdrs":false,"consumer_limits":{},"allow_msg_ttl":false,"metadata":{"_nats.level":"5","_nats.req.level":"0","_nats.ver":"2.16.0-dev"}},"created":"2026-11-22T10:15:30.123456789Z","state":{"messages":0,"bytes":0,"first_seq":0,"first_ts":"0001-01-01T00:00:00Z","last_seq":0,"last_ts":"0001-01-01T00:00:00Z","consumer_count":0},"ts":"2026-11-22T10:15:30.124831Z","did_create":true}␍␊
```

1. The request is a plain `PUB` to the create subject with the configuration as payload. The
   stream name appears both in the subject and in the body; they must match.
2. The client sent only four keys. The reply contains the **normalized** configuration: defaults
   filled in (`-1` limits, `"retention":"limits"`, `"discard":"old"`, `"num_replicas":1`), a
   `duplicate_window` of 2 minutes (120000000000 ns), and the server-owned `_nats.*` metadata.
3. The subject comes back as `"orders.>"`: the server's JSON encoder escapes `>`. The value
   is the same string `orders.>`.
4. The stream info keys (`config`, `created`, `state`, `ts`) sit at the top level beside `type`.
   `did_create` is `true`; a standalone server sets it on every successful create, including an
   identical repeat.
5. Sending the identical request again returns the same kind of success. Sending it with, say,
   `"storage":"memory"` returns error 10058.

### 11.2 An API error

```
C: PUB $JS.API.STREAM.INFO.NOPE _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.2 0␍␊␍␊
S: MSG _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.2 1 158␍␊{"type":"io.nats.jetstream.api.v1.stream_info_response","error":{"code":404,"err_code":10059,"description":"stream not found"},"total":0,"offset":0,"limit":0}␍␊
```

The request asks for the info of a stream that does not exist. It has no body (`0` bytes). The
reply is an ordinary message; nothing at the protocol level marks it as a failure. The client
detects the `error` key and raises a "stream not found" error from `err_code` 10059. The keys
`total`, `offset` and `limit` are present with zero values because the info response structure
always serializes them; they carry no meaning here.

### 11.3 Publish and acknowledgment

```
C: PUB orders.eu.1 _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.3 5␍␊hello␍␊
S: MSG _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.3 1 27␍␊{"stream":"ORDERS","seq":1}␍␊
```

The publish is a core request to the application subject `orders.eu.1`. The stream `ORDERS`
captures it through `orders.>`, stores it as sequence 1, and replies with the PubAck. The PubAck
has no `type` key. Had the client sent `PUB orders.eu.1 5␍␊hello␍␊` without a reply subject, the
message would have been stored in the same way and nothing would have come back.

### 11.4 Duplicate publish

```
C: HPUB orders.eu.2 _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.4 37 41␍␊NATS/1.0␍␊Nats-Msg-Id: order-7781␍␊␍␊paid␍␊
S: MSG _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.4 1 27␍␊{"stream":"ORDERS","seq":2}␍␊
C: HPUB orders.eu.2 _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.5 37 41␍␊NATS/1.0␍␊Nats-Msg-Id: order-7781␍␊␍␊paid␍␊
S: MSG _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.5 1 45␍␊{"stream":"ORDERS","seq":2,"duplicate": true}␍␊
```

1. The first publish carries `Nats-Msg-Id: order-7781` in a 37-byte header block followed by the
   4-byte payload. It is stored as sequence 2.
2. The client did not see the acknowledgment in time (say) and sends the same message again, with
   the same id and a new reply token.
3. The server finds the id in its deduplication table, stores nothing, and answers with the
   original sequence and `"duplicate": true`. Note the space after the colon: this acknowledgment
   is built from a byte template. The stream still has `last_seq` 2.

### 11.5 Publish with a wrong expected sequence

```
C: HPUB orders.eu.1 _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.6 44 51␍␊NATS/1.0␍␊Nats-Expected-Last-Sequence: 1␍␊␍␊shipped␍␊
S: MSG _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.6 1 104␍␊{"error":{"code":400,"err_code":10071,"description":"wrong last sequence: 2"},"stream":"ORDERS","seq":0}␍␊
C: HPUB orders.eu.1 _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.7 44 51␍␊NATS/1.0␍␊Nats-Expected-Last-Sequence: 2␍␊␍␊shipped␍␊
S: MSG _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.7 1 27␍␊{"stream":"ORDERS","seq":3}␍␊
```

1. The client believes the stream's last sequence is 1 and says so with
   `Nats-Expected-Last-Sequence: 1`. The actual last sequence is 2 (scenario 11.4).
2. The server rejects the publish. The reply is an error PubAck: `error` with `err_code` 10071,
   the stream name, and `"seq":0`. The description contains the actual last sequence, 2. Nothing
   was stored.
3. The client re-reads its state and retries with the correct expectation. The message is stored
   as sequence 3.

### 11.6 Publish to a full stream under discard new

Precondition: a stream `JOBS` with `"subjects":["jobs.>"]`, `"max_msgs":2`, `"discard":"new"`
already holds two messages.

```
C: PUB jobs.resize _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.8 5␍␊job-3␍␊
S: MSG _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.8 1 105␍␊{"error":{"code":503,"err_code":10077,"description":"maximum messages exceeded"},"stream":"JOBS","seq":0}␍␊
```

The publish is rejected with the generic store error 10077; `code` is 503. The description
`maximum messages exceeded` is the only indication that the stream is full (`maximum bytes
exceeded` for `max_bytes`, `maximum messages per subject exceeded` for
`discard_new_per_subject`). With `"discard":"old"` the same publish would have succeeded as
sequence 3 and the message at sequence 1 would have been removed.

### 11.7 Publish with no stream on the subject

```
C: PUB invoices.new _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.9 5␍␊inv-1␍␊
S: HMSG _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.9 1 44 44␍␊NATS/1.0 503␍␊Nats-Subject: invoices.new␍␊␍␊␍␊
```

1. No stream captures `invoices.new`, and no other subscriber exists, so the core server finds no
   recipient for a message that has a reply subject. Because the client connected with
   `no_responders`, the server immediately sends the no-responders status to the reply subject: a
   44-byte header block `NATS/1.0 503␍␊Nats-Subject: invoices.new␍␊␍␊` and no payload.
2. This is not a JetStream reply. There is no JSON. The client recognizes it by status 503 and an
   empty payload.
3. Following [section 6.16](#616-synchronous-publish-client-algorithm) the client waits 250 ms and
   sends the request again, up to two more times. If a stream leader had merely been between
   elections, a retry would succeed. Here all three attempts receive the same status and the
   publish fails with "no response from stream".

### 11.8 Direct get by sequence and last by subject

By sequence (form A):

```
C: PUB $JS.API.DIRECT.GET.ORDERS _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.10 9␍␊{"seq":1}␍␊
S: HMSG _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.10 1 124 129␍␊NATS/1.0␍␊Nats-Stream: ORDERS␍␊Nats-Subject: orders.eu.1␍␊Nats-Sequence: 1␍␊Nats-Time-Stamp: 2026-11-22T10:15:31.000482Z␍␊␍␊hello␍␊
```

The reply is the stored message itself as an `HMSG`: a 124-byte header block with the four
metadata headers, then the 5-byte payload `hello`. The `HMSG` subject is the client's inbox; the
message's own subject is in `Nats-Subject`. The stored message had no headers, so the block
contains only the metadata headers.

Last message on a subject (form B), no request body:

```
C: PUB $JS.API.DIRECT.GET.ORDERS.orders.eu.2 _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.11 0␍␊␍␊
S: HMSG _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.11 1 148 152␍␊NATS/1.0␍␊Nats-Msg-Id: order-7781␍␊Nats-Stream: ORDERS␍␊Nats-Subject: orders.eu.2␍␊Nats-Sequence: 2␍␊Nats-Time-Stamp: 2026-11-22T10:15:32.25091Z␍␊␍␊paid␍␊
```

The stored message had a header (`Nats-Msg-Id`); the server keeps it and appends the metadata
headers after it.

A miss:

```
C: PUB $JS.API.DIRECT.GET.ORDERS.orders.us.1 _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.12 0␍␊␍␊
S: HMSG _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.12 1 34 34␍␊NATS/1.0 404 Message Not Found␍␊␍␊␍␊
```

The reply is a 34-byte header block consisting of the status line only, with no payload. The
client maps status 404 to "message not found".

### 11.9 A batch direct get ending in EOB

A batch produces several replies, so the client uses a dedicated subscription instead of the
request mux. The stream holds sequences 1, 2 and 3; the client asks for two messages starting at
sequence 1.

```
C: SUB _INBOX.Ky9Pf34qY6Nb3wWD25RQ5G 2␍␊
C: PUB $JS.API.DIRECT.GET.ORDERS _INBOX.Ky9Pf34qY6Nb3wWD25RQ5G 19␍␊{"seq":1,"batch":2}␍␊
S: HMSG _INBOX.Ky9Pf34qY6Nb3wWD25RQ5G 2 168 173␍␊NATS/1.0␍␊Nats-Stream: ORDERS␍␊Nats-Subject: orders.eu.1␍␊Nats-Sequence: 1␍␊Nats-Time-Stamp: 2026-11-22T10:15:31.000482Z␍␊Nats-Num-Pending: 2␍␊Nats-Last-Sequence: 0␍␊␍␊hello␍␊
S: HMSG _INBOX.Ky9Pf34qY6Nb3wWD25RQ5G 2 192 196␍␊NATS/1.0␍␊Nats-Msg-Id: order-7781␍␊Nats-Stream: ORDERS␍␊Nats-Subject: orders.eu.2␍␊Nats-Sequence: 2␍␊Nats-Time-Stamp: 2026-11-22T10:15:32.25091Z␍␊Nats-Num-Pending: 1␍␊Nats-Last-Sequence: 1␍␊␍␊paid␍␊
S: HMSG _INBOX.Ky9Pf34qY6Nb3wWD25RQ5G 2 64 64␍␊NATS/1.0 204 EOB␍␊Nats-Num-Pending: 1␍␊Nats-Last-Sequence: 2␍␊␍␊␍␊
C: UNSUB 2␍␊
```

1. Each message carries `Nats-Num-Pending` (matching messages remaining after this one: 2, then 1)
   and `Nats-Last-Sequence` (the previous message in this series: 0 for the first, then 1).
2. The series ends with the EOB message: status `204`, description `EOB`, no payload. Its
   `Nats-Num-Pending: 1` tells the client one more message exists; `Nats-Last-Sequence: 2` is the
   last sequence delivered.
3. To continue, the client sends `{"seq":3,"batch":2}`. That reply series would be one message
   followed by an EOB with `Nats-Num-Pending: 0`.

### 11.10 Purge by subject

```
C: PUB $JS.API.STREAM.PURGE.ORDERS _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.13 24␍␊{"filter":"orders.eu.1"}␍␊
S: MSG _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.13 1 83␍␊{"type":"io.nats.jetstream.api.v1.stream_purge_response","success":true,"purged":2}␍␊
```

The stream held two messages on `orders.eu.1` (sequences 1 and 3) and one on `orders.eu.2`. The
purge removes the two matching messages and reports `"purged":2`. Afterwards the stream state is
`messages` 1, `first_seq` 2, `last_seq` 3: the last sequence is not reset, and the next publish
receives sequence 4.

### 11.11 An atomic batch commit

Precondition: `ORDERS` was updated with `"allow_atomic":true`. Its last sequence is 3.

```
C: HPUB orders.eu.10 _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.14 61 68␍␊NATS/1.0␍␊Nats-Batch-Id: b7Qm2xLp␍␊Nats-Batch-Sequence: 1␍␊␍␊created␍␊
S: MSG _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.14 1 0␍␊␍␊
C: HPUB orders.eu.10 61 65␍␊NATS/1.0␍␊Nats-Batch-Id: b7Qm2xLp␍␊Nats-Batch-Sequence: 2␍␊␍␊paid␍␊
C: HPUB orders.eu.11 _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.15 83 90␍␊NATS/1.0␍␊Nats-Batch-Id: b7Qm2xLp␍␊Nats-Batch-Sequence: 3␍␊Nats-Batch-Commit: 1␍␊␍␊created␍␊
S: MSG _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.15 1 56␍␊{"stream":"ORDERS","seq":6,"batch":"b7Qm2xLp","count":3}␍␊
```

1. The first message opens batch `b7Qm2xLp` with `Nats-Batch-Sequence: 1` and is sent as a
   request. The server stages it and replies with a **zero-length** message (`MSG ... 0`). An
   error PubAck here (for example 10174 when the stream does not allow atomic publishing) would
   end the batch.
2. The second message has no reply subject, so the server sends nothing for it.
3. The third message carries `Nats-Batch-Commit: 1` and is sent as a request. The server checks
   and stores all three messages together.
4. The commit reply is a PubAck with `"batch"` and `"count":3`. `"seq":6` is the sequence of the
   last message; the batch occupies sequences 4, 5 and 6. Had any message failed a check, the
   commit reply would be that error and none of the three would be stored.

### 11.12 A counter increment

Precondition: a stream `COUNTERS` with `"subjects":["counters.>"]` and `"allow_msg_counter":true`.

```
C: HPUB counters.hits _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.16 27 27␍␊NATS/1.0␍␊Nats-Incr: +5␍␊␍␊␍␊
S: MSG _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.16 1 39␍␊{"stream":"COUNTERS","seq":1,"val":"5"}␍␊
C: HPUB counters.hits _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.17 27 27␍␊NATS/1.0␍␊Nats-Incr: +3␍␊␍␊␍␊
S: MSG _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.17 1 39␍␊{"stream":"COUNTERS","seq":2,"val":"8"}␍␊
C: PUB $JS.API.DIRECT.GET.COUNTERS.counters.hits _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.18 0␍␊␍␊
S: HMSG _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.18 1 140 151␍␊NATS/1.0␍␊Nats-Incr: +3␍␊Nats-Stream: COUNTERS␍␊Nats-Subject: counters.hits␍␊Nats-Sequence: 2␍␊Nats-Time-Stamp: 2026-11-22T10:16:02.118Z␍␊␍␊{"val":"8"}␍␊
```

1. An increment is an `HPUB` with a `Nats-Incr` header and **no payload** (header size and total
   size are both 27).
2. The acknowledgment carries the new total in `val`, as a string. The first increment starts
   from zero: `"val":"5"`. The second adds 3: `"val":"8"`.
3. Reading the counter is a last-by-subject get. The stored message's payload is the JSON object
   `{"val":"8"}` (11 bytes) and its `Nats-Incr` header is the last increment applied.

### 11.13 A per-message TTL with its delete marker

Precondition: a stream `EVENTS` created with `"subjects":["events.>"]`, `"allow_direct":true`,
`"allow_msg_ttl":true` and `"subject_delete_marker_ttl":60000000000` (the server also turned on
`allow_rollup_hdrs`).

```
C: HPUB events.session.42 _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.19 26 31␍␊NATS/1.0␍␊Nats-TTL: 2m␍␊␍␊login␍␊
S: MSG _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.19 1 27␍␊{"stream":"EVENTS","seq":1}␍␊
```

The message is stored as sequence 1 with a TTL of two minutes. (A TTL shorter than the marker TTL
of 60 seconds, for example `Nats-TTL: 5s`, would have been raised to 60 seconds and the stored
header rewritten to `Nats-TTL: 60`.)

Two minutes later the server removes sequence 1. It was the last message on
`events.session.42`, so the server stores a delete marker on that subject as sequence 2. A
last-by-subject get now returns the marker:

```
C: PUB $JS.API.DIRECT.GET.EVENTS.events.session.42 _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.20 0␍␊␍␊
S: HMSG _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.20 1 192 192␍␊NATS/1.0␍␊Nats-Marker-Reason: MaxAge␍␊Nats-TTL: 1m0s␍␊Nats-Rollup: sub␍␊Nats-Stream: EVENTS␍␊Nats-Subject: events.session.42␍␊Nats-Sequence: 2␍␊Nats-Time-Stamp: 2026-11-22T10:19:05.000211Z␍␊␍␊␍␊
```

1. The payload is empty (header size and total size are both 192), yet there is no status code on
   the first header line: this is a real stored message, not an error.
2. `Nats-Marker-Reason: MaxAge` identifies it as a tombstone. `Nats-TTL: 1m0s` is the marker's own
   lifetime, after which the marker expires as well.
   `Nats-Rollup: sub` is part of the marker as stored.
3. A consumer on the stream receives the marker as an ordinary message at sequence 2 and must not
   hand it to the application as data.

---

## 12. ADR versus server discrepancies

Where an ADR and the server source disagree, the server's behavior is what a client meets. The
"Server" column is what this chapter documents.

| Topic | ADR says | Server does |
|---|---|---|
| API level of `allow_batched` | ADR-50: setting it should set the required API level to 3. | Sets `_nats.req.level` to 4 (source comment: fast batch publishing was added in v2.14). |
| Delete marker reasons | ADR-43: markers with `Nats-Marker-Reason` `Remove` (message delete API) and `Purge` (purge API) in addition to `MaxAge`. | Only `MaxAge` markers are emitted; the constants `Purge` and `Remove` exist but are not used to create markers. Deletes and purges leave no marker. |
| `Nats-Stream-Source` format | ADR-59: five space-separated fields. | Can append a sixth field (origin stream identity); also still parses two legacy forms. |
| Body on Direct Get form B | ADR-31: a request payload on the subject-appended form is an error (408). | A JSON body is tolerated as long as it contains none of `seq`, `last_by_subj`, `next_by_subj`, `multi_last`, `batch`, `start_time`, `up_to_seq`, `up_to_time`; in effect `{"no_hdr":true}` is accepted. |
| `no_hdr` on Direct Get | ADR-52 (no-headers support) is marked deprecated in the ADR index. | `no_hdr` is implemented; on Direct Get it removes all headers including the `Nats-*` metadata. |
| Direct Get status wording | ADR-31 lists `408` for "empty or invalid" requests in general. | Three distinct descriptions: `408 Empty Request`, `408 Malformed Request`, `408 Bad Request`; plus `412 Required Api Level` and `500 <error>`. |
| No-responders status on publish | ADR-22: "a 503 status message". | The status message of this version also carries a `Nats-Subject` header naming the request subject. |

Server behaviors that are not ADR conflicts but contradict what the names suggest:

- The response type `io.nats.jetstream.api.v1.system_overloaded` is declared but never sent; an
  overflowing API queue drops requests silently.
- `did_create` is `true` on every successful standalone create, including idempotent repeats.
- Stream configuration validation errors (10052) have `code` 500 although they are caller errors.
- The `STREAM.TEMPLATE.*` endpoints are gone; only their error codes (10066 to 10068, 10073)
  remain.
- 408 on Direct Get means "malformed request", and 503 on error 10077 can mean "stream full".

Differences between the reference Go client and the server that a new client should not copy:
the Go client's built-in descriptions for 10136, 10138 and 10139 claim `code` 500 while the server
defines 400; its message-delete path assumes an `error` object whenever `success` is false; its
purge discards the `purged` count; and its listers have no guard against an empty page.

---

## 13. Client implementation checklist

Every client rule of this chapter, grouped by area. Section references point to the reasoning.

**Transport and envelope**

1. Build JetStream on core request/reply only; no new protocol elements are needed
   ([1.3](#13-how-the-layering-shapes-the-server-and-therefore-the-client)).
2. Classify every reply: empty payload with status 503 is no responders; no reply is a timeout;
   non-JSON payload is an invalid response; JSON with `error` is an API error; otherwise success
   ([2.3](#23-silence-system-errors-and-no-responders)).
3. Decide success or failure by the presence of `error`, never by `type`; do not validate `type`
   ([2.2](#22-request-and-response-bodies)).
4. Represent API errors as (`code`, `err_code`, `description`); match on `err_code`; never match
   on `description`; fall back to `code` for unknown codes
   ([2.2](#22-request-and-response-bodies)).
5. Treat a timeout as an unknown outcome; do not retry API calls automatically
   ([2.3](#23-silence-system-errors-and-no-responders)).
6. Give every API call a deadline, default 5 seconds, configurable; one deadline covers a whole
   paged iteration ([2.4](#24-the-request-queue-and-dropped-requests), [2.6](#26-paging)).
7. Do not flood the API: bound concurrency of helper-issued requests and avoid polling info
   endpoints in tight loops ([2.4](#24-the-request-queue-and-dropped-requests)).
8. Hold one API prefix per context: `$JS.API`, `$JS.<domain>.API`, or a custom prefix. Apply it to
   every API subject including Direct Get and pull requests. Reject empty domain or prefix. No
   network call when the context is created
   ([2.5](#25-subject-prefix-domains-and-custom-prefixes)).
9. Send only keys the server defines and omit unset optional keys; unknown keys fail with 10025
   under strict decoding ([2.2](#22-request-and-response-bodies)).
10. Parse responses leniently: ignore unknown keys, accept `null` for arrays
    ([3.1](#31-conventions-and-pedantic-mode), [4.6](#46-stream-names)).
11. Page with `offset += items received` until `offset >= total`; stop on an empty page
    ([2.6](#26-paging)).
12. Treat `_nats.*` metadata as server-owned: never set it, ignore it in comparisons. Learn the
    server's capabilities from `api.level` or `_nats.level`
    ([2.7](#27-api-levels-and-_nats-metadata)).
13. If `Nats-Required-Api-Level` is sent, map 10185 and Direct Get status 412 to a "server too
    old" error ([2.7](#27-api-levels-and-_nats-metadata)).

**Stream management**

14. Validate stream names client-side (non-empty; none of `>`, `*`, `.`, space, `/`, `\`, TAB, CR,
    LF) before building a subject ([3.2](#32-identity-and-metadata)).
15. Serialize durations as integer nanoseconds and times as RFC 3339; use the exact enum strings
    ([3.11](#311-enum-strings)).
16. Implement `create` (map 10058), `update` (map 10059) and `create_or_update` (update first,
    create only on 10059). Ignore `did_create`
    ([3.12](#312-create-and-update-rules-that-span-fields)).
17. Updates send the complete configuration; never a partial object
    ([3.12](#312-create-and-update-rules-that-span-fields)).
18. Treat the echoed configuration as the truth and detect silently dropped features
    (`subject_transform`, `sources`, source transforms) by comparing request and response
    ([3.12](#312-create-and-update-rules-that-span-fields)).
19. Express a mirror or source in another domain as `external.api = $JS.<domain>.API`
    ([2.5](#25-subject-prefix-domains-and-custom-prefixes), [3.9](#39-mirror-and-sources)).
20. Send stream info without a body when no options are set; merge subject pages; do not cache
    the subjects map ([4.5](#45-stream-info-request)).
21. Do not assume `messages == last_seq - first_seq + 1` ([4.2](#42-stream-state)).
22. Surface `missing` and `offline` from stream list ([4.7](#47-stream-list)).
23. Purge: reject `seq` together with `keep` client-side; return the `purged` count
    ([5.5](#55-purge)).
24. Message get: decode `hdrs` (base64 of a raw header block) with the core header parser and
    `data` as base64; map 10037 to "message not found" ([5.6](#56-message-get)).
25. Message delete: offer plain (`no_erase: true`) and secure (default) variants; handle a reply
    with neither `error` nor `success` ([5.8](#58-message-delete)).
26. Snapshot and restore: follow the chunk protocols exactly, acknowledging every snapshot chunk
    and inspecting every restore chunk reply ([5.9](#59-snapshot), [5.10](#510-restore)).
27. Account info is the availability probe: map no responders, 10039 and 10076 to "JetStream not
    enabled" errors ([5.14](#514-account-info)).

**Direct Get**

28. Use Direct Get only when the stream has `allow_direct`; otherwise `STREAM.MSG.GET`. Document
    that Direct Get can be stale ([5.7](#57-direct-get)).
29. Decode single replies: status first (404 not found, 503 no responders, others are errors),
    then require `Nats-Stream`, `Nats-Subject`, `Nats-Sequence`, `Nats-Time-Stamp`. An empty
    payload without a status is a valid message ([5.7](#57-direct-get)).
30. Batch and multi gets need a dedicated subscription; read until status 204 (EOB), an error
    status, or the deadline; check continuity with `Nats-Last-Sequence`; page using
    `Nats-Num-Pending`, `Nats-Last-Sequence + 1` and, for multi, `Nats-UpTo-Sequence`
    ([5.7](#57-direct-get)).

**Publishing**

31. An acknowledged publish is a request; fire-and-forget is plain core publish and gives no
    guarantee ([6.1](#61-mechanics-and-the-puback)).
32. Parse the PubAck as JSON: `error` first; then require a non-empty `stream`; expose `seq`,
    `duplicate`, `domain`, `val`, `batch`, `count` ([6.1](#61-mechanics-and-the-puback)).
33. Provide header options for message id, expected stream, expected last sequence, expected last
    subject sequence (with optional subject), expected last message id, TTL and rollup. Send 0
    for expected sequences when explicitly set ([6.4](#64-publish-header-reference)).
34. Never set `Nats-Stream-Source`, `Nats-Request-Info` or `Nats-Marker-Reason`
    ([6.4](#64-publish-header-reference), [7.1](#71-sources)).
35. `duplicate: true` is success; 10158 is retryable; message ids come from the application and
    are stable across retries ([6.5](#65-deduplication)).
36. Map 10071 and 10164 to one "wrong last sequence" error; document the retry ambiguity when a
    message id and an expectation are combined ([6.3](#63-order-of-server-checks),
    [6.6](#66-optimistic-concurrency)).
37. Treat messages with `Nats-Marker-Reason` as tombstones
    ([6.8](#68-per-message-ttl-and-delete-markers)).
38. Counters: publish `Nats-Incr` with an empty payload; treat `val` as an arbitrary-precision
    integer string ([6.9](#69-counters)).
39. Atomic batches: first message as a request with a zero-length success reply; consecutive
    sequences from 1; commit as a request; verify `batch` and `count`; any error ends the batch
    ([6.10](#610-atomic-batch-publishing)).
40. Fast-ingest batches: dedicated wildcard subscription; wait for the first reply; bound
    outstanding acknowledgment intervals (at most 3); ping on silence; classify replies by `type`
    ([6.11](#611-fast-ingest-batch-publishing)).
41. Scheduling: set `Nats-Schedule` and the required `Nats-Schedule-Target`; map 10188 to 10192
    and 10203 ([6.12](#612-message-scheduling)).
42. The 429 ingest reply has a status line **and** a JSON body; parse the body. Treat 10167,
    10210 and 10211 as back-off signals and 10077 as a rejection that is not blindly retryable
    ([6.13](#613-size-limits-full-streams-and-overload)).
43. A success PubAck means accepted, not necessarily retrievable (interest retention)
    ([6.14](#614-retention-rules-visible-to-publishers)).
44. Synchronous publish: retry only on no responders, 250 ms apart, 2 retries by default, within
    the caller's deadline; then fail with "no response from stream"
    ([6.16](#616-synchronous-publish-client-algorithm)).
45. Asynchronous publish: one lazily created wildcard reply subscription; pending table keyed by
    reply token; bounded window (default 4000) with a bounded stall wait (default 200 ms) and a
    distinct stall error; no-responders retry keeps the entry pending; optional per-message
    timeout; complete each future exactly once; callbacks outside the lock
    ([6.17](#617-asynchronous-publish-client-algorithm)).
46. On connection loss fail all pending asynchronous publishes with a "disconnected" error; never
    resend them automatically ([6.17](#617-asynchronous-publish-client-algorithm)).

**Derived data, cluster behavior and events**

47. The subject read back can differ from the subject published (stream subject transform)
    ([7.3](#73-stream-subject-transform)).
48. Parse `Nats-Stream-Source` as five or six space-separated fields, or the legacy two-field form
    ([7.1](#71-sources)).
49. Republished messages are ordinary core messages; gap detection uses `Nats-Last-Sequence` per
    subject ([7.4](#74-republish)).
50. Expect leader elections to appear as timeouts, as no responders on publish, or as 10008;
    offer a "transient" predicate (timeout, no responders on publish, 10008, 10158, 10167) and
    leave retry policy to the application ([8.1](#81-who-answers-and-what-a-leader-gap-looks-like),
    [8.2](#82-timeouts-a-client-should-plan-for)).
51. Recognize the error codes of [section 10](#10-errors-every-client-must-special-case) as named
    errors; pass every other code through with its three fields.
52. Do not depend on advisories for correctness; decode them by `type` and ignore unknown ones
    ([9](#9-advisories)).

---

## 14. Unverified items

Statements in this chapter that could not be confirmed in the server or client source.

1. **Mutability of `compression` and `first_seq`.** The server's update check contains no rule for
   either field; what a change does at the storage level was not traced
   ([3.5](#35-storage-and-replication)).
2. **`did_create` on an idempotent create in a cluster** is inferred from two code paths and was
   not exercised ([3.12](#312-create-and-update-rules-that-span-fields)).
3. **`missing` in a clustered stream list.** That the field is filled from the streams that did
   not answer is verified; the bookkeeping that determines the names was only skimmed
   ([4.7](#47-stream-list)).
4. **Account limit sentinels.** `-1` meaning unlimited is verified only for the server's default
   dynamic account limits, not for every way limits can be configured
   ([5.14](#514-account-info)).
5. **Initial state of an empty stream** in scenario 11.1 (`first_seq` 0 and zero timestamps) is
   illustrative and was not captured from a running server.
6. **No responders for a publish to a `no_ack` stream** (stated as a timeout in
   [6.2](#62-when-a-publish-gets-no-acknowledgment)) is deduced from the core rule that any
   delivery, including one to the stream's internal subscription, suppresses the 503; it was not
   exercised.
7. **Fast-ingest flow control under load.** The acknowledgment cadence logic was read
   (`server/jetstream_batching.go`, `checkFlowControl`), but the subject on which a given flow
   message arrives and the behavior with many concurrent publishers were not exercised; the
   transcript in [6.11](#611-fast-ingest-batch-publishing) is illustrative.
8. **`never` TTL and `max_age`.** That a message with `Nats-TTL: never` is exempt from `max_age`
   is taken from ADR-43; the store code was not read.
9. **Interest retention and schedules** ([6.12](#612-message-scheduling)) is taken from ADR-51.
10. **Introducing versions** of features are quoted from source comments only.
11. Error 10163 (`expected last sequence per subject temporarily unavailable`) is defined but has
    no use site in this server version; whether older servers send it was not checked.

---

## Appendix A. Complete error code table

All 227 errors defined by this server version (`server/errors.json`), `err_code` 10002 to 10228
without gaps. `code` is the HTTP-like class. Descriptions are templates: `{err}`, `{seq}`, `{id}`,
`{subject}` and similar placeholders are replaced at run time, and a description of just `{err}`
means the text is entirely free-form. Constants ending in `F` take such arguments. Distribution by
`code`: 400 (171), 500 (34), 503 (12), 404 (4), 429 (3), 412 (2), 409 (1). Consumer-related codes
are included because the table is the whole file; their use is described in
[03-consumers.md](03-consumers.md). The stream template errors (10066 to 10068, 10073) are legacy:
the template API no longer exists. Error 10163 is defined but never returned by this version.

| `err_code` | `code` | Constant | Description | Notes |
|---:|---:|---|---|---|
| 10002 | 400 | `JSAccountResourcesExceededErr` | resource limits exceeded for account |  |
| 10003 | 400 | `JSBadRequestErr` | bad request |  |
| 10004 | 503 | `JSClusterIncompleteErr` | incomplete results |  |
| 10005 | 400 | `JSClusterNoPeersErrF` | {err} | Error causing no peers to be available |
| 10006 | 500 | `JSClusterNotActiveErr` | JetStream not in clustered mode | deprecates ErrJetStreamNotClustered |
| 10007 | 500 | `JSClusterNotAssignedErr` | JetStream cluster not assigned to this server | deprecates ErrJetStreamNotAssigned |
| 10008 | 503 | `JSClusterNotAvailErr` | JetStream system temporarily unavailable |  |
| 10009 | 500 | `JSClusterNotLeaderErr` | JetStream cluster can not handle request | deprecates ErrJetStreamNotLeader |
| 10010 | 503 | `JSClusterRequiredErr` | JetStream clustering support required |  |
| 10011 | 400 | `JSClusterTagsErr` | tags placement not supported for operation |  |
| 10012 | 500 | `JSConsumerCreateErrF` | {err} | General consumer creation failure string |
| 10013 | 400 | `JSConsumerNameExistErr` | consumer name already in use | deprecates ErrJetStreamConsumerAlreadyUsed |
| 10014 | 404 | `JSConsumerNotFoundErr` | consumer not found |  |
| 10015 | 400 | `JSSnapshotDeliverSubjectInvalidErr` | deliver subject not valid |  |
| 10016 | 400 | `JSConsumerDurableNameNotInSubjectErr` | consumer expected to be durable but no durable name set in subject |  |
| 10017 | 400 | `JSConsumerDurableNameNotMatchSubjectErr` | consumer name in subject does not match durable name in request |  |
| 10018 | 400 | `JSConsumerDurableNameNotSetErr` | consumer expected to be durable but a durable name was not set |  |
| 10019 | 400 | `JSConsumerEphemeralWithDurableInSubjectErr` | consumer expected to be ephemeral but detected a durable name set in subject |  |
| 10020 | 400 | `JSConsumerEphemeralWithDurableNameErr` | consumer expected to be ephemeral but a durable name was set in request |  |
| 10021 | 400 | `JSStreamExternalApiOverlapErrF` | stream external api prefix {prefix} must not overlap with {subject} |  |
| 10022 | 400 | `JSStreamExternalDelPrefixOverlapsErrF` | stream external delivery prefix {prefix} overlaps with stream subject {subject} |  |
| 10023 | 503 | `JSInsufficientResourcesErr` | insufficient resources | deprecates ErrJetStreamResourcesExceeded |
| 10024 | 400 | `JSStreamInvalidExternalDeliverySubjErrF` | stream external delivery prefix {prefix} must not contain wildcards |  |
| 10025 | 400 | `JSInvalidJSONErr` | invalid JSON: {err} |  |
| 10026 | 400 | `JSMaximumConsumersLimitErr` | maximum consumers limit reached |  |
| 10027 | 400 | `JSMaximumStreamsLimitErr` | maximum number of streams reached |  |
| 10028 | 500 | `JSMemoryResourcesExceededErr` | insufficient memory resources available | deprecates ErrMemoryResourcesExceeded |
| 10029 | 500 | `JSMirrorConsumerSetupFailedErrF` | {err} | generic mirror consumer setup failure string |
| 10030 | 400 | `JSMirrorMaxMessageSizeTooBigErr` | stream mirror must have max message size >= source |  |
| 10031 | 400 | `JSMirrorWithSourcesErr` | stream mirrors can not also contain other sources |  |
| 10032 | 400 | `JSMirrorWithStartSeqAndTimeErr` | stream mirrors can not have both start seq and start time configured |  |
| 10033 | 400 | `JSMirrorWithSubjectFiltersErr` | stream mirrors can not contain filtered subjects |  |
| 10034 | 400 | `JSMirrorWithSubjectsErr` | stream mirrors can not contain subjects |  |
| 10035 | 503 | `JSNoAccountErr` | account not found |  |
| 10036 | 503 | `JSClusterUnSupportFeatureErr` | not currently supported in clustered mode |  |
| 10037 | 404 | `JSNoMessageFoundErr` | no message found |  |
| 10038 | 400 | `JSNotEmptyRequestErr` | expected an empty request payload |  |
| 10039 | 503 | `JSNotEnabledForAccountErr` | JetStream not enabled for account | help: This error indicates that JetStream is not enabled for an account account level |
| 10040 | 400 | `JSClusterPeerNotMemberErr` | peer not a member |  |
| 10041 | 500 | `JSRaftGeneralErrF` | {err} | General RAFT error string |
| 10042 | 500 | `JSRestoreSubscribeFailedErrF` | JetStream unable to subscribe to restore snapshot {subject}: {err} |  |
| 10043 | 400 | `JSSequenceNotFoundErrF` | sequence {seq} not found |  |
| 10044 | 400 | `JSClusterServerNotMemberErr` | server is not a member of the cluster |  |
| 10045 | 500 | `JSSourceConsumerSetupFailedErrF` | {err} | General source consumer setup failure string |
| 10046 | 400 | `JSSourceMaxMessageSizeTooBigErr` | stream source must have max message size >= target |  |
| 10047 | 500 | `JSStorageResourcesExceededErr` | insufficient storage resources available | deprecates ErrStorageResourcesExceeded |
| 10048 | 500 | `JSStreamAssignmentErrF` | {err} | Generic stream assignment error string |
| 10049 | 500 | `JSStreamCreateErrF` | {err} | Generic stream creation error string |
| 10050 | 500 | `JSStreamDeleteErrF` | {err} | General stream deletion error string |
| 10051 | 500 | `JSStreamGeneralErrorF` | {err} | General stream failure string |
| 10052 | 500 | `JSStreamInvalidConfigF` | {err} | Stream configuration validation error string |
| 10053 | 500 | `JSStreamLimitsErrF` | {err} | General stream limits exceeded error string |
| 10054 | 400 | `JSStreamMessageExceedsMaximumErr` | message size exceeds maximum allowed |  |
| 10055 | 400 | `JSStreamMirrorNotUpdatableErr` | stream mirror configuration can not be updated |  |
| 10056 | 400 | `JSStreamMismatchErr` | stream name in subject does not match request |  |
| 10057 | 500 | `JSStreamMsgDeleteFailedF` | {err} | Generic message deletion failure error string |
| 10058 | 400 | `JSStreamNameExistErr` | stream name already in use with a different configuration | deprecates ErrJetStreamStreamAlreadyUsed |
| 10059 | 404 | `JSStreamNotFoundErr` | stream not found | deprecates ErrJetStreamStreamNotFound |
| 10060 | 400 | `JSStreamNotMatchErr` | expected stream does not match |  |
| 10061 | 400 | `JSStreamReplicasNotUpdatableErr` | Replicas configuration can not be updated |  |
| 10062 | 500 | `JSStreamRestoreErrF` | restore failed: {err} |  |
| 10063 | 503 | `JSStreamSequenceNotMatchErr` | expected stream sequence does not match |  |
| 10064 | 500 | `JSStreamSnapshotErrF` | snapshot failed: {err} |  |
| 10065 | 400 | `JSStreamSubjectOverlapErr` | subjects overlap with an existing stream |  |
| 10066 | 500 | `JSStreamTemplateCreateErrF` | {err} | Generic template creation failed string |
| 10067 | 500 | `JSStreamTemplateDeleteErrF` | {err} | Generic stream template deletion failed error string |
| 10068 | 404 | `JSStreamTemplateNotFoundErr` | template not found |  |
| 10069 | 500 | `JSStreamUpdateErrF` | {err} | Generic stream update error string |
| 10070 | 400 | `JSStreamWrongLastMsgIDErrF` | wrong last msg ID: {id} |  |
| 10071 | 400 | `JSStreamWrongLastSequenceErrF` | wrong last sequence: {seq} |  |
| 10072 | 500 | `JSTempStorageFailedErr` | JetStream unable to open temp storage for restore |  |
| 10073 | 400 | `JSTemplateNameNotMatchSubjectErr` | template name in subject does not match request |  |
| 10074 | 500 | `JSStreamReplicasNotSupportedErr` | replicas > 1 not supported in non-clustered mode | deprecates ErrReplicasNotSupported |
| 10075 | 503 | `JSPeerRemapErr` | peer remap failed |  |
| 10076 | 503 | `JSNotEnabledErr` | JetStream not enabled | deprecates ErrJetStreamNotEnabled; help: This error indicates that JetStream is not enabled at a global level |
| 10077 | 503 | `JSStreamStoreFailedF` | {err} | Generic error when storing a message failed |
| 10078 | 400 | `JSConsumerConfigRequiredErr` | consumer config required |  |
| 10079 | 400 | `JSConsumerDeliverToWildcardsErr` | consumer deliver subject has wildcards |  |
| 10080 | 400 | `JSConsumerPushMaxWaitingErr` | consumer in push mode can not set max waiting |  |
| 10081 | 400 | `JSConsumerDeliverCycleErr` | consumer deliver subject forms a cycle |  |
| 10082 | 400 | `JSConsumerMaxPendingAckPolicyRequiredErr` | consumer requires ack policy for max ack pending |  |
| 10083 | 400 | `JSConsumerSmallHeartbeatErr` | consumer idle heartbeat needs to be >= 100ms |  |
| 10084 | 400 | `JSConsumerPullRequiresAckErr` | consumer in pull mode requires explicit ack policy on workqueue stream |  |
| 10085 | 400 | `JSConsumerPullNotDurableErr` | consumer in pull mode requires a durable name |  |
| 10086 | 400 | `JSConsumerPullWithRateLimitErr` | consumer in pull mode can not have rate limit set |  |
| 10087 | 400 | `JSConsumerMaxWaitingNegativeErr` | consumer max waiting needs to be positive |  |
| 10088 | 400 | `JSConsumerHBRequiresPushErr` | consumer idle heartbeat requires a push based consumer |  |
| 10089 | 400 | `JSConsumerFCRequiresPushErr` | consumer flow control requires a push based consumer |  |
| 10090 | 400 | `JSConsumerDirectRequiresPushErr` | consumer direct requires a push based consumer |  |
| 10091 | 400 | `JSConsumerDirectRequiresEphemeralErr` | consumer direct requires an ephemeral consumer |  |
| 10092 | 400 | `JSConsumerOnMappedErr` | consumer direct on a mapped consumer |  |
| 10093 | 400 | `JSConsumerFilterNotSubsetErr` | consumer filter subject is not a valid subset of the interest subjects |  |
| 10094 | 400 | `JSConsumerInvalidPolicyErrF` | {err} | Generic delivery policy error; help: Error returned for impossible deliver policies when combined with start sequences etc |
| 10095 | 400 | `JSConsumerInvalidSamplingErrF` | failed to parse consumer sampling configuration: {err} |  |
| 10096 | 500 | `JSStreamInvalidErr` | stream not valid |  |
| 10097 | 400 | `JSStreamHeaderExceedsMaximumErr` | header size exceeds maximum allowed of 64k |  |
| 10098 | 400 | `JSConsumerWQRequiresExplicitAckErr` | workqueue stream requires explicit ack |  |
| 10099 | 400 | `JSConsumerWQMultipleUnfilteredErr` | multiple non-filtered consumers not allowed on workqueue stream |  |
| 10100 | 400 | `JSConsumerWQConsumerNotUniqueErr` | filtered consumer not unique on workqueue stream |  |
| 10101 | 400 | `JSConsumerWQConsumerNotDeliverAllErr` | consumer must be deliver all on workqueue stream |  |
| 10102 | 400 | `JSConsumerNameTooLongErrF` | consumer name is too long, maximum allowed is {max} |  |
| 10103 | 400 | `JSConsumerBadDurableNameErr` | durable name can not contain '.', '*', '>' |  |
| 10104 | 500 | `JSConsumerStoreFailedErrF` | error creating store for consumer: {err} |  |
| 10105 | 400 | `JSConsumerExistingActiveErr` | consumer already exists and is still active |  |
| 10106 | 400 | `JSConsumerReplacementWithDifferentNameErr` | consumer replacement durable config not the same |  |
| 10107 | 400 | `JSConsumerDescriptionTooLongErrF` | consumer description is too long, maximum allowed is {max} |  |
| 10108 | 400 | `JSConsumerWithFlowControlNeedsHeartbeats` | consumer with flow control also needs heartbeats |  |
| 10109 | 400 | `JSStreamSealedErr` | invalid operation on sealed stream |  |
| 10110 | 500 | `JSStreamPurgeFailedF` | {err} | Generic stream purge failure error string |
| 10111 | 500 | `JSStreamRollupFailedF` | {err} | Generic stream rollup failure error string |
| 10112 | 400 | `JSConsumerInvalidDeliverSubject` | invalid push consumer deliver subject | help: Returned when the delivery subject on a Push Consumer is not a valid NATS Subject |
| 10113 | 400 | `JSStreamMaxBytesRequired` | account requires a stream config to have max bytes set |  |
| 10114 | 400 | `JSConsumerMaxRequestBatchNegativeErr` | consumer max request batch needs to be > 0 |  |
| 10115 | 400 | `JSConsumerMaxRequestExpiresTooSmall` | consumer max request expires needs to be >= 1ms |  |
| 10116 | 400 | `JSConsumerMaxDeliverBackoffErr` | max deliver is required to be > length of backoff values |  |
| 10117 | 500 | `JSStreamInfoMaxSubjectsErr` | subject details would exceed maximum allowed |  |
| 10118 | 500 | `JSStreamOfflineErr` | stream is offline |  |
| 10119 | 500 | `JSConsumerOfflineErr` | consumer is offline |  |
| 10120 | 400 | `JSNoLimitsErr` | no JetStream default or applicable tiered limit present |  |
| 10121 | 400 | `JSConsumerMaxPendingAckExcessErrF` | consumer max ack pending exceeds system limit of {limit} |  |
| 10122 | 400 | `JSStreamMaxStreamBytesExceeded` | stream max bytes exceeds account limit max stream bytes |  |
| 10123 | 400 | `JSStreamMoveAndScaleErr` | can not move and scale a stream in a single update |  |
| 10124 | 400 | `JSStreamMoveInProgressErr` | stream move already in progress |  |
| 10125 | 400 | `JSConsumerMaxRequestBatchExceededF` | consumer max request batch exceeds server limit of {limit} |  |
| 10126 | 400 | `JSConsumerReplicasExceedsStream` | consumer config replica count exceeds parent stream |  |
| 10127 | 400 | `JSConsumerNameContainsPathSeparatorsErr` | Consumer name can not contain path separators |  |
| 10128 | 400 | `JSStreamNameContainsPathSeparatorsErr` | Stream name can not contain path separators |  |
| 10129 | 400 | `JSStreamReconfigureNotInProgressErr` | stream reconfiguration not in progress |  |
| 10130 | 400 | `JSStreamNameExistRestoreFailedErr` | stream name already in use, cannot restore |  |
| 10131 | 400 | `JSConsumerCreateFilterSubjectMismatchErr` | Consumer create request did not match filtered subject from create subject |  |
| 10132 | 400 | `JSConsumerCreateDurableAndNameMismatch` | Consumer Durable and Name have to be equal if both are provided |  |
| 10133 | 400 | `JSReplicasCountCannotBeNegative` | replicas count cannot be negative |  |
| 10134 | 400 | `JSConsumerReplicasShouldMatchStream` | consumer config replicas must match interest retention stream's replicas |  |
| 10135 | 400 | `JSConsumerMetadataLengthErrF` | consumer metadata exceeds maximum size of {limit} |  |
| 10136 | 400 | `JSConsumerDuplicateFilterSubjects` | consumer cannot have both FilterSubject and FilterSubjects specified |  |
| 10137 | 400 | `JSConsumerMultipleFiltersNotAllowed` | consumer with multiple subject filters cannot use subject based API |  |
| 10138 | 400 | `JSConsumerOverlappingSubjectFilters` | consumer subject filters cannot overlap |  |
| 10139 | 400 | `JSConsumerEmptyFilter` | consumer filter in FilterSubjects cannot be empty |  |
| 10140 | 400 | `JSSourceDuplicateDetected` | duplicate source configuration detected | source stream, filter and transform (plus external if present) must form a unique combination |
| 10141 | 400 | `JSSourceInvalidStreamName` | sourced stream name is invalid |  |
| 10142 | 400 | `JSMirrorInvalidStreamName` | mirrored stream name is invalid |  |
| 10143 | 400 | `JSMirrorWithFirstSeqErr` | stream mirrors can not have first sequence configured |  |
| 10144 | 400 | `JSSourceMultipleFiltersNotAllowed` | source with multiple subject transforms cannot also have a single subject filter |  |
| 10145 | 400 | `JSSourceInvalidSubjectFilter` | source transform source: {err} |  |
| 10146 | 400 | `JSSourceInvalidTransformDestination` | source transform: {err} |  |
| 10147 | 400 | `JSSourceOverlappingSubjectFilters` | source filters can not overlap |  |
| 10148 | 400 | `JSConsumerAlreadyExists` | consumer already exists | action CREATE is used for an existing consumer with a different config |
| 10149 | 400 | `JSConsumerDoesNotExist` | consumer does not exist | action UPDATE is used for a nonexisting consumer |
| 10150 | 400 | `JSMirrorMultipleFiltersNotAllowed` | mirror with multiple subject transforms cannot also have a single subject filter |  |
| 10151 | 400 | `JSMirrorInvalidSubjectFilter` | mirror transform source: {err} |  |
| 10152 | 400 | `JSMirrorOverlappingSubjectFilters` | mirror subject filters can not overlap |  |
| 10153 | 400 | `JSConsumerInactiveThresholdExcess` | consumer inactive threshold exceeds system limit of {limit} |  |
| 10154 | 400 | `JSMirrorInvalidTransformDestination` | mirror transform: {err} |  |
| 10155 | 400 | `JSStreamTransformInvalidSource` | stream transform source: {err} |  |
| 10156 | 400 | `JSStreamTransformInvalidDestination` | stream transform: {err} |  |
| 10157 | 400 | `JSPedanticErrF` | pedantic mode: {err} |  |
| 10158 | 409 | `JSStreamDuplicateMessageConflict` | duplicate message id is in process |  |
| 10159 | 400 | `JSConsumerPriorityPolicyWithoutGroup` | Setting PriorityPolicy requires at least one PriorityGroup to be set |  |
| 10160 | 400 | `JSConsumerInvalidPriorityGroupErr` | Provided priority group does not exist for this consumer |  |
| 10161 | 400 | `JSConsumerEmptyGroupName` | Group name cannot be an empty string |  |
| 10162 | 400 | `JSConsumerInvalidGroupNameErr` | Valid priority group name must match A-Z, a-z, 0-9, -_/=)+ and may not exceed 16 characters |  |
| 10163 | 503 | `JSStreamExpectedLastSeqPerSubjectNotReady` | expected last sequence per subject temporarily unavailable |  |
| 10164 | 400 | `JSStreamWrongLastSequenceConstantErr` | wrong last sequence |  |
| 10165 | 400 | `JSMessageTTLInvalidErr` | invalid per-message TTL |  |
| 10166 | 400 | `JSMessageTTLDisabledErr` | per-message TTL is disabled |  |
| 10167 | 429 | `JSStreamTooManyRequests` | too many requests |  |
| 10168 | 400 | `JSMessageIncrDisabledErr` | message counters is disabled |  |
| 10169 | 400 | `JSMessageIncrMissingErr` | message counter increment is missing |  |
| 10170 | 400 | `JSMessageIncrPayloadErr` | message counter has payload |  |
| 10171 | 400 | `JSMessageIncrInvalidErr` | message counter increment is invalid |  |
| 10172 | 400 | `JSMessageCounterBrokenErr` | message counter is broken |  |
| 10173 | 400 | `JSMirrorWithCountersErr` | stream mirrors can not also calculate counters |  |
| 10174 | 400 | `JSAtomicPublishDisabledErr` | atomic publish is disabled |  |
| 10175 | 400 | `JSAtomicPublishMissingSeqErr` | atomic publish sequence is missing |  |
| 10176 | 400 | `JSAtomicPublishIncompleteBatchErr` | atomic publish batch is incomplete |  |
| 10177 | 400 | `JSAtomicPublishUnsupportedHeaderBatchErr` | atomic publish unsupported header used: {header} |  |
| 10178 | 400 | `JSConsumerPushWithPriorityGroupErr` | priority groups can not be used with push consumers |  |
| 10179 | 400 | `JSAtomicPublishInvalidBatchIDErr` | atomic publish batch ID is invalid |  |
| 10180 | 412 | `JSStreamMinLastSeqErr` | min last sequence |  |
| 10181 | 400 | `JSConsumerAckPolicyInvalidErr` | consumer ack policy invalid |  |
| 10182 | 400 | `JSConsumerReplayPolicyInvalidErr` | consumer replay policy invalid |  |
| 10183 | 400 | `JSConsumerAckWaitNegativeErr` | consumer ack wait needs to be positive |  |
| 10184 | 400 | `JSConsumerBackOffNegativeErr` | consumer backoff needs to be positive |  |
| 10185 | 412 | `JSRequiredApiLevelErr` | JetStream minimum api level required |  |
| 10186 | 400 | `JSMirrorWithMsgSchedulesErr` | stream mirrors can not also schedule messages |  |
| 10187 | 400 | `JSSourceWithMsgSchedulesErr` | stream source can not also schedule messages |  |
| 10188 | 400 | `JSMessageSchedulesDisabledErr` | message schedules is disabled |  |
| 10189 | 400 | `JSMessageSchedulesPatternInvalidErr` | message schedules pattern is invalid |  |
| 10190 | 400 | `JSMessageSchedulesTargetInvalidErr` | message schedules target is invalid |  |
| 10191 | 400 | `JSMessageSchedulesTTLInvalidErr` | message schedules invalid per-message TTL |  |
| 10192 | 400 | `JSMessageSchedulesRollupInvalidErr` | message schedules invalid rollup |  |
| 10193 | 400 | `JSStreamExpectedLastSeqPerSubjectInvalid` | missing sequence for expected last sequence per subject |  |
| 10194 | 500 | `JSStreamOfflineReasonErrF` | stream is offline: {err} |  |
| 10195 | 500 | `JSConsumerOfflineReasonErrF` | consumer is offline: {err} |  |
| 10196 | 400 | `JSConsumerPriorityGroupWithPolicyNone` | consumer can not have priority groups when policy is none |  |
| 10197 | 400 | `JSConsumerPinnedTTLWithoutPriorityPolicyNone` | PinnedTTL cannot be set when PriorityPolicy is none |  |
| 10198 | 400 | `JSMirrorWithAtomicPublishErr` | stream mirrors can not also use atomic publishing |  |
| 10199 | 400 | `JSAtomicPublishTooLargeBatchErrF` | atomic publish batch is too large: {size} |  |
| 10200 | 400 | `JSAtomicPublishInvalidBatchCommitErr` | atomic publish batch commit is invalid |  |
| 10201 | 400 | `JSAtomicPublishContainsDuplicateMessageErr` | atomic publish batch contains duplicate message id |  |
| 10202 | 400 | `JSClusterServerMemberChangeInflightErr` | cluster member change is in progress |  |
| 10203 | 400 | `JSMessageSchedulesSourceInvalidErr` | message schedules source is invalid |  |
| 10204 | 400 | `JSConsumerInvalidResetErr` | invalid reset: {err} |  |
| 10205 | 400 | `JSBatchPublishDisabledErr` | batch publish is disabled |  |
| 10206 | 400 | `JSBatchPublishInvalidPatternErr` | batch publish pattern is invalid |  |
| 10207 | 400 | `JSBatchPublishInvalidBatchIDErr` | batch publish ID is invalid |  |
| 10208 | 400 | `JSBatchPublishUnknownBatchIDErr` | batch publish ID unknown |  |
| 10209 | 400 | `JSMirrorWithBatchPublishErr` | stream mirrors can not also use batch publishing |  |
| 10210 | 429 | `JSAtomicPublishTooManyInflight` | atomic publish too many inflight |  |
| 10211 | 429 | `JSBatchPublishTooManyInflight` | batch publish too many inflight |  |
| 10212 | 400 | `JSMessageSchedulesSchedulerInvalidErr` | message schedules invalid scheduler |  |
| 10213 | 400 | `JSMirrorDurableConsumerCfgInvalid` | stream mirror consumer config is invalid |  |
| 10214 | 400 | `JSMirrorConsumerRequiresAckFCErr` | stream mirror consumer requires flow control ack policy |  |
| 10215 | 400 | `JSSourceDurableConsumerCfgInvalid` | stream source consumer config is invalid |  |
| 10216 | 400 | `JSSourceDurableConsumerDuplicateDetected` | duplicate stream source consumer detected |  |
| 10217 | 400 | `JSSourceConsumerRequiresAckFCErr` | stream source consumer requires flow control ack policy |  |
| 10218 | 400 | `JSConsumerAckFCRequiresPushErr` | flow control ack policy requires a push based consumer |  |
| 10219 | 400 | `JSConsumerAckFCRequiresFCErr` | flow control ack policy requires flow control |  |
| 10220 | 400 | `JSConsumerAckFCRequiresMaxAckPendingErr` | flow control ack policy requires max ack pending |  |
| 10221 | 400 | `JSConsumerAckFCRequiresNoAckWaitErr` | flow control ack policy requires unset ack wait |  |
| 10222 | 400 | `JSConsumerAckFCRequiresNoMaxDeliverErr` | flow control ack policy requires unset max deliver |  |
| 10223 | 400 | `JSMessageSchedulesTimeZoneInvalidErr` | message schedules time zone is invalid |  |
| 10224 | 400 | `JSClusterRescueErr` | JetStream system rescue not applied: {err} |  |
| 10225 | 400 | `JSConsumerStreamIdentityMismatchF` | consumer's stream identity does not match: {msg} |  |
| 10226 | 400 | `JSStreamReconfigureInProgressErr` | stream reconfiguration already in progress |  |
| 10227 | 400 | `JSMessageSourceHdrNotAllowedErr` | message stream source header is not allowed |  |
| 10228 | 500 | `JSSourceConsumerFlowControlStalledErr` | stream source consumer stalled on flow control |  |

## Appendix B. Status lines used outside the JSON envelope

These replies carry their result in the first line of the header block
([01-core.md](01-core.md)) instead of, or in addition to, a JSON body.

| Where | Status line | Body |
|---|---|---|
| Any request, nothing subscribed | `NATS/1.0 503` (no responders; this version adds a `Nats-Subject` header) | empty |
| Direct Get | `NATS/1.0 404 Message Not Found` | empty |
| Direct Get (multi) | `NATS/1.0 404 No Results` | empty |
| Direct Get | `NATS/1.0 408 Empty Request` | empty |
| Direct Get | `NATS/1.0 408 Malformed Request` | empty |
| Direct Get | `NATS/1.0 408 Bad Request` | empty |
| Direct Get | `NATS/1.0 412 Required Api Level` | empty |
| Direct Get (multi) | `NATS/1.0 413 Too Many Results` | empty |
| Direct Get (multi) | `NATS/1.0 500 <error text>` | empty |
| Direct Get (batch, multi) | `NATS/1.0 204 EOB` with `Nats-Num-Pending`, `Nats-Last-Sequence` and, for multi, `Nats-UpTo-Sequence` | empty |
| Snapshot chunk stream, final message | `NATS/1.0 204` | empty |
| Snapshot chunk stream, failure | `NATS/1.0 408 No Interest`, `NATS/1.0 408 No Flow Response`, `NATS/1.0 500 <error text>` | empty |
| Publish, ingest queue full | `NATS/1.0 429 Too Many Requests` | JSON error PubAck with `err_code` 10167 |
