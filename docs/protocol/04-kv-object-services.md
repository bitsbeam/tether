# 4. Key/Value, Object Store, and Services

This chapter specifies the three higher-level abstractions that NATS clients offer on top of the
core protocol ([01-core.md](01-core.md)) and JetStream ([02-jetstream.md](02-jetstream.md),
[03-consumers.md](03-consumers.md)): Key/Value buckets, Object Store buckets, and Services (the
"micro" framework). None of them is a server feature. Each is a set of naming and usage conventions
that every client library must implement identically so that a value written by one library can be
read by another. The chapter gives the conventions in full: stream configurations, subjects,
headers, JSON schemas, the algorithms behind each operation, byte-exact transcripts, and the points
where the specifications (the NATS Architecture Decision Records, "ADRs"), the server, and the Go
reference client `nats.go` disagree.

Sources: ADR-6 (naming), ADR-8 (Key/Value), ADR-19 (API prefixes), ADR-20 (Object Store), ADR-32
(Services), ADR-43 (per-message TTL), ADR-48 (limit markers), ADR-57 (KV mirrors and sources);
`nats.go` v1.56.0 (`jetstream/kv.go`, `jetstream/object.go`, `micro/`); `nats-server` 2.16.0-dev.

## Contents

1. [Mental model](#1-mental-model)
2. [Key/Value: buckets and the backing stream](#2-keyvalue-buckets-and-the-backing-stream)
3. [Key/Value: operations](#3-keyvalue-operations)
4. [Key/Value: watchers and everything built on them](#4-keyvalue-watchers-and-everything-built-on-them)
5. [Key/Value: mirrors, sources, domains, and API prefixes](#5-keyvalue-mirrors-sources-domains-and-api-prefixes)
6. [Key/Value: errors, required behaviors, discrepancies](#6-keyvalue-errors-required-behaviors-discrepancies)
7. [Object Store: buckets, subjects, and metadata](#7-object-store-buckets-subjects-and-metadata)
8. [Object Store: operations](#8-object-store-operations)
9. [Object Store: errors, required behaviors, discrepancies](#9-object-store-errors-required-behaviors-discrepancies)
10. [Services: discovery, endpoints, and statistics](#10-services-discovery-endpoints-and-statistics)
11. [Services: required behaviors and discrepancies](#11-services-required-behaviors-and-discrepancies)
12. [Scenarios](#12-scenarios)
13. [Client implementation checklist](#13-client-implementation-checklist)
14. [Unverified items](#14-unverified-items)

## 1. Mental model

### 1.1 Three conventions over one substrate

A JetStream stream is an ordered log of messages, each with a subject, optional headers, a payload,
a sequence number, and a timestamp. The three abstractions in this chapter are ways of using that
log, or plain core subscriptions, under fixed names.

| Abstraction | Backed by | Name of the backing object | Subjects | What a "write" is | What a "read" is |
|---|---|---|---|---|---|
| Key/Value bucket | one stream | `KV_<bucket>` | `$KV.<bucket>.<key>` | a JetStream publish to the key's subject | a "last message for subject" lookup (direct get) |
| Object Store bucket | one stream | `OBJ_<bucket>` | `$O.<bucket>.C.<nuid>` (chunks), `$O.<bucket>.M.<name-b64>` (metadata) | N chunk publishes, then one metadata publish with a rollup | a metadata lookup, then an ordered consumer over the chunk subject |
| Service | core subscriptions only | none | user-chosen endpoint subjects, plus `$SRV.PING\|INFO\|STATS…` | a core request | a core reply |

The mapping for Key/Value is worth stating in one sentence, because every operation follows from
it: **a key is a subject, a value is the last message on that subject, a revision is that message's
stream sequence, history is `max_msgs_per_subject`, and a delete is a message with a marker
header.** Compare-and-set is the JetStream `Nats-Expected-Last-Subject-Sequence` publish
precondition; a watch is an ordered consumer filtered on key subjects.

The Object Store applies the same idea twice in one stream: object metadata is a tiny Key/Value
store keyed by the encoded object name (always rolled up to one message per name), and object
content is a run of chunk messages on a subject unique to one upload.

Services use no JetStream at all. A service is a set of queue subscriptions (the endpoints) plus
nine plain subscriptions on well-known `$SRV` subjects that answer discovery requests with JSON.

The reason for this design is that the server stays small and general. The price is that the
contract lives in client libraries: "the clients are the authority on what is valid" (ADR-6).
A client that deviates in a subject template, a header name, or a base64 alphabet produces data
that other clients cannot read, and the server will not complain.

### 1.2 What the server does know

The server has no API named "KV", "object store", or "service". It never validates a key, never
interprets an `ObjectInfo` document, and contains no reference to `$SRV` or `Nats-Service-Error`.
There are exactly these points of contact:

| Server knowledge | Where | Effect |
|---|---|---|
| The header `KV-Operation` with value `PURGE` | subject delete marker logic (`server/sdm.go`) | A KV purge message is treated like a server-generated delete marker: when it later expires by TTL, the server does not generate a further `Nats-Marker-Reason` marker for it. |
| Subject delete markers (`Nats-Marker-Reason: MaxAge\|Purge\|Remove`) | stream config `subject_delete_marker_ttl` | The server writes tombstones itself when the last message of a subject ages out. KV clients must read them as deletes (section 3.4). This is a general stream feature that KV adopted (ADR-48). |
| `$JS.<domain>.API.$KV.>` → `$KV.>` and `$JS.<domain>.API.$OBJ.>` → `$OBJ.>` | JetStream domain subject mappings and leaf node deny lists (`server/jetstream_api.go`) | Lets a client write to a KV bucket in another JetStream domain by prefixing the key subject (section 5.3). Note the object store prefix here is `$OBJ`, which is not the prefix object stores actually use (section 9.3). |
| Mirror direct get | `mirror_direct` | A mirror answers direct gets addressed to its origin stream, which is how KV read replicas work (section 5.1). |

Everything else in this chapter is generic JetStream used in a particular way.

### 1.3 Conventions used in the transcripts

Transcripts show one client connection. `C:` lines are sent by the client and `S:` lines by the
server; `␍␊` marks the CR LF line terminator. Lines beginning with `#` are commentary and are not
on the wire. Unless stated otherwise the client has one wildcard reply subscription,
`SUB _INBOX.x.* 1`, and uses a fresh `_INBOX.x.<n>` reply subject per request, as described in
[01-core.md](01-core.md). `+OK`, `PING`, and `PONG` are omitted.

Large JetStream API responses (stream and consumer info) are abbreviated with `…` and their byte
count is written `<n>`; their full shape is in [02-jetstream.md](02-jetstream.md) and
[03-consumers.md](03-consumers.md). Every other byte count is exact. JSON requests are shown in a
minimal conforming form; key order inside a JSON object is not significant.

## 2. Key/Value: buckets and the backing stream

### 2.1 Naming rules

| Thing | Template or grammar | Limit |
|---|---|---|
| Stream name | `KV_<bucket>` | |
| Stream subjects | `$KV.<bucket>.>` | |
| Key subject | `$KV.<bucket>.<key>` | |
| Bucket name | one or more of `A-Z a-z 0-9 - _` (regex `\A[a-zA-Z0-9_-]+\z`) | 252 characters (the 255-byte stream name limit minus `KV_`) |
| Key | one or more of `A-Z a-z 0-9 - _ / = .`, not starting or ending with `.`, no `..` (regex `\A[-/_=\.a-zA-Z0-9]+\z` plus the dot rules) | bounded only by the subject length limit |
| Search key (watch and list filters) | a key in which a token may contain `*` and the final character may be `>` (regex `\A[-/_=\.a-zA-Z0-9*]*[>]?\z` plus the dot rules) | |
| Reserved keys | keys starting with `_kv` are reserved for internal use (ADR-6) | |

A key may contain dots, so a key is in general several subject tokens. That is deliberate: it lets
a watcher use subject wildcards (`config.*`, `users.>`) over key hierarchies. It is also why a key
may not contain `*`, `>`, or whitespace, and may not have empty tokens.

The server checks none of this: any publish whose subject matches `$KV.<bucket>.>` is stored.

> **Client rule:** Validate the bucket name against `\A[a-zA-Z0-9_-]+\z` and a maximum of 252
> characters before any request, and validate every key against the key rule before every
> operation (the search-key rule for watch and list filters). Raise a client-side error on
> failure. Do not rely on the server.

> **Client rule:** Refuse to write keys that start with `_kv`, but accept them when reading or
> watching. `nats.go` does not enforce the reservation, so such keys can exist.

### 2.2 The stream configuration

A bucket is created with an ordinary `$JS.API.STREAM.CREATE.KV_<bucket>` request
([02-jetstream.md](02-jetstream.md)). The table lists every stream field that the convention fixes
or uses. "User" means the value comes from the bucket options the client exposes.

| JSON key | Required value | Why | What `nats.go` sends | Server-side validation that bites |
|---|---|---|---|---|
| `name` | `KV_<bucket>` | Identifies the stream as a bucket; bucket listing filters on the prefix. | same | Stream name limits. |
| `subjects` | `["$KV.<bucket>.>"]`; **omitted for a mirror** | All keys of the bucket, and nothing else, land in this stream. | same; omitted when `mirror` is set | |
| `description` | User | | user value | |
| `max_msgs_per_subject` | History depth: 1 to 64, default 1 | Each key is a subject; this is how many revisions per key are kept. Overflow silently drops the oldest revision of that key. | `history`, default 1; more than 64 is rejected client-side | None for the 64 limit: it is a client convention. |
| `max_age` | Bucket TTL in nanoseconds, default `0` (never) | Every revision expires this long after it was written. | user `TTL` | |
| `max_msg_size` | Maximum value size, default `-1` | Caps the size of one value (headers included). | user, `0` becomes `-1` | Oversized writes fail with `err_code` 10054. |
| `max_bytes` | Maximum bucket size, default `-1` | | user, `0` becomes `-1` | With `discard: new`, writes fail when the limit is reached. |
| `max_msgs` | `-1` | The number of keys cannot be limited (ADR-8). | `-1` | |
| `max_consumers` | `-1` | Watchers are consumers; do not cap them. | `-1` | |
| `storage` | `"file"` (default) or `"memory"` | | user, default `file` | |
| `num_replicas` | User, default `1` | | user, `0` becomes `1` | |
| `retention` | `"limits"` | A bucket never removes data because it was consumed. | zero value (`limits`) | |
| `discard` | `"new"` | When a size limit is hit, reject the write instead of silently evicting some other key. | `"new"` | Writes over `max_bytes` fail with `maximum bytes exceeded`. `discard_new_per_subject` is **not** set, so per-key history overflow still drops the oldest revision. |
| `allow_rollup_hdrs` | `true` | Purge is a `Nats-Rollup: sub` publish. | `true` | A rollup on a stream without it fails with `err_code` 10111. |
| `deny_delete` | `true` | Individual revisions must not be deletable through the stream API; that would corrupt history silently. | `true` | Cannot be changed from `true` to `false` by an update. |
| `deny_purge` | `false` (omit) | Rollups and `purge_deletes` need purge permission. | omitted | `deny_purge: true` together with `allow_rollup_hdrs: true` is rejected ("roll-ups require the purge permission"). Cannot go from `true` to `false`. |
| `allow_direct` | `true` | All reads use direct get, served by any replica. | `true` | |
| `mirror_direct` | `true` when `mirror` is set | Lets the mirror serve direct gets addressed to the origin. | `true` only for mirrors | The server syncs it to the origin's `allow_direct`. |
| `duplicate_window` | 2 minutes (`120000000000`); if `0 < max_age < 2 min`, equal to `max_age` | The window may not exceed `max_age`. | computed as stated, always sent | `duplicate_window > max_age` is rejected; minimum 100 ms. If omitted the server applies the same default, **except on streams with a mirror or sources**. |
| `compression` | `"s2"` when compression is requested, otherwise `"none"` | | as stated | |
| `allow_msg_ttl` | `true` when limit markers are enabled | Needed for per-key TTL (section 3.9). | `true` iff a limit-marker TTL is given | Cannot be disabled once enabled ("message TTL status can not be disabled"). |
| `subject_delete_marker_ttl` | Limit-marker TTL in nanoseconds, at least 1 s; default absent | Makes the server leave a tombstone when a key ages out, so watchers see the removal. | user `LimitMarkerTTL` | Must be `>= 1s`. Forbidden on mirrors. Setting it makes the server force `allow_msg_ttl: true`, `allow_rollup_hdrs: true`, `deny_purge: false` (unless the request is pedantic). |
| `placement` | User | | user | |
| `republish` | User | Lets every write also be published on a core subject. | user | |
| `metadata` | User | | user | |
| `mirror`, `sources` | User, rewritten as in section 5 | | see section 5 | |

Two ADR-8 details are wrong or stale and must not be copied:

- The ADR-8 (and ADR-20) JSON examples spell the rollup key `rollup_hdrs`. The real key, in both
  the server and `nats.go`, is `allow_rollup_hdrs`.
- ADR-8 says `subject_delete_marker_ttl` "must be a duration longer than a second"; the server
  accepts exactly one second (`>= 1s`), as ADR-48 states.

Limit markers and per-message TTL need a server with JetStream API level 1 or higher (server
2.11+). ADR-8 and ADR-48 require the client to test this with `$JS.API.INFO` (the `api.level`
field of the account info response), not with the connected server's version, because the stream
may live on a different server than the one the client is connected to.

> **Client rule:** Build the stream configuration exactly as in the table. Always send
> `discard: "new"`, `allow_rollup_hdrs: true`, `deny_delete: true`, `allow_direct: true`,
> `max_msgs: -1`, `max_consumers: -1`, and never send `deny_purge: true`. Reject history above 64
> client-side. Use the JSON key `allow_rollup_hdrs`.

> **Client rule:** Compute `duplicate_window` yourself (2 minutes, or `max_age` when
> `0 < max_age < 2 min`) and send it. The server's default is identical for plain buckets, but the
> server does not apply it to mirrored or sourced streams, and an explicit value keeps the
> configuration identical to what other clients create.

> **Client rule:** When the user asks for a limit-marker TTL, first request `$JS.API.INFO` and
> require `api.level >= 1`; otherwise fail with a "limit marker TTLs not supported by server"
> error without creating anything. Validate that the TTL is at least one second. Never offer a way
> to turn `allow_msg_ttl` off again.

### 2.3 Creating, binding, updating, deleting, listing

**Create.** `nats.go` first calls `$JS.API.INFO` and fails if it errors (JetStream not enabled),
then sends `STREAM.CREATE`. Creating a stream with a configuration identical to the existing one
succeeds. A different configuration fails with `err_code` 10058 (`stream name already in use with
a different configuration`), which the client reports as "bucket exists".

`nats.go` has one compatibility path here: buckets created by old clients lack `discard: new` and
`allow_direct: true`. On 10058 it fetches the existing stream, overwrites only those two fields in
the fetched configuration, and if the result equals the desired configuration it issues a
`STREAM.UPDATE` instead of failing. A new client may implement this upgrade or simply report the
conflict.

```
C: PUB $JS.API.STREAM.CREATE.KV_CFG _INBOX.x.1 312␍␊
C: {"name":"KV_CFG","subjects":["$KV.CFG.>"],"retention":"limits","max_consumers":-1,"max_msgs":-1,"max_bytes":-1,"discard":"new","max_age":0,"max_msgs_per_subject":5,"max_msg_size":-1,"storage":"file","num_replicas":1,"duplicate_window":120000000000,"deny_delete":true,"allow_rollup_hdrs":true,"allow_direct":true}␍␊
S: MSG _INBOX.x.1 1 <n>␍␊
S: {"type":"io.nats.jetstream.api.v1.stream_create_response","config":{…},"created":"…","state":{…},"did_create":true}␍␊
```

**Bind** to an existing bucket: validate the name, request `$JS.API.STREAM.INFO.KV_<bucket>`.
`err_code` 10059 (`stream not found`) means "bucket not found". A stream whose
`max_msgs_per_subject` is less than 1 is not a valid bucket ("bad bucket"). Keep the returned
configuration: the handle needs `allow_direct` (section 3.3) and `mirror` (section 5.1).

**Update**: `$JS.API.STREAM.UPDATE.KV_<bucket>` with the full rebuilt configuration; 10059 means
"bucket not found". **Create-or-update** tries one and falls back to the other.

**Delete**: `$JS.API.STREAM.DELETE.KV_<bucket>`. Deleting a bucket is deleting its stream; 10059
means "bucket not found".

**List buckets**: page through `$JS.API.STREAM.NAMES` (names only) or `$JS.API.STREAM.LIST` (full
info, for statuses) with the subject filter `{"subject":"$KV.*.>"}`, keep the names that start
with `KV_`, and strip the prefix. Paging (`offset`, `total`, `limit`) is described in
[02-jetstream.md](02-jetstream.md).

```
C: PUB $JS.API.STREAM.NAMES _INBOX.x.17 21␍␊
C: {"subject":"$KV.*.>"}␍␊
S: MSG _INBOX.x.17 1 122␍␊
S: {"type":"io.nats.jetstream.api.v1.stream_names_response","total":2,"offset":0,"limit":1024,"streams":["KV_CFG","KV_JOBS"]}␍␊
```

> **Client rule:** A bucket handle is bound by a successful stream info lookup, never by assuming
> the stream exists. Map 10059 to "bucket not found" and 10058 to "bucket exists" on every
> management call.

### 2.4 Status

Bucket status is a view over stream info. ADR-8 requires at least these fields:

| Status field | Derived from |
|---|---|
| `bucket` | the bucket name |
| `values` | `state.messages` (this counts all stored revisions and tombstones, not distinct keys) |
| `history` | `config.max_msgs_per_subject` |
| `ttl` | `config.max_age` |
| `limit_marker_ttl` | `config.subject_delete_marker_ttl` |
| `is_compressed` | `config.compression != "none"` |
| `backing_store` | the constant string `JetStream` |
| `bytes` | `state.bytes` |
| `metadata` | `config.metadata` |

ADR-8 also names an `Info()` call that returns the bucket configuration. `nats.go` exposes the
same thing as `Status().Config()`, rebuilding the bucket options from the stream configuration,
and exposes the raw stream info too.

## 3. Key/Value: operations

### 3.1 Headers involved

All of these are defined in [02-jetstream.md](02-jetstream.md) except `KV-Operation`, which is the
only header that the KV convention itself introduces.

| Header | Values | Set by | Meaning for KV |
|---|---|---|---|
| `KV-Operation` | `DEL`, `PURGE` | client, on delete and purge | The message is a tombstone, not a value. Absent on puts. |
| `Nats-Rollup` | `sub` | client, on purge | Server removes all earlier messages on this subject after storing this one. |
| `Nats-Expected-Last-Subject-Sequence` | decimal sequence; `0` means "no message on this subject" | client, on create, update, and conditional delete or purge | Compare-and-set precondition. |
| `Nats-TTL` | Go duration string (`1h0m0s`), integer seconds (`3600`), or `never` | client, on create and purge only | Per-key TTL. |
| `Nats-Marker-Reason` | `MaxAge`, `Purge`, `Remove` | **server**, on subject delete markers | The message is a server-written tombstone. |

Every write is a JetStream publish: a core request to the key subject whose reply is a PubAck
(`{"stream":…,"seq":…}`) or an error envelope
(`{"error":{"code":…,"err_code":…,"description":…},…}`).
The PubAck `seq` is the new revision. As with any JetStream publish, a "no responders" status
means no stream captures the subject, and `nats.go` retries those according to its publish retry
options ([02-jetstream.md](02-jetstream.md)).

### 3.2 Put

Publish the value to the key subject with no headers. The revision is the PubAck `seq`.

```
C: PUB $KV.CFG.color _INBOX.x.2 4␍␊
C: blue␍␊
S: MSG _INBOX.x.2 1 27␍␊
S: {"stream":"KV_CFG","seq":1}␍␊
```

Put is unconditional: it overwrites whatever is there, including a tombstone. Put never carries a
TTL: "we cannot accept [a TTL] on the Put operation as that might have the effect of surfacing
history" (ADR-8, ADR-48). A put with a short TTL over a long-lived value would, on expiry, make the
older revision the latest one again.

> **Client rule:** `put(key, value)` publishes `value` to `$KV.<bucket>.<key>` with no headers
> and returns the PubAck `seq`. Do not accept a TTL option on put.

### 3.3 Get

Reads use **direct get** ([02-jetstream.md](02-jetstream.md)): a request answered by any replica or
mirror of the stream straight from its store, with the stored message as the reply payload and the
message's coordinates in headers. ADR-8 states that buckets do not support disabling direct get and
that the older "stream message get" API is legacy.

Because any replica can answer, **there is no read-after-write guarantee**: a get issued
immediately after a put may return the previous revision or "not found" (ADR-8).

**Latest revision of a key.** Request `$JS.API.DIRECT.GET.KV_<bucket>.$KV.<bucket>.<key>` with an
empty payload. The body must be empty: this endpoint rejects a body carrying `seq`, `last_by_subj`,
`next_by_subj`, `multi_last`, `batch`, `start_time`, `up_to_seq`, or `up_to_time` with status 408.

```
C: PUB $JS.API.DIRECT.GET.KV_CFG.$KV.CFG.color _INBOX.x.3 0␍␊
C: ␍␊
S: HMSG _INBOX.x.3 1 129 133␍␊
S: NATS/1.0␍␊
S: Nats-Stream: KV_CFG␍␊
S: Nats-Subject: $KV.CFG.color␍␊
S: Nats-Sequence: 1␍␊
S: Nats-Time-Stamp: 2026-11-22T10:15:30.123456789Z␍␊
S: ␍␊
S: blue␍␊
```

The response headers are, in this order, the stored message's own headers (if any), then
`Nats-Stream`, `Nats-Subject`, `Nats-Sequence`, and `Nats-Time-Stamp` (RFC 3339 with nanoseconds,
UTC). A client needs all four: `nats.go` treats a response missing any of them as an error.

A key that has never been written, or whose messages have all been removed, yields a status-only
reply with an empty payload:

```
C: PUB $JS.API.DIRECT.GET.KV_CFG.$KV.CFG.missing _INBOX.x.4 0␍␊
C: ␍␊
S: HMSG _INBOX.x.4 1 34 34␍␊
S: NATS/1.0 404 Message Not Found␍␊
S: ␍␊
S: ␍␊
```

Other statuses the direct get endpoint can return are `408 Bad Request`, `408 Malformed Request`,
`408 Empty Request`, and `412 Required Api Level`. For a non-404 status `nats.go` raises an error
built from the `Description` header or, failing that, the text "unable to get message".

A key whose latest message is a tombstone is returned like any other message, with the tombstone
header in front. The client must turn this into "key not found" (section 3.4). Here the key was
deleted at revision 5:

```
C: PUB $JS.API.DIRECT.GET.KV_CFG.$KV.CFG.color _INBOX.x.5 0␍␊
C: ␍␊
S: HMSG _INBOX.x.5 1 148 148␍␊
S: NATS/1.0␍␊
S: KV-Operation: DEL␍␊
S: Nats-Stream: KV_CFG␍␊
S: Nats-Subject: $KV.CFG.color␍␊
S: Nats-Sequence: 5␍␊
S: Nats-Time-Stamp: 2026-11-22T10:15:35.123456789Z␍␊
S: ␍␊
S: ␍␊
```

**A specific revision.** Request `$JS.API.DIRECT.GET.KV_<bucket>` (no subject suffix) with the
payload `{"seq":N}`. A sequence number addresses the whole stream, not one key, so the client must
compare the returned `Nats-Subject` with the key's subject and report "key not found" when they
differ.

```
C: PUB $JS.API.DIRECT.GET.KV_CFG _INBOX.x.6 9␍␊
C: {"seq":1}␍␊
S: HMSG _INBOX.x.6 1 129 133␍␊
S: NATS/1.0␍␊
S: Nats-Stream: KV_CFG␍␊
S: Nats-Subject: $KV.CFG.color␍␊
S: Nats-Sequence: 1␍␊
S: Nats-Time-Stamp: 2026-11-22T10:15:30.123456789Z␍␊
S: ␍␊
S: blue␍␊
```

**Legacy path.** When the bound stream's configuration has `allow_direct: false` (a bucket created
by a very old client), use `$JS.API.STREAM.MSG.GET.KV_<bucket>` with `{"last_by_subj":"<subject>"}`
or `{"seq":N}`. The reply is a JSON envelope in which `data` and `hdrs` (the raw header block) are
base64 (standard alphabet). "Not found" is `err_code` 10037.

```
C: PUB $JS.API.STREAM.MSG.GET.KV_CFG _INBOX.x.7 32␍␊
C: {"last_by_subj":"$KV.CFG.color"}␍␊
S: MSG _INBOX.x.7 1 163␍␊
S: {"type":"io.nats.jetstream.api.v1.stream_msg_get_response","message":{"subject":"$KV.CFG.color","seq":1,"data":"Ymx1ZQ==","time":"2026-11-22T10:15:30.123456789Z"}}␍␊
C: PUB $JS.API.STREAM.MSG.GET.KV_CFG _INBOX.x.8 34␍␊
C: {"last_by_subj":"$KV.CFG.missing"}␍␊
S: MSG _INBOX.x.8 1 130␍␊
S: {"type":"io.nats.jetstream.api.v1.stream_msg_get_response","error":{"code":404,"err_code":10037,"description":"no message found"}}␍␊
```

`nats.go` chooses between the two paths from the `allow_direct` field of the stream info cached at
bind time.

> **Client rule:** Use direct get when the bound stream has `allow_direct: true` (always, for
> buckets created by this client) and the message get API otherwise. Map status 404 and
> `err_code` 10037 to "key not found". For a get by revision, verify `Nats-Subject` equals
> `$KV.<bucket>.<key>` and otherwise report "key not found".

> **Client rule:** Document that reads are not read-after-write consistent. Where a caller needs
> the value it just wrote, use the revision returned by the write rather than a get.

### 3.4 Entries: how each field is derived

An entry is the client's view of one stored message.

| Entry field | Meaning (ADR-8) | From a get | From a watcher message |
|---|---|---|---|
| `bucket` | bucket the entry came from | the handle's bucket name | same |
| `key` | the key | the key that was requested | the message subject with the `$KV.<bucket>.` prefix removed |
| `value` | value bytes | the payload | the payload (empty with the meta-only option) |
| `revision` | unique sequence for this value | `Nats-Sequence` (or `message.seq`) | stream sequence token of the ack reply subject |
| `created` | time the server stored it | `Nats-Time-Stamp` (or `message.time`) | timestamp token of the ack reply subject (nanoseconds since the epoch) |
| `delta` | distance from the latest value; 0 for the latest | always 0 | pending token of the ack reply subject |
| `operation` | `PUT`, `DEL`, or `PURGE` | from headers, below | from headers, below |

The operation is decided by headers, in this order:

1. If `KV-Operation` is present: `DEL` → delete, `PURGE` → purge.
2. Otherwise, if `Nats-Marker-Reason` is present: `MaxAge` → purge, `Purge` → purge,
   `Remove` → delete.
3. Otherwise the entry is a put.

`KV-Operation` takes precedence over `Nats-Marker-Reason`. ADR-8 says that any `KV-Operation`
value "other than unset" marks the key as deleted; `nats.go` treats an unrecognized value as a put
(section 6.3).

A plain `get` hides tombstones: if the operation is not put, the result is "key not found".
Watchers and history surface tombstones as entries with the operation set. `nats.go` implements
this with an internal "key deleted" error that carries the entry (so `create` can read the
tombstone's revision) and that the public get converts to "key not found".

> **Client rule:** Derive the operation from `KV-Operation` first and `Nats-Marker-Reason` second,
> in gets and in watchers alike. Keep an internal get variant that returns the tombstone entry;
> `create` needs it.

### 3.5 Create

Create writes a key only if it does not exist. It is a publish with the precondition that the
subject has no message:

```
C: HPUB $KV.CFG.shape _INBOX.x.9 52 57␍␊
C: NATS/1.0␍␊
C: Nats-Expected-Last-Subject-Sequence: 0␍␊
C: ␍␊
C: round␍␊
S: MSG _INBOX.x.9 1 27␍␊
S: {"stream":"KV_CFG","seq":2}␍␊
```

The server accepts the publish if no message exists for the subject. This is purge-aware: after a
stream-level purge of the subject the check passes again. If a message exists, the server answers
with `err_code` 10071, `wrong last sequence: <seq>`, where `<seq>` is the subject's actual last
sequence (scenario 12.2).

That precondition is too strict for KV semantics: a deleted key still has a message on its
subject, namely the tombstone. ADR-8 therefore prescribes a retry: "on failure that indicates it's
not the first message we should attempt to load the current value and if that's a delete do an
update with `Nats-Expected-Last-Subject-Sequence` equalling to the value of the deleted message".

The algorithm:

1. Publish with `Nats-Expected-Last-Subject-Sequence: 0` (plus `Nats-TTL` if a TTL was given).
   On success return the revision.
2. On a wrong-last-sequence error (`err_code` 10071 or 10164), load the latest entry for the key
   with the internal get that returns tombstones.
3. If the latest entry is a tombstone (delete, purge, or a server marker), publish again with
   `Nats-Expected-Last-Subject-Sequence: <tombstone revision>` and the same TTL. Return that
   result; if it fails with a wrong-last-sequence error, someone else won the race and the result
   is "key exists".
4. If the latest entry is a live value, fail with "key exists".
5. If the get reports "not found" (the conflicting message vanished in between), fail with
   "key exists" as well; the caller can retry.

`nats.go` performs step 2 after *any* error from step 1, then reports "key exists" only if the
first error was 10071 or 10164, and otherwise returns the first error. The wire behavior for the
conflict case is the same.

> **Client rule:** Implement create with the tombstone retry above. Treat `err_code` 10071 and
> 10164 identically as "wrong last sequence". Never implement create as "get, then put": that is
> a race.

### 3.6 Update

Update is compare-and-set: a publish with `Nats-Expected-Last-Subject-Sequence: <revision>`.

```
C: HPUB $KV.CFG.color _INBOX.x.11 52 57␍␊
C: NATS/1.0␍␊
C: Nats-Expected-Last-Subject-Sequence: 1␍␊
C: ␍␊
C: green␍␊
S: MSG _INBOX.x.11 1 27␍␊
S: {"stream":"KV_CFG","seq":4}␍␊
```

A mismatch yields `err_code` 10071 (`wrong last sequence: <actual>`) or 10164 (`wrong last
sequence`, without the number), which the client reports as "key revision mismatch". 10164 is the
form a replicated stream uses when it rejects the publish before proposing it, because another
write to the same subject is already in flight (`nats.go`'s comment: "Replicated (R>1) streams
report CAS conflicts as 10164 instead of 10071"). A client cannot predict which of the two it
gets.

Update sends no `Nats-TTL`, so updating a key that was created with a TTL makes it permanent
(subject to the bucket's `max_age`).

`update(key, value, 0)` is the same wire message as a create without the tombstone retry.

> **Client rule:** `update(key, value, revision)` publishes with
> `Nats-Expected-Last-Subject-Sequence: <revision>` and maps 10071 and 10164 to a "revision
> mismatch" error distinct from "key exists". Do not retry automatically; the caller must re-read.

### 3.7 Delete

Delete publishes an empty message with `KV-Operation: DEL` to the key subject. Earlier revisions
stay in the stream (subject to history), so `history` still shows them, followed by the tombstone.

```
C: HPUB $KV.CFG.color _INBOX.x.12 31 31␍␊
C: NATS/1.0␍␊
C: KV-Operation: DEL␍␊
C: ␍␊
C: ␍␊
S: MSG _INBOX.x.12 1 27␍␊
S: {"stream":"KV_CFG","seq":5}␍␊
```

Optionally the delete is conditional: add `Nats-Expected-Last-Subject-Sequence: <revision>` (only
when the given revision is non-zero); a mismatch is "key revision mismatch". A TTL is not allowed
on a plain delete (`nats.go`: "TTL is not supported on delete"), because an expiring `DEL`
tombstone would resurface the revision before it.

### 3.8 Purge

Purge publishes an empty message with `KV-Operation: PURGE` **and** `Nats-Rollup: sub`. The server
stores the message and then removes every earlier message on that subject, leaving the purge
tombstone as the key's only revision.

```
C: HPUB $KV.CFG.color _INBOX.x.13 51 51␍␊
C: NATS/1.0␍␊
C: KV-Operation: PURGE␍␊
C: Nats-Rollup: sub␍␊
C: ␍␊
C: ␍␊
S: MSG _INBOX.x.13 1 27␍␊
S: {"stream":"KV_CFG","seq":6}␍␊
```

The rollup requires `allow_rollup_hdrs: true` and `deny_purge: false` on the stream; otherwise the
publish fails with `err_code` 10111. The server lower-cases the rollup value before comparing it.
Purge accepts the same optional `Nats-Expected-Last-Subject-Sequence` precondition as delete.

**Purge with TTL.** Because the rollup leaves nothing behind the tombstone, the tombstone itself
may safely expire. A purge may therefore carry `Nats-TTL`, which makes the server remove the
tombstone after that time, leaving the subject empty. The bucket must have `allow_msg_ttl`.

```
C: HPUB $KV.CFG.color _INBOX.x.14 67 67␍␊
C: NATS/1.0␍␊
C: KV-Operation: PURGE␍␊
C: Nats-Rollup: sub␍␊
C: Nats-TTL: 3600␍␊
C: ␍␊
C: ␍␊
S: MSG _INBOX.x.14 1 27␍␊
S: {"stream":"KV_CFG","seq":7}␍␊
```

When this tombstone expires, the server does not write a `Nats-Marker-Reason` marker in its place:
this is the one place where the server inspects `KV-Operation` (section 1.2).

> **Client rule:** Delete = empty payload, `KV-Operation: DEL`. Purge = empty payload,
> `KV-Operation: PURGE`, `Nats-Rollup: sub`. Both accept an optional expected revision. Only purge
> accepts a TTL; reject a TTL on delete client-side.

### 3.9 Per-key TTL and limit markers

Per-key TTL (ADR-48) is JetStream per-message TTL (ADR-43) exposed in exactly two KV operations:
create (section 3.5) and purge (section 3.8). The bucket must have been created with a limit-marker
TTL, which sets `allow_msg_ttl` and `subject_delete_marker_ttl`.

```
C: HPUB $KV.CFG.session _INBOX.x.10 68 73␍␊
C: NATS/1.0␍␊
C: Nats-Expected-Last-Subject-Sequence: 0␍␊
C: Nats-TTL: 3600␍␊
C: ␍␊
C: token␍␊
S: MSG _INBOX.x.10 1 27␍␊
S: {"stream":"KV_CFG","seq":3}␍␊
```

Facts a client must know:

- **Header value.** The server accepts a Go duration string (`1h0m0s`, which is what `nats.go`
  sends), a plain integer number of seconds, or `never`. A client without a Go-style duration
  formatter should send integer seconds. `nats.go` sends the header only when the TTL is greater
  than zero.
- **Errors.** A TTL below one second or an unparsable value fails with `err_code` 10165 (`invalid
  per-message TTL`). A TTL on a bucket without `allow_msg_ttl` fails with `err_code` 10166
  (`per-message TTL is disabled`). ADR-48 says these server errors are simply passed to the user.
- **TTL floor.** When the stream has `subject_delete_marker_ttl > 0` and
  `max_msgs_per_subject != 1`, the server raises a message TTL that is below the marker TTL up to
  the marker TTL and rewrites the stored `Nats-TTL` header as integer seconds. A reader may thus
  see a different `Nats-TTL` than the writer sent.
- **What expiry looks like.** When the last message of a subject is removed by `max_age` or by a
  per-message TTL, a stream with `subject_delete_marker_ttl` writes a marker message on that
  subject: empty payload, header `Nats-Marker-Reason: MaxAge`, itself expiring after the marker
  TTL. The reasons `Purge` and `Remove` are defined by the server and by ADR-48 and must be mapped
  by clients, but the server source read for this chapter only ever writes `MaxAge` markers.
  Readers and watchers map all three to operations as in section 3.4. Without limit markers, an
  expired key just disappears and a running watcher is never told.

> **Client rule:** Offer a TTL option on create and purge only. Send `Nats-TTL` as integer seconds
> (or a Go duration string) and only when positive. Do not pre-check `allow_msg_ttl`; surface
> 10165 and 10166 from the server.

> **Client rule:** Treat a message carrying `Nats-Marker-Reason` as a tombstone everywhere a
> `KV-Operation` tombstone is handled: `MaxAge` and `Purge` are purges, `Remove` is a delete.

## 4. Key/Value: watchers and everything built on them

A watcher is a stream of entries for one or more key filters: first the current state ("initial
data"), then live updates. It is the only KV read primitive besides get; `keys`, `history`, and
`purge_deletes` are all thin layers over it. It is also where client implementations most often go
wrong, so this section is exact about ordering.

### 4.1 The consumer behind a watcher

ADR-8 specifies watchers as "ephemeral consumers reading values using Ordered Consumers" starting
at `last_per_subject`. An ordered consumer ([03-consumers.md](03-consumers.md)) is an ephemeral,
unacknowledged, single-replica, memory-backed consumer that the client recreates transparently
whenever it detects a gap, so that the application sees a gapless, in-order stream.

`nats.go` creates it through its push-consumer API. The consumer configuration is the ordered
consumer baseline plus the watcher options:

| Config key | Value | Source |
|---|---|---|
| `deliver_subject` | a fresh inbox the client has already subscribed to (no queue group) | ordered consumer |
| `ack_policy` | `"none"` | ordered consumer |
| `max_deliver` | `1` | ordered consumer |
| `ack_wait` | 22 hours (`79200000000000`), unused | ordered consumer |
| `flow_control` | `true` | ordered consumer |
| `idle_heartbeat` | 5 seconds (`5000000000`) | ordered consumer |
| `num_replicas` | `1` | ordered consumer |
| `mem_storage` | `true` | ordered consumer |
| `replay_policy` | `"instant"` | ordered consumer |
| `name` | a generated name; no `durable_name` | ordered consumer |
| `filter_subject` | `$KV.<bucket>.<key>` when there is exactly one filter | watcher |
| `filter_subjects` | `["$KV.<bucket>.<k1>", …]` when there are several | watcher |
| `deliver_policy` | see the next table | watcher |
| `opt_start_seq` | only with resume-from-revision | watcher |
| `headers_only` | `true` with the meta-only option | watcher |

The stream is `KV_<bucket>`. Filters are **unprefixed** `$KV.<bucket>.…` subjects even when the
client uses a JetStream domain or API prefix, because a consumer filter is matched against stored
subjects inside the stream (section 5.3).

Watcher options and their effect, applied in this order (a later row overrides an earlier one):

| Option | `deliver_policy` | Other effect |
|---|---|---|
| default | `"last_per_subject"` | The initial data is the latest message of every matching key, tombstones included. |
| include history | `"all"` | The initial data is every stored revision of every matching key. |
| updates only | `"new"` | No initial data. |
| meta only | unchanged | `headers_only: true`: the server strips the payload and adds a `Nats-Msg-Size` header; entries have an empty value. |
| ignore deletes | unchanged | Client-side: tombstone entries are not surfaced. |
| resume from revision N | `"by_start_sequence"`, `opt_start_seq: N` | Starts at stream sequence N. |

"Include history" and "updates only" contradict each other; `nats.go` rejects the combination in
either order with an invalid-option error.

Which keys are watched:

- `watch(key)` takes one search key (section 2.1): an exact key, or a pattern with `*` and `>`.
- `watch_all` is `watch(">")`, giving the filter `$KV.<bucket>.>`.
- `watch_filtered(keys)` takes several search keys and uses `filter_subjects`. An empty list
  watches `>`.
- ADR-8 also lets an empty key mean "the whole bucket". `nats.go` rejects `Watch("")` with "key
  cannot be empty and must be a valid NATS subject".

> **Client rule:** Create watchers as ordered consumers on `KV_<bucket>` with the configuration
> above. Use `filter_subject` for one filter and `filter_subjects` for several. Validate each
> filter with the search-key rule; treat an empty key or an empty list as `>`. Reject "include
> history" together with "updates only".

### 4.2 Processing one delivered message

Each delivered message carries its JetStream metadata in the reply subject
([03-consumers.md](03-consumers.md)):

```
$JS.ACK.<stream>.<consumer>.<delivered>.<stream seq>.<consumer seq>.<timestamp ns>.<pending>
$JS.ACK.<domain>.<account hash>.<stream>.<consumer>.<delivered>.<stream seq>.<consumer seq>.<timestamp ns>.<pending>
```

The first form (9 tokens) is what servers up to 2.15 emit. The second (11 tokens; a domain of `_`
means none) is governed by the server feature flag `js_ack_fc_v2`, which is off in 2.14 and 2.15
and on by default from 2.16, so current servers emit it. The server reserves the right to append
further tokens to the second form. A parser must accept exactly 9 tokens, or 11 and more; `nats.go`
rejects anything else as "invalid format of ACK subject".

For each message on the delivery subject that is not a status message:

1. Parse the reply subject. If it does not parse, drop the message.
2. If the subject is not longer than the prefix `$KV.<bucket>.`, drop the message. Otherwise the
   key is the remainder after the prefix.
3. Determine the operation from headers (section 3.4).
4. Build the entry: `revision` = stream sequence token, `created` = timestamp token (nanoseconds
   since the Unix epoch), `delta` = pending token, `value` = payload.
5. Unless "ignore deletes" is set and the operation is delete or purge, hand the entry to the
   application.
6. Run the end-of-initial-data check (section 4.3). This step runs even when step 5 suppressed the
   entry.

Status messages (`100 Idle Heartbeat`, `100 FlowControl Request`) arrive on the same subject. They
have no `$JS.ACK` reply subject and are handled by the ordered consumer machinery, not by the
watcher (section 4.5).

ADR-8 warns that "the same Entry might be delivered more than once". Consumers of a watcher must
be idempotent with respect to `(key, revision)`.

### 4.3 The end-of-initial-data marker

A watcher must tell the application when it has delivered everything that existed when the watch
started, so that the application can switch from "loading" to "live". ADR-8 calls this the End Of
Initial Data signal; `nats.go` delivers it as a `nil` entry on the updates channel. ADR-8:
"you reach this state the first time any message has a `Pending==0`. This signal must also be sent
if no data is present […] The signal must always be sent."

There are two ways for the initial data to end, and a correct client needs both:

- The last initial message arrives with a pending count of 0.
- There was nothing to deliver, so **no message will ever arrive**. This must be detected from the
  consumer-create response.

The algorithm, as implemented by `nats.go`:

State: `init_done = false`, `init_pending = 0`, `received = 0`, all guarded by one lock that is
also held while delivering an entry.

1. Subscribe to the delivery inbox.
2. Take the lock. Send the consumer-create request and wait for the response **while holding the
   lock**, so that messages already arriving on the inbox wait in step 5.
3. From the response compute `initial = num_pending + delivered.consumer_seq`.
4. If "updates only" is set: `init_done = true`; do not emit a marker here (see below).
   Otherwise, if `initial == 0`: `init_done = true`, emit the marker now. Otherwise
   `init_pending = initial`. Release the lock.
5. For every data message (after steps 1 to 5 of section 4.2, under the lock): if `init_done` is
   false, increment `received`; then if `received >= init_pending` **or** the message's pending
   token is `0`, set `init_done = true` and emit the marker, after the entry.

Why each part exists:

- `num_pending` in the create response is the number of messages the consumer will deliver given
  its start policy and filters. For `last_per_subject` it is the number of matching keys. Adding
  `delivered.consumer_seq` makes the count correct even if the server had already started
  delivering when it built the response.
- `received >= init_pending` ends the initial phase at the right message even when new writes
  arrive during the replay and keep the pending token above zero.
- `pending == 0` ends it even if fewer messages arrive than announced, for example because keys
  were removed between consumer creation and delivery.
- `received` counts messages suppressed by "ignore deletes"; otherwise a bucket whose matching
  keys are all tombstones would never produce the marker.
- The marker is emitted exactly once. After `init_done` the counters are ignored, including after
  an ordered-consumer recreation.

For "updates only" ADR-8 says "The End Of Initial Data marker is sent as soon as the watch
starts", while `nats.go` sends no marker at all in that mode. Section 6.3 gives the recommendation.

> **Client rule:** Emit the end-of-initial-data marker exactly once per watcher, always. Decide it
> from the consumer-create response (`num_pending + delivered.consumer_seq == 0` → immediately)
> and otherwise from the message path (`received >= initial` or pending token `0`), counting
> messages hidden by "ignore deletes".

> **Client rule:** Subscribe to the delivery subject before sending the consumer-create request,
> and do not process delivered messages until the create response has been evaluated. Both must
> happen under one mutual exclusion with the marker state.

### 4.4 Keys, history, purge-deletes

**Keys.** ADR-8: "done using a headers-only Consumer set to deliver last per subject. Any received
message that isn't a Delete/Purge operation gets added to the list based on parsing the subject."
That is a watcher over `>` with "ignore deletes" and "meta only", read until the marker.

- `keys` collects into a list; `nats.go` sorts and de-duplicates it, and reports "no keys found"
  when it is empty.
- `list_keys` is the streaming form: it yields keys as they arrive until the marker. Duplicates
  are possible.
- `list_keys_filtered(filters…)` is the same over `watch_filtered`.

**History.** A watcher on the single key with "include history" (`deliver_policy: "all"`,
`filter_subject` = the key subject), read until the marker. The newest entry has `delta` 0.
Tombstones are included as entries. No entries means "key not found".

**Purge deletes.** This is a `nats.go` extension, not part of ADR-8's interface, that reclaims the
space held by tombstones:

1. Run a default `watch_all` and collect every entry whose operation is delete or purge, until the
   marker. Stop the watcher.
2. Choose the threshold `older_than`: the option value; if unset or zero, 30 minutes; if negative,
   "remove every marker regardless of age".
3. For each collected tombstone, request `$JS.API.STREAM.PURGE.KV_<bucket>` with
   `{"filter":"$KV.<bucket>.<key>"}`. If `older_than > 0` and the tombstone is younger than
   `now - older_than`, add `"keep":1`, which removes the key's older revisions but keeps the
   tombstone itself.

```
C: PUB $JS.API.STREAM.PURGE.KV_CFG _INBOX.x.15 35␍␊
C: {"filter":"$KV.CFG.color","keep":1}␍␊
S: MSG _INBOX.x.15 1 83␍␊
S: {"type":"io.nats.jetstream.api.v1.stream_purge_response","success":true,"purged":0}␍␊
C: PUB $JS.API.STREAM.PURGE.KV_CFG _INBOX.x.16 26␍␊
C: {"filter":"$KV.CFG.shape"}␍␊
S: MSG _INBOX.x.16 1 83␍␊
S: {"type":"io.nats.jetstream.api.v1.stream_purge_response","success":true,"purged":1}␍␊
```

Recent tombstones are kept on purpose: a watcher or mirror that is slightly behind still needs to
observe the delete. The purge request keys are `filter`, `keep`, and `seq`
([02-jetstream.md](02-jetstream.md)); the stream needs `deny_purge: false`.

> **Client rule:** Implement `keys` and `history` on top of the watcher, terminating on the
> end-of-initial-data marker, never on a timeout. Stop (unsubscribe) the watcher when done.

### 4.5 Flow control, heartbeats, recreation, shutdown

Watcher consumers have `flow_control: true` and a 5-second `idle_heartbeat`. The full protocol is
in [03-consumers.md](03-consumers.md); the parts a watcher depends on are:

- **Flow control.** The server periodically sends `NATS/1.0 100 FlowControl Request` with a reply
  subject on the delivery subject and then stops sending data once its window is used up. The
  client must publish an empty message to that reply subject once it has processed everything
  delivered before the request. `nats.go` records the reply subject together with the count of
  messages delivered so far and answers when the application has consumed that many.
- **Stalled heartbeats.** An idle heartbeat is `NATS/1.0 100 Idle Heartbeat` with
  `Nats-Last-Consumer` and `Nats-Last-Stream` headers. When the server is waiting for a flow
  control answer it adds `Nats-Consumer-Stalled: <subject>`; the client must publish an empty
  message to that subject.
- **Gaps and silence.** If a message's consumer sequence is not the expected one, if a
  heartbeat's `Nats-Last-Consumer` disagrees with what the client has seen, or if heartbeats stop
  arriving, the client deletes the consumer and creates a new one on a new inbox with
  `deliver_policy: "by_start_sequence"` and `opt_start_seq` = last received stream sequence + 1.
  The watcher's `init_done`, `received`, and `init_pending` survive the recreation.
- **Stop.** Stopping a watcher unsubscribes the delivery subscription; `nats.go` then closes the
  updates channel from the subscription's closed handler. Removal of the ephemeral consumer
  itself is covered in [03-consumers.md](03-consumers.md).

### 4.6 Pitfalls

These failure modes follow directly from the algorithms above. They are the usual ways a watcher
implementation is wrong while still passing simple tests.

1. **Watcher on an empty bucket hangs.** If the marker is produced only when a message arrives
   with pending 0, then `keys`, `history`, and "wait for initial data" block forever (or until an
   arbitrary timeout) on an empty bucket or an unmatched filter, because the server sends nothing.
   The marker must come from the create response in that case (section 4.3, step 4).
2. **Create-response race.** The server may deliver the first messages before the client has
   processed the consumer-create response. If the message path runs with `init_pending` still 0,
   the first message satisfies `received >= init_pending` and the marker is emitted after one
   entry; if the create-response path then also emits it, the application sees two markers. Hold
   the lock across creation, or queue messages until the response is processed.
3. **Dropped flow control.** A dispatcher that discards every message without a `$JS.ACK` reply,
   or that treats status 100 as noise, never answers flow control. Small buckets work; a bucket
   larger than the server's flow-control window stalls partway through the initial data. The same
   happens if the answer is sent from the application thread and the application is blocked.
   Answer flow control from the connection's reader side, sequenced after the preceding data.
4. **Dropped entries on timeout or overflow.** A watcher hands entries to the application through
   a bounded queue. If the producer side drops an entry when the queue is full, or the consumer
   side discards an entry it had already dequeued when a "next with timeout" call expires, the
   application silently loses a revision, and for `keys` a key. Nothing on the wire reports this;
   the ordered consumer saw no gap. `nats.go` uses a 256-entry channel and **blocks** the delivery
   callback when it is full, "by design": flow control then pushes back on the server. Never drop;
   block, and make timeouts leave the queue intact.
5. **Marker lost to "ignore deletes".** Not counting suppressed tombstones toward `received`, or
   returning early before the marker check, hangs `keys` on a bucket whose last initial message is
   a tombstone.
6. **Restarting the initial phase after recreation.** Resetting `init_done` when the ordered
   consumer is recreated produces a second marker and, with `last_per_subject`, replays state.
   Recreate with `by_start_sequence` and keep the marker state.
7. **Prefix confusion on mirrors and domains.** The key is derived by stripping the read prefix
   from the stored subject. On a handle whose read prefix differs from the stored subjects
   (section 5.1) every message is dropped by step 2 of section 4.2 or yields mangled keys.

> **Client rule:** Deliver watcher entries with back-pressure and no loss: a full application
> queue blocks delivery, and a timed-out read never consumes an entry. Answer flow control and
> stalled heartbeats independently of the application's consumption thread.

## 5. Key/Value: mirrors, sources, domains, and API prefixes

### 5.1 Mirrors

A mirror bucket is a read-only copy of another bucket, usually in another cluster or JetStream
domain, kept up to date by the server ([02-jetstream.md](02-jetstream.md)). ADR-57 requires:

- If the mirror's stream name lacks the `KV_` prefix, prepend it. (Users give bucket names.)
- Always set `mirror_direct: true`.

`nats.go` additionally omits `subjects` for a mirror bucket, as a mirror receives its messages from
the origin. A mirror stores messages under the **origin's** subjects, `$KV.<origin>.<key>`.
`subject_delete_marker_ttl` is forbidden on mirrors.

With `mirror_direct`, the server makes the mirror subscribe (in a queue group) to the origin's
direct get subjects, `$JS.API.DIRECT.GET.KV_<origin>` and `$JS.API.DIRECT.GET.KV_<origin>.>`. A
client that reads from the *origin* bucket may therefore be answered by a mirror; the
`Nats-Stream` response header names the stream that actually answered. This is how KV read
replicas work, and it is another reason reads are not read-after-write consistent.

A handle bound to a mirror bucket must route writes to the origin, since a mirror does not accept
publishes. `nats.go` derives two prefixes when binding (`mapStreamToKVS`), where `<origin>` is the
mirror's stream name with `KV_` removed:

| Bound stream | Read prefix (get, watch, key extraction) | Write prefix | API prefix prepended to writes |
|---|---|---|---|
| Normal bucket | `$KV.<bucket>.` | same as read | only if the context uses a non-default API prefix (section 5.3) |
| Mirror, origin in the same domain (`external` unset) | `$KV.<bucket>.` (the mirror's own name) | `$KV.<origin>.` | as above |
| Mirror with `external.api` set | `$KV.<origin>.` | `<external.api>.$KV.<origin>.` | never |

In the second row the read prefix names the mirror bucket although the stored subjects name the
origin. Whether gets and watches work on such a handle in `nats.go` was not verified (section 14).
The subjects stored in a mirror are the origin's, so a new client should read a mirror with the
origin's prefix in both mirror cases.

> **Client rule:** For a mirror bucket: prefix the mirror source name with `KV_` if missing, set
> `mirror_direct: true`, send no `subjects`, and send no `subject_delete_marker_ttl`. On a handle
> bound to a mirror, publish writes to `$KV.<origin>.<key>` (prefixed with the mirror's
> `external.api` when present) and build read subjects, watch filters, and key extraction from
> `$KV.<origin>.`.

### 5.2 Sources

A sourced bucket aggregates other buckets into one. Unlike a mirror it is writable and keeps its
own `subjects`. ADR-57 requires, per source:

- **Without custom subject transforms:** prefix the source stream name with `KV_` if missing, and
  generate the transform `{"src":"$KV.<source>.>","dest":"$KV.<bucket>.>"}` so that sourced keys
  appear under this bucket's subject space.
- **With custom subject transforms:** preserve `subject_transforms` exactly as given, generate
  nothing, and do not touch the source stream name.

`nats.go` follows this with one exception: it skips the generated transform when the source is
external (`external` set) **and** has the same bucket name as the bucket being created, because
then the subjects already match. Sources are only evaluated when no mirror is configured.

```json
{
  "name": "KV_AGG",
  "subjects": ["$KV.AGG.>"],
  "sources": [
    {"name": "KV_EU", "subject_transforms": [{"src": "$KV.EU.>", "dest": "$KV.AGG.>"}]},
    {"name": "KV_US", "subject_transforms": [{"src": "$KV.US.>", "dest": "$KV.AGG.>"}]}
  ]
}
```

A source or mirror in another JetStream domain is expressed as
`"external": {"api": "$JS.<domain>.API"}`. `nats.go` accepts a `Domain` shortcut on the source and
converts it to that form when it creates or updates the stream; the shortcut itself is never sent.

> **Client rule:** For each source without `subject_transforms`, add the `KV_` prefix if missing
> and generate `$KV.<source>.>` → `$KV.<bucket>.>`, except for an external source with the same
> bucket name. Pass sources that carry their own transforms through untouched.

### 5.3 JetStream domains and API prefixes

A client may be configured with a JetStream domain (API prefix `$JS.<domain>.API`) or an arbitrary
API prefix for cross-account access ([02-jetstream.md](02-jetstream.md)). Every JetStream API
request (stream info, direct get, message get, purge, consumer create) already goes to a prefixed
subject. KV *writes*, however, are publishes to `$KV.…`, which is not an API subject. ADR-19
extends the prefix to them: "we prefix `$KV` with `JS.from-acc1`, resulting in
`JS.from-acc1.$KV`".

| Context | Write subject | Read (direct get) subject | Watch filter |
|---|---|---|---|
| default (`$JS.API`) | `$KV.<bucket>.<key>` | `$JS.API.DIRECT.GET.KV_<bucket>.$KV.<bucket>.<key>` | `$KV.<bucket>.<key>` |
| domain `hub` | `$JS.hub.API.$KV.<bucket>.<key>` | `$JS.hub.API.DIRECT.GET.KV_<bucket>.$KV.<bucket>.<key>` | `$KV.<bucket>.<key>` |
| custom prefix `JS.from-acc1` | `JS.from-acc1.$KV.<bucket>.<key>` | `JS.from-acc1.DIRECT.GET.KV_<bucket>.$KV.<bucket>.<key>` | `$KV.<bucket>.<key>` |

The stream itself always listens on plain `$KV.<bucket>.>`. For a domain the server's built-in
mapping rewrites `$JS.<domain>.API.$KV.>` to `$KV.>`; for a custom prefix the account
export/import configuration must provide the mapping (ADR-19 contains a test configuration). The
subject inside a direct get request and a consumer filter are never prefixed: they are matched
against stored subjects.

```
C: PUB $JS.hub.API.$KV.CFG.color _INBOX.x.18 4␍␊
C: blue␍␊
S: MSG _INBOX.x.18 1 42␍␊
S: {"stream":"KV_CFG","domain":"hub","seq":1}␍␊
```

> **Client rule:** When the JetStream context has a non-default API prefix, publish KV writes to
> `<api prefix>.$KV.<bucket>.<key>`. Do not prefix watch filters or the subject embedded in a
> direct get. On a mirror handle with `external.api`, use that prefix instead (section 5.1).

## 6. Key/Value: errors, required behaviors, discrepancies

### 6.1 Error table

"Client" in the cause column means the condition is detected without a server round trip.

| Client error | Cause on the wire | When |
|---|---|---|
| key exists | PubAck error `err_code` 10071 (`wrong last sequence: <seq>`, code 400) or 10164 (`wrong last sequence`, code 400) | Create, and the latest entry is a live value. |
| key revision mismatch | 10071 or 10164 | Update, or delete/purge with an expected revision. |
| key not found | direct get status `404 Message Not Found`; message get `err_code` 10037 (`no message found`, code 404) | Get of an absent key; get of a key whose latest entry is a tombstone; get by revision whose message belongs to another key; history with no entries. |
| key deleted (internal only) | latest message has `KV-Operation` or `Nats-Marker-Reason` | Used inside create; surfaced to users as key not found. |
| bucket not found | `err_code` 10059 (`stream not found`, code 404) | Bind, update, delete bucket. |
| bucket exists | `err_code` 10058 (`stream name already in use with a different configuration`, code 400) | Create bucket with a conflicting configuration. |
| bad bucket | bound stream has `max_msgs_per_subject < 1` | Bind. |
| invalid bucket name | client | Any bucket call. |
| invalid key | client | Any key operation or watch filter. |
| history too large | client (`history > 64`) | Create or update bucket. |
| no keys found | client (empty result) | `keys`. |
| TTL not supported on delete | client | Delete with a TTL. |
| limit marker TTLs not supported | `$JS.API.INFO` reports `api.level < 1` | Create bucket with a limit-marker TTL. |
| invalid option | client | Watch with "include history" and "updates only". |
| (server error passed through) | `err_code` 10166 (`per-message TTL is disabled`, code 400) | `Nats-TTL` on a bucket without `allow_msg_ttl`. |
| (server error passed through) | `err_code` 10165 (`invalid per-message TTL`, code 400) | TTL unparsable or below 1 s. |
| (server error passed through) | `err_code` 10054 (`message size exceeds maximum allowed`, code 400) | Value plus headers larger than `max_msg_size`. |
| (server error passed through) | `err_code` 10111 (code 500, description is the underlying error such as "rollup not permitted") | Purge on a stream with `allow_rollup_hdrs: false` or `deny_purge: true`. |
| (server error passed through) | store errors `maximum bytes exceeded`, `maximum messages exceeded` | Write to a full bucket (`discard: new`). |

10071 and 10164 mean the same thing; which KV error they become depends only on the operation that
received them. `nats.go` matches API errors by `err_code`, with the side effect that its "key
exists" sentinel (code 10071) also matches a plain 10071 from an update; its documentation warns
"Do not use ErrKeyExists to detect revision conflicts". A new client should use two distinct error
classes selected by operation.

### 6.2 Required client behaviors (ADR MUSTs)

1. The stream is named `KV_<bucket>` and ingests `$KV.<bucket>.>` (ADR-8).
2. History is `max_msgs_per_subject`: minimum 1, maximum 64, default 1 (ADR-8).
3. `duplicate_window` is 2 minutes, or `max_age` when the TTL is 2 minutes or less (ADR-8).
4. Always `discard: new`, rollup headers allowed, `deny_delete: true`, `allow_direct: true`;
   compression means `s2` (ADR-8).
5. Limit markers need `allow_msg_ttl: true` and `subject_delete_marker_ttl >= 1s`; check API level
   1 or higher through `$JS.API.INFO`, not the server version; never support disabling (ADR-8,
   ADR-48).
6. Writes are JetStream requests to `$KV.<bucket>.<key>` (ADR-8).
7. Create uses `Nats-Expected-Last-Subject-Sequence: 0` and retries against a tombstone's revision
   (ADR-8). Create may take a TTL when the bucket allows it (ADR-48).
8. No TTL on put or any other operation except purge, which should accept one (ADR-8, ADR-48).
   Server TTL errors are passed to the user (ADR-48).
9. Gets use direct get; a tombstone is "key not found" in gets and an entry with its operation in
   watchers and history (ADR-8).
10. `Nats-Marker-Reason` `MaxAge` and `Purge` mean purge and `Remove` means delete, in gets and
    watchers (ADR-8, ADR-48).
11. Delete is an empty body with `KV-Operation: DEL`. Purge is `KV-Operation: PURGE` plus
    `Nats-Rollup: sub`, plus `Nats-TTL` when a TTL is given (ADR-8).
12. Keys use a headers-only, last-per-subject consumer and skip tombstones; watch and keys have
    multi-filter variants (ADR-8).
13. Watch uses an ordered consumer starting at `last_per_subject`; supports a key, a wildcard
    range, or the whole bucket; supports multiple filter subjects; supports include-history,
    ignore-deletes, meta-only, and updates-only; and **always** sends the end-of-initial-data
    signal (ADR-8).
14. History is an ordered consumer with deliver-all, filtered on the key subject (ADR-8).
15. Deleting a bucket removes the stream (ADR-8).
16. Mirror and source name prefixing, `mirror_direct`, and transform generation as in section 5
    (ADR-57).
17. With a non-default API prefix, KV write subjects are `<prefix>.$KV.<bucket>.<key>` (ADR-19).
18. An entry exposes bucket, key, value, created, revision, delta, and operation; status exposes
    the fields of section 2.4 with `backing_store` = `JetStream` (ADR-8).
19. Bucket names are `restricted-term` of at most 252 characters; keys are
    `limited-term (dot limited-term)*`; `_kv` keys are reserved (ADR-6).
20. No read-after-write consistency is promised (ADR-8).

### 6.3 Discrepancies and which side to follow

| # | Topic | ADR | Server | `nats.go` | A new client should |
|---|---|---|---|---|---|
| 1 | Rollup config key | examples say `rollup_hdrs` | `allow_rollup_hdrs` | `allow_rollup_hdrs` | Use `allow_rollup_hdrs`. The ADR example is a typo; the other key is ignored by the server. |
| 2 | `_kv` key prefix | reserved (ADR-6) | not checked | not checked | Refuse to write such keys, tolerate them on read. Costs nothing and honors the reservation. |
| 3 | Unknown `KV-Operation` value | "any value other than unset" means deleted | n/a | treated as a put | Follow the ADR: a non-empty unknown value is not a value. Report "key not found" in gets and surface it as a delete in watchers. No current writer emits other values, so this cannot break interoperability, and it fails safe if one is added. |
| 4 | Marker with "updates only" | sent as soon as the watch starts | n/a | never sent | Follow the ADR ("the signal must always be sent"). Code that waits for the marker then works in every mode. |
| 5 | `watch("")` | empty key watches the whole bucket | n/a | rejected as invalid key | Accept an empty key as `>`. It is a pure client-side convenience. |
| 6 | Bucket name length | max 252 | stream name limit applies | no length check | Enforce 252 client-side for a clear error. |
| 7 | Limit-marker TTL minimum | ADR-8: "longer than a second"; ADR-48: `>= 1s` | `>= 1s` | not checked | Validate `>= 1s` client-side. |
| 8 | `duplicate_window` | ADR-8 gives the rule, then notes "this behavior requires no code on the client" | defaults to the same rule, but not for mirrored or sourced streams | always computes and sends it | Compute and send it, as `nats.go` does (section 2.2). |
| 9 | Direct vs. legacy get | direct only; legacy removed in revision 7 | both exist | chosen from the cached stream config's `allow_direct` (an internal `useDirect` flag is set but never read) | Choose from the bound stream's `allow_direct`, to keep working against legacy buckets. |
| 10 | When create looks up the current value | "on failure that indicates it's not the first message" | n/a | after any error from the first publish | Follow the ADR: only after 10071 or 10164. A timeout is not evidence that the key exists. |
| 11 | CAS error code | not specified | 10071 normally; 10164 from clustered pre-proposal checks | handles both | Handle both identically. |
| 12 | `Info()` | returns the bucket configuration | n/a | `Status().Config()` | Either shape; it is local API. |
| 13 | Purge-deletes | not in the interface | n/a | implemented | Implement as in section 4.4; it uses only standard stream purge. |

## 7. Object Store: buckets, subjects, and metadata

### 7.1 Model

An object store bucket holds named binary objects of arbitrary size. Since a NATS message is
limited in size, an object is split into **chunks**, each stored as one message, and described by
one **metadata** message (`ObjectInfo`, JSON) that names the chunk subject, the size, the chunk
count, and a digest.

Both kinds of message live in the same stream on disjoint subject spaces:

- Chunks of one upload share the subject `$O.<bucket>.C.<nuid>`, where the NUID is generated per
  upload. Chunk order is stream order. Re-uploading a name uses a new NUID, so the old content
  stays readable until the new metadata replaces the old.
- Metadata for a name lives on `$O.<bucket>.M.<encoded name>` and is published with
  `Nats-Rollup: sub`, so at most one metadata message per name exists. That message is the
  directory entry.

Reading the directory is "last message per subject on `$O.<bucket>.M.>`"; reading an object is
"all messages on its chunk subject, in order". Deleting an object replaces its metadata with a
copy marked `deleted` and purges its chunk subject.

### 7.2 Naming and encoding

| Component | Template | Notes |
|---|---|---|
| Stream name | `OBJ_<bucket>` | |
| Stream subjects | `$O.<bucket>.C.>` and `$O.<bucket>.M.>` | |
| Chunk subject | `$O.<bucket>.C.<nuid>` | one per upload |
| Metadata subject | `$O.<bucket>.M.<name-encoded>` | one per object name |
| Bucket name | `\A[a-zA-Z0-9_-]+\z` | max 251 characters (ADR-6); `nats.go` has no length check |
| Object NUID | a NUID, 22 characters ([01-core.md](01-core.md)) | generated per put and per link |
| Object name | any non-empty string | never appears on the wire except encoded |
| `name-encoded` | base64 of the UTF-8 name, **URL-safe alphabet (`-` and `_`), with `=` padding** | |
| Digest | `SHA-256=` + base64 of the 32-byte SHA-256 of the content, URL-safe alphabet, with padding | algorithm name upper-case |
| Default chunk size | 131072 bytes (128 KiB) | per-object override |

The name encoding is a lookup key, so it must be byte-identical across clients. ADR-20 only says
"base64 encoded"; the alphabet and the padding are fixed by `nats.go`
(`base64.URLEncoding.EncodeToString`). Examples: `a.txt` → `YS50eHQ=`; the four bytes `x??>`
→ `eD8_Pg==` (with the standard alphabet this would contain `/` and `+`). The `=` padding
characters are legal in a subject token.

The digest is parsed by splitting once at the first `=`: the left part is the algorithm name, the
right part (which itself may end in `=` padding) is the base64 value. A digest that does not split
into two parts or does not decode is an "invalid digest format" error. ADR-20's example:
`SHA-256=IdgP4UYMGt47rgecOqFoLrd24AXukHf5-SVzqQ5Psg8=`.

> **Client rule:** Encode object names for subjects with URL-safe base64 **with** padding
> (Ruby: `Base64.urlsafe_encode64(name)`), and digests as `"SHA-256=" +
> Base64.urlsafe_encode64(sha256_bytes)`. Reject empty object names. Validate bucket names against
> `\A[a-zA-Z0-9_-]+\z`, max 251 characters.

### 7.3 Stream configuration

| JSON key | Required value | Why | What `nats.go` sends |
|---|---|---|---|
| `name` | `OBJ_<bucket>` | | same |
| `subjects` | `["$O.<bucket>.C.>", "$O.<bucket>.M.>"]` | chunk and metadata spaces | same |
| `description` | user | | user |
| `max_age` | bucket TTL in ns, default `0` | objects (metadata and chunks) expire | user `TTL` |
| `max_bytes` | user, default `-1` | | user, `0` becomes `-1` |
| `storage` | `"file"` (default) or `"memory"` | | user |
| `num_replicas` | user, default `1` | | user, `0` becomes `1` |
| `placement` | user | | user |
| `discard` | `"new"` | a full bucket must reject an upload, not evict other objects' chunks | `"new"` |
| `allow_rollup_hdrs` | `true` | metadata is rolled up per subject | `true` |
| `allow_direct` | `true` | metadata lookups use direct get | `true` |
| `compression` | `"s2"` when requested | | as stated |
| `metadata` | user | | user |
| `deny_purge` | `false` (omit) | chunk and metadata subjects are purged on delete, overwrite, and rename | omitted |
| `deny_delete` | `false` (omit) | not set by the convention | omitted |

Everything else is left at the server default: `max_msgs`, `max_consumers`,
`max_msgs_per_subject`, `max_msg_size`, `duplicate_window`, `retention`. Note what is *not* set
compared to KV: no `max_msgs_per_subject` (chunks share a subject, so a per-subject limit would
destroy objects) and no `deny_delete`. Unlike KV, `nats.go` makes no `$JS.API.INFO` pre-check.

The ADR-20 example configuration is not valid JSON as printed (`metadata` is shown as an array,
the `compression` key is unquoted) and repeats the `rollup_hdrs` typo; use the table.

```
C: PUB $JS.API.STREAM.CREATE.OBJ_DOCS _INBOX.x.1 203␍␊
C: {"name":"OBJ_DOCS","subjects":["$O.DOCS.C.>","$O.DOCS.M.>"],"retention":"limits","max_bytes":-1,"discard":"new","max_age":0,"storage":"file","num_replicas":1,"allow_rollup_hdrs":true,"allow_direct":true}␍␊
S: MSG _INBOX.x.1 1 <n>␍␊
S: {"type":"io.nats.jetstream.api.v1.stream_create_response","config":{…},"created":"…","state":{…},"did_create":true}␍␊
```

Management mirrors KV: bind is `STREAM.INFO.OBJ_<bucket>` (10059 → bucket not found), create maps
10058 to "bucket exists", delete removes the stream, and listing pages `STREAM.NAMES` or
`STREAM.LIST` with `{"subject":"$O.*.C.>"}`, keeping names that start with `OBJ_`. ADR-20 requires
bind, create, and delete; `nats.go` adds update, create-or-update, and the two listings.

> **Client rule:** Create object store streams exactly as in the table: both subjects,
> `discard: "new"`, `allow_rollup_hdrs: true`, `allow_direct: true`, and no per-subject limit,
> `deny_delete`, or `deny_purge`.

### 7.4 `ObjectInfo`: the metadata document

The metadata message's payload is one JSON object. ADR-20 defines `ObjectInfo` as embedding
`ObjectMeta`: "the fields in ObjectMeta are serialized in line as if they were direct fields of
ObjectInfo".

| JSON key | Type | Present | Meaning |
|---|---|---|---|
| `name` | string | always | Object name, unencoded. |
| `description` | string | if non-empty | Free text. |
| `headers` | object: string → array of strings | if non-empty | User headers in NATS header shape. |
| `metadata` | object: string → string | if non-empty | User metadata. (ADR-20's example wrongly shows an array.) |
| `options` | object | if non-empty | See the next three rows. |
| `options.link` | object | links only | Present makes this entry a link (section 8.6). |
| `options.link.bucket` | string | in a link | Bucket the link points into. |
| `options.link.name` | string | object links only | Target object name; absent or empty means a link to the whole bucket. |
| `options.max_chunk_size` | unsigned 32-bit integer | if non-zero | Chunk size used for this upload. |
| `bucket` | string | always | Bucket that holds this entry. |
| `nuid` | string | always | The `<nuid>` of the chunk subject. Readers reject an entry with an empty `nuid`. |
| `size` | unsigned 64-bit integer | always | Total content bytes. |
| `mtime` | RFC 3339 timestamp | see below | Modification time; **not authoritative in the stored document**. |
| `chunks` | unsigned 32-bit integer | always | Number of chunk messages. |
| `digest` | string | if non-empty | `SHA-256=<base64url>` of the content. |
| `deleted` | boolean | only when `true` | Tombstone flag. |

About `mtime`: ADR-20 says the modification time "is never stored"; a reader takes it from the
metadata message's server timestamp, and a writer returns the current UTC time from put. `nats.go`
nevertheless serializes the field (its JSON tag has no `omitempty`) with the zero time, so stored
documents written by Go contain `"mtime":"0001-01-01T00:00:00Z"`.

A stored document for an 11-byte object in three chunks, as `nats.go` writes it:

```json
{"name":"greeting.txt","options":{"max_chunk_size":4},"bucket":"DOCS","nuid":"Vq3bTnHcDpLx0a8YkWm2Ze","size":11,"mtime":"0001-01-01T00:00:00Z","chunks":3,"digest":"SHA-256=uU0nuZNNPgilLlLX2n2r-sSE7-N6U4DukIj3rOLvzek="}
```

> **Client rule:** When reading `ObjectInfo`, ignore any stored `mtime` and set the modification
> time from the message timestamp (`Nats-Time-Stamp` of the direct get, or the ack-subject
> timestamp in a watcher). Tolerate `mtime` being absent or present, `options` being absent, and
> unknown keys. When writing, omit empty optional fields.

## 8. Object Store: operations

All metadata writes are synchronous JetStream publishes to the metadata subject with the header
`Nats-Rollup: sub`. All metadata reads are "last message for subject" lookups on the metadata
subject, by direct get when the stream allows it (section 3.3 describes both transports).

### 8.1 Get info

1. Empty name → "name is required".
2. Direct get `$JS.API.DIRECT.GET.OBJ_<bucket>.$O.<bucket>.M.<name-encoded>`.
3. Status 404 (or `err_code` 10037) → "object not found". Stream not found → "bucket not found".
4. Decode the JSON; failure → "object-store meta information invalid".
5. If `deleted` is `true` and the caller did not ask to see deleted entries → "object not found".
   ADR-20: "Do not return info for deleted objects, except with optional convenience methods".
6. Set the modification time from the message timestamp.

### 8.2 Put

ADR-20: "Put will place the contents from the reader into a new object", and "it is an error to
Put when ObjectMeta contains a Link".

The algorithm, following `nats.go`:

1. Reject an empty `name` ("object-store meta information invalid") and metadata that carries
   `options.link` ("link cannot be set when putting the object in bucket"). If no chunk size is
   given, use 131072.
2. Generate a new NUID. The chunk subject is `$O.<bucket>.C.<nuid>`. A fresh NUID per upload means
   the chunks never mix with those of the name's previous content.
3. Look up existing info for the name **including deleted entries** (section 8.1). "Not found" is
   fine; any other error aborts. Remember it for step 9.
4. Read the input in pieces of at most the chunk size. For each non-empty piece: update a running
   SHA-256, publish the piece as a JetStream message to the chunk subject (payload = the raw
   bytes, no headers), and count it. `nats.go` publishes asynchronously and collects the PubAcks
   in the background; a publish error aborts.
5. At end of input set `size` = total bytes, `chunks` = number published, `digest` =
   `SHA-256=<base64url of the hash>`. An absent reader yields `size` 0 and `chunks` 0.
6. Publish the metadata (`ObjectInfo` JSON, header `Nats-Rollup: sub`) to
   `$O.<bucket>.M.<name-encoded>`.
7. Wait for every outstanding PubAck, including the metadata's. On any error or timeout, clean up
   (below) and fail.
8. Set the returned info's modification time to the current UTC time. This is the client's clock,
   not the stored time; a later get-info reports the server's timestamp.
9. If step 3 found an existing entry that was **not** deleted, purge its chunks:
   `$JS.API.STREAM.PURGE.OBJ_<bucket>` with `{"filter":"$O.<bucket>.C.<old nuid>"}`. Errors are
   ignored.
10. Return the info.

**Failure cleanup.** On a read error, a publish error, cancellation, or a timeout at any point
before step 8, wait for the in-flight chunk publishes to settle and then purge the *new* chunk
subject with `{"filter":"$O.<bucket>.C.<nuid>"}`, ignoring errors. No metadata refers to those
chunks, so nothing else would ever remove them.

**Ordering matters.** Chunks first, metadata second: a reader can never find metadata that points
at chunks not yet written. Old chunks are purged only after the new metadata is acknowledged. The
one unavoidable race is a reader that fetched the *old* metadata just before the overwrite and is
still streaming old chunks when step 9 purges them; that reader sees a short read and fails its
digest check.

Scenario 12.6 shows a complete put on the wire.

> **Client rule:** Put publishes chunks to a fresh NUID subject, then the rolled-up metadata, and
> only after every acknowledgment purges the previous NUID's chunks (if the previous entry was not
> already deleted). On any failure purge the new NUID's chunks. Reject metadata that contains a
> link.

> **Client rule:** Await a PubAck for every chunk before reporting success. Pipelining the chunk
> publishes is fine and is what `nats.go` does; fire-and-forget is not, because `discard: new`
> turns a full bucket into per-publish errors.

### 8.3 Get

ADR-20: "Deleted objects should be treated the same as objects that don't exist."

1. Get info (section 8.1); deleted entries are "object not found" unless the caller asks for them.
   An entry with an empty `nuid` is invalid.
2. **Resolve links.** If `options.link` is present: a bucket link (no `link.name`) cannot be read
   ("invalid Get, object is a link to a bucket"); a link into the same bucket restarts the get
   with `link.name`; a link into another bucket binds that bucket and gets `link.name` there.
3. If `size` is 0, return an empty result without creating a consumer.
4. Otherwise create an ordered consumer on `OBJ_<bucket>` with `filter_subject` =
   `$O.<bucket>.C.<nuid>` and `deliver_policy: "all"`. The rest of the configuration is the
   ordered-consumer baseline of section 4.1.
5. For each delivered message: append the payload to the output, feed it to a running SHA-256,
   and read the pending token of the ack reply subject. When it is `0` this was the last chunk:
   finish the output and unsubscribe.
6. When the reader reaches the end, compare the computed SHA-256 with the decoded `digest`. A
   digest that cannot be decoded is "object digest hash has invalid format"; a mismatch is
   "received a corrupt object, digests do not match".

`nats.go` streams: chunks go into a pipe that the caller reads, the digest check happens at
end-of-file, and each read is bounded by the context deadline or the default request timeout. It
verifies only the digest, not `chunks` or `size`.

Flow control applies exactly as for watchers (sections 4.5 and 4.6): an object larger than the
flow-control window is only delivered completely if the client answers flow control requests.

> **Client rule:** Read an object with an ordered consumer filtered on its chunk subject, stop at
> the first message whose pending token is 0, and verify the SHA-256 digest before reporting
> success. A missing chunk (ordered-consumer gap) must trigger recreation from the next stream
> sequence, never a silently short object. Additionally checking the received byte count against
> `size` is cheap and recommended.

### 8.4 Delete

ADR-20: "It's acceptable to no-op for an object that is already deleted. It's an error to delete
an object that does not exist."

1. Get info including deleted entries. Not found → error. Empty `nuid` → invalid.
2. Set `deleted: true`, `size: 0`, `chunks: 0`, and clear `digest`.
3. Publish that document to the metadata subject with `Nats-Rollup: sub`.
4. Purge the chunks: `STREAM.PURGE` with `{"filter":"$O.<bucket>.C.<nuid>"}`.

```
C: HPUB $O.DOCS.M.Z3JlZXRpbmcudHh0 _INBOX.x.20 30 198␍␊
C: NATS/1.0␍␊
C: Nats-Rollup: sub␍␊
C: ␍␊
C: {"name":"greeting.txt","options":{"max_chunk_size":4},"bucket":"DOCS","nuid":"Vq3bTnHcDpLx0a8YkWm2Ze","size":0,"mtime":"0001-01-01T00:00:00Z","chunks":0,"deleted":true}␍␊
S: MSG _INBOX.x.20 1 29␍␊
S: {"stream":"OBJ_DOCS","seq":5}␍␊
C: PUB $JS.API.STREAM.PURGE.OBJ_DOCS _INBOX.x.21 45␍␊
C: {"filter":"$O.DOCS.C.Vq3bTnHcDpLx0a8YkWm2Ze"}␍␊
S: MSG _INBOX.x.21 1 83␍␊
S: {"type":"io.nats.jetstream.api.v1.stream_purge_response","success":true,"purged":3}␍␊
```

`nats.go` does not special-case an already-deleted object: it republishes the tombstone and purges
again. The tombstone metadata stays until the bucket's `max_age` removes it or the name is reused.

### 8.5 Update metadata and rename

ADR-20: only the name, description, and headers can be updated; objects, links, and bucket links
can all be updated; "it is an error to update metadata for a deleted object"; "it is an error to
rename an object to the name of an existing object".

1. Get info for the current name (deleted hidden). Not found → "cannot update meta for a deleted
   object". (`nats.go` returns this same error when the object never existed.)
2. If the new name differs: get info for the new name including deleted. If it exists and is not
   deleted → "an object already exists with that name". Renaming onto a deleted name is allowed.
3. Copy `name`, `description`, `headers`, and `metadata` from the new metadata into the existing
   info. `options`, `nuid`, `size`, `chunks`, and `digest` are untouched.
4. Publish the document to the metadata subject of the **new** name with `Nats-Rollup: sub`.
5. If renamed, purge the old metadata subject: `{"filter":"$O.<bucket>.M.<old name-encoded>"}`.

A rename never touches chunks: the chunk subject is keyed by NUID, not by name.

### 8.6 Links

A link is a metadata entry with `options.link` and no content: `size` 0, `chunks` 0, no digest,
and a freshly generated `nuid` that has no chunks. ADR-20 notes that links "are currently under
discussion whether they are necessary".

**Object link** (`add_link(name, target_info)`): `options.link` = `{"bucket": <target's bucket>,
"name": <target's name>}`.

```json
{"name":"latest","options":{"link":{"bucket":"DOCS","name":"greeting.txt"}},"bucket":"DOCS","nuid":"Zk81wQeRtYuIoP2aSdFgHj","size":0,"mtime":"0001-01-01T00:00:00Z","chunks":0}
```

**Bucket link** (`add_bucket_link(name, other_bucket)`): `options.link` = `{"bucket": <other
bucket>}` with no `name`.

```json
{"name":"archive","options":{"link":{"bucket":"OLD-DOCS"}},"bucket":"DOCS","nuid":"Mn4bVcXzLkJhGfDsApOiUy","size":0,"mtime":"0001-01-01T00:00:00Z","chunks":0}
```

Rules (ADR-20), with the `nats.go` error for each:

| Condition | Result |
|---|---|
| Link name empty | "name is required" |
| Target missing or unnamed (object link) | "object required" |
| Target is deleted | "not allowed to link to a deleted object" |
| Target is itself a link | "not allowed to link to another link" |
| Target bucket missing (bucket link) | "bucket required" |
| Link name is unused | allowed |
| Link name is an existing link or bucket link | allowed; it is overwritten |
| Link name is an existing regular object, not deleted | "an object already exists with that name" |
| Link name is an existing but deleted regular object | ADR-20: allowed. `nats.go`: rejected (section 9.3). |

Links are resolved one level deep on get (section 8.3); since a link may not point at a link, no
loop detection is needed.

### 8.7 Seal

Sealing makes a bucket permanently read-only (ADR-20: "no further modifications will be
allowed"). The client fetches the stream info, sets `sealed: true` in the configuration, and sends
`STREAM.UPDATE`. On sealing, the server forces `max_age: 0`, `discard: new`, `deny_delete: true`,
`deny_purge: true`, and `allow_rollup_hdrs: false`, and a sealed stream cannot be unsealed. Every
later write fails with `err_code` 10109, `invalid operation on sealed stream`.

### 8.8 Watch and list

**Watch** delivers `ObjectInfo` updates. It is an ordered consumer on `OBJ_<bucket>` filtered on
`$O.<bucket>.M.>`:

| Option | Effect |
|---|---|
| default | `deliver_policy: "last_per_subject"`: current entry per name, then updates |
| include history | `deliver_policy: "all"` (because of the rollup there is normally nothing extra) |
| updates only | `deliver_policy: "new"`; no end-of-initial-data marker in `nats.go` |
| ignore deletes | entries with `deleted: true` are not surfaced |

Each message's payload is decoded as `ObjectInfo`; its modification time is the ack-subject
timestamp. Payloads that fail to decode are skipped silently.

The end-of-initial-data marker follows the same contract as for KV, but `nats.go` detects the
empty store differently: before creating the consumer it requests the last message for the
wildcard subject `$O.<bucket>.M.>`; "not found" means the store has no entries and the marker is
sent immediately. Otherwise the marker follows the first message whose pending token is 0. The
server's last-by-subject lookup accepts wildcard subjects, so this works on the direct get path.
This variant has a window that the KV algorithm does not: an entry written between the lookup and
the consumer creation is harmless, but a store emptied in that window produces no marker. A new
client should use the consumer-create-response algorithm of section 4.3 for both. (A store can
become empty without tombstones through `max_age` or a rename's metadata purge.)

`nats.go` uses a 32-entry channel for object watchers and accepts, but ignores, the "meta only"
and "resume from revision" options, which it shares with KV.

**List** is a watch read until the marker, with "ignore deletes" unless the caller asks for
deleted entries (ADR-20: deleted objects are filtered by default). An empty result is "no objects
found".

### 8.9 Status

| Status field | Derived from |
|---|---|
| `bucket` | the bucket name |
| `description` | `config.description` |
| `metadata` | `config.metadata` |
| `ttl` | `config.max_age` |
| `storage` | `config.storage` |
| `replicas` | `config.num_replicas` |
| `sealed` | `config.sealed` |
| `size` | `state.bytes` |
| `backing_store` | the constant string `JetStream` |
| `is_compressed` | `config.compression != "none"` |

## 9. Object Store: errors, required behaviors, discrepancies

### 9.1 Error conditions

These are client-side conditions; the texts are `nats.go`'s.

| Condition | Error text |
|---|---|
| Bucket name invalid | `invalid object-store name` |
| Stream `OBJ_<bucket>` missing (10059) | `bucket not found` |
| Stream exists with a different configuration (10058) | `bucket name already in use` |
| Config missing | `object-store config required` |
| Name empty on put; `nuid` empty; metadata JSON undecodable | `object-store meta information invalid` |
| Name empty on info or link | `name is required` |
| No metadata message; or entry is deleted and deleted entries were not requested | `object not found` |
| Put with `options.link` | `link cannot be set when putting the object in bucket` |
| Digest string malformed | `object digest hash has invalid format` |
| Digest mismatch after reading | `received a corrupt object, digests do not match` |
| Get on a bucket link | `invalid Get, object is a link to a bucket` |
| List on an empty store | `no objects found` |
| Rename target or link name is a live object | `an object already exists with that name` |
| Link target missing | `object required` |
| Link target deleted | `not allowed to link to a deleted object` |
| Link target is a link | `not allowed to link to another link` |
| Bucket link without a bucket | `bucket required` |
| Bucket link to a foreign bucket implementation | `bucket malformed` |
| Update metadata of a deleted (or missing) object | `cannot update meta for a deleted object` |
| Any write on a sealed bucket | server `err_code` 10109, `invalid operation on sealed stream` |
| Upload into a full bucket | server store error `maximum bytes exceeded` |

### 9.2 Required client behaviors (ADR MUSTs)

1. The stream is `OBJ_<bucket>` with subjects `$O.<bucket>.C.>` and `$O.<bucket>.M.>` (ADR-20).
2. Object info is stored as JSON on the metadata subject and is "always rolled up (per subject)"
   (ADR-20).
3. The object name is base64-encoded to form the metadata subject token; the object id is a NUID
   (ADR-20).
4. The digest is `SHA-256=<base64>` with the algorithm name in upper case; SHA-256 is the only
   supported digest (ADR-20).
5. The modification time is never stored; it is the current UTC time on put and the message time
   on get and info (ADR-20).
6. The default chunk size is 128 KiB and can be overridden per object (ADR-20).
7. Compression means `s2` (ADR-20).
8. Put with a link in the metadata is an error (ADR-20).
9. Get treats deleted objects as non-existent; get-info returns nothing for deleted objects unless
   explicitly asked (ADR-20).
10. Update-metadata, delete, seal, and list follow the rules quoted in sections 8.4 to 8.8
    (ADR-20).
11. Add-link and add-bucket-link follow the rules of section 8.6 (ADR-20).
12. Bucket names are `restricted-term`, at most 251 characters (ADR-20, ADR-6).
13. Every API in ADR-20 is required unless marked optional or convenience.

### 9.3 Discrepancies and which side to follow

| # | Topic | ADR | Server | `nats.go` | A new client should |
|---|---|---|---|---|---|
| 1 | Subject prefix | ADR-20: `$O`. ADR-19 names `$OBJ` as the object store prefix. | Domain mappings and leaf node deny lists use `$OBJ.>`; nothing mentions `$O.` | `$O` | Use `$O`. It is what every stored bucket uses; `$OBJ` would be unreadable by other clients. |
| 2 | API prefix on object publishes | ADR-19 implies `<prefix>.$OBJ…` | maps only `$OBJ` | Chunk and metadata publishes always go to bare `$O.…`; `Put` even builds a default-prefix context for them. API calls (info, purge, consumer create, direct get) do use the context's prefix. | Publish to bare `$O.…` and prefix only API requests, matching `nats.go`. Object stores across domains are effectively unspecified; do not invent a mapping. |
| 3 | Name encoding | "base64 encoded" | n/a | URL-safe alphabet with padding | Follow `nats.go` exactly; the encoding is a lookup key. |
| 4 | `mtime` in the stored document | "never stored" | n/a | written as the zero time | Omit it when writing (ADR), tolerate it when reading. Go readers ignore the stored value either way. |
| 5 | Fields updatable by update-metadata | name, description, headers | n/a | also `metadata` | Follow `nats.go`: updating user metadata is the evident intent, and it is one more field in the same document. |
| 6 | Link name equal to a deleted object's name | allowed | n/a | rejected (the existence check includes deleted entries and only exempts links) | Follow the ADR: allow it. The result is an ordinary rollup over the tombstone. |
| 7 | Update-metadata on a missing object | "error to update metadata for a deleted object" | n/a | same error for missing and deleted | Either; distinguishing "not found" from "deleted" is friendlier. |
| 8 | Delete of an already deleted object | no-op acceptable | n/a | republishes and purges again | Either. Republishing is harmless. |
| 9 | Verification on get | digest | n/a | digest only; `size` and `chunks` unchecked | Verify the digest (required); also checking `size` is recommended. |
| 10 | Config example | invalid JSON, `rollup_hdrs` | `allow_rollup_hdrs` | `allow_rollup_hdrs` | Use the table in section 7.3. |
| 11 | Empty-store detection in watch | "same as KV" by implication | wildcard last-by-subject is supported | last-message lookup on `$O.<bucket>.M.>` before creating the consumer | Use the consumer-create response (section 4.3); it has no window. |
| 12 | Unsupported watch options | n/a | n/a | meta-only and resume-from-revision accepted and ignored | Do not offer them on object watchers. |

## 10. Services: discovery, endpoints, and statistics

### 10.1 Model

A service (ADR-32, the "micro" framework) is a named, versioned group of request handlers that
can be discovered and monitored over NATS itself. It uses only the core protocol
([01-core.md](01-core.md)): the server source contains no reference to `$SRV` or to the service
error headers.

A running service instance consists of:

- **Endpoints**: one subscription per endpoint on a user-visible subject, by default in the queue
  group `q`, so that requests are load-balanced across instances of the same service.
- **Control subscriptions**: nine plain (non-queue) subscriptions on `$SRV` subjects that answer
  `PING`, `INFO`, and `STATS` requests with JSON. They are deliberately not in a queue group:
  every instance must answer a discovery request.
- **Identity**: `name` (the *kind* of service, shared by all instances), `id` (unique per
  instance), `version`, and `metadata`.
- **Statistics** per endpoint, maintained by the framework around each handler invocation.

A requester needs nothing special: it sends a core request to the endpoint subject and inspects
the reply's headers for an error.

### 10.2 Service configuration and validation

| Field | Rule | `nats.go` |
|---|---|---|
| `name` | Required. Only `A-Z a-z 0-9 - _`. "Really the *kind* of the service." | regex `^[A-Za-z0-9\-_]+$` |
| `version` | Required. Must be a valid SemVer string; "one of the official semver regex should be used". | the regex suggested on semver.org (below) |
| `description` | Optional text. | |
| `metadata` | Optional string → string map. "Must be immutable once set." | cloned at creation; defaults to an empty map; every read returns a copy |
| `queueGroup` | Optional. Overrides the default queue group `q`. Any characters except whitespace (ADR-6 `queue-name`). | must match `^[^ >]*[>]?$` |
| no queue group | Optional flag: endpoints become plain subscriptions (section 10.5). | `QueueGroupDisabled` |
| `statsHandler` | Optional function called per endpoint when building `STATS`; its JSON-serializable result becomes that endpoint's `data`. | marshal errors are ignored |
| `id` | Assigned at startup, unique per instance. | a NUID |

The SemVer regular expression used by `nats.go`:

```
^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$
```

In Ruby, anchor it with `\A` and `\z` instead of `^` and `$`.

`nats.go`'s `Config` also has an `Endpoint` shortcut: if given, an endpoint **named `default`** is
added on that subject at creation, with the endpoint's metadata and its queue group (falling back
to the service's).

ADR-32 requires the service object to offer at least: `stop(error?)`, `reset()`, `info()`,
`stats()`, a stopped notification (callback or promise) that is "independent of the NATS
connection" so that several services can run on one connection, `addEndpoint(name, handler,
options?)`, and `addGroup(name)`.

> **Client rule:** Validate `name` with `\A[A-Za-z0-9\-_]+\z` and `version` with the semver.org
> regex before subscribing to anything; fail with a validation error. Generate the instance `id`
> as a NUID. Copy the metadata at creation and never expose the internal map.

### 10.3 Control subjects

The prefix is `$SRV`. ADR-32 says the prefix "needs to be overridable much in the way as we do for
`$JS`" and that "the prefix will honor whatever case was specified". The verbs are `PING`, `INFO`,
and `STATS`, in upper case. Each verb is served on three subjects:

| Subject | Who answers |
|---|---|
| `$SRV.<VERB>` | every service instance |
| `$SRV.<VERB>.<name>` | every instance of the service kind `<name>` |
| `$SRV.<VERB>.<name>.<id>` | exactly one instance |

`<name>` and `<id>` appear exactly as configured and generated; there is no case folding. That is
nine subscriptions per instance, none in a queue group:

```
C: SUB $SRV.PING 1␍␊
C: SUB $SRV.PING.EchoService 2␍␊
C: SUB $SRV.PING.EchoService.hG7RmJkP2sTqLw9XcVb4Nd 3␍␊
C: SUB $SRV.INFO 4␍␊
C: SUB $SRV.INFO.EchoService 5␍␊
C: SUB $SRV.INFO.EchoService.hG7RmJkP2sTqLw9XcVb4Nd 6␍␊
C: SUB $SRV.STATS 7␍␊
C: SUB $SRV.STATS.EchoService 8␍␊
C: SUB $SRV.STATS.EchoService.hG7RmJkP2sTqLw9XcVb4Nd 9␍␊
   # the endpoint: a queue subscription in group q
C: SUB svc.echo q 10␍␊
```

A discovery client publishes one request with a reply inbox and collects replies until a timeout
or an expected count: this is a scatter-gather, not a single request-reply.

Endpoint subjects must not start with `$SRV`: "this prefix is reserved for internal handlers".

`nats.go` has the prefix as a constant, `$SRV`, with no override. Its `ControlSubject(verb, name,
id)` helper returns the three forms and rejects an unknown verb and an `id` without a `name`.

> **Client rule:** On start, create plain subscriptions for all nine control subjects before
> reporting the service as started. If any of them fails, stop the service and fail creation.
> Respond to control requests with the JSON of section 10.4 on the request's reply subject.

### 10.4 Response schemas

Every response carries `type`, `name`, `id`, `version`, and `metadata`.

**PING**: type `io.nats.micro.v1.ping_response`. "The intention of `PING` is for clients to
calculate RTT to a service and discover services."

| JSON key | Type | Value |
|---|---|---|
| `type` | string | `io.nats.micro.v1.ping_response` |
| `name` | string | service name |
| `id` | string | instance id |
| `version` | string | service version |
| `metadata` | object string → string | service metadata; `{}` when none |

```json
{"name":"EchoService","id":"hG7RmJkP2sTqLw9XcVb4Nd","version":"1.0.0","metadata":{},"type":"io.nats.micro.v1.ping_response"}
```

**INFO**: type `io.nats.micro.v1.info_response`.

| JSON key | Type | Value |
|---|---|---|
| `type`, `name`, `id`, `version`, `metadata` | | as in PING, with the INFO type string |
| `description` | string | service description; `""` when none |
| `endpoints` | array | one element per endpoint, in the order added; `[]` when none |
| `endpoints[].name` | string | endpoint name |
| `endpoints[].subject` | string | the subject the endpoint is actually subscribed to |
| `endpoints[].queue_group` | string | the queue group actually used; `""` when the endpoint has none |
| `endpoints[].metadata` | object string → string | endpoint metadata |

ADR-32: all INFO fields "reflect the current state of the service […] including endpoints added
after the service was started. Each endpoint reports the `subject` and `queue_group` it actually
uses, including values that come from defaults, such as a subject taken from the endpoint name or
the default queue group `q`." So INFO is computed per request, not cached. (PING can be cached:
`nats.go` builds it once.)

In `nats.go`, an endpoint without metadata serializes `"metadata": null`, because the field has no
`omitempty` and the map is nil. Readers must accept `null`.

**STATS**: type `io.nats.micro.v1.stats_response`.

| JSON key | Type | Value |
|---|---|---|
| `type`, `name`, `id`, `version`, `metadata` | | as in PING, with the STATS type string |
| `started` | string | time the service started, RFC 3339 in UTC |
| `endpoints` | array | one element per endpoint |
| `endpoints[].name` | string | endpoint name |
| `endpoints[].subject` | string | subject in use |
| `endpoints[].queue_group` | string | queue group in use |
| `endpoints[].num_requests` | integer | requests handled (handler returned) |
| `endpoints[].num_errors` | integer | requests that the handler answered with an error |
| `endpoints[].last_error` | string | last error, or `""` when there has been none; always present |
| `endpoints[].processing_time` | integer, nanoseconds | total time spent inside the handler |
| `endpoints[].average_processing_time` | integer, nanoseconds | `processing_time / num_requests`, truncated; `0` when `num_requests` is 0 |
| `endpoints[].data` | any JSON, optional | result of the stats handler for this endpoint; omitted when there is none |

> **Client rule:** Emit exactly these keys and type strings. Durations are integer nanoseconds,
> `started` is an RFC 3339 UTC timestamp, `last_error` is always a string, `metadata` is always an
> object (`{}` when empty), `endpoints` is always an array. Build INFO and STATS from current
> state on every request. When parsing responses from other libraries, accept `null` for
> `metadata`.

### 10.5 Endpoints, groups, subjects, and queue groups

An endpoint has:

| Field | Rule |
|---|---|
| `name` | Required. `\A[A-Za-z0-9\-_]+\z` (ADR-6 `restricted-term`). Names need not be unique: "multiple endpoints can have the same names". |
| `handler` | Required. Receives the request. |
| `subject` | Optional; defaults to `name`. Combined with the group prefix. |
| `metadata` | Optional string → string map, immutable once set. |
| `queueGroup` | Optional override of the inherited queue group. |
| no queue group | Optional flag for a plain subscription. |

Endpoints can be added after the service has started; they cannot be removed individually.

A **group** is a subject prefix shared by endpoints. "Group name should be a valid NATS subject or
an empty string, but cannot contain `>` wildcard." Groups nest.

Subject composition:

| Declared on | Endpoint subject |
|---|---|
| the service | `<subject option>` if given, else `<name>` |
| a group with prefix `P` | `P.<subject option>` if given, else `P.<name>` |
| a group with an empty prefix | as on the service (an empty group adds no prefix and no dot) |
| nested group `addGroup(n)` on a group with prefix `P` | the new prefix is `P.n`; empty parts are skipped when joining |

`nats.go` validates the **final** subject and the final queue group with `^[^ >]*[>]?$` (no
spaces; `>` only as the last character). It does not validate group names separately, so the
"no `>` in a group name" rule is enforced only through the composed subject.

**Queue group resolution.** ADR-32: "All service request handlers operate under the default queue
group `q`", overridable on the service, a group, or an endpoint; and "no `queueGroup` can be set
on the service, a group or an endpoint, and passes down like a `queueGroup`: each level uses its
own setting if it has one, otherwise its parent's, otherwise the default `q`. A level that sets
nothing is not the same as one that sets no `queueGroup`."

Each level therefore has three possible states: *unset*, *named*, or *disabled*. `nats.go`
resolves a level against its parent in this priority order (`resolveQueueGroup`):

1. This level is disabled → no queue group.
2. This level names a queue group → that name.
3. The parent resolved to disabled → no queue group.
4. The parent resolved to a name → that name.
5. Otherwise → `q`.

The service is the root; a group resolves against the service or its parent group; an endpoint
resolves against the service or its group. The result is what INFO and STATS report.

The subscription is `SUB <subject> <queue group> <sid>` or, when disabled,
`SUB <subject> <sid>`. With a plain subscription every instance receives every request, and the
requester gets the first reply.

`nats.go` also lets an endpoint set subscription pending limits (message count and byte count;
`-1` is unlimited; both zero is a validation error).

> **Client rule:** Compose endpoint subjects as `[group prefix + "."] + (subject || name)`,
> skipping empty prefixes. Validate the endpoint name with `\A[A-Za-z0-9\-_]+\z` and the composed
> subject and queue group with `\A[^ >]*[>]?\z`. Reject group names containing `>`.

> **Client rule:** Model the queue group at each level (service, group, endpoint) as tri-state:
> unset, named, or disabled. Resolve with the five-step order above; the default is `q`.

### 10.6 The request handling contract

ADR-32: handlers "operate as any standard subscription handler […] no assumption is made on
whether returning from the callback signals that the request is completed. The framework will
dispatch requests as fast as the handler returns."

For each message on an endpoint subscription the framework:

1. Records the start time.
2. Wraps the message in a request object and calls the handler. The request exposes the payload,
   headers, subject, and reply subject, and offers `respond(data)`, `respond_json(object)`, and
   `respond_error(code, description, data?)`.
3. When the handler returns, updates the endpoint's statistics (section 10.8).

A response is a plain publish to the request's reply subject; additional headers may be attached.
A handler may respond after it has returned (for example from another thread); such a response
"does not change the statistics".

`nats.go` invokes the handler synchronously from the subscription's delivery callback and has no
worker pool of its own, so concurrency per endpoint is whatever the underlying subscription
provides. Whether core subscription callbacks in `nats.go` run strictly one at a time was not
verified for this chapter.

On the service's connection, where the endpoint subscription has sid 10, a request and its
response look like this:

```
S: MSG svc.echo 10 _INBOX.r.1 5␍␊
S: hello␍␊
C: PUB _INBOX.r.1 5␍␊
C: hello␍␊
```

### 10.7 Error responses

ADR-32: services "must include the headers `Nats-Service-Error` and `Nats-Service-Error-Code`" on
error responses.

| Header | Value |
|---|---|
| `Nats-Service-Error` | Human-readable description that "could be shown to the user". |
| `Nats-Service-Error-Code` | "An integer, sent in its decimal form, so it is always safe to parse as a number." |

The payload of an error response is optional application data. "Applications making requests to
the service *must* check if the response is an error by looking for these headers; client
libraries are not required to do this for them." And: "Service API libraries *must* provide an
error formatting function that users can use to produce the properly formatted response headers."

On the service's connection, a request with an empty payload answered with an error:

```
S: MSG svc.echo 10 _INBOX.r.2 0␍␊
S: ␍␊
C: HPUB _INBOX.r.2 77 77␍␊
C: NATS/1.0␍␊
C: Nats-Service-Error: empty request␍␊
C: Nats-Service-Error-Code: 400␍␊
C: ␍␊
C: ␍␊
```

`nats.go`'s `Error(code, description, data)` takes the code as a **string**, requires both code
and description to be non-empty, and does not check that the code is numeric. Its control handlers
use the same mechanism when they fail to respond: code `500`, description
`Error handling <VERB> request: <err>`.

> **Client rule:** `respond_error(code, description, data = nil)` publishes to the reply subject
> with `Nats-Service-Error: <description>` and `Nats-Service-Error-Code: <code>`, where `code` is
> an integer rendered in decimal and `description` is non-empty and contains no CR or LF. Also
> expose the header construction as a standalone formatting helper. On the requesting side, do
> not convert these headers into exceptions inside the generic request API.

### 10.8 How statistics are computed

ADR-32: "A request is counted in the endpoint's statistics when its handler returns:
`num_requests` and `processing_time` are updated then, and `num_errors` and `last_error` only if
the handler responded with an error before returning. A response sent after the handler returns
does not change the statistics."

`nats.go`, around each handler call, under the service lock:

```
start = now
handler(request)
num_requests += 1
processing_time += now - start
average_processing_time = processing_time / num_requests      # integer division, nanoseconds
if request recorded an error:
    num_errors += 1
    last_error = error text
```

The request "records an error" in three cases:

| Case | `last_error` text |
|---|---|
| The handler called the error responder and the publish succeeded | `<code>:<description>` |
| A normal respond failed to publish | the respond error |
| The error responder itself failed to publish | that error |

In addition, an asynchronous connection error attributed to an endpoint's subscription (for
example a slow consumer) increments `num_errors` and sets `last_error` for that endpoint, outside
any request.

`STATS` is built by copying each endpoint's counters and, if a stats handler is configured,
calling it once per endpoint and embedding its JSON as `data`.

> **Client rule:** Update `num_requests`, `processing_time`, and `average_processing_time` when
> the handler returns, including when it raises. Count an error only if the error responder was
> used (or a respond failed) before the handler returned, and set `last_error` to
> `"<code>:<description>"`. Protect the counters so that a concurrent `STATS` request sees a
> consistent snapshot.

### 10.9 Stop, reset, and connection events

**Stop.** ADR-32: "Stop should always drain its service subscriptions." Draining
([01-core.md](01-core.md)) unsubscribes at the server while letting already-received requests be
handled. `nats.go`'s `Stop()`:

1. Is idempotent: a second call does nothing.
2. Drains every endpoint subscription (ignoring "connection closed") and removes the endpoints.
3. Drains the nine control subscriptions.
4. Unregisters the service from the connection's hooks.
5. Marks the service stopped and invokes the done handler asynchronously.

It takes no error argument; ADR-32 allows an optional one that is passed to the stopped
notification.

**Reset.** ADR-32: "`reset()` to reset any tracked metrics". `nats.go` resets every endpoint's
statistics to just `name`, `subject`, and `queue_group` (counters zero, `last_error` empty, `data`
gone) and also sets `started` to the current UTC time.

**Connection events.** `nats.go` keeps a registry of services per connection. The first service
on a connection saves the connection's closed and error handlers and installs its own; the last
one removed restores them. When the connection closes, every service on it is stopped and then the
saved closed handler runs. On an asynchronous connection error, each service checks whether the
subscription belongs to it; if so and an error handler is configured, the handler is called with
the subscription's subject and the error text, and then the saved error handler runs. Hence its
documentation: "Set the connection's closed and error handlers before adding a service; handlers
set afterwards replace the ones the service relies on."

The done handler and control-request error callbacks run on a small asynchronous dispatcher (a
queue of 100 callbacks drained by one worker; closing it flushes what is queued and later pushes
are dropped), so user callbacks never run on the connection's read path.

> **Client rule:** `stop` drains all endpoint and control subscriptions, is idempotent, marks the
> service stopped, and fires the stopped notification exactly once. Closing the connection stops
> every service on it. Several services must be able to share one connection.

## 11. Services: required behaviors and discrepancies

### 11.1 Required client behaviors (ADR-32 MUSTs)

1. Validate `name` (`A-Z a-z 0-9 - _`) and `version` (official SemVer regex).
2. Service and endpoint metadata are immutable once set.
3. Assign a unique `id` at startup.
4. Subscribe to `$SRV.<VERB>`, `$SRV.<VERB>.<name>`, and `$SRV.<VERB>.<name>.<id>` for `PING`,
   `STATS`, and `INFO`, and respond on all of them. The prefix must be overridable and
   case-preserving.
5. Use the response type strings and schemas of section 10.4. INFO reflects current state,
   including endpoints added later and the effective subject and queue group.
6. The default queue group is `q`; it can be overridden, or disabled, at service, group, and
   endpoint level with the stated inheritance.
7. Endpoint subject = group prefix + (`subject` or `name`); an empty group adds no prefix.
8. A group name cannot contain `>`.
9. `stop()` drains the service's subscriptions. Provide `reset()`, `info()`, `stats()`, a stopped
   notification, `addEndpoint`, and `addGroup`.
10. Error responses carry `Nats-Service-Error` and `Nats-Service-Error-Code` (a decimal integer).
    The library provides an error formatting function.
11. Statistics are updated when the handler returns. `average_processing_time` is truncated and is
    0 with no requests. `last_error` is an empty string when there has been none.
12. Endpoint subjects do not use the `$SRV` prefix.
13. The feature is called "service" / "services" in user-facing API and documentation.

### 11.2 Discrepancies and which side to follow

| # | Topic | ADR-32 | `nats.go` | A new client should |
|---|---|---|---|---|
| 1 | `$SRV` prefix override | must be overridable, case-preserving | constant, no override | Offer the override with default `$SRV`; it is a MUST and costs one option. |
| 2 | Error code type | integer in decimal | arbitrary non-empty string | Accept an integer. Requesters are told the header is "always safe to parse as a number". |
| 3 | Endpoint `metadata` when unset | an object | `null` | Emit `{}`; accept `null` when reading. |
| 4 | `reset()` | resets tracked metrics | also resets `started` | Follow `nats.go`: `started` then marks the start of the window the counters cover, and monitoring tools built against the reference client expect that. |
| 5 | `stop(error?)` | optional error argument | no argument | Accept an optional error and pass it to the stopped notification. |
| 6 | Group name validation | cannot contain `>` | only the composed subject is validated | Validate the group name itself, for an earlier and clearer error. |
| 7 | What counts as an error | handler responded with an error before returning | also failed responds and async subscription errors such as slow consumers | Count handler error responses and failed responds; surfacing subscription errors in `last_error` is optional. |
| 8 | Default endpoint from config | not specified | `Config.Endpoint` creates an endpoint named `default` | Optional convenience; if offered, use the name `default`. |
| 9 | Endpoint removal on stop | n/a | matches endpoints by subject | Remove all endpoints; do not rely on subject uniqueness. |

The server takes no position on any of these: services exist only in clients.

## 12. Scenarios

The scenarios 12.1 to 12.3 and 12.5 continue one story on the bucket `CFG` (history 5), so the
revision numbers carry over.

### 12.1 Create a bucket, put a key, get it

```
   # 1. create the bucket
C: PUB $JS.API.STREAM.CREATE.KV_CFG _INBOX.x.1 312␍␊
C: {"name":"KV_CFG","subjects":["$KV.CFG.>"],"retention":"limits","max_consumers":-1,"max_msgs":-1,"max_bytes":-1,"discard":"new","max_age":0,"max_msgs_per_subject":5,"max_msg_size":-1,"storage":"file","num_replicas":1,"duplicate_window":120000000000,"deny_delete":true,"allow_rollup_hdrs":true,"allow_direct":true}␍␊
S: MSG _INBOX.x.1 1 <n>␍␊
S: {"type":"io.nats.jetstream.api.v1.stream_create_response","config":{…},"created":"…","state":{…},"did_create":true}␍␊
   # 2. put color=blue
C: PUB $KV.CFG.color _INBOX.x.2 4␍␊
C: blue␍␊
S: MSG _INBOX.x.2 1 27␍␊
S: {"stream":"KV_CFG","seq":1}␍␊
   # 3. get color
C: PUB $JS.API.DIRECT.GET.KV_CFG.$KV.CFG.color _INBOX.x.3 0␍␊
C: ␍␊
S: HMSG _INBOX.x.3 1 129 133␍␊
S: NATS/1.0␍␊
S: Nats-Stream: KV_CFG␍␊
S: Nats-Subject: $KV.CFG.color␍␊
S: Nats-Sequence: 1␍␊
S: Nats-Time-Stamp: 2026-11-22T10:15:30.123456789Z␍␊
S: ␍␊
S: blue␍␊
```

- The create request is a minimal conforming configuration (section 2.2). `nats.go` additionally
  sends zero-valued keys; they do not change the result.
- The put's PubAck is 27 bytes; `seq` 1 is the revision.
- The get's header block is 129 bytes and the total is 133: the 4-byte value follows the blank
  line. The entry is `{bucket: CFG, key: color, value: "blue", revision: 1,
  created: 2026-11-22T10:15:30.123456789Z, delta: 0, operation: PUT}`.

### 12.2 Create on an existing key, and on a deleted key

**Existing key.** The key holds a live value, so create must fail.

```
   # 1. create color=red: rejected, the subject already has a message at sequence 1
C: HPUB $KV.CFG.color _INBOX.x.4 52 55␍␊
C: NATS/1.0␍␊
C: Nats-Expected-Last-Subject-Sequence: 0␍␊
C: ␍␊
C: red␍␊
S: MSG _INBOX.x.4 1 104␍␊
S: {"error":{"code":400,"err_code":10071,"description":"wrong last sequence: 1"},"stream":"KV_CFG","seq":0}␍␊
   # 2. load the latest entry to see whether it is a tombstone
C: PUB $JS.API.DIRECT.GET.KV_CFG.$KV.CFG.color _INBOX.x.5 0␍␊
C: ␍␊
S: HMSG _INBOX.x.5 1 129 133␍␊
S: NATS/1.0␍␊
S: Nats-Stream: KV_CFG␍␊
S: Nats-Subject: $KV.CFG.color␍␊
S: Nats-Sequence: 1␍␊
S: Nats-Time-Stamp: 2026-11-22T10:15:30.123456789Z␍␊
S: ␍␊
S: blue␍␊
```

The latest entry carries no `KV-Operation` or `Nats-Marker-Reason`, so it is a live value and the
client reports "key exists". The error description contains the subject's actual last sequence.

**Deleted key.** The key's latest entry is a tombstone, so create must succeed.

```
   # 1. somebody deletes the key
C: HPUB $KV.CFG.color _INBOX.x.6 31 31␍␊
C: NATS/1.0␍␊
C: KV-Operation: DEL␍␊
C: ␍␊
C: ␍␊
S: MSG _INBOX.x.6 1 27␍␊
S: {"stream":"KV_CFG","seq":2}␍␊
   # 2. create color=red: still rejected, the tombstone is a message on the subject
C: HPUB $KV.CFG.color _INBOX.x.7 52 55␍␊
C: NATS/1.0␍␊
C: Nats-Expected-Last-Subject-Sequence: 0␍␊
C: ␍␊
C: red␍␊
S: MSG _INBOX.x.7 1 104␍␊
S: {"error":{"code":400,"err_code":10071,"description":"wrong last sequence: 2"},"stream":"KV_CFG","seq":0}␍␊
   # 3. load the latest entry: it is a DEL tombstone at revision 2
C: PUB $JS.API.DIRECT.GET.KV_CFG.$KV.CFG.color _INBOX.x.8 0␍␊
C: ␍␊
S: HMSG _INBOX.x.8 1 148 148␍␊
S: NATS/1.0␍␊
S: KV-Operation: DEL␍␊
S: Nats-Stream: KV_CFG␍␊
S: Nats-Subject: $KV.CFG.color␍␊
S: Nats-Sequence: 2␍␊
S: Nats-Time-Stamp: 2026-11-22T10:15:31.123456789Z␍␊
S: ␍␊
S: ␍␊
   # 4. retry as an update that expects the tombstone's revision
C: HPUB $KV.CFG.color _INBOX.x.9 52 55␍␊
C: NATS/1.0␍␊
C: Nats-Expected-Last-Subject-Sequence: 2␍␊
C: ␍␊
C: red␍␊
S: MSG _INBOX.x.9 1 27␍␊
S: {"stream":"KV_CFG","seq":3}␍␊
```

Step 2 fails because `Nats-Expected-Last-Subject-Sequence: 0` means "no message on this subject",
and the tombstone is a message. Step 3's reply has an empty payload and the stored `KV-Operation`
header ahead of the direct get headers. Step 4 is a compare-and-set against the tombstone, so two
clients racing to re-create the key cannot both succeed: the loser gets 10071 again and reports
"key exists". History for the key is now `blue` (1), `DEL` (2), `red` (3).

### 12.3 Optimistic update conflict

Two writers have both read `color` at revision 3.

```
   # writer A: update color=green, expecting revision 3
C: HPUB $KV.CFG.color _INBOX.a.1 52 57␍␊
C: NATS/1.0␍␊
C: Nats-Expected-Last-Subject-Sequence: 3␍␊
C: ␍␊
C: green␍␊
S: MSG _INBOX.a.1 1 27␍␊
S: {"stream":"KV_CFG","seq":4}␍␊
   # writer B (its own connection): update color=black, also expecting revision 3
C: HPUB $KV.CFG.color _INBOX.b.1 52 57␍␊
C: NATS/1.0␍␊
C: Nats-Expected-Last-Subject-Sequence: 3␍␊
C: ␍␊
C: black␍␊
S: MSG _INBOX.b.1 1 104␍␊
S: {"error":{"code":400,"err_code":10071,"description":"wrong last sequence: 4"},"stream":"KV_CFG","seq":0}␍␊
   # writer B: re-read, re-apply its change, retry against revision 4
C: PUB $JS.API.DIRECT.GET.KV_CFG.$KV.CFG.color _INBOX.b.2 0␍␊
C: ␍␊
S: HMSG _INBOX.b.2 1 129 134␍␊
S: NATS/1.0␍␊
S: Nats-Stream: KV_CFG␍␊
S: Nats-Subject: $KV.CFG.color␍␊
S: Nats-Sequence: 4␍␊
S: Nats-Time-Stamp: 2026-11-22T10:15:32.123456789Z␍␊
S: ␍␊
S: green␍␊
C: HPUB $KV.CFG.color _INBOX.b.3 52 57␍␊
C: NATS/1.0␍␊
C: Nats-Expected-Last-Subject-Sequence: 4␍␊
C: ␍␊
C: black␍␊
S: MSG _INBOX.b.3 1 27␍␊
S: {"stream":"KV_CFG","seq":5}␍␊
```

Writer B's first attempt is rejected with the actual last sequence, 4. The client library reports
"key revision mismatch" and does **not** retry by itself: only the application knows whether its
change still makes sense on top of `green`. On a replicated stream the same conflict may come back
as `err_code` 10164 with the description `wrong last sequence` and no number.

### 12.4 Watch from an empty bucket through the first put

A watcher on all keys of the empty bucket `JOBS`. The delivery inbox is `_INBOX.w1` with sid 2.

```
   # 1. subscribe to the delivery inbox BEFORE creating the consumer
C: SUB _INBOX.w1 2␍␊
   # 2. create the ordered consumer
C: PUB $JS.API.CONSUMER.CREATE.KV_JOBS.wK3pQ9Zt.$KV.JOBS.> _INBOX.x.1 322␍␊
C: {"stream_name":"KV_JOBS","config":{"name":"wK3pQ9Zt","deliver_policy":"last_per_subject","ack_policy":"none","ack_wait":79200000000000,"max_deliver":1,"filter_subject":"$KV.JOBS.>","replay_policy":"instant","flow_control":true,"idle_heartbeat":5000000000,"deliver_subject":"_INBOX.w1","num_replicas":1,"mem_storage":true}}␍␊
S: MSG _INBOX.x.1 1 <n>␍␊
S: {"type":"io.nats.jetstream.api.v1.consumer_create_response","stream_name":"KV_JOBS","name":"wK3pQ9Zt","config":{…},"delivered":{"consumer_seq":0,"stream_seq":0},…,"num_pending":0}␍␊
   #    num_pending + delivered.consumer_seq = 0  ->  emit the end-of-initial-data marker now
   # 3. five seconds of silence: the server proves the consumer is alive
S: HMSG _INBOX.w1 2 75 75␍␊
S: NATS/1.0 100 Idle Heartbeat␍␊
S: Nats-Last-Consumer: 0␍␊
S: Nats-Last-Stream: 0␍␊
S: ␍␊
S: ␍␊
   # 4. another client puts job.1=queued; the watcher receives it
S: MSG $KV.JOBS.job.1 2 $JS.ACK.KV_JOBS.wK3pQ9Zt.1.1.1.1795342530123456789.0 6␍␊
S: queued␍␊
```

- The create response announces nothing to deliver, so the marker is emitted from the response.
  No message would ever have triggered it.
- The heartbeat has no reply subject and no payload; its two headers say that nothing has been
  delivered yet. It is consumed by the ordered-consumer logic.
- The data message's reply subject yields: delivered 1, stream sequence 1 (the revision), consumer
  sequence 1, timestamp `1795342530123456789` ns, pending 0. The entry is
  `{key: job.1, value: "queued", revision: 1, delta: 0, operation: PUT}`. `init_done` is already
  true, so no second marker follows.

### 12.5 Purge, then watch

```
   # 1. purge color: the tombstone becomes revision 6; revisions 1 to 5 of the key are removed
C: HPUB $KV.CFG.color _INBOX.x.1 51 51␍␊
C: NATS/1.0␍␊
C: KV-Operation: PURGE␍␊
C: Nats-Rollup: sub␍␊
C: ␍␊
C: ␍␊
S: MSG _INBOX.x.1 1 27␍␊
S: {"stream":"KV_CFG","seq":6}␍␊
   # 2. start a watcher on the whole bucket
C: SUB _INBOX.w2 2␍␊
C: PUB $JS.API.CONSUMER.CREATE.KV_CFG.wK3pQ9Zt.$KV.CFG.> _INBOX.x.2 320␍␊
C: {"stream_name":"KV_CFG","config":{"name":"wK3pQ9Zt","deliver_policy":"last_per_subject","ack_policy":"none","ack_wait":79200000000000,"max_deliver":1,"filter_subject":"$KV.CFG.>","replay_policy":"instant","flow_control":true,"idle_heartbeat":5000000000,"deliver_subject":"_INBOX.w2","num_replicas":1,"mem_storage":true}}␍␊
S: MSG _INBOX.x.2 1 <n>␍␊
S: {"type":"io.nats.jetstream.api.v1.consumer_create_response","stream_name":"KV_CFG","name":"wK3pQ9Zt","config":{…},"delivered":{"consumer_seq":0,"stream_seq":0},…,"num_pending":1}␍␊
   #    num_pending + delivered.consumer_seq = 1  ->  expect one initial message
S: HMSG $KV.CFG.color 2 $JS.ACK.KV_CFG.wK3pQ9Zt.1.6.1.1795342535123456789.0 51 51␍␊
S: NATS/1.0␍␊
S: KV-Operation: PURGE␍␊
S: Nats-Rollup: sub␍␊
S: ␍␊
S: ␍␊
   #    entry{key=color, revision=6, operation=PURGE, delta=0}, then the marker
```

After the rollup the subject holds exactly one message, the purge tombstone. A default watcher
(`last_per_subject`) therefore receives one initial message. Its headers are the stored ones;
`Nats-Rollup` is stored with the message and is harmless to readers. The entry has operation
`PURGE` and an empty value, and since `received (1) >= init_pending (1)` and the pending token is
0, the marker follows immediately. With "ignore deletes" the entry would be suppressed and only
the marker emitted; `keys` on this bucket reports "no keys found".

### 12.6 Put and get a three-chunk object

Bucket `DOCS`, object name `greeting.txt` (encoded `Z3JlZXRpbmcudHh0`), content `hello world` (11
bytes), chunk size 4, NUID `Vq3bTnHcDpLx0a8YkWm2Ze`.

**Put.**

```
   # 1. look up existing info for the name: none
C: PUB $JS.API.DIRECT.GET.OBJ_DOCS.$O.DOCS.M.Z3JlZXRpbmcudHh0 _INBOX.x.2 0␍␊
C: ␍␊
S: HMSG _INBOX.x.2 1 34 34␍␊
S: NATS/1.0 404 Message Not Found␍␊
S: ␍␊
S: ␍␊
   # 2. publish the chunks to the new NUID's subject (pipelined; acks may arrive later)
C: PUB $O.DOCS.C.Vq3bTnHcDpLx0a8YkWm2Ze _INBOX.x.3 4␍␊
C: hell␍␊
C: PUB $O.DOCS.C.Vq3bTnHcDpLx0a8YkWm2Ze _INBOX.x.4 4␍␊
C: o wo␍␊
C: PUB $O.DOCS.C.Vq3bTnHcDpLx0a8YkWm2Ze _INBOX.x.5 3␍␊
C: rld␍␊
S: MSG _INBOX.x.3 1 29␍␊
S: {"stream":"OBJ_DOCS","seq":1}␍␊
S: MSG _INBOX.x.4 1 29␍␊
S: {"stream":"OBJ_DOCS","seq":2}␍␊
S: MSG _INBOX.x.5 1 29␍␊
S: {"stream":"OBJ_DOCS","seq":3}␍␊
   # 3. publish the metadata with a per-subject rollup
C: HPUB $O.DOCS.M.Z3JlZXRpbmcudHh0 _INBOX.x.6 30 248␍␊
C: NATS/1.0␍␊
C: Nats-Rollup: sub␍␊
C: ␍␊
C: {"name":"greeting.txt","options":{"max_chunk_size":4},"bucket":"DOCS","nuid":"Vq3bTnHcDpLx0a8YkWm2Ze","size":11,"mtime":"0001-01-01T00:00:00Z","chunks":3,"digest":"SHA-256=uU0nuZNNPgilLlLX2n2r-sSE7-N6U4DukIj3rOLvzek="}␍␊
S: MSG _INBOX.x.6 1 29␍␊
S: {"stream":"OBJ_DOCS","seq":4}␍␊
```

The metadata payload is the document shown in section 7.4. Its digest is the SHA-256 of
`hello world`. Had step 1 found a live entry, a final
`$JS.API.STREAM.PURGE.OBJ_DOCS` with `{"filter":"$O.DOCS.C.<old nuid>"}` would follow. Had any
publish failed, the client would instead purge the new chunk subject,
`{"filter":"$O.DOCS.C.Vq3bTnHcDpLx0a8YkWm2Ze"}`, and report the error.

**Get.**

```
   # 1. read the metadata (the stored Nats-Rollup header is echoed first)
C: PUB $JS.API.DIRECT.GET.OBJ_DOCS.$O.DOCS.M.Z3JlZXRpbmcudHh0 _INBOX.x.7 0␍␊
C: ␍␊
S: HMSG _INBOX.x.7 1 162 380␍␊
S: NATS/1.0␍␊
S: Nats-Rollup: sub␍␊
S: Nats-Stream: OBJ_DOCS␍␊
S: Nats-Subject: $O.DOCS.M.Z3JlZXRpbmcudHh0␍␊
S: Nats-Sequence: 4␍␊
S: Nats-Time-Stamp: 2026-11-22T10:15:34.123456789Z␍␊
S: ␍␊
S: {"name":"greeting.txt","options":{"max_chunk_size":4},"bucket":"DOCS","nuid":"Vq3bTnHcDpLx0a8YkWm2Ze","size":11,"mtime":"0001-01-01T00:00:00Z","chunks":3,"digest":"SHA-256=uU0nuZNNPgilLlLX2n2r-sSE7-N6U4DukIj3rOLvzek="}␍␊
   # 2. read the chunks through an ordered consumer filtered on the chunk subject
C: SUB _INBOX.o1 2␍␊
C: PUB $JS.API.CONSUMER.CREATE.OBJ_DOCS.oB7xN2mQ.$O.DOCS.C.Vq3bTnHcDpLx0a8YkWm2Ze _INBOX.x.8 332␍␊
C: {"stream_name":"OBJ_DOCS","config":{"name":"oB7xN2mQ","deliver_policy":"all","ack_policy":"none","ack_wait":79200000000000,"max_deliver":1,"filter_subject":"$O.DOCS.C.Vq3bTnHcDpLx0a8YkWm2Ze","replay_policy":"instant","flow_control":true,"idle_heartbeat":5000000000,"deliver_subject":"_INBOX.o1","num_replicas":1,"mem_storage":true}}␍␊
S: MSG _INBOX.x.8 1 <n>␍␊
S: {"type":"io.nats.jetstream.api.v1.consumer_create_response","stream_name":"OBJ_DOCS","name":"oB7xN2mQ","config":{…},…,"num_pending":3}␍␊
S: MSG $O.DOCS.C.Vq3bTnHcDpLx0a8YkWm2Ze 2 $JS.ACK.OBJ_DOCS.oB7xN2mQ.1.1.1.1795342530123456789.2 4␍␊
S: hell␍␊
S: MSG $O.DOCS.C.Vq3bTnHcDpLx0a8YkWm2Ze 2 $JS.ACK.OBJ_DOCS.oB7xN2mQ.1.2.2.1795342530123457789.1 4␍␊
S: o wo␍␊
S: MSG $O.DOCS.C.Vq3bTnHcDpLx0a8YkWm2Ze 2 $JS.ACK.OBJ_DOCS.oB7xN2mQ.1.3.3.1795342530123458789.0 3␍␊
S: rld␍␊
   # 3. pending token is 0: last chunk. Verify the digest, then tear down
C: UNSUB 2␍␊
```

The pending tokens count down 2, 1, 0. The concatenated payloads are `hello world`; its SHA-256,
URL-safe base64 encoded, must equal the part of `digest` after `SHA-256=`. The returned info's
modification time is `Nats-Time-Stamp` from step 1, not the stored `mtime`.

### 12.7 A service answers a request and is discovered

Service `EchoService`, version `1.0.0`, id `hG7RmJkP2sTqLw9XcVb4Nd`, one endpoint `echo` on
`svc.echo` in the default queue group. On start, the service connection sends the ten `SUB` lines of section
10.3.

**The requester's connection.** A successful request and a failing one:

```
C: SUB _INBOX.r.* 1␍␊
C: PUB svc.echo _INBOX.r.1 5␍␊
C: hello␍␊
S: MSG _INBOX.r.1 1 5␍␊
S: hello␍␊
C: PUB svc.echo _INBOX.r.2 0␍␊
C: ␍␊
S: HMSG _INBOX.r.2 1 77 77␍␊
S: NATS/1.0␍␊
S: Nats-Service-Error: empty request␍␊
S: Nats-Service-Error-Code: 400␍␊
S: ␍␊
S: ␍␊
```

The second reply is an `HMSG` whose 77 bytes are all headers. The requester's library returns it
as an ordinary message; the application checks for `Nats-Service-Error`.

**The service's connection**, for the same two requests (section 10.6 and 10.7 show the frames),
and then a discovery by a monitoring tool. `PING` to all services:

```
S: MSG $SRV.PING 1 _INBOX.d.1 0␍␊
S: ␍␊
C: PUB _INBOX.d.1 124␍␊
C: {"name":"EchoService","id":"hG7RmJkP2sTqLw9XcVb4Nd","version":"1.0.0","metadata":{},"type":"io.nats.micro.v1.ping_response"}␍␊
```

`INFO` to all services, received on the "all" subscription (sid 4):

```
S: MSG $SRV.INFO 4 _INBOX.d.2 0␍␊
S: ␍␊
C: PUB _INBOX.d.2 239␍␊
C: {"name":"EchoService","id":"hG7RmJkP2sTqLw9XcVb4Nd","version":"1.0.0","metadata":{},"type":"io.nats.micro.v1.info_response","description":"echoes requests","endpoints":[{"name":"echo","subject":"svc.echo","queue_group":"q","metadata":{}}]}␍␊
```

`STATS` addressed to this one instance, received on sid 9:

```
S: MSG $SRV.STATS.EchoService.hG7RmJkP2sTqLw9XcVb4Nd 9 _INBOX.d.3 0␍␊
S: ␍␊
C: PUB _INBOX.d.3 359␍␊
C: {"name":"EchoService","id":"hG7RmJkP2sTqLw9XcVb4Nd","version":"1.0.0","metadata":{},"type":"io.nats.micro.v1.stats_response","started":"2026-11-22T10:15:00.123456789Z","endpoints":[{"name":"echo","subject":"svc.echo","queue_group":"q","num_requests":2,"num_errors":1,"last_error":"400:empty request","processing_time":184000,"average_processing_time":92000}]}␍␊
```

After the two requests above, the endpoint has handled 2 requests, 1 of which the handler answered
with an error; `last_error` is the code and description joined by a colon; the average is
184000 / 2 = 92000 ns. A tool discovering all instances sends one request and keeps reading
replies from its inbox until a timeout, since each instance answers separately.

## 13. Client implementation checklist

Every client rule of this chapter, grouped by feature.

**Key/Value: buckets**

1. Validate bucket names: `\A[a-zA-Z0-9_-]+\z`, at most 252 characters.
2. Stream name `KV_<bucket>`, subjects `["$KV.<bucket>.>"]` (omitted for mirrors).
3. Send `max_msgs_per_subject` = history (1 to 64, default 1; reject more than 64 client-side),
   `max_age` = TTL, `max_msg_size` and `max_bytes` (default `-1`), `max_msgs: -1`,
   `max_consumers: -1`, `discard: "new"`, `allow_rollup_hdrs: true`, `deny_delete: true`,
   `allow_direct: true`; never `deny_purge: true`.
4. Compute and send `duplicate_window`: 2 minutes, or `max_age` when `0 < max_age < 2 min`.
5. `compression: "s2"` when compression is requested.
6. For a limit-marker TTL: require `api.level >= 1` from `$JS.API.INFO`, require the TTL to be at
   least 1 s, set `allow_msg_ttl: true` and `subject_delete_marker_ttl`. Never disable
   `allow_msg_ttl`.
7. Bind by `STREAM.INFO`; map 10059 to "bucket not found", 10058 to "bucket exists",
   `max_msgs_per_subject < 1` to "bad bucket". Keep the stream configuration on the handle.
8. Delete a bucket by deleting its stream. List buckets with the `$KV.*.>` subject filter and the
   `KV_` name prefix.
9. Status exposes bucket, values, history, TTL, limit-marker TTL, compression, bytes, metadata,
   and `backing_store` = `JetStream`.

**Key/Value: keys and writes**

10. Validate keys: `\A[-/_=\.a-zA-Z0-9]+\z`, no leading or trailing dot, no `..`. Refuse to write
    keys starting with `_kv`.
11. Put: publish the value to `$KV.<bucket>.<key>`, no headers, no TTL; revision = PubAck `seq`.
12. Create: `Nats-Expected-Last-Subject-Sequence: 0`; on 10071 or 10164 load the latest entry
    including tombstones; if it is a tombstone, retry with its revision as the expected sequence;
    otherwise "key exists". Optional TTL via `Nats-TTL`.
13. Update: `Nats-Expected-Last-Subject-Sequence: <revision>`; 10071 or 10164 is "revision
    mismatch", distinct from "key exists"; no automatic retry; no TTL.
14. Delete: empty payload, `KV-Operation: DEL`; optional expected revision; no TTL.
15. Purge: empty payload, `KV-Operation: PURGE`, `Nats-Rollup: sub`; optional expected revision;
    optional `Nats-TTL`.
16. `Nats-TTL` only on create and purge, only when positive, as integer seconds or a Go duration
    string. Pass 10165 and 10166 through.
17. With a non-default API prefix, publish writes to `<api prefix>.$KV.<bucket>.<key>`.

**Key/Value: reads**

18. Get the latest value with `$JS.API.DIRECT.GET.KV_<bucket>.$KV.<bucket>.<key>` and an empty
    body when `allow_direct` is true; otherwise `STREAM.MSG.GET` with `last_by_subj`.
19. Get a revision with `$JS.API.DIRECT.GET.KV_<bucket>` and `{"seq":N}`; verify `Nats-Subject`
    matches the key, else "key not found".
20. Status 404 and `err_code` 10037 are "key not found". Require `Nats-Stream`, `Nats-Subject`,
    `Nats-Sequence`, and `Nats-Time-Stamp` on direct get replies.
21. Operation: `KV-Operation` (`DEL`, `PURGE`) first, then `Nats-Marker-Reason` (`MaxAge` and
    `Purge` → purge, `Remove` → delete), else put. Treat an unknown `KV-Operation` value as a
    delete.
22. A tombstone is "key not found" in get, and an entry in watch and history. Keep an internal get
    that returns the tombstone.
23. Entries: revision = stream sequence, created = message timestamp, delta = pending count (0 for
    gets).
24. Do not promise read-after-write consistency.

**Key/Value: watchers**

25. Use an ordered consumer on `KV_<bucket>`: `ack_policy: none`, `flow_control: true`,
    `idle_heartbeat` 5 s, `mem_storage: true`, `num_replicas: 1`, `max_deliver: 1`.
26. Filters are unprefixed `$KV.<bucket>.<search key>`; one filter uses `filter_subject`, several
    use `filter_subjects`; empty means `>`. Validate search keys.
27. Deliver policy: `last_per_subject` by default, `all` with include-history, `new` with
    updates-only, `by_start_sequence` with resume-from-revision. Meta-only sets `headers_only`.
    Reject include-history with updates-only.
28. Subscribe to the delivery subject before creating the consumer; evaluate the create response
    before processing any delivered message, under one lock.
29. Emit the end-of-initial-data marker exactly once and always: immediately when
    `num_pending + delivered.consumer_seq == 0` (and immediately for updates-only); otherwise
    after the message where `received >= initial` or the pending token is 0.
30. Count messages suppressed by ignore-deletes toward `received`.
31. Parse 9-token and 11-or-more-token `$JS.ACK` subjects. Derive the key by stripping the read
    prefix; drop messages that do not carry a key.
32. Answer flow control requests and stalled heartbeats independently of the application's
    consumption; never drop a status message unprocessed.
33. Never drop an entry: block delivery when the application queue is full, and make timed reads
    non-destructive.
34. On ordered-consumer recreation keep the marker state and resume with `by_start_sequence`.
35. `keys` = watch all + ignore-deletes + meta-only until the marker; `history` = watch one key +
    include-history until the marker; neither terminates on a timeout. Stop the watcher after.
36. Purge-deletes: collect tombstones with a watch, then `STREAM.PURGE` per key with `filter`, and
    `keep: 1` for tombstones younger than the threshold (default 30 minutes).

**Key/Value: mirrors and sources**

37. Mirror: `KV_` prefix on the mirror name, `mirror_direct: true`, no `subjects`, no limit
    markers.
38. On a mirror handle, write to `$KV.<origin>.<key>` (with `external.api` as prefix when set) and
    read with the origin's subject prefix.
39. Source without transforms: `KV_` prefix and a generated `$KV.<source>.>` → `$KV.<bucket>.>`
    transform (skipped for an external source with the same bucket name). Source with transforms:
    untouched.

**Object Store**

40. Validate bucket names: `\A[a-zA-Z0-9_-]+\z`, at most 251 characters. Reject empty object
    names.
41. Stream `OBJ_<bucket>`, subjects `$O.<bucket>.C.>` and `$O.<bucket>.M.>`, `discard: "new"`,
    `allow_rollup_hdrs: true`, `allow_direct: true`; no per-subject limit, `deny_delete`, or
    `deny_purge`.
42. Metadata subject token = URL-safe base64 of the name **with padding**. Digest =
    `SHA-256=` + URL-safe padded base64 of the SHA-256.
43. Publish chunk and metadata messages to bare `$O.…` subjects; use the API prefix only for API
    requests.
44. Every metadata publish carries `Nats-Rollup: sub`.
45. Put: reject links; new NUID; look up the old entry (including deleted); publish chunks
    (default 131072 bytes) and await every PubAck; publish metadata; then purge the old NUID's
    chunks if the old entry was live. On failure purge the new NUID's chunks.
46. Get info: last message on the metadata subject; hide `deleted` entries unless asked; take the
    modification time from the message timestamp, never from stored `mtime`.
47. Get: resolve a link one level (bucket links are an error); skip the consumer for `size` 0;
    read chunks with an ordered consumer on the chunk subject until the pending token is 0; verify
    the digest; treat malformed digests and mismatches as errors.
48. Delete: republish the metadata with `deleted: true`, `size: 0`, `chunks: 0`, no digest; then
    purge the chunk subject.
49. Update metadata: copy name, description, headers, and metadata; refuse a rename onto a live
    name; publish under the new name; purge the old metadata subject on rename.
50. Links: metadata with `options.link` (`bucket`, and `name` for object links), size 0, a fresh
    NUID. Never link to a deleted object or to a link; never overwrite a live regular object.
51. Seal: stream update with `sealed: true`; irreversible.
52. Watch: ordered consumer on `$O.<bucket>.M.>`, `last_per_subject` by default, same marker
    algorithm as KV. List = watch until the marker, hiding deleted entries by default.
53. Tolerate `mtime` present or absent, `metadata: null`, and unknown keys when decoding.

**Services**

54. Validate the service name (`\A[A-Za-z0-9\-_]+\z`), the version (semver.org regex), endpoint
    names (same as the service name rule), and composed subjects and queue groups
    (`\A[^ >]*[>]?\z`).
55. Generate a NUID instance id; copy metadata at creation; metadata is immutable.
56. Subscribe, without a queue group, to `$SRV.<VERB>`, `$SRV.<VERB>.<name>`, and
    `$SRV.<VERB>.<name>.<id>` for `PING`, `INFO`, and `STATS`. Make the `$SRV` prefix
    configurable. Fail creation if any subscription fails.
57. Respond with `io.nats.micro.v1.ping_response`, `info_response`, and `stats_response` documents
    using the exact keys of section 10.4; build INFO and STATS per request.
58. Endpoint subject = `[group prefix.]` + (`subject` or `name`); empty group prefixes add
    nothing; group names may not contain `>`; endpoints may not use the `$SRV` prefix.
59. Queue group per level is unset, named, or disabled; resolve own-disabled, own-name,
    parent-disabled, parent-name, then `q`. Report the effective value in INFO and STATS (`""`
    when none).
60. Error responses carry `Nats-Service-Error` and `Nats-Service-Error-Code` (decimal integer);
    provide a standalone formatting helper; do not auto-raise on the requester side.
61. Update statistics when the handler returns: `num_requests`, `processing_time` (ns),
    truncated `average_processing_time`, and, if an error response was sent, `num_errors` and
    `last_error` = `"<code>:<description>"`.
62. `stop` drains all subscriptions, is idempotent, and fires the stopped notification once;
    `reset` zeroes the counters and resets `started`; a closed connection stops its services;
    several services can share one connection.

## 14. Unverified items

The following statements could not be confirmed in source for this chapter. They are marked where
they appear and repeated here.

1. **Reads on a non-external mirror handle in `nats.go`.** The code keeps the read prefix
   `$KV.<mirror bucket>.` while a mirror stores the origin's subjects. Whether gets and watches
   work there was not exercised. The recommendation in section 5.1 to read with the origin's
   prefix is derived from how mirrors store subjects, not from a reference implementation.
2. **When the server returns 10164 instead of 10071.** 10164 was confirmed only in the clustered
   pre-proposal path for subjects with in-flight writes; a full enumeration was not done. Clients
   must simply treat both the same.
3. **`nats.go` subscription callback concurrency** as relied on by the service framework (one
   callback at a time per subscription) was not read.
4. **Ordered-consumer recovery in `nats.go`** was read only as far as the reset step (new inbox,
   `by_start_sequence`, consumer delete); the triggering conditions are specified in
   [03-consumers.md](03-consumers.md) and summarized, not re-derived, in section 4.5.
5. **Paging of `STREAM.NAMES` and `STREAM.LIST`** for bucket listing: only the call sites and the
   response shape were read.
6. **`Nats-Marker-Reason` values `Purge` and `Remove`**: the constants exist in the server and the
   mapping is specified by ADR-48, but no code path that writes them was found; only `MaxAge`
   markers are produced by the stores.
7. **The pitfalls of section 4.6** are derived from the watcher algorithm and from `nats.go`'s
   comments ("we will block […] that is by design"); they are not quoted from an ADR.
8. **ADR-54 (KV codecs) and ADR-58 (roadmap)** are proposals outside the implemented contract and
   are not covered.
9. **In-memory store handling of wildcard last-by-subject lookups** (used by the object watcher's
   empty-store check) was confirmed for the file store only.
