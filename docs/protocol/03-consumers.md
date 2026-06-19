# 3. JetStream Consumers

This chapter describes how messages are read back out of a stream. It covers the consumer as a
server-side object (its configuration, its management API and its state), the two delivery
protocols (pull and push), the acknowledgement protocol that both share, the client-side constructs
built on top of them (ordered consumers, priority groups), the advisories a consumer emits, and a
set of byte-exact scenarios. Everything here builds on the core wire protocol, headers and
request/reply in [01-core.md](01-core.md) and on the JetStream concepts, API envelope and error
format in [02-jetstream.md](02-jetstream.md). Key-value and object stores, which are built from the
pieces described here, are in [04-kv-object-services.md](04-kv-object-services.md).

Facts are taken from nats-server at `a0385c9af` (`VERSION` `2.16.0-dev`, JetStream API level 5), the
Go reference client nats.go v1.56.0 and the ADRs. Where an ADR and the server disagree the text
says what the server does; section 11 collects those cases.

Contents:

1. [Mental model](#1-mental-model)
2. [ConsumerConfig](#2-consumerconfig)
3. [The consumer API](#3-the-consumer-api)
4. [The pull protocol](#4-the-pull-protocol)
5. [The push protocol](#5-the-push-protocol)
6. [Acknowledgements](#6-acknowledgements)
7. [Ordered consumers](#7-ordered-consumers)
8. [Priority groups](#8-priority-groups)
9. [Advisories and metrics](#9-advisories-and-metrics)
10. [Scenarios](#10-scenarios)
11. [ADR versus server discrepancies](#11-adr-versus-server-discrepancies)
12. [Client implementation checklist](#12-client-implementation-checklist)

## 1. Mental model

### 1.1 A consumer is a cursor with acknowledgement state

A stream is an ordered log. A **consumer** is a named object that lives on the server next to one
stream and remembers how far a particular reader has got. It holds:

- a **cursor**: the stream sequence of the next message to deliver;
- a **filter**: which subjects of the stream this reader wants;
- a **pending table**: messages delivered but not yet acknowledged, each with a deadline;
- a **redelivery queue**: messages whose deadline passed or that were negatively acknowledged;
- for pull consumers, a **wait queue** of outstanding pull requests.

Because this state is on the server, a client can disconnect, crash or be replaced by another
process and the reading position survives. For the same reason several clients can share one
consumer: the server hands each message to exactly one of them, which is how work is load-balanced.
The server, not the client, decides what is delivered next; a client only asks for messages (pull)
or listens for them (push) and reports what happened to each one (ack).

Consumer state is authoritative only on the consumer's **leader**. A non-replicated consumer has
one server; a replicated one has a Raft group whose leader does all delivery. Several protocol
events (`409 Leadership Change`, lost pull requests, a cleared pin) exist only because that leader
can move.

### 1.2 Two sequences and a delivery count

Every delivery is described by three numbers. A client meets all three in the acknowledgement
subject of each message (section 6.1).

| Number | Meaning | Behavior |
|---|---|---|
| stream sequence | position of the message in the stream | stable for the life of the message; shared by all consumers |
| consumer sequence | position of this delivery in the consumer's delivery history | starts at 1 and increases by exactly 1 for **every** delivery, including redeliveries |
| delivery count | how many times this message has been delivered by this consumer | 1 on first delivery, +1 per redelivery |

A redelivered message therefore keeps its stream sequence, gets a new consumer sequence and a
higher delivery count. The gap-free consumer sequence is what makes loss detectable (section 7).

### 1.3 Durable and ephemeral

The server calls a consumer **durable** exactly when its configuration has a `durable_name`. A
consumer created with only a `name` is an **ephemeral** consumer that happens to have a
client-chosen name. The difference is lifetime:

| Kind | Removed by the server when |
|---|---|
| Durable, `inactive_threshold` unset | never; only an explicit delete removes it |
| Durable, `inactive_threshold` set | inactive for that long |
| Ephemeral | inactive for `inactive_threshold`, which the server stamps to 5 seconds when unset |

"Inactive" is defined in section 3.14. Ephemeral consumers exist so that abandoned readers do not
accumulate; durable consumers exist so that a reader's position outlives every client.

### 1.4 Pull and push

A consumer is **push** exactly when its configuration has a `deliver_subject`; otherwise it is
**pull**.

- A **push** consumer publishes messages to `deliver_subject` as fast as it is allowed to,
  whenever some subscription shows interest in that subject. The receiver has no say in the pace,
  so two bolt-on mechanisms exist: flow control (the server pauses until the client answers a
  marker message) and idle heartbeats (the client can tell a quiet consumer from a dead one).
- A **pull** consumer delivers nothing until a client asks. The client publishes a request to
  `$JS.API.CONSUMER.MSG.NEXT.<stream>.<consumer>` naming a batch size, a byte budget and a
  lifetime, and the server delivers up to that much to the request's reply subject.

Pull is the default for new clients, for these reasons:

- Demand is explicit. A request is bounded in count, bytes and time, so the client can never be
  sent more than it asked for and needs no separate flow-control protocol.
- Every way a request can end has a status code (section 4.3), so the client always knows where
  it stands.
- Any number of clients can pull from one consumer without coordinating; scaling out is adding a
  process.
- Newer server features (priority groups, section 8) exist only for pull consumers, and the
  reference client's current API offers its full feature set (fetch, iterate, ordered consumers)
  only for pull.

Push remains necessary for the server's own stream mirroring and sourcing and for existing
deployments, so a client that claims completeness must implement it, but it is the secondary path.

### 1.5 Consumers and stream retention

The stream's retention policy (defined in [02-jetstream.md](02-jetstream.md)) decides what an
acknowledgement means for storage:

| Retention | Role of consumers | Consumer-side consequences |
|---|---|---|
| `limits` | none; messages leave only by limits | any number of independent consumers, any ack policy |
| `interest` | a message is kept while some consumer that wants it has not acknowledged it | acks are forwarded to the stream so it can remove messages; deleting a consumer or changing its filter triggers cleanup of messages nobody wants any more |
| `workqueue` | each message is consumed once, then removed | consumers must not overlap, must use explicit acks and must start at the beginning (section 3.13) |

On `interest` and `workqueue` streams a consumer with `ack_policy` `none` acknowledges to the
stream at delivery time. A message that exhausts `max_deliver` is **not** removed from the stream;
the server keeps tracking it.

### 1.6 Conventions used in this chapter

- **Durations** in JSON are integers counting nanoseconds (`30000000000` is 30 seconds).
  **Times** are RFC 3339 strings in UTC.
- **Errors** from the JSON API are written `code/err_code`, for example `400/10094`, meaning the
  response envelope carries `"error":{"code":400,"err_code":10094,"description":"..."}`. The
  envelope and the complete code table are in [02-jetstream.md](02-jetstream.md). Match on
  `err_code`; descriptions are quoted here exactly but are for humans.
- **Statuses** are header-only messages whose first header line is `NATS/1.0 <code> <description>`
  (format in [01-core.md](01-core.md)). They are written `409 Consumer Deleted`.
- `<stream>` and `<consumer>` in subjects are single tokens.

## 2. ConsumerConfig

`ConsumerConfig` is the JSON object sent in a create or update request and returned, with server
defaults filled in, inside every `ConsumerInfo`. The server applies defaults and then validates on
**both** create and update, before it even looks up whether the consumer exists.

The tables below group the fields by purpose. Columns: the JSON key (all are omitted from server
output when zero, except `deliver_policy`, `ack_policy`, `replay_policy` and `num_replicas`, which
are always present), the type, the server default, the rules with the exact error each violation
returns, and whether an update may change the field. "No" in the last column means the update is
rejected with `500/10012` whose description is the quoted text.

Unknown fields in a request are ignored (with a server-side warning) unless the server is
configured with JetStream `strict`, in which case they fail the request.

### 2.1 Identity and documentation

| JSON key | Type | Default | Rules and errors | Update |
|---|---|---|---|---|
| `durable_name` | string | none | Makes the consumer durable. Name rules of section 3.11. Longer than 255 bytes: `400/10102` `consumer name is too long, maximum allowed is 255`. If `name` is also set they must be equal: `400/10132` `Consumer Durable and Name have to be equal if both are provided`. Must equal the consumer token of the create subject: `400/10017` `consumer name in subject does not match durable name in request`. | identity |
| `name` | string | The server overwrites it with the consumer token of the create subject. On the legacy ephemeral create subject with no name, the server generates 8 base-62 characters. | Name rules of section 3.11. | identity |
| `description` | string | none | Longer than 4096 bytes: `400/10107` `consumer description is too long, maximum allowed is 4096`. | yes |
| `metadata` | object of string to string | The server adds `_nats.req.level` (stored). Responses additionally carry `_nats.ver` (server version) and `_nats.level` (server API level), which are never stored. | Sum of all key and value byte lengths above 131072: `400/10135` `consumer metadata exceeds maximum size of 128KB`. | yes |

> **Client rule:** Create consumers on `$JS.API.CONSUMER.CREATE.<stream>.<name>` and put the same
> value in `config.name`. Set `config.durable_name` to that same value only when a durable is
> wanted. Treat metadata keys starting with `_nats.` as server-owned: do not send them, and ignore
> them when comparing a requested configuration with a returned one.

### 2.2 Starting point and replay

| JSON key | Type | Default | Rules and errors | Update |
|---|---|---|---|---|
| `deliver_policy` | enum string | `"all"` | See the table below. On a `workqueue` stream it must be `all` (section 3.13). | no: `deliver policy can not be updated` |
| `opt_start_seq` | uint64 | 0 | Required, and only allowed, with `by_start_sequence`. | no: `start sequence can not be updated` |
| `opt_start_time` | RFC 3339 time | none | Required, and only allowed, with `by_start_time`. | no: `start time can not be updated` |
| `replay_policy` | enum string | `"instant"` | `"original"` delays each delivery by the gap between the stored timestamps of consecutive messages, until the consumer has caught up with the sequence that was the stream's last when it started. | no: `replay policy can not be updated` |

Start-position rules. Every violation returns `400/10094` with a description of the form
`consumer delivery policy is deliver <policy>, but optional start <sequence|time> is also set` or
`... but optional <thing> is not set`.

| `deliver_policy` | `opt_start_seq` | `opt_start_time` | First message delivered |
|---|---|---|---|
| `all` | must be 0 | must be absent | the stream's first sequence |
| `last` | must be 0 | must be absent | the stream's last message; with filters, the newest message among the filtered subjects |
| `last_per_subject` | must be 0 | must be absent | the last message of **each** subject matching the filter; requires `filter_subject` or `filter_subjects` (otherwise `... optional filter subject is not set`) |
| `new` | must be 0 | must be absent | the stream's last sequence + 1, that is, only messages stored after creation |
| `by_start_sequence` | required, greater than 0 | must be absent | `opt_start_seq`, raised to the stream's first sequence if that is higher |
| `by_start_time` | must be 0 | required | the first message whose stored timestamp is at or after the time |

Clamping: on an empty stream delivery starts at stream sequence 1; a start beyond the end becomes
last + 1; a start before the first sequence becomes the first sequence. The consumer sequence always
starts at 1 regardless of the starting stream sequence.

### 2.3 Filtering

| JSON key | Type | Default | Rules and errors | Update |
|---|---|---|---|---|
| `filter_subject` | string | none: all subjects of the stream | Not together with `filter_subjects`: `400/10136` `consumer cannot have both FilterSubject and FilterSubjects specified`. Not a valid subject: `500/10052`. When the create subject carries a filter it must be identical: `400/10131` `Consumer create request did not match filtered subject from create subject`. | yes |
| `filter_subjects` | array of string | none | Empty element: `400/10139` `consumer filter in FilterSubjects cannot be empty`. Overlapping elements: `400/10138` `consumer subject filters cannot overlap`. Used with a filter in the create subject: `400/10137` `consumer with multiple subject filters cannot use subject based API`. Not a valid subject: `500/10052`. Details in section 3.12. | yes |
| `headers_only` | bool | false | When true each delivery carries the message's headers plus `Nats-Msg-Size: <original payload length>` and an empty payload (section 4.2). | yes |

The server does not check that a filter is a subset of the stream's subjects; a filter that matches
nothing simply never delivers.

### 2.4 Acknowledgement behavior

| JSON key | Type | Default | Rules and errors | Update |
|---|---|---|---|---|
| `ack_policy` | enum string | `"none"` when the field is omitted (it is the zero value) | An unknown string fails JSON decoding (`400/10025`). `workqueue` streams require `explicit` (section 3.13). `flow_control` has its own constraints (section 5.5). | no: `ack policy can not be updated` |
| `ack_wait` | duration | 30 s when the policy is `explicit` or `all`. Overwritten by `backoff[0]` when `backoff` is set. | Negative: `400/10183` `consumer ack wait needs to be positive`. | yes; pending deadlines are re-evaluated within 100 ms |
| `max_ack_pending` | int | Values below -1 become -1. When 0: the stream's `consumer_limits.max_ack_pending` if set; otherwise, when the ack policy is not `none`, 1000, lowered to the server-wide or account limit if those are smaller. | Greater than 0 on a push consumer with `ack_policy` `none`: `400/10082` `consumer requires ack policy for max ack pending`. Above the server, account or stream limit: `400/10121` `consumer max ack pending exceeds system limit of <n>`. | yes |
| `sample_freq` | string | `""`: no sampling | A percentage such as `"100"` or `"25%"`; a trailing `%` is stripped and the rest parsed as an integer. Unparseable or negative: `400/10095` `failed to parse consumer sampling configuration: <err>`. Sampled explicit acks emit the ack metric of section 9. | yes |

Semantics of each policy are in section 6.5; `max_ack_pending` in section 6.9.

> **Client rule:** Always send `ack_policy`, `deliver_policy` and `replay_policy` explicitly. An
> omitted `ack_policy` means `none`, which silently turns an at-least-once consumer into an
> at-most-once one.

### 2.5 Redelivery

| JSON key | Type | Default | Rules and errors | Update |
|---|---|---|---|---|
| `max_deliver` | int | -1 (unlimited) when 0 or below -1 | With `backoff`: `len(backoff) > max_deliver` (and `max_deliver` not -1): `400/10116` `max deliver is required to be > length of backoff values`. | yes |
| `backoff` | array of duration | none | Any negative element: `400/10184` `consumer backoff needs to be positive`. The length rule above. | yes; the length rule is re-checked |

Despite the wording of 10116, the server accepts `max_deliver` equal to the backoff length; only a
smaller value is rejected. Semantics are in sections 6.6 to 6.8.

### 2.6 Pull request limits (pull only)

| JSON key | Type | Default | Rules and errors | Update |
|---|---|---|---|---|
| `max_waiting` | int | 512 on a pull consumer when 0; negative becomes 0 | Non-zero on a push consumer: `400/10080` `consumer in push mode can not set max waiting`. Caps the number of pull requests queued at once (section 4.7). | no: `max waiting can not be updated` |
| `max_batch` | int | Negative becomes 0. When 0 on a pull consumer and the server has a `max_request_batch` limit, that limit. | Above the server limit: `400/10125` `consumer max request batch exceeds server limit of <n>`. Caps `batch` of a pull request. | yes |
| `max_expires` | duration | Negative becomes 0 | Non-zero and below 1 ms: `400/10115` `consumer max request expires needs to be >= 1ms`. Caps `expires` of a pull request. | yes |
| `max_bytes` | int | Negative becomes 0 | Caps `max_bytes` of a pull request. | yes |

The three request caps are enforced per pull request with `409` statuses, not at create time
(section 4.3). `max_batch`, `max_expires` and `max_bytes` are not rejected on a push consumer; they
just have no effect there.

### 2.7 Push delivery (push only)

| JSON key | Type | Default | Rules and errors | Update |
|---|---|---|---|---|
| `deliver_subject` | string | none | Its presence makes the consumer push. Contains wildcards: `400/10079` `consumer deliver subject has wildcards`. Invalid, or in a reserved space (`$JS.API.>`, `$JS.ACK.>`, `$JS.FC.>`, `$JS.SNAPSHOT.>`, `$JS.EVENT.>`, `$JS.<domain>.API.>`, service-import and gateway reply subjects): `400/10112` `invalid push consumer deliver subject`. Overlaps a subject of the stream: `400/10081` `consumer deliver subject forms a cycle`. | The value may change, unless the **old** subject still has interest: `400/10013` `consumer name already in use`. Pull to push: `can not update pull consumer to push based`; push to pull: `can not update push consumer to pull based`. |
| `deliver_group` | string | none | Queue group the client must subscribe with (section 5.1). The server does not validate it. | accepted (no rule in the server; not confirmed by test) |
| `flow_control` | bool | false | On a pull consumer: `400/10089` `consumer flow control requires a push based consumer`. Without `idle_heartbeat`: `400/10108` `consumer with flow control also needs heartbeats`. | no: `flow control can not be updated` |
| `idle_heartbeat` | duration | Negative becomes 0 | On a pull consumer: `400/10088` `consumer idle heartbeat requires a push based consumer`. Greater than 0 and below 100 ms: `400/10083` `consumer idle heartbeat needs to be >= 100ms`. | no: `heart beats can not be updated` |
| `rate_limit_bps` | uint64, **bits** per second | 0: unlimited | On a pull consumer: `400/10086` `consumer in pull mode can not have rate limit set`. | yes |

Note that a pull consumer's heartbeat is a field of each pull request, not of the configuration.

### 2.8 Lifecycle, placement and internal fields

| JSON key | Type | Default | Rules and errors | Update |
|---|---|---|---|---|
| `inactive_threshold` | duration | Negative becomes 0. When 0: the stream's `consumer_limits.inactive_threshold` if set. If still 0 and there is no `durable_name`: 5 s. A durable with 0 is never cleaned up. | Above the stream's `consumer_limits.inactive_threshold`: `400/10153` `consumer inactive threshold exceeds system limit of <duration>`. | yes |
| `num_replicas` | int | 0, meaning inherit. Effective value: the stream's replica count, except that a non-durable consumer on a `limits` stream with 0 gets 1. | Above the stream's count: `400/10126` `consumer config replica count exceeds parent stream`. Negative: `400/10133` `replicas count cannot be negative`. On `interest` and `workqueue` streams it must be 0 or equal to the stream's: `400/10134` `consumer config replicas must match interest retention stream's replicas`. Above 1 on a non-clustered server: `500/10074` `replicas > 1 not supported in non-clustered mode`. | yes |
| `mem_storage` | bool | false | Keeps the consumer's state in memory even on a file-backed stream. | no: `storage type can not be updated` |
| `pause_until` | RFC 3339 time | none | Honored on create. On an update through the create API the server silently substitutes the stored value; use the pause API (section 3.7). | pause API only |
| `direct` | bool | false | Internal (mirrors and sources). Requires push: `400/10090`; requires non-durable: `400/10091`. | no: `direct can not be updated` |
| `sourcing` | bool | false | Internal marker for stream sourcing. | no: `sourcing can not be updated` |

A client has no reason to set `direct` or `sourcing`.

### 2.9 Priority groups (pull only)

| JSON key | Type | Default | Rules and errors | Update |
|---|---|---|---|---|
| `priority_policy` | enum string | `"none"` | An unknown string fails JSON decoding (`400/10025`). Any policy other than `none` on a push consumer: `400/10178` `priority groups can not be used with push consumers`. | no rule in the server (ADR-42 says it must not change) |
| `priority_groups` | array of string | none | Policy set but list empty: `400/10159` `Setting PriorityPolicy requires at least one PriorityGroup to be set`. Empty element: `400/10161` `Group name cannot be an empty string`. Element not matching `^[a-zA-Z0-9/_=-]{1,16}$`: `400/10162`. Set while the policy is `none`: `400/10196` `consumer can not have priority groups when policy is none`. | no rule in the server |
| `priority_timeout` | duration | Negative becomes 0. 2 minutes when the policy is `pinned_client` and the value is 0. | Greater than 0 while the policy is `none`: `400/10197` `PinnedTTL cannot be set when PriorityPolicy is none`. | yes |

The protocol is in section 8.

### 2.10 Enum strings

| Field | Exact values | Notes |
|---|---|---|
| `deliver_policy` | `"all"`, `"last"`, `"new"`, `"by_start_sequence"`, `"by_start_time"`, `"last_per_subject"` | The decoder also accepts `"undefined"` as `all`. |
| `ack_policy` | `"none"`, `"all"`, `"explicit"`, `"flow_control"` | `flow_control` needs API level 4. |
| `replay_policy` | `"instant"`, `"original"` | |
| `priority_policy` | `"none"`, `"overflow"`, `"pinned_client"`, `"prioritized"` | |
| `action` (create request) | `"create"`, `"update"`, `""` | Anything else fails decoding with `unknown consumer action`. |

### 2.11 Create-time checks not tied to one field

| Condition | Error |
|---|---|
| No `config` in the request | `400/10078` `consumer config required` |
| Stream, account or server consumer limit reached (`direct` and `sourcing` consumers are not counted) | `400/10026` `maximum consumers limit reached` |
| No account limits tier applies | `400/10120` `no JetStream default or applicable tiered limit present` |
| The cluster cannot place the consumer | `503/10023` `insufficient resources` |
| The consumer's store could not be created | `500/10104` `error creating store for consumer: <err>` |
| The stream's placement names a preferred server | `500/10052` `preferred server not permitted in placement` |

Four consumer errors exist in the server's error table but are never returned at this version:
10085 `consumer in pull mode requires a durable name`, 10093
`consumer filter subject is not a valid subset of the interest subjects`, 10092 and 10019.

### 2.12 Pedantic mode

Many rules above are silent adjustments: a negative value becomes 0, a missing value inherits a
limit. A create request with `"pedantic": true` turns each such adjustment into an error instead,
always `400/10157` with description `pedantic mode: <text>`. The texts are:
`max_deliver must be set to -1`, `max_waiting must not be negative`,
`max_ack_pending must be set to -1`, `max_batch must not be negative`,
`max_expires must not be negative`, `max_bytes must not be negative`,
`idle_heartbeat must not be negative`, `inactive_threshold must not be negative`,
`priority_timeout must not be negative`, `first backoff value has to equal batch AckWait`,
`max_ack_pending must be set if it's configured in stream limits`,
`inactive_threshold must be set if it's configured in stream limits` and
`max_request_batch must be set if it's JetStream limits are set`.

Pedantic mode exists so that a tool can guarantee the stored configuration is exactly what it sent.

### 2.13 API levels

The server records in `metadata["_nats.req.level"]` the lowest API level able to run the consumer:

| Configuration uses | Level |
|---|---|
| `pause_until` (non-zero) | 1 |
| any of `priority_policy` (not `none`), `priority_groups`, `priority_timeout` | 1 |
| `ack_policy` `flow_control` | 4 |
| nothing of the above | 0 |

A client can demand a minimum level on any consumer request, including `MSG.NEXT` and `RESET`, by
sending the header `Nats-Required-Api-Level: <integer>`. When the value is not an integer or
exceeds the server's level, JSON endpoints answer `412/10185`
`JetStream minimum api level required` and `MSG.NEXT` answers the status
`412 Required Api Level`. The reference client never sends this header; it detects unsupported
features after the fact by comparing the configuration it sent with the one echoed back.

> **Client rule:** After a create or update, compare the returned `config` with the request for
> every feature that an older server would silently drop (`filter_subjects`, `priority_*`,
> `pause_until`, `metadata`). A missing field in the echo means the server did not apply it.

## 3. The consumer API

All subjects are under `$JS.API` (or `$JS.<domain>.API`, see [02-jetstream.md](02-jetstream.md)).
Each is a request with a reply subject; each JSON response is the common envelope (`type`, optional
`error`) plus the fields listed.

### 3.1 Subjects

| Operation | Subject | Request payload | Response `type` |
|---|---|---|---|
| Create or update | `$JS.API.CONSUMER.CREATE.<stream>.<consumer>` | create request | `io.nats.jetstream.api.v1.consumer_create_response` |
| Create or update, filter in subject | `$JS.API.CONSUMER.CREATE.<stream>.<consumer>.<filter>` | create request | same |
| Legacy ephemeral create | `$JS.API.CONSUMER.CREATE.<stream>` | create request | same |
| Legacy durable create | `$JS.API.CONSUMER.DURABLE.CREATE.<stream>.<consumer>` | create request | same |
| Info | `$JS.API.CONSUMER.INFO.<stream>.<consumer>` | empty | `io.nats.jetstream.api.v1.consumer_info_response` |
| Delete | `$JS.API.CONSUMER.DELETE.<stream>.<consumer>` | empty | `io.nats.jetstream.api.v1.consumer_delete_response` |
| Names | `$JS.API.CONSUMER.NAMES.<stream>` | optional `{"offset":N}` | `io.nats.jetstream.api.v1.consumer_names_response` |
| List | `$JS.API.CONSUMER.LIST.<stream>` | optional `{"offset":N}` | `io.nats.jetstream.api.v1.consumer_list_response` |
| Pull | `$JS.API.CONSUMER.MSG.NEXT.<stream>.<consumer>` | section 4.1 | not JSON: messages and statuses |
| Pause or resume | `$JS.API.CONSUMER.PAUSE.<stream>.<consumer>` | optional `{"pause_until":"<time>"}` | `io.nats.jetstream.api.v1.consumer_pause_response` |
| Unpin | `$JS.API.CONSUMER.UNPIN.<stream>.<consumer>` | `{"group":"<name>"}` | `io.nats.jetstream.api.v1.consumer_unpin_response` |
| Reset | `$JS.API.CONSUMER.RESET.<stream>.<consumer>` | empty or `{"seq":N}` | `io.nats.jetstream.api.v1.consumer_reset_response` |
| Leader stepdown | `$JS.API.CONSUMER.LEADER.STEPDOWN.<stream>.<consumer>` | optional `{"placement":{...}}` | `io.nats.jetstream.api.v1.consumer_leader_stepdown_response` |
| Remove peer (admin) | `$JS.API.CONSUMER.PEER.REMOVE.<stream>.<consumer>` | `{"peer":"<server name or peer id>"}` | `io.nats.jetstream.api.v1.consumer_remove_peer_response` |
| Evacuate peer (admin) | `$JS.API.CONSUMER.PEER.EVACUATE.<stream>.<consumer>` | `{"peer":"..."}` | `io.nats.jetstream.api.v1.consumer_evacuate_peer_response` |

`MSG.NEXT` and `RESET` are special: they are not served by the account-wide API dispatcher. The
consumer's **leader** holds a subscription on exactly those two subjects and drops it when it loses
leadership or stops. When no leader exists, or the consumer does not exist, nobody is subscribed,
and a client that connected with `no_responders` receives the core `503` no-responders status
(derived from the subscription handling; not confirmed by a live test).

### 3.2 Create and update

Request body:

| JSON key | Type | Meaning |
|---|---|---|
| `stream_name` | string | Must equal the stream token of the subject: `400/10056` `stream name in subject does not match request`. |
| `config` | ConsumerConfig | Section 2. |
| `action` | string, always present | `"create"`, `"update"` or `""` (create or update). |
| `pedantic` | bool, optional | Section 2.12. |

The subject decides how the request is interpreted:

| Subject shape | Interpretation | Rules |
|---|---|---|
| `...CREATE.<stream>.<consumer>` | current form | `config.name` is forced to the subject token. `durable_name`, if set, must equal it (`400/10017`). Without `durable_name` the result is a named ephemeral. |
| `...CREATE.<stream>.<consumer>.<a.b.c>` | current form with filter | Everything after the consumer token, joined with `.`, must equal `config.filter_subject` (`400/10131`); `filter_subjects` must be empty (`400/10137`). |
| `...CREATE.<stream>` | legacy ephemeral | The name comes from `config.name`, or is generated. `durable_name` set: `400/10020` `consumer expected to be ephemeral but a durable name was set in request`. |
| `...DURABLE.CREATE.<stream>.<consumer>` | legacy durable | Wrong token count: `400/10016`. `durable_name` empty: `400/10018` `consumer expected to be durable but a durable name was not set`. Token differs from `durable_name`: `400/10017`. |

The filter-in-subject form exists so that an operator can write subject permissions that restrict
which filters an account may consume. It can express only a single filter.

Further checks: a missing stream gives `404/10059` `stream not found`; a `\` or `/` in the consumer
token gives `400/10127` `Consumer name can not contain path separators`.

`action` semantics:

| `action` | Consumer does not exist | Consumer exists |
|---|---|---|
| `""` | create | update, subject to the update rules of section 2 |
| `"create"` | create | If the request, after defaults are applied, equals the stored configuration: success, returning the existing consumer (the call is idempotent). Otherwise `400/10148` `consumer already exists`. |
| `"update"` | `400/10149` `consumer does not exist` | update |

`""` exists for backward compatibility and is dangerous: a typo in a name creates a new consumer,
and a request meant to create can silently reconfigure a consumer another process relies on.

An update that violates an update rule returns `500/10012` with the rule's text. On a `workqueue`
stream an update re-checks filter uniqueness (`400/10100`).

Response: the envelope plus a `ConsumerInfo` (section 3.3) flattened into the same object. The
response to a create reports the consumer's initial state.

The `Nats-Stream-Identity` request header is evaluated only for the internal `direct` and
`sourcing` consumers (mismatch: `400/10225`); ordinary clients do not send it.

```
C: PUB $JS.API.CONSUMER.CREATE.ORDERS.worker _INBOX.Hq7Rk2Vx9LmPz4WbT8cYsD.aK3x9QpL 183␍␊
C: {"stream_name":"ORDERS","config":{"name":"worker","durable_name":"worker","deliver_policy":"all","ack_policy":"explicit","replay_policy":"instant","num_replicas":0},"action":"create"}␍␊
```

Error response body:

```json
{"type":"io.nats.jetstream.api.v1.consumer_create_response","error":{"code":400,"err_code":10148,"description":"consumer already exists"}}
```

> **Client rule:** Offer three distinct operations (create, update, create-or-update) mapped to
> `"create"`, `"update"` and `""`, and always use the `CREATE.<stream>.<consumer>` subject (with
> the filter suffix only when exactly one filter is configured). Never use the legacy subjects.
> Map `10148` and `10149` to distinct, catchable errors.

### 3.3 ConsumerInfo

| JSON key | Type | Meaning |
|---|---|---|
| `stream_name` | string | Stream the consumer belongs to. |
| `name` | string | Consumer name. |
| `created` | time | Creation time. |
| `config` | ConsumerConfig | The effective configuration, defaults applied, with `_nats.ver` and `_nats.level` added to `metadata`. |
| `delivered` | SequenceInfo | `consumer_seq`: last consumer sequence delivered. `stream_seq`: the consumer's stream cursor minus one, that is, the last stream sequence delivered or skipped. `last_active`: time of the last delivery. |
| `ack_floor` | SequenceInfo | Highest consumer and stream sequence below which everything is acknowledged. `last_active`: time of the last ack. |
| `num_ack_pending` | int | Messages delivered and awaiting acknowledgement. |
| `num_redelivered` | int | Messages that currently have a redelivery count. Reported as 0 when `max_deliver` is 1. |
| `num_waiting` | int | Pull only: pull requests currently queued. |
| `num_pending` | uint64 | Messages matching the filter that have not been delivered yet. |
| `cluster` | ClusterInfo, optional | Present when the consumer has a Raft group. |
| `push_bound` | bool, optional | Push only: there is interest on the deliver subject (and group) right now. |
| `paused` | bool, optional | True while `now < pause_until`. |
| `pause_remaining` | duration, optional | Time until auto-resume, only while paused. |
| `ts` | time | When this info was produced. |
| `priority_groups` | array of PriorityGroupState, optional | State of the **first** configured group only. |

`SequenceInfo`: `consumer_seq` (uint64), `stream_seq` (uint64), `last_active` (time, optional).

`PriorityGroupState`: `group` (string), `pinned_client_id` (string, optional), `pinned_ts` (time).
Because of how Go encodes time values, an unpinned group still carries
`"pinned_ts":"0001-01-01T00:00:00Z"`; treat that zero time as absent. (This follows from the
encoder's rules, not from a line of server code.)

`ClusterInfo` (all optional): `name`, `raft_group`, `leader`, `leader_since` (time),
`system_account` (bool), `traffic_account`, `replicas` (array of PeerInfo), `desired`.
`PeerInfo`: `name`, `current` (bool), `offline` (bool, optional), `active` (duration since last
contact), `lag` (optional), `peer`, `pending` (optional).

A faithful example for a freshly created durable pull consumer (wrapped here; the server sends one
line):

```json
{"type":"io.nats.jetstream.api.v1.consumer_create_response",
 "stream_name":"ORDERS","name":"worker","created":"2026-11-22T10:00:00.123456789Z",
 "config":{"durable_name":"worker","name":"worker","deliver_policy":"all","ack_policy":"explicit",
           "ack_wait":30000000000,"max_deliver":-1,"replay_policy":"instant","max_waiting":512,
           "max_ack_pending":1000,"num_replicas":0,
           "metadata":{"_nats.level":"5","_nats.req.level":"0","_nats.ver":"2.16.0-dev"}},
 "delivered":{"consumer_seq":0,"stream_seq":0},"ack_floor":{"consumer_seq":0,"stream_seq":0},
 "num_ack_pending":0,"num_redelivered":0,"num_waiting":0,"num_pending":5,
 "ts":"2026-11-22T10:00:00.123987654Z"}
```

### 3.4 Info

The payload must be empty (or `{}`): otherwise `400/10038` `expected an empty request payload`.
Errors: `404/10059` `stream not found`, `404/10014` `consumer not found`.

In a cluster only the consumer's leader answers. Other servers stay silent, or after a 500 ms delay
answer `503/10008` `JetStream system temporarily unavailable`, `500/10119` `consumer is offline` or
`500/10195` `consumer is offline: <reason>`.

> **Client rule:** An info request can legitimately time out or return `503/10008` during a leader
> election. Treat both as retryable; treat only `10014` and `10059` as "does not exist".

### 3.5 Delete

Empty payload required (`400/10038`). Success is the envelope plus `"success": true`.
Errors: `404/10059`, `404/10014`, `500/10051` with the underlying error text. Pull requests waiting
on the consumer receive `409 Consumer Deleted` (section 4.7), and a `DELETED` advisory is published.

### 3.6 Names and list paging

Both endpoints accept an optional `{"offset":N}`; the payload is parsed only when it starts with
`{` or `[`, and a negative offset is treated as 0. Both responses add the paging fields `total`,
`offset` and `limit` to the envelope and return results sorted by name.

| Endpoint | `limit` | Body |
|---|---|---|
| `NAMES` | 1024 | `"consumers": ["a","b",...]` |
| `LIST` | 256 | `"consumers": [ConsumerInfo,...]`, plus optional `"missing": [name,...]` and `"offline": {name: reason}` |

In a cluster `LIST` asks every consumer's leader and waits up to 4 seconds; consumers that did not
answer in time are named in `missing`.

```
C: PUB $JS.API.CONSUMER.NAMES.ORDERS _INBOX.Hq7Rk2Vx9LmPz4WbT8cYsD.bM4y0RqN 12␍␊
C: {"offset":0}␍␊
S: MSG _INBOX.Hq7Rk2Vx9LmPz4WbT8cYsD.bM4y0RqN 1 124␍␊
S: {"type":"io.nats.jetstream.api.v1.consumer_names_response","total":2,"offset":0,"limit":1024,"consumers":["audit","worker"]}␍␊
```

> **Client rule:** Page by requesting with `offset` 0, then adding the number of entries received
> to `offset`, until `offset >= total` or a page comes back empty. Surface `missing` and `offline`
> from `LIST` rather than dropping them: they are consumers that exist but could not report.

### 3.7 Pause and resume

A paused consumer delivers nothing until a deadline, then resumes by itself. Pausing exists so that
an operator can stop processing without deleting the consumer or disturbing its clients.

- Request: `{"pause_until":"2026-11-22T12:00:00Z"}` pauses until that time. An empty payload, or a
  zero time, resumes.
- Response fields: `paused` (bool, true when `now < pause_until`), `pause_until` (time, always
  present; the zero time `0001-01-01T00:00:00Z` after a resume), `pause_remaining` (duration,
  optional). A deadline in the past yields `paused: false`.
- Errors: `400/10025` (bad JSON), `404/10059`, `404/10014`, `500/10104`.
- `config.pause_until` is honored when a consumer is **created**; later create-API updates ignore
  the field.
- While paused the server still accepts and queues pull requests, still sends their heartbeats and
  still expires them with `408 Request Timeout`; a `no_wait` request without `expires` still gets
  `404 No Messages`. Push idle heartbeats continue. (Derived from the delivery loop's wait state;
  not confirmed by a live test.)
- At the deadline the leader resumes delivery and publishes the pause advisory with
  `paused: false`.

```
C: PUB $JS.API.CONSUMER.PAUSE.ORDERS.worker _INBOX.Hq7Rk2Vx9LmPz4WbT8cYsD.cN5z1SrP 38␍␊
C: {"pause_until":"2026-11-22T12:00:00Z"}␍␊
S: MSG _INBOX.Hq7Rk2Vx9LmPz4WbT8cYsD.cN5z1SrP 1 142␍␊
S: {"type":"io.nats.jetstream.api.v1.consumer_pause_response","paused":true,"pause_until":"2026-11-22T12:00:00Z","pause_remaining":7199876543210}␍␊
```

> **Client rule:** A paused consumer looks, to a pulling client, exactly like an empty stream with
> working heartbeats. Do not treat a long silence with heartbeats as an error.

### 3.8 Unpin

Used with the `pinned_client` priority policy (section 8.3) to force the server to choose a new
pinned client. Request `{"group":"<name>"}`. The success response is the bare envelope. Errors:
bad JSON `400/10025`; empty group `400/10025` `invalid JSON: consumer group not specified`;
malformed group name `400/10162`; group not configured on the consumer `400/10160`
`Provided priority group does not exist for this consumer`; `404/10059`; `404/10014`. In a cluster
only the consumer leader answers.

### 3.9 Reset

Reset rewinds or advances a consumer's position without deleting it.

- Request: an empty payload resets to the acknowledgement floor (the next delivery is stream
  sequence `ack_floor.stream_seq + 1`). `{"seq":N}` with `N > 0` makes the next delivered message
  the first one at or after stream sequence N.
- An explicit `seq` is allowed only for `deliver_policy` `all` (any N), `by_start_sequence`
  (N at or above `opt_start_seq`, otherwise `below start seq`) and `by_start_time` (N at or above
  the sequence for the start time, otherwise `below start time`). Any other policy: `not allowed`.
  The error is `400/10204` `invalid reset: <text>`.
- Effect: the pending table, redelivery counts and redelivery queue are cleared; the **consumer
  sequence restarts**, so the next delivery has consumer sequence 1; the ack floor becomes
  consumer sequence 0, stream sequence N - 1.
- Response: envelope, flattened `ConsumerInfo` and `"reset_seq": N`. The info reports `delivered`
  and `ack_floor` as `{"consumer_seq":0,"stream_seq":N-1}`.
- A replicated consumer answers only once the reset is committed, and holds delivery meanwhile.

> **Client rule:** A consumer sequence can go backwards (to 1) at any time because somebody else
> reset the consumer. A regular consumer must tolerate that silently. An ordered consumer sees it
> as a gap and recovers by its normal reset procedure (section 7).

### 3.10 Leader stepdown and peer administration

`LEADER.STEPDOWN` asks the consumer's Raft leader to step down; the optional
`{"placement":{...}}` suggests the next leader. Responses carry `"success": true`. On a
non-clustered server: `503/10010` `JetStream clustering support required`. A consumer without a
Raft group answers success immediately. A Raft failure: `500/10041`. `PEER.REMOVE` and
`PEER.EVACUATE` take `{"peer":"..."}` and answer `"success": true`; their handlers were not
examined beyond the request and response shapes.

A stepdown is how an operator triggers the leadership-change behavior of section 4.7 on purpose.

### 3.11 Name rules

The server accepts a consumer name that is non-empty and contains none of: space, tab, carriage
return, line feed, form feed, `.`, `*`, `>`, `\`, `/`. Any other byte is accepted. ADR-6 is
stricter on paper (no NUL, at most 255 bytes).

| Violation | Error |
|---|---|
| `\` or `/` in the consumer token of the create subject | `400/10127` `Consumer name can not contain path separators` |
| forbidden character in `config.durable_name` | `500/10052` `consumer durable name can not contain '.', '*', '>', '\\', '/'` |
| forbidden character in `config.name` | `500/10052` `consumer name can not contain '.', '*', '>', '\\', '/'` |
| last-resort check on the final name | `400/10103` `durable name can not contain '.', '*', '>'` |
| `durable_name` longer than 255 bytes | `400/10102` `consumer name is too long, maximum allowed is 255` |

A name containing `.`, `*`, `>` or whitespace cannot even be expressed as one token of the create
subject, so with the current create subject most of these errors are unreachable: the request goes
to a different subject, or is a protocol error. The server has no length check for a non-durable
`name`.

> **Client rule:** Validate names before building a subject: non-empty, at most 255 bytes, none of
> the characters above, and no NUL. A name is interpolated into a subject, so an unvalidated name
> is a subject-injection bug, not just a server error.

### 3.12 Filter rules

- `filter_subject` and `filter_subjects` are mutually exclusive (`400/10136`).
- `filter_subjects` may not contain an empty string (`400/10139`).
- No two entries may overlap: for every pair, if one is a subset match of the other (which
  includes being identical) the request fails with `400/10138`. `["ORDERS.*", "ORDERS.new"]`
  is rejected; `["ORDERS.new", "ORDERS.paid"]` is fine.
- `filter_subjects` cannot be combined with a filter in the create subject (`400/10137`).
- Filters can be updated; the server recomputes `num_pending`, and on an `interest` stream removes
  messages that no consumer wants any more.

### 3.13 Stream-retention constraints

On a `workqueue` stream (checked for ordinary consumers, not `direct` or `sourcing` ones):

| Rule | Error |
|---|---|
| `ack_policy` must be `explicit` (or `flow_control`) | `400/10098` `workqueue stream requires explicit ack` |
| a pull consumer with `ack_policy` `none` (caught earlier) | `400/10084` `consumer in pull mode requires explicit ack policy on workqueue stream` |
| `deliver_policy` must be `all` | `400/10101` `consumer must be deliver all on workqueue stream` |
| a consumer without a filter while another consumer exists | `400/10099` `multiple non-filtered consumers not allowed on workqueue stream` |
| filters that collide with any other consumer's filters (an unfiltered consumer collides with everything) | `400/10100` `filtered consumer not unique on workqueue stream` |
| `num_replicas` neither 0 nor the stream's count | `400/10134` |

These rules follow from the definition: a work item must go to exactly one consumer and be removed
when that consumer acknowledges it, so two consumers may never want the same subject, and a
consumer may not skip items by starting late.

On an `interest` stream only the replica rule (`400/10134`) is enforced.

### 3.14 Limits and inactivity cleanup

A stream may carry `consumer_limits` with `inactive_threshold` and `max_ack_pending`. Each acts as
both the default (when the consumer leaves the field at 0) and the ceiling (`400/10153`,
`400/10121`) for consumers of that stream. Other ceilings: the server and account-tier
`max_ack_pending` (`400/10121`), the server `max_request_batch` (`400/10125`), and the consumer
count limits (`400/10026`).

Inactivity thresholds:

| Consumer | Timer |
|---|---|
| no `durable_name`, `inactive_threshold` 0 | configuration stamped to 5 s; actual timer 5 s plus 100 to 999 ms of jitter |
| any, `inactive_threshold` greater than 0 | that value (pull consumers add the same jitter) |
| durable, `inactive_threshold` 0 | never deleted |

What keeps a consumer alive:

- **Push:** interest on the deliver subject. The timer starts when interest disappears and is
  cancelled when it returns. An ack restarts a running timer.
- **Pull:** the consumer survives while any valid pull request is waiting, or while the last
  request, delivery or request expiry was less than the threshold ago. Any ack counts as activity.
  With unacknowledged messages outstanding the server additionally waits until their latest ack
  deadline plus the threshold.
- The clock restarts when a server starts or acquires leadership of the consumer.

When the timer fires the consumer is deleted and a `DELETED` advisory is published.

> **Client rule:** An ephemeral pull consumer dies a few seconds after the last pull request
> expires. A client that wants to keep one alive across idle periods must either keep a pull
> request outstanding at all times (the continuous algorithm of section 4.8 does) or set
> `inactive_threshold` explicitly. After any pause longer than the threshold, expect the consumer
> to be gone and be ready to recreate it.

## 4. The pull protocol

A pull is an asynchronous conversation, not a request with one reply. The client publishes a
**pull request** carrying a reply subject. The server queues the request on the consumer and, as
messages become available, publishes each one to that reply subject. The conversation ends when the
requested count or byte budget is used up, when the request expires, or when the server sends a
terminating status. A client may have any number of pull requests outstanding at once, on the same
or different reply subjects.

### 4.1 The request

Publish to `$JS.API.CONSUMER.MSG.NEXT.<stream>.<consumer>` **with a reply subject**. A request
without a reply subject is dropped silently.

The payload is whitespace-trimmed and then interpreted as:

| Payload | Meaning |
|---|---|
| empty | one message, no expiry: wait indefinitely |
| ASCII integer, for example `10` | legacy form: that many messages, no expiry |
| a JSON object (first byte `{`) | the fields below |
| anything else | treated as one message, no expiry |

JSON fields (all optional):

| JSON key | Type | Meaning |
|---|---|---|
| `batch` | int | Maximum number of messages. Must not exceed the consumer's `max_batch` when that is set. A JSON request with `batch` 0 or absent still delivers one message (derived from the wait-queue code); always send `batch` of at least 1. |
| `expires` | duration | Server-side lifetime of the request. 0 or absent: never expires. Must not exceed `max_expires` when that is set; a request without `expires` is not checked against `max_expires`. |
| `max_bytes` | int | Byte budget. Each message costs `len(subject) + len(ack reply subject) + len(header block) + len(payload)`. Must not exceed the consumer's `max_bytes` when that is set. |
| `no_wait` | bool | Do not wait when nothing is available (section 4.4). |
| `idle_heartbeat` | duration | Ask for `100 Idle Heartbeat` on this request's reply subject whenever the request has been idle this long. Requires `expires`: when `idle_heartbeat * 2 > expires` the request fails with `400 Bad Request - heartbeat value too large`. |
| `group` | string | Priority group. Required when the consumer has a priority policy (section 8). |
| `min_pending` | int64 | `overflow` policy only. |
| `min_ack_pending` | int64 | `overflow` policy only. |
| `id` | string | `pinned_client` policy only: the pin id last received in a `Nats-Pin-Id` header. |
| `priority` | int | `prioritized` policy only: 0 to 9, lower is served first. |

The request is decoded leniently: unknown keys are ignored. ADR-42 mentions a `failover` field; the
server has no such field and ignores it.

An optional `Nats-Required-Api-Level` header may accompany the request (section 2.13), in which case
the request is an `HPUB`.

```
C: PUB $JS.API.CONSUMER.MSG.NEXT.ORDERS.worker _INBOX.e5JvN0uXq1ZtB6yRw3GhCa 63␍␊
C: {"expires":30000000000,"batch":10,"idle_heartbeat":15000000000}␍␊
```

> **Client rule:** Always send the JSON form with `batch >= 1`. Send `expires` on every request
> except a deliberate `no_wait`-only request: a request without `expires` stays queued on the
> server until it is satisfied, the client's subscription goes away, or the consumer is deleted,
> and it cannot carry a heartbeat. Check `expires >= 2 * idle_heartbeat` before sending.

### 4.2 How messages arrive

Each message is published by the server to the pull request's reply subject, but it reaches the
client looking like the original message:

- the **subject** of the `MSG`/`HMSG` is the subject the message was stored under;
- the **reply subject** is the acknowledgement subject (`$JS.ACK...`, section 6.1);
- the subscription it arrives on is the one covering the pull's reply subject.

```
S: MSG ORDERS.new 2 $JS.ACK._.szMpdrwD.ORDERS.worker.1.34.12.1763800000000000000.5 5␍␊
S: hello␍␊
```

Consequences and variations:

- A client cannot tell from a message which pull request it belongs to, unless it gives every pull
  its own reply subject. The algorithms below work without that knowledge.
- When several pull requests are waiting the server serves them round-robin: after delivering one
  message to a request that still wants more, it moves that request to the back of the queue.
- `headers_only` consumers deliver an empty payload and append `Nats-Msg-Size: <n>` to the
  message's headers, creating a header block if the message had none:

  ```
  S: HMSG ORDERS.new 2 $JS.ACK._.szMpdrwD.ORDERS.worker.1.34.12.1763800000000000000.5 30 30␍␊
  S: NATS/1.0␍␊
  S: Nats-Msg-Size: 5␍␊
  S: ␍␊
  S: ␍␊
  ```

- `pinned_client` consumers add `Nats-Pin-Id: <id>` to **every** delivered message (section 8.3).
  The server checks the byte budget before adding this header, so a client that counts the header
  sees a message slightly larger than what the server deducted.
- **A batch that is filled produces no terminating status.** The request simply ends after its
  last message. The one exception: when the message count is exhausted while byte budget remains,
  the server sends `409 Batch Completed`. When the byte budget reaches exactly zero the request
  ends silently after that message.
- A replicated consumer with acks sends a message only after the delivery is committed by its Raft
  group; a `408` that races with such deliveries is sent after the last of them.

A message is distinguished from a status as follows: a **status** has a header block whose first
line carries a status code, and an empty payload. Everything else is a message. The reference
client's exact test is "payload non-empty, or no headers, or no status code: user message".

> **Client rule:** Classify every inbound message on a pull subscription before anything else:
> status (a status code in the first line of the header block, and no payload) or user message.
> Never hand a status to the application.

### 4.3 Every status the server can send

All statuses are header-only messages published to the pull request's reply subject, with no reply
subject of their own. "Pending headers" means the status carries
`Nats-Pending-Messages: <remaining count>` and `Nats-Pending-Bytes: <remaining bytes>`, in that
order: what the ended request had not delivered.

| Status line (exact) | Pending headers | Sent when | Ends the pull | Ends the consumer | Client action |
|---|---|---|---|---|---|
| `100 Idle Heartbeat` | no; carries `Nats-Last-Consumer` and `Nats-Last-Stream` | the request was idle for its `idle_heartbeat` | no | no | restart the heartbeat timer; nothing else |
| `400 Bad Request - <json error>` | no | the JSON did not decode | yes | no | programming error: raise |
| `400 Bad Request - heartbeat value too large` | no | `idle_heartbeat * 2 > expires` | yes | no | raise |
| `400 Bad Request - Not a Overflow Priority consumer` | no | `min_pending` or `min_ack_pending` on a consumer without the `overflow` policy | yes | no | raise |
| `400 Bad Request - Not a Pinned Client Priority consumer` | no | `id` on a consumer without the `pinned_client` policy | yes | no | raise |
| `400 Bad Request - Priority must be between 0 and 9` | no | `priority` out of range | yes | no | raise |
| `400 Bad Request - Priority Group missing` | no | consumer has a priority policy and `group` is empty | yes | no | raise |
| `400 Bad Request - Invalid Priority Group` | no | `group` is not one of the consumer's `priority_groups` | yes | no | raise |
| `404 No Messages` | no | `no_wait` and nothing to deliver (section 4.4) | yes | no | normal end of a no-wait fetch |
| `408 Request Timeout` | **yes** | `expires` elapsed | yes | no | normal end; subtract the pending headers |
| `408 Requests Pending` | no | `no_wait`, messages exist, but fewer than other waiting requests already want | yes | no | treat like `404` |
| `408 Interest Expired` | **yes** | the request came through an account import and interest in its reply is gone | yes | no | treat like `408 Request Timeout` |
| `409 Batch Completed` | **yes** (`Nats-Pending-Messages` is 0 or less) | message count exhausted with byte budget left | yes | no | normal end; subtract the pending headers |
| `409 Message Size Exceeds MaxBytes` | **yes** | the next message is larger than the request's remaining byte budget | yes | no | normal end; subtract the pending headers. If no message was received at all, the budget is smaller than one message: raise the budget or report |
| `409 Exceeded MaxRequestBatch of <n>` | no | `batch` above the consumer's `max_batch` | yes | no | configuration error: report; lower the batch |
| `409 Exceeded MaxRequestExpires of <duration>` | no | `expires` above `max_expires`; the duration is Go text such as `30s` | yes | no | report; lower `expires` |
| `409 Exceeded MaxRequestMaxBytes of <n>` | no | `max_bytes` above the consumer's `max_bytes` | yes | no | report; lower `max_bytes` |
| `409 Exceeded MaxWaiting` | no | wait queue full **and** the request has no `idle_heartbeat` | yes | no | back off, retry |
| `409 Consumer is push based` | no | `MSG.NEXT` sent to a push consumer | yes | **yes** for this handle | stop; raise |
| `409 Consumer Deleted` | no | the consumer was deleted while the request waited | yes | **yes** | stop; raise |
| `409 Leadership Change` | no | a new leader of a replicated consumer disowns requests made to the previous leader | yes | no | every outstanding pull is void: reset accounting and pull again |
| `409 Server Shutdown` | **yes** | the server is shutting down (non-replicated consumers only) | yes | no | reconnect logic takes over; pull again when connected |
| `412 Required Api Level` | no | `Nats-Required-Api-Level` not satisfied | yes | no | raise |
| `423 Nats-Pin-Id mismatch` | no | the request's `id` differs from the current pin (checked on arrival) | yes | no | forget the pin id; pull again without it |
| `423 Nats-Wrong-Pin-Id` | **yes** | a queued request's `id` is not the current pin (checked at delivery time) | yes | no | forget the pin id; pull again without it |
| `503` (no description) with `Nats-Subject: <MSG.NEXT subject>` | no | core no-responders: nothing is subscribed to the pull subject (no leader, or the consumer is gone); needs `no_responders` in `CONNECT` | yes | no, but see below | retry after a delay; an ordered consumer recreates |

Not sent: the server has no status for "max ack pending reached". When `max_ack_pending` stalls a
consumer, requests simply stay queued and keep receiving heartbeats.

Classes (ADR-37, confirmed against the server):

| Class | Statuses | Behavior |
|---|---|---|
| Informational | `100 Idle Heartbeat` | never ends anything |
| Normal end of one pull | `404`, every `408`, `409 Batch Completed`, `409 Message Size Exceeds MaxBytes` | silent |
| End of one pull, worth a warning | `409 Exceeded Max...`, `409 Leadership Change`, `409 Server Shutdown`, both `423`, `503` | report through the error callback; keep consuming |
| Caller error | every `400`, `412` | raise; stop |
| End of the consumer | `409 Consumer Deleted`, `409 Consumer is push based` | raise; stop |

ADR-37 does not list `408 Requests Pending`, `409 Batch Completed`, `409 Leadership Change`,
`409 Server Shutdown`, `412` or `423 Nats-Pin-Id mismatch`; the server sends all of them and each
ends only the one request. The reference client treats `409 Consumer is push based` as a generic
non-terminal 409; since no pull on that consumer can ever succeed, terminating is the better
behavior.

> **Client rule:** Match the code first, then the description. For 409 compare the description
> case-insensitively against the known texts (`Consumer Deleted`, `Leadership Change`,
> `Batch Completed`, `Message Size Exceeds MaxBytes`, `Server Shutdown`,
> `Consumer is push based`, prefix `Exceeded Max`); treat an unknown 409 as a non-terminal warning
> and an unknown code as a non-terminal error. On **any** terminating status, read
> `Nats-Pending-Messages` and `Nats-Pending-Bytes` if present (absent means 0); this keeps the
> client correct when a future server adds the headers to more statuses.

### 4.4 `no_wait`, with and without `expires`

`no_wait` asks "give me what you have now". On arrival the server computes what is available (the
consumer's `num_pending` plus the redelivery queue) and decides:

1. Nothing available and no `expires`: immediate `404 No Messages`.
2. Something available, but less than the sum of batches already queued by other requests:
   immediate `408 Requests Pending`.
3. Otherwise the request is queued.

A queued `no_wait` request is re-examined whenever the delivery loop runs out of messages:

- past its `expires`: `408 Request Timeout` with pending headers;
- no `expires`, or at least one message already delivered to it: `404 No Messages`;
- `expires` set and nothing delivered yet: it stays queued until `expires`, then
  `408 Request Timeout`.

| `no_wait` | `expires` | Behavior |
|---|---|---|
| false | absent | wait forever for `batch` messages |
| false | set | wait up to `expires`; then `408 Request Timeout` with what was not delivered |
| true | absent | deliver what is available now, then `404 No Messages`; if nothing is available, `404` immediately |
| true | set | wait up to `expires` for the **first** messages; once at least one was delivered and the stream is drained, `404 No Messages`; if nothing ever arrives, `408 Request Timeout` at `expires` |

The end-of-stream check does not run while the consumer is stalled on `max_ack_pending` or waiting
for a reset, so a `no_wait` request without `expires` can stay open in those states.

ADR-13 says "no wait takes precedence over expires"; the server implements the combined behavior
above.

### 4.5 Heartbeats

A pull heartbeat belongs to one request. It is the same status a push consumer sends:

```
S: HMSG _INBOX.e5JvN0uXq1ZtB6yRw3GhCa 2 77 77␍␊
S: NATS/1.0 100 Idle Heartbeat␍␊
S: Nats-Last-Consumer: 12␍␊
S: Nats-Last-Stream: 34␍␊
S: ␍␊
S: ␍␊
```

- The first heartbeat is due `idle_heartbeat` after the request arrives. Each message delivered to
  the request, and each heartbeat sent, pushes the next one out by `idle_heartbeat`.
- Heartbeats continue while the consumer is paused, while it is stalled on `max_ack_pending`, and
  while the request is held back by a priority policy. They say "the server still holds your
  request", not "there are no messages".
- Heartbeats stop when the request ends for any reason, including silently.

The purpose is to detect the cases where a request dies without a status: the leader stepped down
(its wait queue is discarded silently), the server was killed, a network path between servers was
cut, or the request was dropped because the wait queue was full.

> **Client rule:** While at least one pull with `idle_heartbeat` is outstanding, run a timer of
> `2 * idle_heartbeat`. Restart it on **every** inbound message on the pull subscription (user
> message, heartbeat or any other status). If it fires, assume every outstanding pull is lost.
> Stop the timer while the connection is down and restart it after reconnecting.

### 4.6 Pending accounting

A client that keeps pulls overlapping (section 4.8) must know how many messages the server still
owes it, without being able to attribute messages to requests. It keeps two counters,
`pending_msgs` and `pending_bytes`, with these rules (ADR-37):

1. When a pull request is published: add its `batch` to `pending_msgs` and its `max_bytes` to
   `pending_bytes`.
2. For every user message: subtract 1 from `pending_msgs` and the message's size (the formula of
   section 4.1, which the client can compute from what it received) from `pending_bytes`.
3. For every terminating status that carries pending headers: subtract `Nats-Pending-Messages` and
   `Nats-Pending-Bytes`. That is exactly what the ended request will now never deliver.
4. Never let either counter go below zero.

The pending headers exist for rule 3. Without them a client could not know how much of an expired
request was unfulfilled, and its counter would drift upward until it stopped pulling.

A terminating status **without** pending headers leaves the counters too high. The reference
client handles the important cases explicitly (`409 Leadership Change` and `423` zero both
counters, because all requests are void) and leaves the rest (`404`, `503`, `409 Exceeded...`) to
the missed-heartbeat path, which zeroes the counters and pulls again.

### 4.7 Lifecycle events for a waiting pull

| Event | What the server does | What the client sees |
|---|---|---|
| Wait queue full (`max_waiting`) | First tries to expire stale requests (at most once per second). Then rejects the new request. | `409 Exceeded MaxWaiting` if the request had no `idle_heartbeat`; **nothing** if it had one (the server relies on the client noticing missing heartbeats, to avoid a retry storm) |
| Client unsubscribes or disconnects | Removes the request the next time the queue is scanned. | nothing |
| Request arrived through an account import and interest is gone | Removes the request. | `408 Interest Expired` with pending headers |
| Gateways or leaf nodes in the path | A request younger than 2 s is assumed to still have interest. | nothing |
| Delivery to the reply subject failed | Rolls back the delivery count, credits the request, queues the message for immediate redelivery. | nothing |
| Leader steps down | The old leader discards its wait queue and drops the `MSG.NEXT` subscription. | nothing from the old leader |
| New leader elected (replicated consumer) | Notifies every request the group knew about. | `409 Leadership Change` |
| Consumer deleted | Notifies every waiting request (not when the consumer merely moves between servers). | `409 Consumer Deleted` |
| Server shutdown | Notifies waiting requests of non-replicated consumers. | `409 Server Shutdown` with pending headers |
| Consumer paused | Accepts and queues requests; delivers nothing; heartbeats and expiry continue. | heartbeats, then `408` |
| `max_ack_pending` reached | Stops delivering new messages (redeliveries still flow). | heartbeats, then `408` |

Receiving a request, delivering a message and expiring a request all count as activity for the
inactivity timer (section 3.14).

### 4.8 The recommended algorithm for continuous consumption

This is the algorithm of the reference client's `Consume` (callback) and `Messages` (iterator)
APIs. It keeps a local buffer topped up by issuing a new pull **before** the previous one is
exhausted, so that delivery never pauses for a round trip.

Parameters and defaults:

| Parameter | Default | Notes |
|---|---|---|
| `max_messages` | 500 | size of the first pull and ceiling of `pending_msgs` |
| `max_bytes` | 0 (off) | when set, pulls are driven by bytes: `batch` is 1,000,000 and `max_bytes` carries the budget |
| `expires` | 30 s | minimum 1 s |
| `idle_heartbeat` | 15 s with the default `expires` | ADR-37: half of `expires`, clamped to between 500 ms and 30 s |
| `threshold_messages` | half of `max_messages`, rounded up | refill when `pending_msgs` drops **below** it |
| `threshold_bytes` | half of `max_bytes`, rounded up | same for bytes |

State: one subscription, `pending_msgs`, `pending_bytes`, a heartbeat timer, the consumer's pin id
(section 8.3) if any.

1. **Subscribe once.** Create a fresh inbox subject and subscribe to it with an ordinary
   subscription. Every pull request of this consume session uses this same subject as its reply
   subject. One subscription for the session's lifetime means no subscribe/unsubscribe traffic per
   pull and a single ordered stream of messages and statuses.
2. **First pull.** Publish
   `{"expires":<expires>,"batch":<max_messages>,"idle_heartbeat":<hb>}` (plus `max_bytes` and the
   priority fields when used). Set `pending_msgs = max_messages`, `pending_bytes = max_bytes`.
   Start the heartbeat timer at `2 * idle_heartbeat`.
3. **For every inbound message**, restart the heartbeat timer, then classify:
   - `100`: nothing more.
   - **User message:** remember `Nats-Pin-Id` if present; hand the message to the application;
     subtract from the counters (rule 2 of section 4.6); run the refill check.
   - `408 Request Timeout`, `409 Batch Completed`, `409 Message Size Exceeds MaxBytes`: subtract
     the pending headers (rule 3); run the refill check. Report nothing.
   - `409 Leadership Change`: zero both counters; run the refill check (which pulls at once);
     report a warning.
   - `423` (either text): clear the pin id; zero both counters; refill; report a warning.
   - `409 Consumer Deleted`, any `400`: report the error and stop the session.
   - anything else (`404`, `503`, `409 Server Shutdown`, other `409`): report a warning; change
     nothing. If the pull really is dead, step 5 recovers.
4. **Refill check.** If `pending_msgs < threshold_messages` (or, in byte mode,
   `pending_bytes < threshold_bytes`), publish a new pull for the difference:
   `batch = max_messages - pending_msgs` (in byte mode: `batch` 1,000,000 and
   `max_bytes = max_bytes - pending_bytes`), then set the counters back to their maximums. The
   previous pull is still open on the server; the two overlap, and each ends by itself. The client
   never cancels a pull.
5. **Missed heartbeat.** If the heartbeat timer fires: report a warning, set the counters to the
   maximums, publish a full-size pull, restart the timer. A single missed window is not fatal.
6. **Disconnect.** When the connection goes down, stop the heartbeat timer. When it comes back
   (the subscription is replayed by the connection layer, see [01-core.md](01-core.md)), set the
   counters to the maximums, publish a full-size pull and restart the timer. Pull requests made
   before the disconnect were most likely dropped, because the server removes a request once it
   finds no interest in its reply subject. Messages sent to the dead connection stay pending on
   the consumer and are redelivered after `ack_wait`.
7. **Stop.** Unsubscribe. Messages already buffered are discarded and, with acks, will be
   redelivered after `ack_wait`. A **drain** variant stops issuing pulls, lets buffered messages
   reach the application, then unsubscribes.

With the defaults: the first pull asks for 500; after the 251st message `pending_msgs` is 249,
below 250, so the client pulls 251 and is owed 500 again.

Three request shapes follow from the mode:

| Mode | `batch` per pull | `max_bytes` per pull | Refill trigger |
|---|---|---|---|
| by messages | `max_messages - pending_msgs` | omitted | messages |
| by bytes | 1,000,000 | first pull: the budget; refills: budget minus `pending_bytes` | bytes |
| by messages with a per-request byte cap | `max_messages - pending_msgs` | always the cap | messages only; bytes are not tracked |

Notes on the reference implementation that a port should decide about deliberately:

- In callback mode it subtracts from the counters **after** the application callback returns, and
  suspends the heartbeat timer during the callback. A slow callback therefore delays the refill.
  In iterator mode it subtracts before returning the message.
- The iterator's heartbeat timer runs only while a `next` call is blocked; a missed heartbeat is
  returned from `next` as an error, and the following `next` call pulls again.
- A stop-after-N option caps the first pull to N and every refill to
  `N - delivered - pending_msgs`.
- It issues the reconnect pull and the missed-heartbeat pull without knowing whether the consumer
  still exists. If it does not, every pull yields `503` and the session reports warnings forever
  until the application stops it.

A permitted refinement, not used by the reference client: subscribe to a wildcard inbox
(`<inbox>.*`) and give each pull request its own reply subject (`<inbox>.<n>`). Messages still
arrive on one subscription in order, but because a message or status is delivered to the reply
subject of the request it belongs to, the client can account per request and knows exactly what a
status without pending headers has cancelled. The server-side behavior this relies on (delivery to
the request's own reply subject) is stated in section 4.2.

> **Client rule:** Use one subscription per consume session and overlapping pulls with a refill
> threshold. Keep the counters by the rules of section 4.6. Recover from silence with the
> `2 * idle_heartbeat` timer, from reconnects with a fresh full-size pull, and stop only on
> `409 Consumer Deleted`, `409 Consumer is push based`, a `400`/`412`, or connection close.

### 4.9 The one-shot fetch algorithm

A fetch asks for one bounded batch and returns when it is complete. It is simpler than continuous
consumption and slower when called in a loop, because nothing overlaps and each call subscribes
and unsubscribes.

Variants and their request:

| Operation | Request |
|---|---|
| fetch N messages | `{"expires":30000000000,"batch":N,"idle_heartbeat":5000000000}` |
| fetch up to B bytes | `{"expires":30000000000,"batch":1000000,"max_bytes":B,"idle_heartbeat":5000000000}` |
| fetch N, do not wait | `{"batch":N,"no_wait":true}` |
| next (one message) | fetch with N = 1 |

The heartbeat is 5 s when `expires` is 10 s or more and omitted otherwise; a caller-supplied
heartbeat must satisfy `expires >= 2 * idle_heartbeat`.

1. **Create a new inbox and a new subscription for this call**, with no client-side pending
   limits (the request itself bounds what can arrive). A per-call inbox isolates concurrent
   fetches: each one sees only its own messages and its own terminating status, so two fetches on
   one consumer cannot steal each other's `408`.
2. Publish the request with that inbox as reply subject (include the pin id if one is known).
3. Start a client-side deadline of `expires + 1 s` (1 s for a `no_wait` request without
   `expires`). It guards against a lost `408`.
4. If a heartbeat was requested, start a timer of `2 * idle_heartbeat`, restarted by every
   inbound message.
5. For each inbound message:
   - `100`: continue.
   - **User message:** remember `Nats-Pin-Id`; deliver it; count it. Finish successfully when the
     count reaches `batch`, or when `max_bytes` is set and the received size reaches it.
   - `404`, any `408`, `409 Message Size Exceeds MaxBytes`: finish successfully with what was
     received.
   - `423`: clear the pin id; finish with an error.
   - any other status (`409 Consumer Deleted`, `409 Leadership Change`, `503`, `400`, ...):
     finish with that error.
6. The deadline of step 3 finishes successfully with what was received; the heartbeat timer of
   step 4 finishes with a "no heartbeat" error.
7. On finish: stop the timers, unsubscribe, close the result.

Because the fetch finishes by count the moment the last message arrives, a trailing
`409 Batch Completed` (possible when a byte budget is set) arrives after the fetch is over and is
discarded with its subscription.

A fetch does not listen for reconnects: one that spans a disconnect ends by its heartbeat timer or
its deadline. Any number of fetches may run at once on one consumer, also alongside a continuous
session; they share nothing but the pin id.

> **Client rule:** Give every fetch its own inbox subscription and tear it down when the fetch
> ends. Return partial results on `404`/`408`; raise on everything else. Always arm a client-side
> deadline slightly longer than `expires`: the `408` is a normal message and can be lost.

## 5. The push protocol

### 5.1 Delivery, interest and binding

A push consumer publishes to its `deliver_subject`. As with pull, the client sees the stored
subject as the message subject and the ack subject as the reply.

The consumer is **active** only while there is interest in exactly `deliver_subject` with exactly
the configured `deliver_group`. The client therefore subscribes to `deliver_subject` as a plain
subscription when there is no `deliver_group`, and as a queue subscription with that group name
when there is one. `push_bound` in `ConsumerInfo` reports whether the consumer is active.

ADR-15 asks clients to refuse subscriptions that cannot work, using consumer info:

| Situation | Error |
|---|---|
| `push_bound` true, no `deliver_group`, plain subscribe | already bound ("duplicate subscription") |
| plain subscribe, consumer has a `deliver_group` | consumer was created for a queue group |
| queue subscribe, group differs from `deliver_group` | consumer was created for a different queue group |
| queue subscribe, consumer has no `deliver_group` | consumer was created without a deliver group |
| queue subscribe, consumer has `flow_control` or `idle_heartbeat` | not supported over queue groups |
| pull requested but `deliver_subject` is set, or the reverse | wrong consumer type |

Flow control and heartbeats cannot work over a queue group because each member sees only a share
of the messages and status messages go to one arbitrary member.

Without interest the delivery loop parks. An ephemeral consumer, or a durable with
`inactive_threshold`, then starts its inactivity timer and is deleted if interest does not return;
a plain durable waits indefinitely and resumes from its cursor when interest returns. A failed
delivery marks the consumer inactive and rolls back the delivery count. Changing `deliver_subject`
by update makes all pending messages due for redelivery at once.

> **Client rule:** Subscribe to the deliver subject **before** creating a push consumer (or use an
> existing consumer's subject), so that no message is published into the void. Unsubscribing from
> an ephemeral push consumer's deliver subject is, after the inactivity threshold, equivalent to
> deleting it.

### 5.2 Idle heartbeats

With `idle_heartbeat` set, the server publishes a heartbeat to `deliver_subject` whenever that
long passes without a delivery, while the consumer is active:

```
S: HMSG deliver.orders 2 77 77␍␊
S: NATS/1.0 100 Idle Heartbeat␍␊
S: Nats-Last-Consumer: 12␍␊
S: Nats-Last-Stream: 34␍␊
S: ␍␊
S: ␍␊
```

- `Nats-Last-Consumer` is the last consumer sequence delivered (0 if none).
- `Nats-Last-Stream` is the consumer's stream cursor minus one: the last stream sequence this
  consumer delivered **or skipped past**. ADR-9 describes it as the stream's newest sequence; that
  is not what the server sends.
- There is no reply subject. A heartbeat is never answered, unless it carries
  `Nats-Consumer-Stalled` (section 5.3).
- The minimum interval is 100 ms.

A heartbeat serves two purposes: liveness (silence means the consumer or the path to it is gone)
and tail-loss detection (if `Nats-Last-Consumer` is greater than the last consumer sequence the
client received, messages were lost after the last one it saw).

ADR-15 says to raise an alarm after 3 intervals without any message; the reference client uses 2
intervals everywhere and, for push, only reports the condition without resubscribing.

### 5.3 Flow control

With `flow_control` set (which requires `idle_heartbeat`), the server limits how many bytes it has
sent without hearing from the client:

- The window starts at 2 MiB (one sixteenth of the 32 MiB limit) and doubles with each answered
  flow-control request, up to the limit: a slow start.
- When the unanswered bytes exceed half the window and no request is outstanding, the server sends
  a **flow-control request** in line with the messages. When they exceed the whole window it stops
  delivering.
- The request is a header-only status **with a reply subject**:

  ```
  S: HMSG deliver.orders 2 $JS.FC._.szMpdrwD.ORDERS.push1.aB3x 36 36␍␊
  S: NATS/1.0 100 FlowControl Request␍␊
  S: ␍␊
  S: ␍␊
  ```

- The client answers by publishing an **empty message** to that reply subject:

  ```
  C: PUB $JS.FC._.szMpdrwD.ORDERS.push1.aB3x 0␍␊
  C: ␍␊
  ```

- The server accepts an answer only for the **latest** request it sent; anything else is ignored.

The answer must be sent when the client's processing **reaches** the request in its queue, not when
it is read from the socket. That is the whole point: the request is a marker in the stream, and
answering it says "I have consumed everything before this marker". Answering early defeats flow
control.

The reply subject has the form `$JS.FC.<stream>.<consumer>.<4 chars>` (old layout) or
`$JS.FC.<domain or _>.<account hash>.<stream>.<consumer>.<4 chars>` (current layout). Treat it as
opaque.

If the request or its answer is lost, the consumer would stall forever. So while a request is
outstanding, every idle heartbeat carries the request's reply subject:

```
S: HMSG deliver.orders 2 137 137␍␊
S: NATS/1.0 100 Idle Heartbeat␍␊
S: Nats-Last-Consumer: 12␍␊
S: Nats-Last-Stream: 34␍␊
S: Nats-Consumer-Stalled: $JS.FC._.szMpdrwD.ORDERS.push1.aB3x␍␊
S: ␍␊
S: ␍␊
```

The client answers by publishing the same empty message to the header's value. The reference
client does not implement this recovery path; ADR-15 requires it.

Both the heartbeat and the flow-control request use code 100. They are told apart by description
(`Idle Heartbeat` versus `FlowControl Request`); the flow-control request is also the only one with
a reply subject. Confusing them stalls the consumer permanently.

> **Client rule:** On a push subscription: (1) status `100` whose description starts with
> `FlowControl`: when processing reaches it, publish an empty payload to its reply subject;
> (2) status `100` whose description starts with `Idle`: restart the heartbeat timer, compare
> `Nats-Last-Consumer` with the last consumer sequence seen, and if `Nats-Consumer-Stalled` is
> present publish an empty payload to its value; (3) remember the last flow-control subject
> answered and do not answer the same one twice. Never deliver either status to the application.

### 5.4 Other statuses on a push subscription

The reference client recognizes `409 Consumer Deleted` (terminal) and `409 Leadership Change`
(reported, not terminal) on a push subscription, and ignores every other status. In this server
both statuses are sent only to waiting pull requests; whether another server version publishes
them to a deliver subject was not verified.

### 5.5 Acknowledging through flow control: `ack_policy` `flow_control`

This policy (API level 4) is meant for the server's own stream sourcing but is available to
clients. It behaves like `ack_policy` `all`, except that acknowledgements travel on flow-control
answers instead of `$JS.ACK` subjects, so that a high-volume reader sends one small message per
window rather than one per message.

Constraints, each with its error:

| Rule | Error |
|---|---|
| push consumer | `400/10218` `flow control ack policy requires a push based consumer` |
| `flow_control` true | `400/10219` `flow control ack policy requires flow control` |
| `idle_heartbeat` between 1 s and 1 min | `500/10052` |
| `max_ack_pending` greater than 0 | `400/10220` `flow control ack policy requires max ack pending` |
| no `ack_wait`, no `backoff` | `400/10221` `flow control ack policy requires unset ack wait` |
| `max_deliver` not greater than 0 | `400/10222` `flow control ack policy requires unset max deliver` |

Outside pedantic mode the server fills in `flow_control: true` and a 1 s heartbeat when they are
missing.

Protocol differences:

- No `$JS.ACK` subscription exists for the consumer; publishing to a message's reply subject does
  nothing.
- The flow-control answer carries headers naming how far the client has processed. When
  `Nats-Last-Stream` is greater than 0 the server acknowledges everything up to that stream
  sequence:

  ```
  C: HPUB $JS.FC._.szMpdrwD.ORDERS.push1.aB3x 58 58␍␊
  C: NATS/1.0␍␊
  C: Nats-Last-Consumer: 12␍␊
  C: Nats-Last-Stream: 34␍␊
  C: ␍␊
  C: ␍␊
  ```

- The server also sends flow-control requests when `max_ack_pending` is reached, on an idle
  heartbeat while messages are unacknowledged, and 1 s after the end of a burst when the heartbeat
  interval is longer than 1 s.

### 5.6 Rate limiting and replay

`rate_limit_bps` throttles a push consumer with a token bucket of `rate_limit_bps / 8` bytes per
second. The burst size is the stream's `max_msg_size`, else the account's maximum payload, else the
server's. Each message costs its subject, ack subject, headers and payload bytes.

`replay_policy` `original` (both consumer types) spaces deliveries by the original gaps between
stored timestamps until the consumer catches up.

## 6. Acknowledgements

Acknowledgement is the same for pull and push: the client publishes a short payload to the
**reply subject of the message**. No request to an API subject is involved, which is why acking is
cheap and why a message can be acknowledged by any connection that knows its reply subject.

### 6.1 The acknowledgement subject

The reply subject of a delivered message is generated by the server and encodes everything a
client needs to know about the delivery. Two layouts exist:

```
old, 9 tokens:
$JS.ACK.<stream>.<consumer>.<delivered>.<stream seq>.<consumer seq>.<timestamp>.<pending>

current, 11 tokens:
$JS.ACK.<domain>.<account hash>.<stream>.<consumer>.<delivered>.<stream seq>.<consumer seq>.<timestamp>.<pending>
```

| Token | Meaning |
|---|---|
| `<domain>` | The server's JetStream domain, or `_` when it has none. |
| `<account hash>` | 8 characters derived from the account name: the first 8 bytes of its SHA-256, each mapped by `byte mod 62` into `0-9A-Za-z`. The global account `$G` hashes to `szMpdrwD`. |
| `<stream>`, `<consumer>` | Names. |
| `<delivered>` | Delivery count of this message, starting at 1. |
| `<stream seq>` | Stream sequence of the message. |
| `<consumer seq>` | Consumer sequence of this delivery. |
| `<timestamp>` | The time the message was stored, in nanoseconds since the Unix epoch. |
| `<pending>` | The consumer's `num_pending` at delivery time: matching messages not yet delivered, after this one. |

Which layout is emitted is a server setting. This server emits the **11-token** layout by default;
servers before 2.16 emit the 9-token layout. The server accepts acknowledgements on both, so a
client just publishes to whatever subject it was given. The domain and account hash exist so that
acks can be routed back across domains and account boundaries without subject rewriting.

Parsing rule (the same one the server applies):

1. Split the subject on `.`.
2. Require token 0 to be `$JS` and token 1 to be `ACK`.
3. If there are exactly 9 tokens, the fields start at index 2 and there is no domain or hash.
   If there are 11 or more, domain is token 2 (`_` means none), account hash is token 3, and the
   fields start at index 4. Any other count (fewer than 9, or exactly 10) is not a JetStream
   message.
4. From the start index: stream, consumer, delivered, stream sequence, consumer sequence,
   timestamp, pending. All five numbers are unsigned decimal.
5. Ignore any tokens after `pending`. ADR-15 reserves the right to append one (it mentions a
   random 12th token); this server emits exactly 11.

Example: `$JS.ACK._.szMpdrwD.ORDERS.worker.2.34.13.1763800000000000000.5` is the second delivery of
stream sequence 34, consumer sequence 13, stored at 2025-11-22T08:26:40Z, with 5 more messages
waiting. The old-layout equivalent is `$JS.ACK.ORDERS.worker.2.34.13.1763800000000000000.5`.

The whole `$JS.` prefix is reserved for reply subjects: when a client publishes a message whose
**reply** subject starts with `$JS.`, the server rejects the publish as a reply-subject violation.
An ack subject can therefore only ever come from the server.

> **Client rule:** Parse message metadata from the reply subject with the rule above, accepting 9
> tokens and 11-or-more tokens and rejecting everything else. Publish acks to the reply subject
> exactly as received; never rebuild it. A message whose reply subject does not parse is not a
> JetStream message: ack operations on it must fail locally.

### 6.2 Acknowledgement payloads

| Payload | Matched by | Name | Effect |
|---|---|---|---|
| empty, `+ACK`, `+OK` | whole payload | ack | The message is done. `+OK` is a deprecated synonym. |
| `-NAK` | prefix | negative ack | Redeliver as soon as possible (the message goes to the tail of the redelivery queue). Emits the `MSG_NAKED` advisory. |
| `-NAK {"delay": <ns>}` | prefix, then JSON | delayed nak | Redeliver after the delay (integer nanoseconds). |
| `-NAK <duration>` | prefix, then text not starting with `{` | delayed nak | Same, with a Go duration string such as `5s` or `1m30s`. An unparseable value is logged and treated as a plain `-NAK`. |
| `+WPI` | **whole payload** | in progress | Restarts the message's ack-wait clock from now. Can be sent any number of times. |
| `+TERM` | prefix | terminate | Never redeliver; counts as an ack. Emits the `MSG_TERMINATED` advisory. |
| `+TERM <reason>` | prefix | terminate with reason | The trimmed text after `+TERM` becomes the advisory's `reason`. |
| `+NXT` | prefix | ack and pull | Acks, then treats the rest of the payload as a pull request (empty, an integer, or the JSON of section 4.1) whose reply subject is the reply subject of this publish. |
| anything else | | | No state change. |

Headers on an ack message are ignored. A `-NAK` or `+WPI` for a message that is not pending (already
acked, or its consumer sequence is at or below the ack floor) is ignored. Because `+WPI` is matched
exactly, `+WPI ` with trailing text does nothing.

The reference client sends a delayed nak as `-NAK {"delay": 5000000000}` (with a space after the
colon) and a plain `-NAK` when the delay is zero or negative. It does not expose `+NXT`.

ADR-15 defines the client-side contract:

- `+ACK`, `-NAK` and `+TERM` are **terminal**: after one has been sent for a message, further ack
  calls on that message object are refused locally. Mark the message only after the publish
  succeeded, so that a failed ack can be retried.
- `+WPI` is not terminal.
- With `ack_policy` `none` the client should not send acks at all. (The reference client does not
  check the policy; an ack to such a consumer goes nowhere.)

> **Client rule:** Provide `ack`, `nak`, `nak(delay)`, `in_progress`, `term` and `term(reason)`,
> each publishing the exact payload above to the message's reply subject. Track a per-message
> "terminally acked" flag set after a successful publish; reject a second terminal ack locally.
> Encode a nak delay as integer nanoseconds in the JSON form.

### 6.3 Ack-ack: confirmed acknowledgement

A plain ack is a fire-and-forget publish. It can be lost, in which case the message is redelivered
after `ack_wait`. Where that matters (for example on a `workqueue` stream, where the ack is what
removes the message), the client can ask for confirmation: publish the ack **as a request**, with a
reply subject, and the server answers with an **empty message** once the ack is processed. This is
called a double ack or synchronous ack.

| Case | Is a reply sent |
|---|---|
| `+ACK` or `+TERM`, non-replicated consumer | yes, after the state is stored |
| `+ACK` or `+TERM`, replicated consumer | yes, after the ack is committed by the Raft group |
| `+ACK` for a message already acked | yes (derived) |
| `+ACK` at or below the floor with `ack_policy` `all` | yes |
| `+ACK` for a stream sequence the current leader has not delivered yet | **no**: the ack is ignored and the request times out |
| consumer or stream closed | no |
| `-NAK`, `+WPI` | yes, immediately, even when ignored |
| unrecognized payload | yes (derived: the reply is sent regardless of the payload match) |
| `+NXT` | never: the reply subject is the pull's inbox |
| ack subject that does not parse | no |
| `ack_policy` `none` or `flow_control` | no ack subscription exists, so with `no_responders` the client gets a `503` (derived) |

The reply proves that the server received the ack, not that the ack changed anything.

> **Client rule:** Implement double ack as a request to the ack subject with payload `+ACK` and a
> timeout (the reference client's default is 5 s); success is any reply, and its content is
> ignored. On timeout, do not mark the message acked: the caller may retry.

### 6.4 What the client sees per delivery

For the application, each message exposes, parsed from the reply subject: stream, consumer, domain,
stream sequence, consumer sequence, delivery count, timestamp and pending count. A delivery count
above 1 means "this is a redelivery"; there is no separate header for it.

### 6.5 Acknowledgement policies

| Policy | What the server tracks | What an ack does |
|---|---|---|
| `none` | Nothing. The ack floor advances at delivery. | The server does not listen for acks. |
| `explicit` | Every delivery, in the pending table. | Removes exactly that message. The floor advances only when the acked message is the lowest outstanding one; it then jumps to just below the next outstanding message, or to the last delivered one if none is outstanding. |
| `all` | Every delivery. | Acking stream sequence N acknowledges **every** pending message at or below N and sets the floor to N. Messages skipped by a filter are covered because the floor is a stream sequence. |
| `flow_control` | Like `all`. | Acks arrive on flow-control answers (section 5.5). |

With `explicit`, the floor is computed from the consumer sequence of the message's **original**
pending entry, even when the ack names a later redelivery of it.

With `ack_policy` `all` a client that processes in order can ack every tenth message and still be
fully acknowledged. The cost is that a `-NAK` or a lost message cannot be expressed precisely:
acking a later message acks the earlier one too.

### 6.6 Ack wait and redelivery

Every pending entry has a timestamp. A timer scans the table; an entry older than its deadline is
expired:

- Expired messages are sorted by stream sequence and appended to the redelivery queue.
- **Redeliveries are served before new messages and are not held back by `max_ack_pending`.**
- A redelivery keeps its stream sequence, gets the next consumer sequence, and its delivery count
  goes up by one.
- If the message was removed from the stream in the meantime, the server terminates it internally
  and emits `MSG_TERMINATED` with reason `Unacknowledged message was deleted` or
  `Message deleted by stream limits`.
- On a pull consumer a redelivery still needs a waiting pull request to be delivered to.

The deadline is `ack_wait` (default 30 s) unless `backoff` is configured.

### 6.7 Backoff

`backoff` is a list of waits for successive redeliveries. When set, `ack_wait` is overwritten with
`backoff[0]`. With n the number of redeliveries the message has already had (0 after the first
delivery), the wait before the next redelivery is `backoff[min(n, len - 1)]`: the list is walked
once and its last element repeats.

Interactions:

- A plain `-NAK` ignores backoff and redelivers immediately.
- A delayed `-NAK` sets the entry's timestamp to `now - ack_wait + delay`. Without backoff the
  message becomes due exactly `delay` from now. With backoff the deadline in force is
  `backoff[n]`, not `ack_wait`, so the effective wait is `delay + backoff[n] - backoff[0]`
  (derived from the code, not measured).
- `+WPI` restarts the current step's wait from now.

### 6.8 `max_deliver`

`max_deliver` bounds the delivery count. -1 (or 0, which the server stores as -1) is unlimited.

- At ack-wait expiry, if the message has already been delivered `max_deliver` times it is **not**
  queued again. The server emits the `MAX_DELIVERIES` advisory (its `deliveries` field is that
  count), removes the entry from the pending table, lets the ack floor move past it, and wakes a
  consumer stalled on `max_ack_pending`.
- On the `-NAK` path the check happens when the redelivery is about to be served, with the same
  result.
- **The message stays in the stream.** Nothing delivers it to this consumer again. On `interest`
  and `workqueue` streams the server keeps the message because it is still tracked.

There is no dead-letter queue in the protocol. The `MAX_DELIVERIES` advisory, which carries the
stream sequence, is the hook: an application subscribes to it and fetches the message by sequence
(see [02-jetstream.md](02-jetstream.md)) to park it elsewhere.

### 6.9 `max_ack_pending`

While the number of pending (delivered, unacknowledged) messages is at or above `max_ack_pending`,
the server delivers no **new** messages. Delivery resumes on an ack, a term, a max-deliver
exhaustion or a configuration update. No status tells the client; a pull just sees heartbeats and
eventually `408`. The limit applies only when the ack policy is not `none`; the default is 1000.

This is the consumer-wide flow-control knob shared by all clients of a consumer. A consumer whose
clients have stopped acking looks exactly like an empty stream, so when diagnosing "no messages",
compare `num_ack_pending` with `max_ack_pending` in the consumer info.

> **Client rule:** Nothing in the client's local buffer should outlive `ack_wait`. A client that
> pulls 500 messages into a buffer and processes one per second with a 30 s `ack_wait` will see
> most of them redelivered while they are still queued locally. Keep the buffer (`max_messages`)
> proportionate to the processing rate, or send `+WPI`, and treat a delivery count above 1 as
> normal.

## 7. Ordered consumers

### 7.1 The contract

An ordered consumer gives the application every message of a stream (or of a filter) **in stream
order, exactly once, with no gaps**, for as long as the application keeps reading, without the
application acknowledging anything. It is the tool for replaying a stream, building a cache or
watching keys ([04-kv-object-services.md](04-kv-object-services.md) relies on it).

The server has no "ordered" flag. An ordered consumer is a client-side construct: a cheap,
throw-away server consumer plus a client that checks every delivery and, whenever anything is
wrong, discards that server consumer and creates a new one starting right after the last message
it handed to the application. It rests on three server guarantees:

- the consumer sequence starts at 1 and increases by exactly 1 per delivery;
- every message's ack subject carries its stream sequence and consumer sequence;
- idle heartbeats carry the last consumer and stream sequence.

Because recovery is "recreate from stream sequence X", the consumer needs no durable state and no
acknowledgements, which is what makes it cheap: memory storage, one replica, `ack_policy` `none`.

### 7.2 Configuration

ADR-17 specifies the ordered consumer as a **push** consumer:

| Field | Value |
|---|---|
| `durable_name` | not set |
| `deliver_subject` | an inbox chosen by the client |
| `ack_policy` | `none` |
| `max_deliver` | 1 |
| `flow_control` | true |
| `idle_heartbeat` | 5 s by default |
| `mem_storage` | true |
| `num_replicas` | 1 |
| `ack_wait` | "something large like 22 hours" |

and forbids combining it with pull, a durable name, binding to an existing consumer, or a deliver
group.

The reference client's current API implements it on a **pull** consumer instead (ADR-17 and ADR-37
do not describe this variant; it is documented here from the client source):

| Field | Value |
|---|---|
| `name` | `<prefix>_<serial>`; no `durable_name` |
| `deliver_policy` | first consumer: the user's choice. Every later one: `by_start_sequence` |
| `opt_start_seq` | first consumer: the user's value for `by_start_sequence`, else absent. Later: last delivered stream sequence + 1 |
| `opt_start_time` | first consumer only, for `by_start_time` |
| `ack_policy` | `none` |
| `inactive_threshold` | the user's value, else 5 minutes |
| `num_replicas` | 1 |
| `mem_storage` | true |
| `headers_only`, `metadata` | the user's values |
| `filter_subject` / `filter_subjects` | exactly one filter: `filter_subject` (and the filter-in-subject create form). Otherwise `filter_subjects`. `last_per_subject` with no filter: `[">"]` |
| `replay_policy` | not set (`instant`) |

`<prefix>` is a user-supplied name prefix or a fresh NUID; `<serial>` starts at 1 and increases by
one for each recreated consumer, so the names are `Xn4Tq8Lw2Zc6Vb0Jm5Rk9P_1`,
`Xn4Tq8Lw2Zc6Vb0Jm5Rk9P_2`, and so on. The consumer is created with action `""`.

Messages are then read with the ordinary continuous pull algorithm of section 4.8 (heartbeat 5 s
when `expires` is at least 10 s).

### 7.3 Detecting that something is wrong

The client keeps a cursor: `stream_seq` (stream sequence of the last message given to the
application) and `consumer_seq` (consumer sequence of the last message received from the current
server consumer; 0 for a new one).

| Signal | Meaning |
|---|---|
| a message whose consumer sequence is not `consumer_seq + 1` | a message was lost in transit, or the server consumer was reset or replaced |
| a heartbeat whose `Nats-Last-Consumer` is not `consumer_seq` | the tail was lost: the server delivered messages the client never saw (ADR-15; the reference pull implementation does not check this) |
| no message or heartbeat for `2 * idle_heartbeat` | the consumer, its leader or the path is gone |
| `409 Consumer Deleted` | the consumer was removed (inactivity, operator) |
| `503` on a pull | nobody serves the consumer: it is gone (inactivity, a restarted server with memory storage) |
| reconnect | pulls were lost; with a 5-minute inactivity threshold the consumer may also be gone |

A third-party `CONSUMER.RESET` (section 3.9) restarts the consumer sequence at 1, which appears as
a sequence mismatch and is handled like any other.

A message from a **previous** server consumer can still arrive after a reset. The reference client
recognizes it by the consumer name in its ack subject (the serial after the last `_`) and drops it.

`409 Leadership Change` and `423` do **not** trigger a reset; the pull algorithm handles them.

### 7.4 The reset algorithm

1. Stop the current pull subscription (unsubscribe).
2. Delete the old server consumer in the background, ignoring errors (it would also expire by
   itself).
3. Set `consumer_seq = 0`.
4. Build the next configuration: next serial, `deliver_policy` `by_start_sequence`,
   `opt_start_seq = stream_seq + 1`. If no message was ever delivered (`stream_seq` is 0), reuse
   the original start configuration instead.
5. Create it. On failure retry with backoff: first retry after 1 s, then doubling, capped at 10 s
   (1, 2, 4, 8, 10, 10, ... seconds), for a configurable number of attempts (default unlimited),
   aborting as soon as the application stops the consumer. Each attempt has a 10 s timeout.
6. Start a new pull session on the new consumer. Its first message must have consumer sequence 1.

The mismatching message itself is dropped, not delivered: it will come again from the new
consumer, in order.

Caveat: step 4's fallback means that a reset before the first message re-evaluates `new` or `last`
at recreation time, so with `deliver_policy` `new` a message published between the two creations
is skipped.

### 7.5 Restrictions in the reference client

- An ordered consumer object is used either for continuous consumption or for fetches, never both,
  and for one continuous session at a time.
- A fetch on an ordered consumer resets (recreates) the server consumer **on every call**, starting
  after the last message the previous fetch received; it does no gap checking inside a fetch.
  It is correct but slow.
- Info on an ordered consumer reports the current server consumer, whose name changes over time.
- There is no ordered push consumer in the reference client's current API.

> **Client rule:** Implement ordered consumption as: ephemeral pull consumer, `ack_policy` `none`,
> `mem_storage` true, `num_replicas` 1, explicit `inactive_threshold`; verify
> `consumer seq == last + 1` on every message; on mismatch, missed heartbeats, `409 Consumer
> Deleted`, `503` or reconnect, run the reset of section 7.4. Never deliver a message whose
> consumer sequence did not match, and never deliver a message from a superseded consumer.
> Applications must not ack ordered-consumer messages.

## 8. Priority groups

Priority groups (ADR-42, API level 1) let the clients of one **pull** consumer be treated
unequally: some only take work when there is a backlog, one is designated the sole receiver, or
some are preferred over others. The consumer is configured with `priority_policy` and
`priority_groups` (section 2.9); every pull request then names its `group`.

ADR-42 says the first implementation should allow only one group per consumer. This server does
not reject several names, but it keeps a single pin for the whole consumer and reports only the
first group's state, so a client should configure exactly one.

Rules common to all policies, checked when a pull request arrives:

- `group` is required: `400 Bad Request - Priority Group missing`.
- `group` must be configured: `400 Bad Request - Invalid Priority Group`.
- The legacy empty and integer request payloads carry no group and bypass these checks; do not use
  them with a priority consumer.

### 8.1 `overflow`

A request may carry `min_pending` and/or `min_ack_pending`. It is served only when a threshold it
set is met: `min_pending <= num_pending` **or** `min_ack_pending <= num_ack_pending`. A request
with neither is served normally. A request whose thresholds are not met is skipped, stays queued,
and keeps receiving heartbeats.

Use: primary workers pull without thresholds; overflow workers (for example in another region) pull
with `min_pending: 1000` and receive nothing until the backlog exceeds 1000.

Using either field on a consumer with a different policy:
`400 Bad Request - Not a Overflow Priority consumer`.

### 8.2 `prioritized`

A request carries `priority` from 0 to 9 (default 0). The wait queue is kept sorted by priority,
lowest first, first-in first-out within a level; a served request that still wants messages goes
to the back of its own level. A higher number is served only when no lower-numbered request is
waiting. Out of range: `400 Bad Request - Priority must be between 0 and 9`.

### 8.3 `pinned_client`

Exactly one client at a time receives messages; the others wait as standbys. This gives ordered
processing with automatic failover.

- **Pinning.** When no client is pinned, the first waiting request that carries **no** `id`
  becomes pinned. The server generates a pin id (a NUID), records the time, starts a timer of
  `priority_timeout` (default 2 minutes), and emits the `PINNED` advisory.
- **Learning the id.** Every message delivered by the consumer carries `Nats-Pin-Id: <id>`. The
  client stores it and sends it as `"id"` in every later pull request for the consumer.
- **While pinned.** A request whose `id` matches is served, and its arrival restarts the pin
  timer. A request with no `id` is kept queued as a standby (it gets heartbeats and eventually a
  `408`). A request with a different `id` is rejected.
- **Losing the pin.** (a) `priority_timeout` passes without a new pull from the pinned client:
  the pin is dropped and `UNPINNED` is emitted with reason `timeout`. (b) The unpin API
  (section 3.8): reason `admin`. (c) A leader change clears the pin silently.
- **After the pin is gone**, the old pinned client's requests still carry the old id. They are
  rejected, and the first request without an id becomes the new pin.

The two `423` statuses:

| Status | Checked | Condition | Pending headers |
|---|---|---|---|
| `423 Nats-Pin-Id mismatch` | when the request arrives | a pin exists and the request's `id` is non-empty and different | no |
| `423 Nats-Wrong-Pin-Id` | when the server is about to deliver | the queued request carries an `id` that is not the current pin, or there is no pin and it carries any `id` | yes |

On either, the client forgets its pin id and continues pulling without one; it is now a standby
and may or may not be chosen as the next pin.

Other rules:

- `id` on a consumer with a different policy:
  `400 Bad Request - Not a Pinned Client Priority consumer`.
- If the newly pinned request cannot take the next message because of its `max_bytes`, the pin is
  released again.
- The pinned client must pull again within `priority_timeout`, so `expires` must be comfortably
  shorter (ADR-42 suggests 1 minute with the 2-minute default).
- State is visible in consumer info: `priority_groups: [{"group","pinned_client_id","pinned_ts"}]`.

> **Client rule:** Store one pin id per consumer handle. Set it from `Nats-Pin-Id` on every
> delivered message; send it as `id` in every pull (first, refill, reconnect, heartbeat recovery,
> fetch). On any `423`: clear it, zero the pending counters, pull again without `id`, and report a
> non-fatal warning (a fetch ends with an error instead). Validate before pulling that a group is
> given when the consumer has `priority_groups` and that none is given when it has not.

## 9. Advisories and metrics

The server publishes events about a consumer into the consumer's account. An event is published
only if something is subscribed to its subject. Every event has `type`, `id` (a NUID) and
`timestamp` (RFC 3339, UTC), followed by the fields below. `domain` is present when the server has
a JetStream domain.

| Event | Subject | `type` | Fields | Published when |
|---|---|---|---|---|
| Created | `$JS.EVENT.ADVISORY.CONSUMER.CREATED.<stream>.<consumer>` | `io.nats.jetstream.advisory.v1.consumer_action` | `stream`, `consumer`, `action` (`"create"`), `domain` | a consumer is created |
| Deleted | `$JS.EVENT.ADVISORY.CONSUMER.DELETED.<stream>.<consumer>` | same | `stream`, `consumer`, `action` (`"delete"`), `domain` | a consumer is deleted, including by inactivity |
| Pause | `$JS.EVENT.ADVISORY.CONSUMER.PAUSE.<stream>.<consumer>` | `io.nats.jetstream.advisory.v1.consumer_pause` | `stream`, `consumer`, `paused` (bool), `pause_until` (time), `domain` | pause state changes, the deadline passes, or a consumer is created paused |
| Max deliveries | `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.<stream>.<consumer>` | `io.nats.jetstream.advisory.v1.max_deliver` | `stream`, `consumer`, `stream_seq`, `deliveries`, `domain` (no `consumer_seq`) | a message exhausts `max_deliver` (section 6.8) |
| Nak | `$JS.EVENT.ADVISORY.CONSUMER.MSG_NAKED.<stream>.<consumer>` | `io.nats.jetstream.advisory.v1.nak` | `stream`, `consumer`, `consumer_seq`, `stream_seq`, `deliveries`, `domain` | a valid `-NAK` is received |
| Terminated | `$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.<stream>.<consumer>` | `io.nats.jetstream.advisory.v1.terminated` | `stream`, `consumer`, `consumer_seq`, `stream_seq`, `deliveries`, `reason` (optional), `domain` | `+TERM`, or the server terminates an unacked message that was removed from the stream |
| Ack sample | `$JS.EVENT.METRIC.CONSUMER.ACK.<stream>.<consumer>` | `io.nats.jetstream.metric.v1.consumer_ack` | `stream`, `consumer`, `consumer_seq`, `stream_seq`, `ack_time` (ns between delivery and ack), `deliveries`, `domain` | a sampled explicit ack (`sample_freq`) |
| Leader elected | `$JS.EVENT.ADVISORY.CONSUMER.LEADER_ELECTED.<stream>.<consumer>` | `io.nats.jetstream.advisory.v1.consumer_leader_elected` | `account` (optional), `stream`, `consumer`, `leader`, `replicas` (array of PeerInfo), `domain` | a replicated consumer elects a leader; also published to the system account |
| Quorum lost | `$JS.EVENT.ADVISORY.CONSUMER.QUORUM_LOST.<stream>.<consumer>` | `io.nats.jetstream.advisory.v1.consumer_quorum_lost` | `account` (optional), `stream`, `consumer`, `replicas`, `domain` | a replicated consumer loses quorum; at most once per 10 s |
| Pinned | `$JS.EVENT.ADVISORY.CONSUMER.PINNED.<stream>.<consumer>` | `io.nats.jetstream.advisory.v1.consumer_group_pinned` | `account` (optional), `stream`, `consumer`, `domain`, `group`, `pinned_id` | a new pin is assigned |
| Unpinned | `$JS.EVENT.ADVISORY.CONSUMER.UNPINNED.<stream>.<consumer>` | `io.nats.jetstream.advisory.v1.consumer_group_unpinned` | `account` (optional), `stream`, `consumer`, `domain`, `group`, `reason` (`"admin"` or `"timeout"`) | unpin API or pin timeout |
| API audit | `$JS.EVENT.ADVISORY.API` | `io.nats.jetstream.advisory.v1.api_audit` | `server`, `client`, `subject`, `request`, `response`, `domain` | every JSON API call, including the consumer ones |

The `action` field's possible values are `"create"`, `"delete"` and `"modify"`. Server-side
termination reasons are `Unacknowledged message was deleted` and
`Message deleted by stream limits`. The pause advisory's `pause_until` is always present; after a
resume it is the zero time (a consequence of how Go encodes time values).

A client library needs none of these to function. They are how an application builds dead-letter
handling (`MAX_DELIVERIES`, `MSG_TERMINATED`), monitoring (`MSG_NAKED`, the ack metric) and
failover awareness (`PINNED`, `UNPINNED`, `LEADER_ELECTED`).

## 10. Scenarios

Conventions: `C:` is sent by the client, `S:` by the server, `␍␊` is CRLF, and lines starting
with `--` are commentary, not bytes. The connection was opened with `headers` and `no_responders`
enabled. The account is the global account (hash `szMpdrwD`) and the server has no JetStream
domain, so ack subjects start with `$JS.ACK._.szMpdrwD.`. Stream `ORDERS` captures `ORDERS.>`.
Subscription 1 is the client's shared reply inbox, `_INBOX.Hq7Rk2Vx9LmPz4WbT8cYsD.*`. Subjects,
sequence numbers, ids and timestamps are illustrative; every byte count is exact for the bytes
shown. For header messages the first count is the header block including its blank line, the
second is header block plus payload.

### 10.1 Create a durable pull consumer

```
C: SUB _INBOX.Hq7Rk2Vx9LmPz4WbT8cYsD.* 1␍␊
C: PUB $JS.API.CONSUMER.CREATE.ORDERS.worker _INBOX.Hq7Rk2Vx9LmPz4WbT8cYsD.aK3x9QpL 183␍␊
C: {"stream_name":"ORDERS","config":{"name":"worker","durable_name":"worker","deliver_policy":"all","ack_policy":"explicit","replay_policy":"instant","num_replicas":0},"action":"create"}␍␊
S: MSG _INBOX.Hq7Rk2Vx9LmPz4WbT8cYsD.aK3x9QpL 1 644␍␊
S: {"type":"io.nats.jetstream.api.v1.consumer_create_response","stream_name":"ORDERS","name":"worker","created":"2026-11-22T10:00:00.123456789Z","config":{"durable_name":"worker","name":"worker","deliver_policy":"all","ack_policy":"explicit","ack_wait":30000000000,"max_deliver":-1,"replay_policy":"instant","max_waiting":512,"max_ack_pending":1000,"num_replicas":0,"metadata":{"_nats.level":"5","_nats.req.level":"0","_nats.ver":"2.16.0-dev"}},"delivered":{"consumer_seq":0,"stream_seq":0},"ack_floor":{"consumer_seq":0,"stream_seq":0},"num_ack_pending":0,"num_redelivered":0,"num_waiting":0,"num_pending":5,"ts":"2026-11-22T10:00:00.123987654Z"}␍␊
```

The response echoes the configuration with the server's defaults: `ack_wait` 30 s, `max_deliver`
-1, `max_waiting` 512, `max_ack_pending` 1000. `num_pending` 5 says five stored messages match.
Sending the identical request again succeeds with the same answer; sending it with, say,
`"ack_wait":60000000000` fails with `400/10148` because the action is `"create"`.

### 10.2 Fetch a batch that completes

The stream holds five messages. The client fetches three on a per-call inbox.

```
C: SUB _INBOX.e5JvN0uXq1ZtB6yRw3GhCa 2␍␊
C: PUB $JS.API.CONSUMER.MSG.NEXT.ORDERS.worker _INBOX.e5JvN0uXq1ZtB6yRw3GhCa 61␍␊
C: {"expires":30000000000,"batch":3,"idle_heartbeat":5000000000}␍␊
S: MSG ORDERS.new 2 $JS.ACK._.szMpdrwD.ORDERS.worker.1.1.1.1763800001000000000.4 7␍␊
S: order-1␍␊
S: MSG ORDERS.new 2 $JS.ACK._.szMpdrwD.ORDERS.worker.1.2.2.1763800002000000000.3 7␍␊
S: order-2␍␊
S: MSG ORDERS.new 2 $JS.ACK._.szMpdrwD.ORDERS.worker.1.3.3.1763800003000000000.2 7␍␊
S: order-3␍␊
C: UNSUB 2␍␊
C: PUB $JS.ACK._.szMpdrwD.ORDERS.worker.1.1.1.1763800001000000000.4 4␍␊
C: +ACK␍␊
C: PUB $JS.ACK._.szMpdrwD.ORDERS.worker.1.2.2.1763800002000000000.3 4␍␊
C: +ACK␍␊
C: PUB $JS.ACK._.szMpdrwD.ORDERS.worker.1.3.3.1763800003000000000.2 4␍␊
C: +ACK␍␊
```

No status follows the third message: a filled batch ends silently. The client knows the fetch is
complete because it counted three messages, and unsubscribes. Each message's subject is its stored
subject, not the inbox. The last token of the ack subject counts down the messages still waiting
(4, 3, 2).

### 10.3 Fetch with `no_wait` on a drained stream

Everything has been delivered. The client asks for whatever is available.

```
C: SUB _INBOX.Jw2Lp6Xc0Vn4Bz8Qm1Tk5R 3␍␊
C: PUB $JS.API.CONSUMER.MSG.NEXT.ORDERS.worker _INBOX.Jw2Lp6Xc0Vn4Bz8Qm1Tk5R 27␍␊
C: {"batch":10,"no_wait":true}␍␊
S: HMSG _INBOX.Jw2Lp6Xc0Vn4Bz8Qm1Tk5R 3 28 28␍␊
S: NATS/1.0 404 No Messages␍␊
S: ␍␊
S: ␍␊
C: UNSUB 3␍␊
```

The status arrives with the inbox as its subject, no reply subject, a 28-byte header block and no
payload. The fetch returns an empty batch without error. Had two messages been available, the
server would have sent them and then the same `404`.

### 10.4 A pull that times out with pending headers

Two messages are available; the client asks for ten and waits up to five seconds. With `expires`
below ten seconds the fetch requests no heartbeat.

```
C: SUB _INBOX.Rb7Yd3Fh9Ks1Mv5Pq0Uw4Z 4␍␊
C: PUB $JS.API.CONSUMER.MSG.NEXT.ORDERS.worker _INBOX.Rb7Yd3Fh9Ks1Mv5Pq0Uw4Z 33␍␊
C: {"expires":5000000000,"batch":10}␍␊
S: MSG ORDERS.new 4 $JS.ACK._.szMpdrwD.ORDERS.worker.1.4.4.1763800004000000000.1 7␍␊
S: order-4␍␊
S: MSG ORDERS.new 4 $JS.ACK._.szMpdrwD.ORDERS.worker.1.5.5.1763800005000000000.0 7␍␊
S: order-5␍␊
-- five seconds after the request
S: HMSG _INBOX.Rb7Yd3Fh9Ks1Mv5Pq0Uw4Z 4 81 81␍␊
S: NATS/1.0 408 Request Timeout␍␊
S: Nats-Pending-Messages: 8␍␊
S: Nats-Pending-Bytes: 0␍␊
S: ␍␊
S: ␍␊
C: UNSUB 4␍␊
```

`Nats-Pending-Messages: 8` is the part of the batch the server did not deliver.
`Nats-Pending-Bytes: 0` because the request had no byte budget. The fetch returns two messages
without error. Had the `408` been lost, the client's own deadline (six seconds) would have ended
the fetch the same way.

### 10.5 Continuous consumption with heartbeats and a refill

To keep the transcript short the session uses `max_messages` 4 (so the refill threshold is 2),
`expires` 30 s and `idle_heartbeat` 15 s. The consumer `worker` is new and the stream holds two
messages; three more are published during the session. `pending` is the client's message counter.

```
-- t = 0 s: subscribe once, first pull; pending = 4
C: SUB _INBOX.e5JvN0uXq1ZtB6yRw3GhCa 2␍␊
C: PUB $JS.API.CONSUMER.MSG.NEXT.ORDERS.worker _INBOX.e5JvN0uXq1ZtB6yRw3GhCa 62␍␊
C: {"expires":30000000000,"batch":4,"idle_heartbeat":15000000000}␍␊
S: MSG ORDERS.new 2 $JS.ACK._.szMpdrwD.ORDERS.worker.1.1.1.1763800001000000000.1 7␍␊
S: order-1␍␊
S: MSG ORDERS.new 2 $JS.ACK._.szMpdrwD.ORDERS.worker.1.2.2.1763800002000000000.0 7␍␊
S: order-2␍␊
-- pending = 2, not below the threshold: no refill
-- t = 15 s: the request has been idle for 15 s
S: HMSG _INBOX.e5JvN0uXq1ZtB6yRw3GhCa 2 75 75␍␊
S: NATS/1.0 100 Idle Heartbeat␍␊
S: Nats-Last-Consumer: 2␍␊
S: Nats-Last-Stream: 2␍␊
S: ␍␊
S: ␍␊
-- t = 20 s: a message is published to the stream
S: MSG ORDERS.new 2 $JS.ACK._.szMpdrwD.ORDERS.worker.1.3.3.1763800020000000000.0 7␍␊
S: order-3␍␊
-- pending = 1, below 2: refill with batch = 4 - 1 = 3; pending = 4
C: PUB $JS.API.CONSUMER.MSG.NEXT.ORDERS.worker _INBOX.e5JvN0uXq1ZtB6yRw3GhCa 62␍␊
C: {"expires":30000000000,"batch":3,"idle_heartbeat":15000000000}␍␊
-- t = 21 s and 22 s: two more messages are published
S: MSG ORDERS.new 2 $JS.ACK._.szMpdrwD.ORDERS.worker.1.4.4.1763800021000000000.0 7␍␊
S: order-4␍␊
S: MSG ORDERS.new 2 $JS.ACK._.szMpdrwD.ORDERS.worker.1.5.5.1763800022000000000.0 7␍␊
S: order-5␍␊
-- pending = 2
-- t = 37 s: the second request has been idle for 15 s
S: HMSG _INBOX.e5JvN0uXq1ZtB6yRw3GhCa 2 75 75␍␊
S: NATS/1.0 100 Idle Heartbeat␍␊
S: Nats-Last-Consumer: 5␍␊
S: Nats-Last-Stream: 5␍␊
S: ␍␊
S: ␍␊
-- t = 50 s: the second request (sent at t = 20 s) expires with 2 undelivered
S: HMSG _INBOX.e5JvN0uXq1ZtB6yRw3GhCa 2 81 81␍␊
S: NATS/1.0 408 Request Timeout␍␊
S: Nats-Pending-Messages: 2␍␊
S: Nats-Pending-Bytes: 0␍␊
S: ␍␊
S: ␍␊
-- pending = 2 - 2 = 0, below 2: refill with batch 4; pending = 4
C: PUB $JS.API.CONSUMER.MSG.NEXT.ORDERS.worker _INBOX.e5JvN0uXq1ZtB6yRw3GhCa 62␍␊
C: {"expires":30000000000,"batch":4,"idle_heartbeat":15000000000}␍␊
```

Points to notice:

- The first request asked for four messages and received its fourth at t = 21 s. It ended there
  without any status, and nothing happens at t = 30 s when it would have expired.
- Between t = 20 s and t = 21 s two requests are open on the server at once. The message at
  t = 21 s is served from the older one and the next from the newer one; the client neither knows
  nor cares.
- The `408` is the normal, silent way a quiet session renews its request. Its pending header is
  what lets the counter return to zero.
- The acks the application sends are omitted; they are independent of the pull traffic.

### 10.6 A missed heartbeat

The session of 10.5 continues. Its last pull was sent at t = 50 s. The consumer's leader then
fails without warning.

```
-- t = 50 s: last inbound message (the 408); heartbeat timer armed for 30 s
-- t = 65 s: a heartbeat was due; nothing arrives
-- t = 80 s: the timer fires. Report "no heartbeat"; pending = 4; pull again
C: PUB $JS.API.CONSUMER.MSG.NEXT.ORDERS.worker _INBOX.e5JvN0uXq1ZtB6yRw3GhCa 62␍␊
C: {"expires":30000000000,"batch":4,"idle_heartbeat":15000000000}␍␊
-- case A: a leader is serving again. Messages or heartbeats resume; nothing else to do.
-- case B: nobody is subscribed to the pull subject
S: HMSG _INBOX.e5JvN0uXq1ZtB6yRw3GhCa 2 71 71␍␊
S: NATS/1.0 503␍␊
S: Nats-Subject: $JS.API.CONSUMER.MSG.NEXT.ORDERS.worker␍␊
S: ␍␊
S: ␍␊
-- report "no responders"; the 503 restarted the timer, so the next attempt is at t = 110 s
```

The `503` has no description and names the subject that had no subscriber. In case B the session
keeps retrying once per heartbeat window. For a durable consumer during a leader election that is
the right behavior. For a consumer that no longer exists it never succeeds; a client may shorten
the wait by issuing a consumer info request after a `503` and stopping on `404/10014`.

### 10.7 The consumer is deleted during a pull

```
C: PUB $JS.API.CONSUMER.MSG.NEXT.ORDERS.worker _INBOX.e5JvN0uXq1ZtB6yRw3GhCa 62␍␊
C: {"expires":30000000000,"batch":4,"idle_heartbeat":15000000000}␍␊
-- another client deletes the consumer
S: HMSG _INBOX.e5JvN0uXq1ZtB6yRw3GhCa 2 33 33␍␊
S: NATS/1.0 409 Consumer Deleted␍␊
S: ␍␊
S: ␍␊
C: UNSUB 2␍␊
```

The status goes to every request that was waiting. It is terminal: the client reports the error,
stops the session and unsubscribes. An ordered consumer instead recreates its consumer
(section 7.4). If no request was waiting at the moment of deletion, the client learns of it from
the `503` to its next pull.

### 10.8 Leadership change

The consumer is replicated. Its leader steps down while a pull is waiting; a new leader is
elected.

```
-- pending = 4, one pull outstanding
S: HMSG _INBOX.e5JvN0uXq1ZtB6yRw3GhCa 2 34 34␍␊
S: NATS/1.0 409 Leadership Change␍␊
S: ␍␊
S: ␍␊
-- report a warning; pending = 0; refill at once
C: PUB $JS.API.CONSUMER.MSG.NEXT.ORDERS.worker _INBOX.e5JvN0uXq1ZtB6yRw3GhCa 62␍␊
C: {"expires":30000000000,"batch":4,"idle_heartbeat":15000000000}␍␊
S: MSG ORDERS.new 2 $JS.ACK._.szMpdrwD.ORDERS.worker.1.6.6.1763800090000000000.0 7␍␊
S: order-6␍␊
```

The **new** leader sends the status, to every pull the group had recorded; the old leader says
nothing. There are no pending headers because all of the client's pulls are void, which is why
the counter is set to zero rather than reduced. If the status is lost, the missed-heartbeat timer
has the same effect one window later.

### 10.9 Ack, delayed nak, in progress, terminate

Five messages, stream sequences 6 to 10, were delivered once each (ack subjects ending
`.1.<n>.<n>.<timestamp>.<pending>`).

```
-- 6: plain ack, fire and forget
C: PUB $JS.ACK._.szMpdrwD.ORDERS.worker.1.6.6.1763800090000000000.4 4␍␊
C: +ACK␍␊
-- 7: double ack: the ack is a request; the reply is an empty message
C: PUB $JS.ACK._.szMpdrwD.ORDERS.worker.1.7.7.1763800091000000000.3 _INBOX.Hq7Rk2Vx9LmPz4WbT8cYsD.dP6a2TsQ 4␍␊
C: +ACK␍␊
S: MSG _INBOX.Hq7Rk2Vx9LmPz4WbT8cYsD.dP6a2TsQ 1 0␍␊
S: ␍␊
-- 8: negative ack, redeliver in 5 s
C: PUB $JS.ACK._.szMpdrwD.ORDERS.worker.1.8.8.1763800092000000000.2 26␍␊
C: -NAK {"delay": 5000000000}␍␊
-- 9: still working; restart the ack-wait clock (repeat as needed), then ack
C: PUB $JS.ACK._.szMpdrwD.ORDERS.worker.1.9.9.1763800093000000000.1 4␍␊
C: +WPI␍␊
C: PUB $JS.ACK._.szMpdrwD.ORDERS.worker.1.9.9.1763800093000000000.1 4␍␊
C: +ACK␍␊
-- 10: poison message; never redeliver
C: PUB $JS.ACK._.szMpdrwD.ORDERS.worker.1.10.10.1763800094000000000.0 17␍␊
C: +TERM bad payload␍␊
-- about 5 s after the nak, provided a pull is waiting: message 8 again
S: MSG ORDERS.new 2 $JS.ACK._.szMpdrwD.ORDERS.worker.2.8.11.1763800092000000000.0 7␍␊
S: order-8␍␊
```

The redelivery of message 8 has delivery count 2, the same stream sequence 8 and stored timestamp,
and a new consumer sequence 11. The nak produced a `MSG_NAKED` advisory and the term a
`MSG_TERMINATED` advisory with `"reason":"bad payload"`. After these acks the consumer's ack floor
is stream sequence 7: message 8 is still outstanding, so 9 and 10, although done, are above the
floor.

### 10.10 Redelivery after ack wait, with backoff and `max_deliver`

Consumer `retry` has `"ack_policy":"explicit","backoff":[1000000000,5000000000],"max_deliver":3`;
the server therefore set its `ack_wait` to 1 s. It started at stream sequence 7. A pull is
outstanding on subscription 5 throughout, and the application never acknowledges. A monitoring
subscription 9 listens on the advisory subject.

```
-- t = 0 s: first delivery
S: MSG ORDERS.new 5 $JS.ACK._.szMpdrwD.ORDERS.retry.1.7.1.1763800200000000000.0 7␍␊
S: order-7␍␊
-- t = about 1 s: backoff[0] elapsed; second delivery
S: MSG ORDERS.new 5 $JS.ACK._.szMpdrwD.ORDERS.retry.2.7.2.1763800200000000000.0 7␍␊
S: order-7␍␊
-- t = about 6 s: backoff[1] elapsed; third delivery
S: MSG ORDERS.new 5 $JS.ACK._.szMpdrwD.ORDERS.retry.3.7.3.1763800200000000000.0 7␍␊
S: order-7␍␊
-- t = about 11 s: backoff[1] elapsed again (the last element repeats); max_deliver reached
S: MSG $JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.ORDERS.retry 9 191␍␊
S: {"type":"io.nats.jetstream.advisory.v1.max_deliver","id":"R6mEw1Zq8Yb3Nc5Tg0Vk7J","timestamp":"2026-11-22T10:03:31.004512Z","stream":"ORDERS","consumer":"retry","stream_seq":7,"deliveries":3}␍␊
```

Across the three deliveries the delivery count goes 1, 2, 3, the stream sequence stays 7 and the
consumer sequence goes 1, 2, 3. After the advisory the message is no longer pending for this
consumer and will not be delivered to it again, but it is still in the stream. Each redelivery
consumed one unit of the outstanding pull's batch, like any message. The times are approximate:
the server checks deadlines on a timer.

### 10.11 Push consumer flow control

Push consumer `push1` has `"deliver_subject":"deliver.orders","flow_control":true,
"idle_heartbeat":5000000000`. The client subscribed to `deliver.orders` as subscription 6 before
creating it.

```
S: MSG ORDERS.new 6 $JS.ACK._.szMpdrwD.ORDERS.push1.1.11.11.1763800300000000000.900 8␍␊
S: order-11␍␊
S: MSG ORDERS.new 6 $JS.ACK._.szMpdrwD.ORDERS.push1.1.12.12.1763800301000000000.899 8␍␊
S: order-12␍␊
-- (many messages later) more than half the window is unanswered: the server inserts a marker
S: HMSG deliver.orders 6 $JS.FC._.szMpdrwD.ORDERS.push1.aB3x 36 36␍␊
S: NATS/1.0 100 FlowControl Request␍␊
S: ␍␊
S: ␍␊
S: MSG ORDERS.new 6 $JS.ACK._.szMpdrwD.ORDERS.push1.1.13.13.1763800302000000000.898 8␍␊
S: order-13␍␊
-- the application has processed 11 and 12; processing reaches the marker
C: PUB $JS.FC._.szMpdrwD.ORDERS.push1.aB3x 0␍␊
C: ␍␊
```

The marker's subject is the deliver subject and its reply subject is where the answer goes. The
answer is an empty publish, sent when the client's in-order processing reaches the marker, not when
the marker is read. If the answer never reaches the server, delivery stops once the window is full
and the next idle heartbeat says why:

```
S: HMSG deliver.orders 6 137 137␍␊
S: NATS/1.0 100 Idle Heartbeat␍␊
S: Nats-Last-Consumer: 13␍␊
S: Nats-Last-Stream: 13␍␊
S: Nats-Consumer-Stalled: $JS.FC._.szMpdrwD.ORDERS.push1.aB3x␍␊
S: ␍␊
S: ␍␊
C: PUB $JS.FC._.szMpdrwD.ORDERS.push1.aB3x 0␍␊
C: ␍␊
```

The heartbeat also confirms the client has seen everything: its last consumer sequence was 13.

### 10.12 Ordered consumer: gap and reset

An ordered consumer over `ORDERS` with name prefix `Xn4Tq8Lw2Zc6Vb0Jm5Rk9P`. The first server
consumer, serial 1, was created with `deliver_policy` `all`. Cursor: `stream_seq` 0,
`consumer_seq` 0.

```
C: SUB _INBOX.Gt3Hn7Kp1Ld5Ms9Qv2Wx6Y 7␍␊
C: PUB $JS.API.CONSUMER.MSG.NEXT.ORDERS.Xn4Tq8Lw2Zc6Vb0Jm5Rk9P_1 _INBOX.Gt3Hn7Kp1Ld5Ms9Qv2Wx6Y 63␍␊
C: {"expires":30000000000,"batch":500,"idle_heartbeat":5000000000}␍␊
S: MSG ORDERS.new 7 $JS.ACK._.szMpdrwD.ORDERS.Xn4Tq8Lw2Zc6Vb0Jm5Rk9P_1.1.1.1.1763800001000000000.4 7␍␊
S: order-1␍␊
S: MSG ORDERS.new 7 $JS.ACK._.szMpdrwD.ORDERS.Xn4Tq8Lw2Zc6Vb0Jm5Rk9P_1.1.2.2.1763800002000000000.3 7␍␊
S: order-2␍␊
-- cursor: stream_seq 2, consumer_seq 2. The next message is lost in transit.
S: MSG ORDERS.new 7 $JS.ACK._.szMpdrwD.ORDERS.Xn4Tq8Lw2Zc6Vb0Jm5Rk9P_1.1.4.4.1763800004000000000.1 7␍␊
S: order-4␍␊
-- consumer sequence 4, expected 3: drop this message and reset
C: UNSUB 7␍␊
C: PUB $JS.API.CONSUMER.DELETE.ORDERS.Xn4Tq8Lw2Zc6Vb0Jm5Rk9P_1 _INBOX.Hq7Rk2Vx9LmPz4WbT8cYsD.eQ7b3UtR 0␍␊
C: ␍␊
C: PUB $JS.API.CONSUMER.CREATE.ORDERS.Xn4Tq8Lw2Zc6Vb0Jm5Rk9P_2 _INBOX.Hq7Rk2Vx9LmPz4WbT8cYsD.fR8c4VuS 252␍␊
C: {"stream_name":"ORDERS","config":{"name":"Xn4Tq8Lw2Zc6Vb0Jm5Rk9P_2","deliver_policy":"by_start_sequence","opt_start_seq":3,"ack_policy":"none","replay_policy":"instant","inactive_threshold":300000000000,"num_replicas":1,"mem_storage":true},"action":""}␍␊
-- the delete and create responses arrive on subscription 1 (not shown)
C: SUB _INBOX.Cz8Df2Gj6Kn0Pr4Su7Vy1B 8␍␊
C: PUB $JS.API.CONSUMER.MSG.NEXT.ORDERS.Xn4Tq8Lw2Zc6Vb0Jm5Rk9P_2 _INBOX.Cz8Df2Gj6Kn0Pr4Su7Vy1B 63␍␊
C: {"expires":30000000000,"batch":500,"idle_heartbeat":5000000000}␍␊
S: MSG ORDERS.new 8 $JS.ACK._.szMpdrwD.ORDERS.Xn4Tq8Lw2Zc6Vb0Jm5Rk9P_2.1.3.1.1763800003000000000.2 7␍␊
S: order-3␍␊
S: MSG ORDERS.new 8 $JS.ACK._.szMpdrwD.ORDERS.Xn4Tq8Lw2Zc6Vb0Jm5Rk9P_2.1.4.2.1763800004000000000.1 7␍␊
S: order-4␍␊
```

The application receives `order-1`, `order-2`, `order-3`, `order-4`: in order, each once. The
first copy of `order-4` was discarded because it arrived out of sequence; it came again from the
second consumer, whose consumer sequence restarted at 1 while the stream sequence continued at 3.
Messages keep an ack subject even though the policy is `none`; the client uses it only for the
sequence numbers and never publishes to it.

### 10.13 Pinned client, with both pin errors

Consumer `leader` has `"priority_policy":"pinned_client","priority_groups":["main"]`. Client A and
client B each run a consume session; an operator uses a third connection. Each block is one
connection.

Client A pulls first and becomes the pinned client:

```
C: SUB _INBOX.e5JvN0uXq1ZtB6yRw3GhCa 2␍␊
C: PUB $JS.API.CONSUMER.MSG.NEXT.ORDERS.leader _INBOX.e5JvN0uXq1ZtB6yRw3GhCa 49␍␊
C: {"expires":60000000000,"batch":10,"group":"main"}␍␊
S: HMSG ORDERS.new 2 $JS.ACK._.szMpdrwD.ORDERS.leader.1.1.1.1763800001000000000.0 49 56␍␊
S: NATS/1.0␍␊
S: Nats-Pin-Id: 9IGr1pJUGNwPdSRcRZpk4X␍␊
S: ␍␊
S: order-1␍␊
-- A stores pin id 9IGr1pJUGNwPdSRcRZpk4X
```

Client B pulls without an id and is queued as a standby; it receives nothing:

```
C: SUB _INBOX.Jw2Lp6Xc0Vn4Bz8Qm1Tk5R 2␍␊
C: PUB $JS.API.CONSUMER.MSG.NEXT.ORDERS.leader _INBOX.Jw2Lp6Xc0Vn4Bz8Qm1Tk5R 49␍␊
C: {"expires":60000000000,"batch":10,"group":"main"}␍␊
```

The operator unpins the group:

```
C: PUB $JS.API.CONSUMER.UNPIN.ORDERS.leader _INBOX.Hq7Rk2Vx9LmPz4WbT8cYsD.gS9d5WvT 16␍␊
C: {"group":"main"}␍␊
S: MSG _INBOX.Hq7Rk2Vx9LmPz4WbT8cYsD.gS9d5WvT 1 59␍␊
S: {"type":"io.nats.jetstream.api.v1.consumer_unpin_response"}␍␊
```

A new message is published. The server walks its wait queue. Client A's request is still there and
is tied to the old pin, so it is rejected at delivery time, with what it had left:

```
S: HMSG _INBOX.e5JvN0uXq1ZtB6yRw3GhCa 2 83 83␍␊
S: NATS/1.0 423 Nats-Wrong-Pin-Id␍␊
S: Nats-Pending-Messages: 9␍␊
S: Nats-Pending-Bytes: 0␍␊
S: ␍␊
S: ␍␊
```

Client B's request has no id, so B becomes the pinned client and receives the message with a new
pin id:

```
S: HMSG ORDERS.new 2 $JS.ACK._.szMpdrwD.ORDERS.leader.1.2.2.1763800002000000000.0 49 56␍␊
S: NATS/1.0␍␊
S: Nats-Pin-Id: 9IGr1pJUGNwPdSRcRZpk7d␍␊
S: ␍␊
S: order-2␍␊
```

Suppose client A had a refill pull in flight, sent with the old id before it processed the `423`.
That request is rejected on arrival, because a different pin now exists:

```
C: PUB $JS.API.CONSUMER.MSG.NEXT.ORDERS.leader _INBOX.e5JvN0uXq1ZtB6yRw3GhCa 78␍␊
C: {"expires":60000000000,"batch":5,"id":"9IGr1pJUGNwPdSRcRZpk4X","group":"main"}␍␊
S: HMSG _INBOX.e5JvN0uXq1ZtB6yRw3GhCa 2 37 37␍␊
S: NATS/1.0 423 Nats-Pin-Id mismatch␍␊
S: ␍␊
S: ␍␊
-- A clears its pin id, zeroes its counters and pulls again without an id: it is now the standby
C: PUB $JS.API.CONSUMER.MSG.NEXT.ORDERS.leader _INBOX.e5JvN0uXq1ZtB6yRw3GhCa 49␍␊
C: {"expires":60000000000,"batch":10,"group":"main"}␍␊
```

The two errors differ only in when the server noticed: `Nats-Wrong-Pin-Id` for a request already
queued (with pending headers, because the request had a remainder), `Nats-Pin-Id mismatch` for a
request rejected on arrival (nothing was queued, so nothing is pending). The client's reaction to
both is the same. The server stamps the pinned request itself with the pin id, which is why A's
first request, sent without an id, was later rejected as carrying the wrong one.

## 11. ADR versus server discrepancies

| Topic | The ADR says | The server does |
|---|---|---|
| Ack subject layout (ADR-15) | 9 tokens, or 11 and possibly a 12th random token | emits exactly 11 tokens by default (9 before 2.16); accepts 9 or any count of 11 and more |
| `Nats-Last-Stream` in heartbeats (ADR-9) | the stream's newest sequence | the consumer's stream cursor minus one |
| `no_wait` with `expires` (ADR-13) | `no_wait` takes precedence | combined behavior: wait up to `expires` for the first message, then `404` at end of stream (section 4.4) |
| Pull consumers must be durable (ADR-13) | required | not enforced; error 10085 exists but is never returned |
| Multiple-filter error codes (ADR-34) | 10136 overlap, 10134 both set, 10135 subject API | 10138 overlap, 10136 both set, 10137 subject API |
| One priority group per consumer (ADR-42) | more than one is an error | several are accepted; one pin and one reported state for the whole consumer |
| Priority configuration is immutable (ADR-42) | only `priority_timeout` may change | no update rule for `priority_policy` or `priority_groups` |
| Pinned advisory (ADR-42) | has a `client` field | has no such field |
| Pull request `failover` field (ADR-42) | documented as not implemented | absent; ignored if sent |
| `409` for exceeding max ack pending (ADR-15) | listed as an error status | no such status exists on the consumer path; delivery just stalls |
| `409 Exceeded MaxWaiting` text | sometimes quoted as `Exceeded MaxWaiting of <n>` | exactly `Exceeded MaxWaiting`; and not sent at all when the request has a heartbeat |
| Statuses classified by ADR-37 | omits `408 Requests Pending`, `409 Batch Completed`, `409 Leadership Change`, `409 Server Shutdown`, `412 Required Api Level`, `423 Nats-Pin-Id mismatch` | sends all of them |
| Missed-heartbeat threshold | ADR-15: 3 intervals (push); ADR-37: 2 intervals (pull) | not a server matter; the reference client uses 2 everywhere |
| Ordered consumer (ADR-17) | a push consumer with flow control and heartbeats | no server concept; the reference client's current API builds it on an ephemeral pull consumer |
| Filter must be a subset of stream subjects | error 10093 exists | never returned; an unmatched filter is accepted |
| Message 10116 | "max deliver is required to be > length of backoff values" | `max_deliver` equal to the length is accepted |

## 12. Client implementation checklist

Every client rule of this chapter, grouped by area.

**Configuration and management**

1. Create with `$JS.API.CONSUMER.CREATE.<stream>.<consumer>`, `config.name` equal to the subject
   token, and `durable_name` equal to it only for durables. Append the filter to the subject only
   when exactly one filter is configured. Never use the legacy create subjects.
2. Validate consumer names locally before building any subject: non-empty, at most 255 bytes, no
   whitespace, `.`, `*`, `>`, `/`, `\` or NUL. Validate priority group names against
   `^[a-zA-Z0-9/_=-]{1,16}$`.
3. Always send `deliver_policy`, `ack_policy` and `replay_policy`. Encode durations as integer
   nanoseconds and times as RFC 3339.
4. Expose create, update and create-or-update as three operations (`"create"`, `"update"`, `""`).
   Map `err_code` 10148 (already exists) and 10149 (does not exist) to distinct errors, 10014 to
   "consumer not found" and 10059 to "stream not found".
5. After create or update, compare the echoed `config` with the request to detect features an
   older server dropped. Ignore `metadata` keys starting with `_nats.` when comparing.
6. Treat an info timeout and `503/10008` as retryable; only 10014 and 10059 mean absence.
7. Page `NAMES` and `LIST` by `offset` until `offset >= total`; surface `missing` and `offline`.
8. Treat the zero time in `pinned_ts` and `pause_until` as "not set".
9. Tolerate a consumer sequence that restarts (someone called reset) on non-ordered consumers.
10. Expect an ephemeral consumer to disappear after `inactive_threshold` (5 s by default) without
    a waiting pull or deliver-subject interest.

**Pull requests**

11. Send the JSON request form with `batch >= 1` and a reply subject. Send `expires` unless the
    request is a deliberate `no_wait`-only request. Ensure `expires >= 2 * idle_heartbeat`.
12. Classify every inbound message first: a status has a status code in its header block and no
    payload; everything else is a user message. Never surface a status as a message.
13. Match statuses by code, then (for 409) by case-insensitive description. Unknown 409: warning.
    Unknown code: non-terminal error.
14. On any terminating status read `Nats-Pending-Messages` and `Nats-Pending-Bytes` when present.
15. Terminal for the consumer: `409 Consumer Deleted`, `409 Consumer is push based`. Terminal as
    caller errors: every `400`, `412`. Everything else ends at most one pull.
16. Treat `503` on a pull as "no leader or no consumer": retry with delay; optionally confirm with
    an info request.

**Continuous consumption**

17. One inbox subscription per session; every pull of the session replies to it.
18. Keep `pending_msgs` and `pending_bytes`: add on pull, subtract per message (size is subject +
    reply + header block + payload), subtract pending headers on terminating statuses, floor at 0.
19. Refill when a counter drops below its threshold (default half of the maximum) by pulling the
    difference; let pulls overlap; never cancel a pull.
20. Run a `2 * idle_heartbeat` timer restarted by every inbound message; when it fires, warn, reset
    the counters and send a full-size pull.
21. On `409 Leadership Change` and on `423`: zero the counters and pull immediately.
22. On disconnect stop the timer; on reconnect reset the counters, send a full-size pull, restart
    the timer.
23. Provide stop (unsubscribe, discard buffered) and drain (deliver buffered, then unsubscribe).
24. Size the local buffer so that messages do not sit in it longer than `ack_wait`.

**Fetch**

25. A new inbox subscription per fetch, with no client-side pending limits; unsubscribe when done.
26. Finish by count, by received bytes, on `404`, any `408`, or
    `409 Message Size Exceeds MaxBytes` (all success, possibly partial); finish with an error on
    any other status.
27. Arm a client deadline of `expires + 1 s` (1 s for `no_wait` without `expires`) and, when a
    heartbeat was requested, a `2 * idle_heartbeat` timer.
28. Allow concurrent fetches on one consumer; they share only the pin id.

**Push**

29. Subscribe to `deliver_subject` (as a queue subscription with `deliver_group` when set) before
    the consumer can deliver. Refuse combinations that cannot work (section 5.1).
30. Distinguish the two `100` statuses by description. `FlowControl Request`: publish an empty
    payload to its reply subject when in-order processing reaches it. `Idle Heartbeat`: restart
    the liveness timer; if `Nats-Consumer-Stalled` is present publish an empty payload to its
    value.
31. Remember the last flow-control subject answered; do not answer it twice.
32. Compare `Nats-Last-Consumer` in heartbeats with the last consumer sequence seen (not on queue
    subscriptions) and report a gap.
33. Report missing heartbeats after 2 (reference client) or 3 (ADR-15) intervals of silence.

**Acknowledgements**

34. Parse the ack subject accepting exactly 9 tokens or at least 11; ignore tokens beyond the
    pending count; map domain `_` to none. Anything else is not a JetStream message.
35. Publish acks to the reply subject exactly as received.
36. Payloads: `+ACK`, `-NAK`, `-NAK {"delay": <ns>}`, `+WPI`, `+TERM`, `+TERM <reason>`.
37. `+ACK`, `-NAK`, `+TERM` are terminal per message object: set a flag after a successful
    publish and refuse a second terminal ack locally. `+WPI` is repeatable.
38. Double ack: request to the ack subject with `+ACK`; any reply is success; on timeout leave the
    message unacked.
39. Expose metadata from the ack subject: stream, consumer, domain, stream sequence, consumer
    sequence, delivery count, timestamp (nanoseconds), pending.
40. Never publish a message with a reply subject starting with `$JS.`; the server rejects it.

**Ordered consumers**

41. Ephemeral pull consumer named `<prefix>_<serial>`, `ack_policy` `none`, `mem_storage` true,
    `num_replicas` 1, `inactive_threshold` 5 minutes unless overridden.
42. Deliver a message only if its consumer sequence is the previous one plus 1 and its consumer
    name is the current one.
43. Reset on sequence mismatch, missed heartbeats, `409 Consumer Deleted`, `503`, and reconnect:
    unsubscribe, delete the old consumer in the background, create the next serial with
    `by_start_sequence` at last stream sequence + 1 (or the original start configuration if
    nothing was delivered), retrying after 1, 2, 4, 8, 10, 10, ... seconds.
44. Do not reset on `409 Leadership Change` or `423`.

**Priority groups**

45. Require a `group` on every pull when the consumer has `priority_groups`; refuse one otherwise.
46. Keep one pin id per consumer handle, learned from `Nats-Pin-Id` on any delivered message and
    sent as `id` in every subsequent pull.
47. On `423`: clear the pin id, zero the counters, pull without `id`, report a warning.
48. Keep `expires` well below `priority_timeout` for `pinned_client` consumers.
49. Send `min_pending`/`min_ack_pending` only to `overflow` consumers and `priority` (0 to 9) only
    to `prioritized` consumers; the server answers `400` otherwise.
