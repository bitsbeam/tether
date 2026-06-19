# NATS Core Client Protocol

This chapter describes everything that happens on a NATS client connection below JetStream: the
text protocol itself, the connection handshake, TLS, authentication, subjects, headers,
request/reply, keepalive, errors, cluster awareness and the WebSocket transport. It is written from
the server's point of view (what the server parses, emits and enforces) so that a client can be
derived from it. Facts are taken from the nats-server source at `v2.15.0-573` (`VERSION`
`2.16.0-dev`), from the Go client `nats.go` v1.56.0 where a convention lives only in clients, and
from the architecture decision records (ADRs). JetStream is covered in
[02-jetstream.md](02-jetstream.md) and [03-consumers.md](03-consumers.md); key-value, object store
and services in [04-kv-object-services.md](04-kv-object-services.md).

Notation: `␍␊` is the two bytes CR LF (`0x0D 0x0A`). In transcripts, `C:` marks bytes sent by the
client and `S:` bytes sent by the server. "Closes" always means the server closes the TCP
connection.

Contents

1. [Mental model](#1-mental-model)
2. [Wire format fundamentals](#2-wire-format-fundamentals)
3. [Connection lifecycle](#3-connection-lifecycle)
4. [Protocol verbs](#4-protocol-verbs)
5. [Subjects](#5-subjects)
6. [Headers](#6-headers)
7. [Request/reply](#7-requestreply)
8. [Authentication](#8-authentication)
9. [Keepalive](#9-keepalive)
10. [Errors and connection closes](#10-errors-and-connection-closes)
11. [Cluster awareness and reconnection](#11-cluster-awareness-and-reconnection)
12. [WebSocket transport](#12-websocket-transport)
13. [Other core features visible to clients](#13-other-core-features-visible-to-clients)
14. [Scenarios](#14-scenarios)
15. [Client implementation checklist](#15-client-implementation-checklist)
16. [Unverified items](#16-unverified-items)

---

## 1. Mental model

NATS core is subject-based publish/subscribe carried over one long-lived connection per client.

- **One connection, two byte streams.** A client opens a single TCP connection (optionally wrapped
  in TLS, or carried inside WebSocket frames) and keeps it open. Both sides write an unframed
  stream of protocol operations. There are no channels, streams or request ids at the transport
  level; everything is multiplexed by *subject* and by client-chosen *subscription ids* (sids).
- **Text control lines, length-prefixed payloads.** Every operation starts with a line of ASCII
  text terminated by `␍␊` (the *control line*). Operations that carry a message put a decimal byte
  count on the control line; exactly that many payload bytes follow, then a `␍␊`. The payload is
  opaque binary. This makes the protocol trivial to parse incrementally and to debug with telnet.
- **The server is a router with no memory.** For each published message the server looks up the
  subscriptions whose subject pattern matches, copies the message to each of those connections'
  outbound buffers, and forgets it. Nothing is stored and nothing is retried.
- **Interest-based, at-most-once delivery.** A message published to a subject nobody is subscribed
  to is discarded silently. A subscriber that is too slow is disconnected rather than allowed to
  apply back-pressure to the server. The publisher gets no acknowledgment (other than the optional,
  rarely used `+OK`). A message is therefore delivered zero or one time to each subscriber.
- **Twelve operations.** Server to client: `INFO`, `MSG`, `HMSG`, `PING`, `PONG`, `+OK`, `-ERR`.
  Client to server: `CONNECT`, `PUB`, `HPUB`, `SUB`, `UNSUB`, `PING`, `PONG`.

Everything else in NATS is a convention layered on `PUB`, `SUB` and `MSG`:

| Feature | How it is built |
|---|---|
| Request/reply | A `PUB` with a *reply subject*; the responder publishes to that subject; the requester has a subscription on it ([section 7](#7-requestreply)). |
| Load balancing | Subscriptions that share a *queue group* name; the server picks one member per message ([section 5.5](#55-queue-groups)). |
| Message metadata | `HPUB`/`HMSG`, which are `PUB`/`MSG` with a header block in front of the payload ([section 6](#6-headers)). |
| Status signaling | A message with headers whose first line carries a status code, e.g. `NATS/1.0 503` for "no responders". |
| JetStream, KV, object store | Request/reply to service subjects under `$JS.API.>` whose responder is the server itself; stored messages are delivered as ordinary `MSG`/`HMSG` whose reply subject encodes the acknowledgment address. |
| Monitoring and "who am I" | Request/reply to subjects under `$SYS.REQ.>` ([section 13.2](#132-client-usable-system-subjects)). |

A client that implements this chapter correctly has the complete transport for all later chapters.

Two design decisions shape most of the rules below. First, the server protects itself, not the
client: malformed input is answered by closing the connection, frequently without any explanation.
Second, the server validates very little (subjects on publish, header blocks, sid formats are all
unchecked), so correctness and safety of what goes on the wire is the client's job.

---

## 2. Wire format fundamentals

### 2.1 How the server parses

The server parser (`server/parser.go`, `parse`) is a byte-at-a-time state machine. Its exact
tolerances matter because a client's own parser should be at least as tolerant for what the server
emits, and a client's writer should emit only the canonical form.

| Rule | Detail |
|---|---|
| Operation names are case-insensitive | Matched letter by letter: `pub`, `Pub` and `PUB` are the same. This holds for `CONNECT`, `INFO`, `PUB`, `HPUB`, `SUB`, `UNSUB`, `PING`, `PONG`, `+OK`, `-ERR`. |
| First byte selects the operation | For a client connection: `P`/`p` (PUB, PING, PONG), `H`/`h` (HPUB), `S`/`s` (SUB), `U`/`u` (UNSUB), `C`/`c` (CONNECT), `I`/`i` (INFO), `+` (+OK), `-` (-ERR). Any other first byte is a parse error, including `M` (a client may not send `MSG`), and including `␍`, `␊` or a space: **a blank line between operations is a parse error.** |
| Separator after the operation name | One or more spaces or TABs. At least one is required after `PUB`, `HPUB`, `SUB`, `UNSUB` and `-ERR`. After `CONNECT` and `INFO` it is optional (`CONNECT{...}` parses). |
| Argument splitting | `PUB`/`HPUB` arguments are split on space and TAB. `SUB`/`UNSUB` arguments are split on space, TAB, CR and LF. Runs of separators collapse; there are no empty arguments. |
| Line terminator | A control line ends at `␊`. A preceding `␍` is dropped. **A bare `␊` is accepted** for every control line. Quirk: a `␍` anywhere in the argument makes the parser drop the last byte before `␊`, whatever it is. |
| `PING`, `PONG`, `+OK` | After the letters, every byte up to `␊` is ignored: `PING␊`, `PING␍␊` and `PING anything␍␊` are all a PING. |
| Payload terminator | Strict. After exactly the announced number of payload bytes the next two bytes must be `␍` then `␊`; anything else is a parse error. |
| Control lines may span reads | The server reassembles a control line split across TCP segments. A client parser must do the same for everything it receives. |

What the server *emits* is always canonical: upper-case operation names, single-space separators,
`␍␊` terminators. The one irregularity is `INFO`, which has a space between the closing `}` of its
JSON and the `␍␊` ([section 3.2](#32-info)).

> **Client rule:** Emit only the canonical form: upper-case operation, single spaces, `␍␊`
> terminators, no blank lines, no trailing whitespace. Never send `MSG` or `HMSG`. When parsing
> server output, split control-line arguments on space and TAB only, accept any amount of either
> between arguments, and tolerate trailing whitespace before `␍␊` (needed for `INFO`).

### 2.2 Limits

| Limit | Default | Server config | Enforcement and what the client sees |
|---|---|---|---|
| Control line | 4096 bytes | `max_control_line` | Checked against the **argument bytes only** (everything after the operation name and its separator, excluding `␍␊`). Exceeding it: `-ERR 'maximum control line exceeded'␍␊`, then close. The check also runs on a partially received line, so the error can arrive before the client finished writing it. It applies to the `CONNECT` JSON as well: a CONNECT carrying a user JWT and signature must fit in 4096 bytes by default. The value is not advertised in `INFO`. |
| Payload | 1048576 bytes (1 MiB) | `max_payload` | Advertised per connection as `max_payload` in `INFO`. It is the minimum of the server option, the account limit and the user limit, so the value in the post-CONNECT `INFO` ([section 3.6](#36-the-connect_info-info-and-async-info)) can be smaller than in the initial one. Enforced on the `PUB`/`HPUB` control line: if the announced size (for `HPUB`, the *total* size, headers plus payload) exceeds it: `-ERR 'Maximum Payload Violation'␍␊`, then close, before any payload byte is consumed. A user limit of -1 means unlimited. |
| Size field | 9 decimal digits | none | A size must be 1 to 9 ASCII digits, no sign. Empty, longer, or non-digit: close with **no** `-ERR`. Leading zeros are allowed (`007`) and are forwarded verbatim into the `MSG` the subscribers receive. |
| Pending outbound bytes per client | 64 MiB | `max_pending` | If the bytes queued for a client exceed it, the server closes the connection as a slow consumer. **No `-ERR`**, and queued data is not flushed ([section 10.3](#103-slow-consumers)). |
| Write deadline | 10 s | `write_deadline` | If a socket write to the client blocks longer, the server closes the connection as a slow consumer. No `-ERR`. |
| Connections | 65536 | `max_connections` | `INFO` is sent, then `-ERR 'maximum connections exceeded'␍␊`, then close. |
| Subscriptions per connection | unlimited | `max_subscriptions`, account/user limits | At the limit, a `SUB` gets `-ERR 'maximum subscriptions exceeded'␍␊`, is ignored, and the connection **stays open**. If a limit is lowered below the current count at runtime, the same `-ERR` is sent and the server closes about 20 ms later. |
| Subject tokens on `SUB` | unlimited | `max_sub_tokens` | `-ERR 'Permissions Violation for Subscription to "<subject>", too many tokens'␍␊`; not closed. |
| Subject length | none | none | There is no subject, token or sid length limit other than the control line limit applied to the whole argument string. |

> **Client rule:** Before sending `PUB`/`HPUB`, compare the total size with the `max_payload` from
> the most recent `INFO` and fail the publish locally if it is larger. Sending it would cost the
> whole connection. Also refuse locally any control line whose arguments exceed 4096 bytes (the
> server default; the real value is not discoverable).

### 2.3 Three kinds of violation

The server reacts to bad input in one of three ways, and a client must be prepared for all three.

1. **`-ERR` and the connection stays open.** Used for things that are wrong with one operation but
   leave the stream in sync: permission violations, invalid subscription subject, subscription
   limit, pedantic publish-subject check.
2. **`-ERR` and close.** Used when the server can name the problem: an unknown operation
   (`-ERR 'Unknown Protocol Operation'␍␊`), control line or payload limit, authentication
   failures, stale connection.
3. **Silent close.** Used when an operation was recognized but its arguments are malformed: wrong
   argument count for `PUB`/`HPUB`/`SUB`/`UNSUB`, a bad size field, header size larger than total
   size, `HPUB` without header support, malformed `CONNECT` or `INFO` JSON. The server logs the
   reason and closes; the client sees only end-of-file. Slow-consumer closes, server shutdown and
   an operator kick are silent too.

The complete catalog is in [section 10](#10-errors-and-connection-closes).

> **Client rule:** Never rely on an `-ERR` preceding a close. Treat end-of-file at any time as a
> disconnect and enter the reconnect procedure ([section 11.4](#114-reconnecting)).

---

## 3. Connection lifecycle

The common path, without TLS, is:

```
S: INFO {...} ␍␊
C: CONNECT {...}␍␊
C: PING␍␊
S: PONG␍␊
S: INFO {...,"connect_info":true,...} ␍␊
```

The server speaks first. The client learns the server's capabilities from `INFO`, optionally
upgrades to TLS, describes itself and authenticates with `CONNECT`, and uses a `PING`/`PONG` round
trip to learn that `CONNECT` was accepted. After that both sides may send any operation at any
time.

### 3.1 What the server does on accept

In order (`server/server.go`, `createClient`):

1. Creates the connection state with default options (`verbose`, `pedantic` and `echo` all
   **true**) and binds it to the global account `$G`.
2. Builds the `INFO`, generating a per-connection `nonce` if authentication may need one.
3. Writes `INFO` immediately, unless TLS-first is configured ([section 3.3](#33-tls)).
4. If the server is shutting down or in lame duck mode, closes at once without `-ERR`.
5. If `max_connections` is exceeded: `-ERR 'maximum connections exceeded'␍␊`, close.
6. Handles TLS-first fallback waiting, TLS sniffing (`allow_non_tls`) and, if configured, a PROXY
   protocol header from a load balancer.
7. Performs the TLS handshake if required. With `connection_rate_limit` configured, a throttled
   connection gets `-ERR 'Connection throttling is active. Please try again later.'␍␊` and is
   closed.
8. Sends the delayed `INFO` when TLS-first was used.
9. Starts the authentication timer (only when authentication is required) and the ping timer, and
   begins reading.

### 3.2 INFO

```
INFO <json> ␍␊
```

The server emits the literal `INFO`, one space, a JSON object, **one space**, and `␍␊`. The server
applies no length limit to the `INFO` it sends; with many `connect_urls` it can exceed 4096 bytes.

The JSON is produced by Go's `encoding/json` from a struct, so keys appear in the fixed order of
the table below, keys marked "always" are always present, and the others are absent when they hold
the zero value (`false`, `0`, `""`, empty list).

| JSON key | Type | Present | Meaning |
|---|---|---|---|
| `server_id` | string | always | The server's public nkey (56 characters), generated at start. Unique per server process. |
| `server_name` | string | always | Configured `server_name`, otherwise equal to `server_id`. |
| `version` | string | always | Server version, e.g. `2.16.0-dev`. |
| `proto` | int | always | Server protocol level, `1`. Level 1 means the server understands `echo` in `CONNECT`; a client should not send `echo` to a server reporting less. |
| `git_commit` | string | if known | First 7 characters of the build commit. |
| `go` | string | always | Go runtime version the server was built with. |
| `host` | string | always | The `client_advertise` host if configured, else the listen host (default `0.0.0.0`). For WebSocket clients, the WebSocket listener host. Not a reliable address to dial. |
| `port` | int | always | Same logic as `host` (default 4222). |
| `headers` | bool | always | Whether the server supports message headers (`true` unless `no_header_support`). |
| `auth_required` | bool | if true | The server requires credentials in `CONNECT`. Absent when a `no_auth_user` is configured, even though other users exist. |
| `tls_required` | bool | if true | The client must upgrade to TLS before `CONNECT`. |
| `tls_verify` | bool | if true | The server requires a client certificate. |
| `tls_available` | bool | if true | TLS is configured but optional (`allow_non_tls`). |
| `max_payload` | int | always | Effective maximum message size for this connection at the time this `INFO` was generated. |
| `jetstream` | bool | if true | JetStream is enabled on this server. |
| `client_id` | uint64 | always for clients | Server-assigned connection id (CID). |
| `client_ip` | string | usually | The client's IP address as the server sees it. Absent from the initial `INFO` when the server is configured for PROXY protocol; it then appears in later `INFO`s. |
| `nonce` | string | initial `INFO` only | 15 characters: unpadded URL-safe base64 of 11 random bytes. Present when the server has nkey users, runs in operator (JWT) mode, has trusted proxies, or is configured to always send one. Never present in later `INFO`s. |
| `cluster` | string | if clustered | Cluster name. |
| `domain` | string | if set | JetStream domain. |
| `connect_urls` | []string | if known | `host:port` strings (no scheme) of servers a client can connect to: this server's own client addresses first, then those of other cluster members. A standalone server sends none. For WebSocket clients the list contains WebSocket addresses instead. |
| `ldm` | bool | if true | Lame duck mode: this server is about to shut down ([section 11.2](#112-lame-duck-mode)). |
| `connect_info` | bool | if true | Marks the `INFO` sent in response to the client's first `PING` after `CONNECT`. |
| `remote_account` | string | with `connect_info` | Name of the account the connection was bound to. |
| `acc_is_sys` | bool | with `connect_info`, if true | The account is the system account. |
| `api_lvl` | int | always in practice | JetStream API level (`5` at this server version). |
| `xkey` | string | unless FIPS mode | The server's public curve key. Used by auth-callout services; a normal client ignores it. |

The same struct also serves routes, gateways and leaf nodes, so the following keys exist in the
schema but are never populated for client connections: `ip`, `cluster_dynamic`, `ws_connect_urls`,
`compression`, `import`, `export`, `lnoc`, `lnocu`, `ln`, `info_on_connect`, `route_pool_size`,
`route_pool_idx`, `route_account`, `route_acc_add_reqid`, `gossip_mode`, `gateway`, `gateway_urls`,
`gateway_url`, `gateway_cmd`, `gateway_cmd_payload`, `gateway_nrp`, `gateway_iom`, `leafnode_urls`.

Example of an initial `INFO` from a standalone server without authentication (values
illustrative, key order real):

```
S: INFO {"server_id":"NZDOCIS2JHTLGMXGEDN73U55XTPLPFT7V4SEH2KVJ72CEUVW75EFR6ED","server_name":"n1","version":"2.16.0-dev","proto":1,"git_commit":"a0385c9","go":"go1.25.4","host":"0.0.0.0","port":4222,"headers":true,"max_payload":1048576,"client_id":7,"client_ip":"127.0.0.1","api_lvl":5,"xkey":"XPZOM7WBBR6QMW4WXFOGO6MVN6A6WFHYM6L3VFZ5ZFKKIBJ5J6WJIBAG"} ␍␊
```

> **Client rule:** Parse `INFO` as: the four letters (any case), optional whitespace, then JSON
> running to the end of the line; trim trailing whitespace before decoding. Do not impose a line
> length limit on `INFO`. Ignore unknown keys and treat absent boolean keys as `false`. Keep the
> most recent `INFO` as the connection's server info, but keep the `nonce` from the initial `INFO`
> separately, since later ones do not carry it.

A client may itself send `INFO {...}`; the server decodes it and does nothing. Invalid JSON closes
the connection silently. There is no reason to send it.

### 3.3 TLS

TLS is negotiated *inside* the NATS handshake by default: the server sends `INFO` in the clear,
and the client decides from it whether to start a TLS handshake on the same socket.

| Server configuration | What happens on the wire |
|---|---|
| No `tls {}` block | Plain TCP. `INFO` has neither `tls_required` nor `tls_available`. |
| `tls {}` (explicit TLS, the default) | S: plaintext `INFO` with `"tls_required":true`. The client starts a TLS handshake as TLS client. `CONNECT` and everything after travel inside TLS. No second `INFO` is sent after the handshake. |
| `tls {}` with `allow_non_tls: true` | `INFO` has `"tls_available":true` and no `tls_required`. The server looks at the first bytes the client sends: if the first byte is `0x16` (a TLS ClientHello record) it performs a TLS handshake, otherwise it treats the bytes as plaintext protocol. |
| `tls { handshake_first: true }` (TLS-first, strict) | The server sends nothing until the TLS handshake completes, then sends `INFO` (with `"tls_required":true`) inside TLS. A client that waits for a plaintext `INFO` never receives one and is cut off by the TLS timeout. |
| `tls { handshake_first: auto }` or a duration (TLS-first with fallback) | The server waits for the fallback delay (`auto` means 50 ms) for the client to send anything. If any byte arrives it proceeds with the TLS handshake and sends `INFO` inside TLS. If nothing arrives it sends a plaintext `INFO` and continues as explicit TLS. |
| `tls { verify: true }` | `INFO` has `"tls_verify":true`; the client must present a certificate in the handshake. With `pinned_certs` the certificate is additionally checked against a fixed set and the connection closed on mismatch. |

Timing and failure:

- The server allows `tls.timeout` (default 2 s) for the handshake to complete. On expiry it
  enqueues `-ERR 'Secure Connection - TLS Required'␍␊` and closes. Whether those bytes reach a
  peer that never completed the handshake was not determined ([section 16](#16-unverified-items)).
  A handshake that fails outright is closed without `-ERR`.
- The authentication timer ([section 8.9](#89-authentication-timeout)) starts only after the TLS
  handshake.
- In-process connections (a client embedded in the server process) always get a fallback delay and
  see `"tls_required":false,"tls_available":true`.

ADR-40 specifies the client side: TLS-first requires a client option (`tls_first`) because the
client cannot discover it; the feature exists since server 2.10.4. A client must upgrade when
`INFO` says `tls_required` *or* when the user demanded TLS (a `tls://` URL scheme or a "TLS
required" option); in the second case the client enforces TLS whether or not the server asks for
it, so against a server without TLS the attempt fails in the handshake and `CONNECT` is never sent
in the clear.

Everything in a plaintext `INFO` arrives before the server has been authenticated. The Go client
therefore does not add `connect_urls` from a pre-TLS `INFO` to its server pool; it relies on the
`connect_info` `INFO` that arrives inside TLS after the first `PONG`
([section 3.6](#36-the-connect_info-info-and-async-info)).

> **Client rule:** Implement three TLS modes. (1) *Explicit:* read plaintext `INFO`; if
> `tls_required` is true, or the user requires TLS, perform the TLS handshake, then send
> `CONNECT`. (2) *Opportunistic:* if `tls_available` is true and the user asked for TLS, start the
> handshake immediately after `INFO` (the first byte sent must be the ClientHello). (3) *TLS-first:*
> when the user sets the option, start the TLS handshake immediately after TCP connect, without
> waiting for `INFO`, and read `INFO` inside TLS. Verify the server certificate against the host
> name the user supplied, not against `host` from `INFO`. Do not trust `connect_urls` received
> before the TLS handshake.

### 3.4 CONNECT

```
CONNECT <json>␍␊
```

`CONNECT` is the only operation that carries JSON from the client. The server decodes the JSON
**over** the connection's existing options, which start as `verbose: true, pedantic: true, echo:
true`. Consequences:

- A key that is absent keeps its previous value. **Absent `verbose`, `pedantic` and `echo` mean
  `true`.**
- Unknown keys are ignored.
- Malformed JSON closes the connection silently.
- The whole JSON counts against the control line limit.

| JSON key | Type | Effective default | Meaning |
|---|---|---|---|
| `verbose` | bool | **true** | The server sends `+OK␍␊` after `CONNECT`, each `SUB`, each `UNSUB` and each accepted `PUB`/`HPUB`. Doubles server output; real clients set it to `false`. |
| `pedantic` | bool | **true** | The only effect: on `PUB`/`HPUB` the subject is checked for empty tokens and wildcard tokens; a bad one draws `-ERR 'Invalid Publish Subject'␍␊`. The message is **still processed** and the connection stays open. |
| `tls_required` | bool | false | Decoded but never read by the server for client connections. Informational. |
| `nkey` | string | `""` | Public user nkey for nkey authentication ([section 8.3](#83-nkey-challenge)). |
| `jwt` | string | `""` | User JWT ([section 8.4](#84-jwt-and-credentials-files)). Discarded when the server is not in operator mode. |
| `sig` | string | `""` | Signature over the `INFO` `nonce`. |
| `auth_token` | string | `""` | Token authentication. |
| `user` | string | `""` | User name. |
| `pass` | string | `""` | Password, in clear text. |
| `name` | string | `""` | Connection name, shown in server logs and monitoring. |
| `lang` | string | `""` | Client implementation language, shown in monitoring. The route listener uses a non-empty `lang` to detect a client that dialed the wrong port. |
| `version` | string | `""` | Client library version, shown in monitoring. |
| `protocol` | int | 0 | `0`: original protocol. `1`: the client can handle `INFO` at any time. It enables the `connect_info` `INFO` and all asynchronous `INFO`s, and nothing else. Any other value: `-ERR 'invalid client protocol'␍␊`, close. |
| `echo` | bool | **true** | When `false`, messages published on this connection are not delivered to this connection's own subscriptions ([section 5.6](#56-echo)). |
| `headers` | bool | false | The client can send `HPUB` and receive `HMSG`. Effective only if `INFO` said `"headers":true`. |
| `no_responders` | bool | false | Enables the 503 "no responders" status message ([section 7.3](#73-no-responders)). Requires `headers` to be effective; otherwise `-ERR 'no responders requires headers support'␍␊`, close. |
| `account`, `new_account` | string, bool | `""`, false | Legacy. A non-empty `account` or a true `new_account` is an authentication failure. Never send them. |
| `proxy_sig` | string | `""` | Added by a trusted protocol-aware proxy (ADR-55); not for ordinary clients. |
| `import`, `export`, `remote_account` | | | Routes and leaf nodes only. |

Server processing order (`server/client.go`, `processConnect`): cancel the authentication timer;
re-arm the ping timer to the short "first ping" interval ([section 9.1](#91-server-to-client));
decode the JSON; record `echo`, `protocol`, `verbose`; enable headers if both sides support them;
authenticate; reject `account`/`new_account`; bind to `$G` if authentication assigned no account;
check `protocol`; check `no_responders` against headers; send `+OK` if verbose.

Because authentication runs before the `protocol` and `no_responders` checks, a client with bad
credentials *and* a bad `protocol` sees only `-ERR 'Authorization Violation'␍␊`.

A second `CONNECT` on an established connection is processed again: the connection's
subscriptions are removed from its account and authentication is re-run. No client should depend
on this.

A typical `CONNECT` from a modern client:

```
C: CONNECT {"verbose":false,"pedantic":false,"tls_required":false,"name":"billing","lang":"ruby","version":"0.1.0","protocol":1,"echo":true,"headers":true,"no_responders":true}␍␊
```

> **Client rule:** Always send `verbose`, `pedantic` and `echo` explicitly; omit none of the three.
> Send `"verbose":false,"pedantic":false`. Send `"protocol":1` and handle `INFO` at any point in
> the stream. Set `headers` and `no_responders` to the value of `headers` from `INFO` (ADR-40:
> clients hard-code header support and must not expose it as a user option). Send `lang` and
> `version`. Omit credential keys that are not in use rather than sending empty strings. Never
> send `account` or `new_account`.

### 3.5 The CONNECT + PING idiom

A successful `CONNECT` produces no output in non-verbose mode, and a failed one produces `-ERR`
followed by a close. To turn "no output" into a positive signal, every client sends `PING␍␊`
immediately after `CONNECT` and waits. The server source acknowledges the idiom ("Most client libs
send a CONNECT+PING and wait for a PONG from the server").

Because the server processes one connection's input strictly in order and answers every `PING`
with one `PONG`, the first `PONG` proves that `CONNECT` was parsed, authentication succeeded, and
the `protocol`/`no_responders` checks passed. Before it, the client must expect one of:

| Server sends | Meaning |
|---|---|
| `PONG␍␊` | Connected. |
| `+OK␍␊` then `PONG␍␊` | Connected; the client left `verbose` on. |
| `-ERR '...'␍␊` then EOF | Rejected: authorization, protocol version, limits ([section 10](#10-errors-and-connection-closes)). |
| EOF | Rejected silently (e.g. malformed `CONNECT` JSON, server shutting down). |
| `PING␍␊` | A server keepalive or RTT probe that happened to be sent first; answer it and keep waiting. The server avoids probing before the first `PONG` during the first 2 s after accept, but the client should not depend on that. |
| `INFO ...` | Cannot happen before the first `PONG` on this server version, but harmless to accept. |

> **Client rule:** After `CONNECT`, send `PING` in the same write. Consider the connection
> established only when the matching `PONG` arrives. ADR-40 defines a connection timeout (default
> 5 s) for establishing the network connection; bound the rest of the handshake (reading `INFO`,
> TLS, waiting for the `PONG`) with a deadline as well, since a server that accepts TCP and then
> stalls would otherwise hang the client. Send nothing else until the `PONG` is seen: operations
> pipelined behind the `PING` are silently lost if `CONNECT` is rejected.

### 3.6 The `connect_info` INFO and async INFO

When `CONNECT` declared `"protocol":1`, the server answers the **first** `PING` after `CONNECT`
with `PONG␍␊` immediately followed by a full `INFO` that additionally carries:

- `"connect_info":true`
- `remote_account`: the account this connection ended up in
- `acc_is_sys`: `true` if that is the system account
- `max_payload` as limited by the authenticated user and account
- the current `connect_urls`
- `client_ip`, also when it was withheld from the initial `INFO`

According to the Go client's version check, servers send this `INFO` from version 2.12 on; a
client must not wait for it unconditionally when talking to older servers.

After that first `PONG`, and only for `"protocol":1` clients, the server may send an `INFO` at any
time. Each is a complete document (all always-present keys, this connection's `client_id`,
`client_ip` and `max_payload`), never a delta, and never contains a `nonce`.

| Trigger | What differs |
|---|---|
| First `PING` after `CONNECT` | `connect_info`, `remote_account`, `acc_is_sys` as above. |
| Cluster membership changed (a server joined, left, or entered lame duck mode) | Updated `connect_urls`. TCP clients are notified only when the set of client URLs changed, WebSocket clients only when the set of WebSocket URLs changed. Suppressed by `cluster { no_advertise: true }`. |
| This server enters lame duck mode | `"ldm":true`, and `connect_urls` without this server's own addresses. Sent once. |

> **Client rule:** Accept `INFO` between any two operations for the life of the connection.
> Replace the stored server info with each new document (preserving the original `nonce`), update
> `max_payload`, merge `connect_urls` into the server pool
> ([section 11.1](#111-server-discovery)), and raise a lame-duck event when `ldm` is true. An
> `INFO` must never be mistaken for a reply to anything.

### 3.7 Connections that skip CONNECT

`CONNECT` is mandatory only when the server requires authentication. The gate is: while the
server is still waiting for `CONNECT` on an authenticated listener, any operation whose first
letter is not `C`/`c` is answered with `-ERR 'Authorization Violation'␍␊` and a close. (With a
`no_auth_user` configured, such an operation instead authenticates the connection as that user
and is processed.)

When authentication is not required the gate does not exist: `PUB`, `SUB` and `PING` work on a
fresh connection. Such a connection runs with the defaults, which means `verbose` and `pedantic`
are on, headers are off, and no asynchronous `INFO` is sent. This is what makes NATS usable from
telnet; a client library always sends `CONNECT`.

---

## 4. Protocol verbs

Sections 3.2 and 3.4 covered `INFO` and `CONNECT`. This section specifies the rest. For every
client-to-server verb, "parse error" means the silent close of
[section 2.3](#23-three-kinds-of-violation) unless an `-ERR` is named.

### 4.1 PUB

```
PUB <subject> [reply-to] <#bytes>␍␊<payload>␍␊
```

- Two arguments are subject and size; three are subject, reply subject and size. Any other count
  is a parse error (silent close).
- `#bytes` follows the size rules of [section 2.2](#22-limits). Zero is valid:
  `PUB foo 0␍␊␍␊`.
- The size is checked against `max_payload` when the control line is complete, before the payload
  is read.
- Subject and reply may contain any bytes except space, TAB, CR and LF. **The server performs no
  subject validation on publish** unless `pedantic` is on
  ([section 5.2](#52-what-the-server-validates)).
- After the payload has been read the server applies account subject mapping, checks publish
  permissions, routes the message, and (verbose only) sends `+OK`. A publish that fails the
  permission check gets `-ERR` and no `+OK`.

```
C: PUB greet.joe 5␍␊hello␍␊
C: PUB time _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F 0␍␊␍␊
```

### 4.2 HPUB

```
HPUB <subject> [reply-to] <#header-bytes> <#total-bytes>␍␊<headers><payload>␍␊
```

- Three arguments are subject, header size, total size; four add the reply subject in second
  position. Any other count is a parse error.
- `#header-bytes` is the length of the header block including its terminating blank line;
  `#total-bytes` is header bytes plus payload bytes ([section 6.2](#62-byte-counting)).
- Requires that the connection has headers enabled (server supports them and `CONNECT` said
  `"headers":true`). Otherwise the connection is closed silently.
- A missing or malformed header size, a missing or malformed total size, or a header size larger
  than the total size closes the connection silently.
- `max_payload` is compared against the **total** size.
- The header block's content is not validated in any way.
- A header size of `0` is accepted; the message is then treated as having no headers and is
  delivered as a plain `MSG`.

Byte-exact examples from ADR-4:

```
C: HPUB SUBJECT REPLY 23 30␍␊NATS/1.0␍␊Header: X␍␊␍␊PAYLOAD␍␊
C: HPUB SUBJECT REPLY 23 23␍␊NATS/1.0␍␊Header: X␍␊␍␊␍␊
C: HPUB SUBJECT REPLY 48 55␍␊NATS/1.0␍␊Header1: X␍␊Header1: Y␍␊Header2: Z␍␊␍␊PAYLOAD␍␊
```

In the first, the header block is `NATS/1.0␍␊` (10) + `Header: X␍␊` (11) + `␍␊` (2) = 23 bytes
and the payload `PAYLOAD` is 7, total 30. The second has headers and an empty payload. The third
repeats a key.

> **Client rule:** Use `PUB` when a message has no headers and `HPUB` only when it has at least
> one header or a status line. Compute both sizes from the exact bytes written. Never send `HPUB`
> on a connection whose `INFO` reported `"headers":false`; fail the publish locally.

### 4.3 SUB

```
SUB <subject> [queue-group] <sid>␍␊
```

- Two arguments are subject and sid; three are subject, queue group and sid. Any other count is a
  parse error (silent close).
- The **sid** is an opaque byte string chosen by the client. The server imposes no format, charset
  or length rule beyond "no space, TAB, CR or LF" and the control line limit. It need not be
  numeric. The server uses it as the key of the connection's subscription table and echoes it
  byte-for-byte in every `MSG`/`HMSG` for that subscription.
- **Duplicate sid:** if the connection already has a subscription with that sid, the new `SUB` is
  silently ignored. The existing subscription is kept even if the subject or queue differ. In
  verbose mode `+OK` is still sent.
- Checks, in order: subscribe permission (queue-aware; the queue name `_sys_` is always refused),
  `max_sub_tokens`, subscription count limit, subject syntax
  ([section 5.2](#52-what-the-server-validates)). Every one of these failures sends an `-ERR` and
  **leaves the connection open**; the subscription simply does not exist.
- An invalid subject yields `-ERR 'Invalid Subject'␍␊`.

```
C: SUB orders.* 1␍␊
C: SUB orders.created workers 2␍␊
```

> **Client rule:** Allocate sids from a per-connection counter that is never reused during the
> life of the client object, and render them as decimal ASCII. Keep the same sid for a
> subscription across reconnects. Because subscription failures arrive as asynchronous `-ERR`
> lines that do not name the sid, keep subject and queue for each sid so errors can be matched
> ([section 10.2](#102-permission-violations)).

### 4.4 UNSUB

```
UNSUB <sid> [max-msgs]␍␊
```

- One or two arguments; any other count is a parse error (silent close).
- `UNSUB <sid>` removes the subscription immediately.
- `UNSUB <sid> <max>` sets an auto-unsubscribe threshold: the subscription is removed
  automatically once it has delivered `max` messages.
- An unknown sid is ignored (verbose mode still sends `+OK`).

The threshold is an **absolute count on the subscription**, not "this many more". The server keeps
a per-subscription counter of messages delivered since the `SUB`. On `UNSUB <sid> <max>`:

| Condition | Result |
|---|---|
| `max` parses, `max > 0` and `max >` delivered count | Threshold stored. The subscription stays until the delivered count reaches `max`; the message that reaches it **is delivered**, then the subscription is removed. |
| `max` absent, `0`, not a valid size (non-digit, more than 9 digits), or `max <=` delivered count | Immediate unsubscribe. |

A later `UNSUB <sid> <larger>` replaces the threshold; a later plain `UNSUB <sid>` removes the
subscription at once. The counter lives on one server connection: after a reconnect and a fresh
`SUB` it starts again at zero.

Messages already written to the socket before the server processed the `UNSUB` still arrive
afterwards. ADR-47 puts it this way for the auto-unsubscribe case: the unsub can be processed
after a reply has already passed through the server, so the client must still count manually.

> **Client rule:** Track the delivered count per subscription locally and enforce the
> auto-unsubscribe limit locally: drop any message beyond the limit, and drop any `MSG` whose sid
> is unknown (it raced with an `UNSUB`). When replaying subscriptions after a reconnect, send
> `UNSUB <sid> <max - delivered>` after the `SUB`, or, if nothing remains, do not resubscribe at
> all (this is what the Go client does).

### 4.5 MSG and HMSG

```
MSG <subject> <sid> [reply-to] <#bytes>␍␊<payload>␍␊
HMSG <subject> <sid> [reply-to] <#header-bytes> <#total-bytes>␍␊<headers><payload>␍␊
```

- Arguments are separated by single spaces and the line always ends in `␍␊`.
- `MSG` has three arguments (no reply) or four; `HMSG` has four (no reply) or five.
- `sid` is exactly the bytes from the `SUB`.
- `subject` is the subject the message was *delivered* on. It is the published subject unless the
  account has subject mappings or the subscription is on an imported stream
  ([section 13.3](#133-accounts-mappings-imports-and-exports)). With a wildcard subscription it is
  the concrete subject, never the pattern.
- `reply-to` is the publisher's reply subject (with internal gateway routing prefixes removed).
- The size tokens are the **publisher's raw digit strings**, forwarded unchanged. If a publisher
  wrote `PUB foo 007`, subscribers receive `MSG foo 1 007`.
- `HMSG` is used if and only if the message has a non-empty header block **and** the receiving
  connection enabled headers. If the receiver did not, it gets a plain `MSG` with the header block
  cut off and the size reduced to `total - header`.
- For one subscriber connection, each `MSG`/`HMSG` frame (control line, payload, `␍␊`) is written
  contiguously; frames never interleave with each other or with other operations.

> **Client rule:** Parse `MSG`/`HMSG` arguments by splitting on space and TAB only (ADR-6: a
> subject may contain other whitespace-like bytes such as form feed). Decide whether a reply
> subject is present from the argument count. Parse sizes as decimal with leading zeros allowed.
> Then read exactly `#bytes` (or `#total-bytes`) plus two bytes, and treat anything other than
> `␍␊` in those two as a fatal protocol error that tears the connection down. For `HMSG`, the
> first `#header-bytes` are the header block and the remainder is the payload.

### 4.6 PING and PONG

```
PING␍␊
PONG␍␊
```

- The server answers every `PING` with exactly one `PONG`, in order, including before `CONNECT`
  on a server that does not require authentication.
- A `PONG` from the client resets the server's count of unanswered pings. An unsolicited `PONG`
  is harmless.
- `PING`/`PONG` carry no identifiers. They are matched purely by order.

Because input from one connection is processed sequentially, a `PONG` certifies that the server
has processed everything the client wrote before the corresponding `PING`. This is what clients
call *flush*: write `PING`, wait for the matching `PONG`.

> **Client rule:** Keep a FIFO queue of pending `PONG` waiters. Every `PING` the client sends
> (connect handshake, flush, keepalive) pushes one entry; every `PONG` received pops one. On
> disconnect, fail all waiters. Answer every server `PING` with `PONG` promptly, ahead of queued
> user publishes if the write path allows it.

### 4.7 +OK and -ERR

```
+OK␍␊
-ERR '<text>'␍␊
```

- `+OK` is sent only in verbose mode, after `CONNECT`, `SUB`, `UNSUB` and each accepted
  `PUB`/`HPUB`. Never for `PING`/`PONG`.
- `-ERR` text is wrapped in single quotes. The text itself can contain double quotes (subjects are
  quoted that way) and its letter case is inconsistent between messages.
- `-ERR` never closes the connection by itself; whether a close follows depends on the error
  ([section 10.1](#101--err-catalog)).
- Client to server: `+OK` is parsed and ignored. An `-ERR` sent by a client is logged and the
  server **closes the connection**.

> **Client rule:** Parse `-ERR` by taking the rest of the line, trimming whitespace, and removing
> one pair of surrounding single quotes if present. Classify by case-insensitive prefix or
> substring match against the catalog in section 10.1. Accept and ignore `+OK`. Never send `+OK`
> or `-ERR`.

---

## 5. Subjects

### 5.1 Structure and wildcards

A subject is a case-sensitive byte string split into *tokens* by `.` (period). The server stores
subscriptions in a trie keyed by token and matches a published subject token by token.

Two wildcards exist, and they are wildcards **only when they are an entire token**:

| Wildcard | Matches | Example |
|---|---|---|
| `*` | Exactly one token at that position. | `orders.*.created` matches `orders.eu.created`, not `orders.created` or `orders.eu.x.created`. |
| `>` | One **or more** remaining tokens; valid only as the last token. | `orders.>` matches `orders.eu` and `orders.eu.created`, but **not** `orders`. |

Inside a longer token (`foo*`, `a>b`) the characters `*` and `>` are ordinary. Wildcards have
meaning only in subscriptions (and in permission entries); in a published subject they are just
bytes (see below).

### 5.2 What the server validates

Far less than one would expect:

| Operation | Checked | On violation |
|---|---|---|
| `SUB` subject | Every token is non-empty, and nothing follows a `>` token. **No character checks**: control bytes, non-UTF-8 and form feed are all accepted (space, TAB, CR, LF cannot occur because they split arguments). | `-ERR 'Invalid Subject'␍␊`; no subscription; connection stays open. |
| `PUB`/`HPUB` subject, `pedantic` off | **Nothing.** A subject with an empty token (`foo..bar`, `.foo`, `foo.`) matches no one and vanishes. A subject containing a `*` or `>` token is looked up literally: publishing to `foo.*` reaches subscriptions on `foo.*` and `foo.>`, not one on `foo.bar`. | Silent. |
| `PUB`/`HPUB` subject, `pedantic` on | No empty token, and no token that is exactly `*` or `>`. Checked on the control line, against the subject before account mapping. | `-ERR 'Invalid Publish Subject'␍␊`; the message is **still processed**; not closed. |
| Reply subject | No syntax check. Rejected only for reserved prefixes ([section 5.4](#54-reserved-prefixes-and-names)). | `-ERR 'Permissions Violation for Publish with Reply of "<reply>"'␍␊`; message dropped; not closed. |
| Queue group name | Any bytes without space, TAB, CR, LF. `_sys_` is always refused. | `-ERR 'Permissions Violation for Subscription to "<subject>" using queue "<queue>"'␍␊`. |

The server does contain stricter validators (rejecting whitespace inside tokens, NUL, invalid
UTF-8), but they are used for configuration, permissions, mappings and JetStream, not on the core
`PUB`/`SUB` path.

One trap follows from argument splitting (ADR-40 points it out): a subject containing a space
changes the meaning of the line instead of producing an error. `SUB foo bar 1` is a subscription
to `foo` in queue group `bar`; `PUB foo bar 5` publishes to `foo` with reply `bar`; one more
stray token and the argument count is wrong, which closes the connection silently.

> **Client rule:** Validate before writing, because the server will not (ADR-6 sets the minimum:
> reject empty subjects and subjects, replies or queue names containing whitespace). Reject, with
> a local error: an empty subject, reply or queue name; any containing space, TAB, CR or LF; a
> subscription subject with an empty token or with `>` anywhere but last. For publish subjects,
> additionally reject empty tokens and whole-token wildcards unless the user explicitly disables
> the check. Apply the same checks to reply subjects.

### 5.3 Lengths

There is no maximum subject length, token length or token count on the core path. The effective
bound is the control line limit on the whole argument string of the `PUB`/`HPUB`/`SUB` line
(subject plus reply or queue plus sid or sizes plus separators). `max_sub_tokens`, if configured,
caps the number of tokens on `SUB` only.

### 5.4 Reserved prefixes and names

| Prefix or name | Purpose | What a client observes |
|---|---|---|
| `_INBOX.` | Client convention for reply subjects. | Nothing; the server does not treat it specially. |
| `$JS.` | JetStream API and acknowledgment subjects. | Publishing *to* `$JS.API.>` is the normal JetStream API. Using any subject starting with `$JS.` (and longer than 4 bytes) as a **reply** subject is a reply violation. ADR-6 lists only `$JS.ACK.` as reserved; the server at this version is broader. |
| `_R_.` | Server-generated replies for service imports. | Not allowed as a client reply subject. |
| `_GR_.`, legacy `$GR.` | Gateway-routed replies. | Publishing to such a subject: `-ERR 'Permissions Violation for Publish to "<subject>"'`, dropped. As a reply subject: reply violation. |
| `$NRG.` | Raft groups. | Publishing from a client outside the system account is a publish violation. |
| `$SYS` | The default system account's name and the prefix of system subjects (`$SYS.REQ.>`, `$SYS.ACCOUNT.>`, `$SYS.SERVER.>`, `$SYS._INBOX.>`). | No hard-coded check; isolation comes from accounts. A few `$SYS.REQ.>` services are imported into every account ([section 13.2](#132-client-usable-system-subjects)). |
| `$G` | Name of the global account. An account name, **not** a subject prefix. | Connections that authenticate without an account land here. |
| `$MQTT.` | MQTT bridge internals. | Nothing for a NATS client. |
| `_sys_` | Reserved **queue group** name. | `SUB` with it is a subscription violation. |
| `$SYS.REQ.USER.AUTH` | Auth callout request subject. | Users of the auth callout account are automatically denied on it. |

All violations in this table are of the "`-ERR`, connection stays open" kind.

> **Client rule:** Generate reply subjects only under the inbox prefix (default `_INBOX.`). If
> the inbox prefix is user-configurable, reject prefixes beginning with `$JS.`, `_R_.`, `_GR_.` or
> `$GR.`, and prefixes containing wildcards or whitespace.

### 5.5 Queue groups

A subscription created with a queue group name is a member of that group. For each published
message the server delivers one copy to every matching plain subscription **and** one copy to one
member of each matching queue group. Plain subscribers and different groups are independent of one
another.

- Group identity is the **queue name alone**, across all matching subjects. `SUB foo.* q 1` and
  `SUB foo.bar q 2` are members of the same group for a message published to `foo.bar`; only one
  of them receives it.
- Selection is random (uniform among local members; weighted when members on other servers are
  involved). The server starts at a random member and delivers to the **first member that can take
  the message**, so a member that cannot (closed, excluded by `echo:false`, filtered by a deny
  permission) is skipped rather than causing a loss.
- When a message arrives from another server in the cluster, members on the local server are
  preferred.
- Queue names have no wildcard meaning in `SUB`. In permission entries of the form
  `"<subject> <queue>"` the queue part may contain wildcards.

### 5.6 Echo

By default a connection receives its own publishes on its own matching subscriptions. With
`"echo":false` in `CONNECT`, the server skips delivery whenever the publishing connection and the
subscribing connection are the same. The scope is the **connection**, not the process or the
user: two connections of one application still see each other's messages. It applies to queue
subscriptions as well. `proto >= 1` in `INFO` signals that the server supports it.

### 5.7 Delivery ordering

The server reads and dispatches one connection's input sequentially, and appends each delivered
message whole to the subscriber's outbound buffer. Therefore, **on a single server, messages from
one publishing connection arrive at one subscribing connection in publish order**, across all of
that subscriber's subscriptions. Messages from different publishers interleave in arbitrary order.

No ordering guarantee is stated in any ADR, and the multi-server paths (routes, gateways, leaf
nodes) were not analyzed; treat cross-server ordering as unverified.

---

## 6. Headers

Headers attach metadata to a message in the style of HTTP/1.1 header fields (ADR-4). They exist
only in `HPUB` and `HMSG`. The server uses them for status signaling (no responders, JetStream
status) and for JetStream publish options; otherwise it forwards them untouched.

### 6.1 Wire format

```
NATS/1.0␍␊
<name>: <value>␍␊
<name>: <value>␍␊
␍␊
```

- The first line is the version line, exactly `NATS/1.0` followed by `␍␊`. On messages generated
  by the server it may carry an inline status: `NATS/1.0 <code>` or `NATS/1.0 <code> <description>`,
  separated by single spaces; the description may itself contain spaces.
- Zero or more field lines follow, each `name`, a colon, the value, `␍␊`. Writers emit a single
  space after the colon; readers trim whitespace around the value.
- A blank line (`␍␊`) ends the block. The smallest legal block is `NATS/1.0␍␊␍␊`, 12 bytes.

ADR-4 rules for names and values: a name is one or more printable ASCII characters (33 to 126)
except `:`; a value is any ASCII except CR and LF. A key may repeat; repeated keys are serialized
as one line per value, never comma-joined.

### 6.2 Byte counting

`#header-bytes` covers the whole block: from the `N` of `NATS/1.0` up to and **including** the
blank line's `␍␊`. `#total-bytes` is `#header-bytes` plus the payload length. The frame's final
`␍␊` after the payload is counted in neither.

```
NATS/1.0␍␊            10 bytes
Header: X␍␊           11 bytes
␍␊                     2 bytes     header-bytes = 23
PAYLOAD                7 bytes     total-bytes  = 30
␍␊                    (not counted)
```

The server depends on this definition: when it strips headers for a subscriber without header
support it sends the bytes from offset `#header-bytes` on, which is only the clean payload if the
count includes the blank line.

### 6.3 Status lines

A message whose version line carries a three-digit code is a *status message*: it is information
from the server (or a service) rather than application data. In core NATS exactly one exists:
`NATS/1.0 503`, no responders ([section 7.3](#73-no-responders)), with no description. JetStream
adds 100, 204, 400, 404, 408, 409 and others, each with descriptions that matter; see
[03-consumers.md](03-consumers.md).

The Go client decodes the version line as follows, and its behavior is the de facto contract:
the line must begin with the 8 bytes `NATS/1.0`; if anything follows, the next byte must be a
space or TAB; the rest is trimmed; its first three characters are the status code and whatever
remains, trimmed, is the description. The code and description are exposed as the pseudo-headers
`Status` and `Description`.

### 6.4 Case sensitivity and repeated keys

Header names are **case-sensitive and never canonicalized**. Every server-side lookup is an exact
byte comparison of the name preceded by `␍␊` and followed by `:`. The server source states the
intent: header name case is preserved. This is load-bearing: JetStream names such as `Nats-TTL`
and `Nats-UpTo-Sequence` would not survive HTTP-style canonicalization.

Repeated keys are legal on the wire, but **every server-side lookup stops at the first match**. A
second `Nats-Msg-Id` line is ignored, not merged.

Because a lookup requires the name to be preceded by `␍␊`, a field at byte offset 0 can never be
found: the version line is structurally required even though the server never checks its content.

ADR-4 specifies the client API: preserve case; all operations are case-sensitive; "get" returns
the first value of a key and "values" all of them; set, append and delete operate on exact names.
A case-insensitive lookup mode is optional.

### 6.5 The client's sanitization duty

**The server does not validate a header block it receives.** `HPUB` processing checks only the
size arithmetic. A block without the `NATS/1.0` line is accepted and forwarded verbatim to
subscribers. A value containing `␍␊` therefore injects an extra header line that the server's own
lookups will honor (for example a forged `Nats-Msg-Id` or `Nats-Expected-Stream`), and a value
containing `␍␊␍␊` moves the header/payload boundary.

> **Client rule:** When serializing headers: always write `NATS/1.0␍␊` first; reject (or drop)
> names that are empty or contain anything outside printable ASCII 33 to 126 or a `:`; reject
> values containing CR or LF (the Go client replaces them with spaces; rejecting is safer); write
> `name: value␍␊` once per value in insertion order; end with `␍␊`. Never fold case.
>
> **Client rule:** When parsing a received block: require the 8-byte prefix `NATS/1.0` and treat
> its absence as a malformed message (deliver an error, do not tear down the connection); parse an
> inline status as described in 6.3; split each remaining line at the first `:`; trim the value;
> keep names exactly as received; keep all values of repeated keys in order; stop at the blank
> line. Tolerate lines ending in a bare LF.

### 6.6 Header stripping

If a message with headers is delivered to a connection that did not enable headers, the server
sends `MSG` with only the payload. The headers are lost, including status lines: to such a client
a status message is indistinguishable from an empty application message, which is why
`no_responders` cannot be enabled without `headers`. A modern client always enables headers and
never observes stripping on its own connection, but its messages may be stripped on the way to
older subscribers.

### 6.7 Reserved header names

Names beginning with `Nats-` belong to the server and JetStream. The ones relevant to core:

| Header | Set by | Meaning |
|---|---|---|
| `Nats-Subject` | server | On the 503 status message: the subject the undeliverable request was published to. |
| `Nats-Trace-Dest`, `Nats-Trace-Only` | client | Message tracing ([section 13.1](#131-message-tracing)). |
| `Nats-Trace-Hop`, `Nats-Trace-Origin-Account` | server | Tracing internals; a client must not set them. |
| `Nats-Request-Info` | server | Information about the requesting client (JSON) that the server attaches to requests on the service-import path. |
| `Nats-Server-Xkey` | server | Auth callout. |

The JetStream publish and delivery headers (`Nats-Msg-Id`, `Nats-Expected-*`, `Nats-Rollup`,
`Nats-TTL`, `Nats-Sequence`, `Nats-Time-Stamp`, `Nats-Stream`, `Nats-Last-Sequence`,
`Nats-Num-Pending`, `Nats-Pin-Id`, and so on) are specified in [02-jetstream.md](02-jetstream.md)
and [03-consumers.md](03-consumers.md).

---

## 7. Request/reply

### 7.1 Mechanics

Request/reply is a pattern, not a protocol feature. The requester publishes a message with a
reply subject; a responder receives that reply subject in its `MSG` and publishes its answer to
it; the requester receives the answer on a subscription that matches the reply subject. The server
adds exactly three things: it refuses reserved prefixes in reply subjects
([section 5.4](#54-reserved-prefixes-and-names)), it can tell the requester that nobody was
listening ([section 7.3](#73-no-responders)), and it can grant responders a temporary permission
to publish to the reply subject ([section 7.4](#74-reply-permissions)).

Since any number of subscribers may match the request subject, a request can produce zero, one or
many replies. Queue groups are the usual way to make it "at most one".

### 7.2 Inboxes

An *inbox* is a unique subject used as a reply address. The naming is a client convention; the
server does not care.

- **Inbox subject:** `_INBOX.` followed by a NUID, 7 + 22 = 29 characters. A NUID is a 22-character
  base-62 string (alphabet `0-9A-Za-z`): a 12-character cryptographically random prefix and a
  10-character sequence that starts random and increases by a random step of 33 to 332 per id;
  when the sequence overflows `62^10` the prefix is regenerated.
- **One subscription per request** (the original style): `SUB <inbox> <sid>`, `UNSUB <sid> 1`,
  `PUB <subject> <inbox> ...`. Simple, but costs two extra operations and a change to the
  server's subscription table for every request.
- **Muxed inbox** (what every modern client does): on the first request the client creates one
  NUID inbox prefix and subscribes once to `<prefix>.*`, e.g. `SUB _INBOX.<nuid>.* <sid>`. Each
  request then uses the reply subject `_INBOX.<nuid>.<token>` where `<token>` is unique among
  in-flight requests (the Go client uses 8 random base-62 characters, giving a 38-character reply
  subject). Incoming messages on the wildcard subscription are routed to the waiting request by
  the last token. The subscription lives as long as the connection and is re-established on
  reconnect like any other.

The Go client's response router has one defensive fallback worth copying: if the delivered subject
does not start with the mux prefix (the server rewrote it, which can happen across account
imports) and exactly one request is pending, the message is given to that request.

> **Client rule:** Implement requests on a muxed inbox: one lazily created wildcard subscription
> per connection, a map from token to waiting request, entries removed on reply, timeout or
> cancellation. A reply for an unknown token is dropped silently (it is a late reply). Keep the
> inbox prefix stable for the life of the client object, so that after a reconnect the replayed
> wildcard subscription still matches the reply subjects of requests in flight; leave those
> requests pending across a reconnect (a reply published after the resubscribe still arrives,
> anything else ends in the request's own timeout) and fail them when the connection is closed for
> good. This is what the Go client does. Use a fresh inbox subscription with `UNSUB <sid> <n>`
> only for patterns that need a dedicated subscription.

### 7.3 No responders

Without help, a request to a subject with no subscribers looks exactly like a slow responder: the
requester waits for its full timeout. With `"no_responders":true` in `CONNECT` the server reports
the condition immediately.

The server sends the status message when **all** of these hold at the moment it routes the
request:

1. The published message had a reply subject.
2. The message was delivered to no subscriber, neither locally nor through the cluster.
3. The publisher's `CONNECT` had `"no_responders":true` (which required `"headers":true`).
4. The **publishing connection itself** has a plain (non-queue) subscription matching the reply
   subject.

It then writes to the publisher, on that matching subscription's sid:

```
HMSG <reply> <sid> <n> <n>␍␊NATS/1.0 503␍␊Nats-Subject: <subject>␍␊␍␊␍␊
```

where `n = 32 + len(subject)`: `NATS/1.0 503␍␊` is 14 bytes, `Nats-Subject: ` is 14, the
subject, `␍␊` is 2, the blank line is 2. Properties of this frame:

- The delivered subject is the request's reply subject, so it lands in the requester's inbox like
  a real reply.
- It has no reply subject of its own, a zero-length payload (header size equals total size), and
  no status description.
- `Nats-Subject` carries the subject the request was published to, after account subject mapping.
- It is sent whether the request was a `PUB` or an `HPUB`.

If condition 4 fails, nothing is sent: a requester whose reply subscription is on a *different
connection*, or is a queue subscription, gets no 503 and must rely on its timeout. A 503 is also
only a statement about the instant of publication; a responder that subscribes a millisecond
later does not help.

ADR-47 classifies a status arriving in place of a reply as terminal for the request. ADR-22 lets
JetStream publishes retry on 503, because in a clustered JetStream a 503 can simply mean a
leadership election is in progress; the Go client waits 250 ms and retries twice (see
[02-jetstream.md](02-jetstream.md)).

> **Client rule:** Recognize no-responders as: a message with headers, status code `503`, and an
> empty payload. Fail the matching request immediately with a distinct "no responders" error. Do
> not surface it as a message to user code in the request path. On a plain subscription that the
> user manages (e.g. a reply subject they supplied to `publish`), deliver it as a message with
> its status visible.

### 7.4 Reply permissions

A responder whose publish permissions are restricted cannot normally publish to arbitrary inbox
subjects. The server-side permission `allow_responses` solves this without wildcards: when a
message **with a reply subject** is delivered to a connection that has `allow_responses` and is
not otherwise allowed to publish to that reply subject, the server records a temporary grant for
exactly that subject on that connection.

| Setting | Default when enabled | Meaning |
|---|---|---|
| `max` | 1 | Number of messages the responder may publish to the reply subject. |
| `expires` | 2 minutes | Lifetime of the grant. |

Each publish to a granted subject consumes one use; the grant disappears when its count or its
time is exceeded. After that, a publish to the reply subject is an ordinary publish violation
(`-ERR 'Permissions Violation for Publish to "<reply>"'␍␊`, connection stays open). No grant is
recorded for JetStream API request subjects.

> **Client rule:** A responder cannot know how many replies it may send. Report a publish
> permission violation on a reply subject to the user as an asynchronous error; do not retry.

### 7.5 Multiple replies (request-many)

ADR-47 (status: partially implemented) specifies how a client gathers several replies to one
request. It is purely client-side.

| Control | Rule |
|---|---|
| Total timeout | Always in force; defaults to the connection's request timeout. The wait for the *first* message always uses the total timeout. |
| Stall timer | Optional. Maximum gap between subsequent messages; reaching it ends the request normally. A value below 1 or not less than the total timeout counts as not supplied. Each wait is the lesser of the stall time and the remaining total time. |
| Max messages | Optional. The request completes when this many replies have arrived. |
| Sentinel | Optional. A user predicate that ends the request; the "standard sentinel" is a message with an empty payload. |

Further rules: request-many is not recoverable (a client does not retry it by itself); a status
message such as 503 is terminal and should be reported together with the end-of-data indication;
time spent in blocking user callbacks must not count against the timers; the user must be told
when the request has ended if the delivery mechanism does not make that obvious. Named strategies
are just presets: *timeout* (total only, the default), *stall* (stall = the lesser of one tenth of
the total and the default timeout), *max responses*.

Subscription handling: the client always unsubscribes when the request completes. With a
dedicated inbox and a maximum, it may send `UNSUB <sid> <max>` right after `SUB` as an
optimization but must still count locally. With the muxed inbox it must not close the shared
subscription and cannot use that optimization; the token stays registered until completion.

---

## 8. Authentication

All authentication data travels in `CONNECT`, optionally bound to the connection by a signature
over the `nonce` from the initial `INFO`. The server never sends a challenge other than that
nonce and never explains a failure.

### 8.1 What the server decides, and uniform failure

The server tries mechanisms in a fixed order: custom authenticator, trusted-proxy check, user JWT
(operator mode), nkey users, user/password or TLS-certificate users, single token, single
user/password; an auth callout, if configured, can take over the decision.

If the server does not require authentication it accepts **any** `CONNECT`, whatever credentials
it carries.

Every credential failure produces the same output:

```
S: -ERR 'Authorization Violation'␍␊
```

followed by a close. Wrong password, unknown user, bad signature, expired or revoked JWT,
disallowed connection type, source address or time of day all look identical to the client; the
real reason exists only in server logs and system events. The one exception is an account
connection limit: `-ERR 'maximum account active connections exceeded'␍␊` and close.

> **Client rule:** Choose the mechanism from what the user configured, never from what the server
> appears to want, with one check: if the user configured an nkey and the initial `INFO` has no
> `nonce`, fail locally (the server is not set up for nkeys; the Go client does the same). Treat
> `Authorization Violation` during connect as an authentication error for that server and move on
> to the next server. ADR-40: when the client finally gives up, this must be reported as the last
> error.

### 8.2 Token and user/password

```
C: CONNECT {"verbose":false,"pedantic":false,"auth_token":"s3cr3t",...}␍␊
C: CONNECT {"verbose":false,"pedantic":false,"user":"alice","pass":"wonderland",...}␍␊
```

- The client always sends the secret in clear text. A server may *store* it as a bcrypt hash
  (stored value beginning `$2a$`, `$2b$`, `$2x$` or `$2y$`), in which case it hashes what the
  client sent; otherwise it compares in constant time. The client cannot tell and need not care.
- In a server URL, `nats://user:pass@host` supplies user and password, and user info without a
  password (`nats://token@host`) is taken as a token. That mapping is a client convention (Go
  client).

> **Client rule:** Use TLS whenever a token or password is sent. Never log the `CONNECT` line.

### 8.3 NKey challenge

An nkey is an Ed25519 key pair with a text encoding; a user's public key is a 56-character string
beginning with `U`, its private *seed* a string beginning with `SU`. The server configuration
lists public keys; the client proves possession of the seed.

1. The initial `INFO` carries `nonce` (15 characters).
2. The client signs **the nonce string exactly as received**: the 15 ASCII bytes of the JSON
   string value, not the 11 bytes they decode to.
3. The signature is plain Ed25519 (64 bytes), encoded as **unpadded URL-safe base64** (86
   characters). The server also accepts standard padded base64 as a fallback.
4. `CONNECT` carries `"nkey":"<public key>"` and `"sig":"<signature>"`.

A missing `sig` is a failure. The byte layout of the nkey text encoding (prefix bytes, base32,
checksum) is defined by the `nkeys` library and is not covered here; use an nkeys implementation.

### 8.4 JWT and credentials files

In operator mode (decentralized authentication) the server trusts a chain of JWTs: an operator
signs accounts, an account signs users. The client presents its **user JWT** and proves possession
of the user's nkey seed.

- `CONNECT` carries `"jwt":"<user JWT>"` and `"sig":"<signature>"`. The signed bytes and the
  encoding are exactly as in 8.3.
- The signature is verified against the user public key in the JWT's `sub` claim. The `nkey` field
  of `CONNECT` plays no part; the Go client refuses to combine a JWT with an nkey option.
- If the user JWT is a **bearer token** (its claims set `bearer_token`), the server skips the
  nonce signature entirely and `sig` may be omitted. An account can forbid bearer tokens.
- Without a JWT in `CONNECT` the server falls back to a configured `default_sentinel` JWT or
  fails.
- Outside operator mode the server discards the `jwt` field.
- Remember the control line limit: JWT plus signature plus the rest of `CONNECT` must fit in 4096
  bytes unless the operator raised `max_control_line`.

Things the server checks beyond the signature, all reported as `Authorization Violation`: JWT
validity and expiry, issuer trust and signing-key scope, account expiry, revocation, allowed
connection types (`STANDARD` for TCP clients, `WEBSOCKET` for WebSocket clients), source network
allow-list, permitted times of day.

**The `.creds` file.** JWT and seed are usually distributed together in a *credentials file*
(also called a chained file). The format is a client-side convention; the server never sees it.
The layout, as it appears in the Go client's test fixtures:

```
-----BEGIN NATS USER JWT-----
eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOi...
------END NATS USER JWT------

************************* IMPORTANT *************************
NKEY Seed printed below can be used to sign and prove identity.
NKEYs are sensitive and should be treated as secrets.

-----BEGIN USER NKEY SEED-----
SUAMK2FG4MI6UE3ACF3FK3OIQBCEIEZV7NSWFFEW63UXMRLFM2XLAXK4GY
------END USER NKEY SEED------
```

Note the asymmetric decoration: five dashes before `BEGIN`, six around `END`. The JWT is the
single line after the `BEGIN NATS USER JWT` line and the seed is the single line after the
`BEGIN USER NKEY SEED` line. The Go client delegates extraction to
`nkeys.ParseDecoratedJWT` and `nkeys.ParseDecoratedNKey`, whose exact tolerance (dash counts,
other key types, a bare JWT without decoration) was not verified because the `nkeys` source was
not available; the Ruby client `nats-pure` simply takes the line following the first line that
contains the `BEGIN ...` marker. The Go client also accepts the JWT and the seed as two separate
files.

The Go client re-reads the file and re-derives the key pair for every connect attempt and wipes
the seed from memory afterwards. Re-reading matters functionally: it is how a rotated credentials
file is picked up on reconnect.

> **Client rule:** Model nkey and JWT authentication as two callbacks, evaluated on **every**
> connect and reconnect attempt: one returning the JWT (absent for plain nkey auth) and one that
> signs a nonce. Put `base64url_nopad(sign(nonce_bytes))` in `sig`. For a `.creds` path, read the
> file inside the callbacks, extract the line after each `BEGIN` marker, strip whitespace, and do
> not retain the seed.

### 8.5 TLS certificate mapping

With `verify_and_map` the user is derived from the client certificate presented in the TLS
handshake; `CONNECT` carries no credentials (a `user` sent anyway is ignored with a server
warning). The server tries, in order: e-mail SANs, DNS SANs, URI SANs, then the subject
distinguished name in RFC 2253 form and variants of it. The client's only job is to present the
certificate.

### 8.6 Auth callout

With auth callout the server forwards the `CONNECT` credentials (`jwt`, `nkey`, `sig`,
`auth_token`, `user`, `pass`, `name`, `lang` and so on, with client and TLS details) to an
external service on `$SYS.REQ.USER.AUTH` and binds the connection as that service decides. The
client sends an ordinary `CONNECT` with whatever the service expects. The callout shares the
authentication timeout, so connects can take noticeably longer. Failure is the standard
`Authorization Violation`.

### 8.7 `no_auth_user`

A server may designate a default user for connections that present no credentials. Then `INFO`
does **not** say `auth_required`, and a `CONNECT` without `user`, `pass`, `auth_token` (and
without `nkey`) is bound to that user with its permissions. A client cannot distinguish this from
an open server except through the `connect_info` `INFO` or `$SYS.REQ.USER.INFO`.

### 8.8 Expiry and revocation

Authentication is not a one-time event. For a JWT user the server arms a timer for the earlier of
the JWT's expiry and the end of its allowed time-of-day window.

| Event | Server output | Then |
|---|---|---|
| User JWT expires | `-ERR 'User Authentication Expired'␍␊` | close |
| Account JWT expires | `-ERR 'Account Authentication Expired'␍␊` | close |
| User revoked in the account | `-ERR 'User Authentication Revoked'␍␊` | close |

ADR-40 notes that a reconnect with the same expired credentials is then refused with
`Authorization Violation`, and that in practice clients have been observed to get a bare
disconnect instead of `User Authentication Revoked`. The remaining lifetime can be queried from
`$SYS.REQ.USER.INFO` ([section 13.2](#132-client-usable-system-subjects)).

> **Client rule:** Treat all three as: report to the asynchronous error handler, then reconnect
> normally. Because the credential callbacks run on each attempt, refreshed credentials are picked
> up automatically. To avoid hammering a server with dead credentials, follow the Go client: if
> the same authentication error is received twice in a row from the same server, stop
> reconnecting and close, unless the user opted out of that behavior.

### 8.9 Authentication timeout

When authentication is required the server allows a fixed time for `CONNECT` to arrive: 2 s by
default without TLS, `tls.timeout + 1` (3 s by default) with TLS, counted from the end of the TLS
handshake; configurable under `authorization { timeout }`. On expiry:

```
S: -ERR 'Authentication Timeout'␍␊
```

and close. The timer is not armed on servers without authentication.

> **Client rule:** Send `CONNECT` as soon as `INFO` is parsed and TLS is up. Signing callbacks
> that block (hardware tokens, remote signers) eat into a budget of roughly two seconds.

---

## 9. Keepalive

Both sides probe the connection with `PING` and expect `PONG`. This catches half-open TCP
connections and peers that are alive but stuck.

### 9.1 Server to client

| Item | Value |
|---|---|
| `ping_interval` | Default 2 minutes. |
| `ping_max` | Default 2 outstanding pings. |
| First timer after `CONNECT` | Shortened to about 2 s (2.0 to 2.4 s with jitter), unless the server disables it. Because the server has no round-trip-time measurement yet, **it sends a `PING` at that point** to every new client. |
| Later timer ticks | Every `ping_interval`. The `PING` is skipped if the client sent a message, a `SUB`/`UNSUB` or a `PING` within the last interval, unless an RTT measurement is due (at most once per hour). |
| What counts as client activity | `PUB`/`HPUB`, `SUB`, `UNSUB` and `PING`. A `PONG` alone does not count as activity; it only clears the outstanding-ping counter. |
| Stale detection | When the timer fires, a `PING` is due, and `ping_max` pings are already unanswered: `-ERR 'Stale Connection'␍␊`, close. With defaults that is two unanswered `PING`s, detected at the third tick. |
| RTT probes | The server may send an extra `PING` at other times to measure round-trip time, for example when it first delivers a request to a connection. |

### 9.2 Client to server

ADR-40 requires the client to run its own timer: default interval 2 minutes, default maximum of
2 outstanding pings; "if two consecutive PONGs are missed, connection is marked as lost
triggering reconnect attempt". The Go client implements it as: on each tick, increment the
outstanding count; if it now exceeds the maximum, declare the connection stale and reconnect;
otherwise send `PING` and re-arm. Any `PONG` resets the count to zero.

> **Client rule:** Answer every `PING` with `PONG` immediately; a reader that is blocked in user
> code must not delay it. Run a ping timer (default 2 min, max 2 outstanding); on overflow treat
> the connection as dead: close the socket and start the reconnect procedure with a
> "stale connection" error. Client-side keepalive pings share the FIFO `PONG` queue with flushes
> (section 4.6). Expect a server `PING` about two seconds after every connect.

---

## 10. Errors and connection closes

### 10.1 `-ERR` catalog

Every `-ERR` text a client connection can receive from this server version, with exact spelling
and case. `<...>` marks variable parts.

**Followed by a close:**

| Text | Cause |
|---|---|
| `Unknown Protocol Operation` | The parser did not recognize an operation (bad first byte, misspelled verb, blank line, payload not followed by `␍␊`). |
| `maximum control line exceeded` | Control line arguments longer than `max_control_line`. No further detail is sent. |
| `Maximum Payload Violation` | `PUB`/`HPUB` size above `max_payload`. |
| `Authorization Violation` | Any authentication failure; an operation other than `CONNECT` first on a server requiring authentication; legacy `account`/`new_account` in `CONNECT`. |
| `Authentication Timeout` | No `CONNECT` within the authentication timeout. |
| `User Authentication Expired` | User JWT or its allowed time window expired. |
| `Account Authentication Expired` | Account JWT expired. |
| `User Authentication Revoked` | User revoked. |
| `invalid client protocol` | `protocol` in `CONNECT` not 0 or 1. |
| `no responders requires headers support` | `no_responders` without effective `headers`. |
| `maximum connections exceeded` | Server `max_connections` reached. Sent after the initial `INFO`. |
| `maximum account active connections exceeded` | The account's connection limit was reached during `CONNECT`. |
| `Connection throttling is active. Please try again later.` | TLS `connection_rate_limit` exceeded. ADR-40: treat like `maximum connections exceeded`. |
| `Secure Connection - TLS Required` | TLS handshake not completed within `tls.timeout`. |
| `Stale Connection` | The client stopped answering `PING`. |
| `attempted to connect to route port` | A client dialed the cluster port. |
| `attempted to connect to leaf node port` | A client dialed the leaf node port. |
| `attempted to connect to gateway port` | A client dialed the gateway port. |

**Connection stays open:**

| Text | Cause and effect |
|---|---|
| `Invalid Subject` | `SUB` with an empty token or a misplaced `>`. No subscription created. |
| `Invalid Publish Subject` | Pedantic mode only. The message is processed anyway. |
| `Permissions Violation for Publish to "<subject>"` | Publish not permitted (or reserved subject, or tracing destination not permitted). Message dropped. |
| `Permissions Violation for Publish with Reply of "<reply>"` | Reply subject uses a reserved prefix. Message dropped. |
| `Permissions Violation for Subscription to "<subject>"` | Subscribe not permitted. No subscription created. |
| `Permissions Violation for Subscription to "<subject>" using queue "<queue>"` | As above, for a queue subscription. |
| `Permissions Violation for Subscription to "<subject>", too many tokens` | `max_sub_tokens` exceeded. |
| `Permissions Violation for Subscription to "<subject>" (sid "<sid>")` and `... using queue "<queue>" (sid "<sid>")` | An **existing** subscription was removed because permissions changed at runtime. The only errors that name the sid. |
| `maximum subscriptions exceeded` | At `SUB`: subscription not created, connection stays open. If a limit is lowered at runtime below the current count: the same text, then a close about 20 ms later. |
| `Failed Account Registration` | Internal error binding the connection to its account. ADR-40 has no reproduction for it. |

Facts that shape error handling:

- **Case is inconsistent.** Some texts are lower case, others title case. Match
  case-insensitively.
- **The variable parts are Go-quoted** (`%q`): wrapped in double quotes, with non-printable bytes
  and embedded quotes backslash-escaped. Since subjects, queues and sids cannot contain spaces,
  the quoted tokens never contain spaces either, which is what makes ADR-40's patterns
  `/(Publish|Subscription) to (\S+)/` and `/using queue "(\S+)"/` workable.
- **The client cannot tell from an `-ERR` alone whether a close follows** (ADR-40 says so
  explicitly). It must classify by text and still be ready for EOF at any time.

### 10.2 Permission violations

Permissions are per connection: allow and deny lists for publish and for subscribe, plus the
optional response permission of [section 7.4](#74-reply-permissions). No allow list means "allow
all"; deny overrides allow.

- **Publish** is checked per message after the payload has been read (the result is cached per
  subject). A violation drops the message and sends the `-ERR`. With account subject mapping in
  effect the check, and the subject in the error text, use the mapped subject.
- **Subscribe** is checked at `SUB`. A violation means the subscription was never created. The
  error does not carry the sid.
- **Deny entries and wildcard subscriptions:** a wildcard or queue subscription that merely
  *overlaps* a deny entry is accepted, and the server silently withholds the denied messages at
  delivery time. There is no `-ERR` for those.
- **Runtime changes** (configuration reload, JWT update) can remove an established subscription;
  the client is told with the `(sid "<sid>")` variant.

ADR-40 specifies the client reaction: a subscription violation is **terminal for the
subscription** (the server holds no interest for it and will never deliver on it) but not for the
connection. The client finds the first subscription matching the subject and queue from the error
text and notifies it; the server sends one error per offending `SUB`, so several subscriptions on
the same subject each get their turn. A publish violation is an asynchronous runtime error. In
both cases the general asynchronous error handler is notified as well, and nothing is thrown into
unrelated user code.

> **Client rule:** On `Permissions Violation for Subscription`: parse subject and optional queue
> (strip the double quotes and the trailing `,` of the "too many tokens" form), locate the
> subscription (by sid when present, otherwise the first live subscription with that subject and
> queue), mark it failed with a permission error, and notify both that subscription and the
> connection's async error handler. Keep it in the resubscribe set and clear the error on
> reconnect: permissions may have been fixed (Go client behavior). On `Permissions Violation for
> Publish`: notify the async error handler; if the subject is a pending request's subject, fail
> that request.

### 10.3 Slow consumers

The server never blocks on a slow client and never tells it that it is slow.

- If more than `max_pending` bytes (default 64 MiB) are queued for a client, or a socket write
  blocks longer than `write_deadline` (default 10 s), the server logs "Slow Consumer Detected" and
  closes the connection. **There is no `Slow Consumer` `-ERR`**; that phrase exists only in server
  monitoring. Data still queued is discarded, not flushed.
- Before that point, when a subscriber's queue exceeds 75% of `max_pending`, the server slows the
  *publishers* feeding it: a publishing client connection is paused 2 to 5 ms per delivery to the
  stalled subscriber, at most 10 ms per read pass. With `no_fast_producer_stall: true` the server
  instead drops the message for that subscriber.

> **Client rule:** Never let user message handlers block the socket reader. Read continuously
> into per-subscription queues with their own limits, and when such a queue overflows, drop
> messages and report a client-side slow consumer error to the async handler. An unexplained EOF
> on a busy subscriber most likely means the server judged the client a slow consumer.

### 10.4 Closes that send nothing

| Cause | Notes |
|---|---|
| Wrong argument count or bad size in `PUB`/`HPUB`/`SUB`/`UNSUB` | The server logs the parse error. |
| `HPUB` on a connection without headers | |
| Header size greater than total size | |
| Malformed JSON in `CONNECT` or `INFO` | |
| The client sent `-ERR` | |
| Slow consumer | Section 10.3. |
| Server shutdown, lame duck eviction | Section 11.2. |
| Operator kick (`$SYS.REQ.SERVER.<id>.KICK`) | |
| Connection accepted while the server is shutting down | Right after `INFO`. |
| TLS handshake failure, pinned certificate mismatch | |
| PROXY protocol header failure | Only behind a misconfigured load balancer. |

---

## 11. Cluster awareness and reconnection

A NATS deployment is usually several servers. A client connects to one of them and must be able to
move to another when its server goes away. The protocol supports this with two things only: the
`connect_urls` list and the lame duck notice. Everything else is client behavior, specified by
ADR-40.

### 11.1 Server discovery

`connect_urls` in `INFO` lists `host:port` addresses (no scheme) of all servers in the cluster
that accept clients, the current server's first. Details:

- A server listening on `0.0.0.0` or `::` advertises the addresses of its non-loopback
  interfaces; `client_advertise` replaces that with one configured `host:port`.
- The list is updated through async `INFO` when servers join or leave.
- `cluster { no_advertise: true }` suppresses it.
- WebSocket clients receive WebSocket addresses under the same key.

ADR-40: the client stores these URLs and uses them when reconnecting; it must offer an option to
ignore advertised servers (default: use them). The Go client's bookkeeping is a good model: the
pool distinguishes servers the user configured from "implicit" ones learned from `INFO`; on each
`INFO` with a non-empty list, implicit servers no longer listed are removed (never the current
server, never configured ones) and new ones are appended; an empty or absent list changes
nothing. Discovered addresses inherit the scheme and TLS settings of the current connection, and
credentials in a configured URL apply only to that URL.

> **Client rule:** Maintain a server pool of entries `{url, configured?, reconnect attempts, last
> error}`. Seed it from user configuration (shuffled unless the user asks to retain order). Merge
> `connect_urls` from every `INFO` as described, unless discovery is disabled. For TLS: when a
> discovered address is a bare IP and the current server was reached by host name, remember that
> host name on the new pool entry and use it as the expected certificate name when dialing the IP
> (the Go client does this); otherwise verify against the discovered host.

### 11.2 Lame duck mode

Lame duck mode (LDM) is a graceful shutdown that spreads the reconnect load:

1. The server stops accepting new connections.
2. It sends **one** async `INFO` with `"ldm":true` to every `protocol:1` client. The
   `connect_urls` in it exclude the server's own addresses (and are absent if advertising is off
   or the server is alone).
3. It waits the grace period (default 10 s).
4. It closes client connections in random order, spread over the remaining lame duck duration
   (default 2 minutes in total). The close is silent: no `-ERR`, just EOF.

ADR-5 (partially implemented) says a client receiving `ldm` should start an orderly
self-disconnect, and notes that clients have no automatic support for moving while keeping state.
In practice clients expose an event and otherwise wait for the close, then reconnect normally.

> **Client rule:** On `"ldm":true`: update the server pool from that `INFO` (which removes the
> departing server's address from the advertised set) and raise a lame-duck event to the user.
> Nothing else is required: the ordinary reconnect path handles the eventual EOF. A client may
> offer to reconnect proactively to another server, which is what ADR-5 intends.

### 11.3 Server-initiated closes

| Cause | `-ERR` first? |
|---|---|
| Shutdown, lame duck, kick | no |
| Slow consumer | no |
| Stale connection | `Stale Connection` |
| Authentication expiry or revocation | yes |
| Unknown operation | `Unknown Protocol Operation` |
| Malformed arguments | no |
| Payload or control line limit | yes |

### 11.4 Reconnecting

ADR-40 defines the expected behavior.

**Detection.** A connection is lost when (1) the configured number of consecutive `PONG`s is
missed (section 9.2), or (2) the socket reports an error or EOF.

**Procedure.**

1. Attempt to reconnect immediately.
2. If that fails, back off exponentially **with jitter** between attempts.
3. Try the servers of the pool in random order unless the user disabled randomization.
4. A successful reconnect resets the backoff.
5. After reconnecting, **resubscribe every subscription**.
6. Notify the user of every state change (disconnected, reconnecting, reconnected, closed).

**Options and defaults.**

| Option | ADR-40 default | Go client default | Meaning |
|---|---|---|---|
| Ping interval | 2 min | 2 min | Section 9.2. |
| Max pings out | 2 | 2 | Section 9.2. |
| Connection timeout | 5 s | 2 s | Time allowed to establish the network connection. |
| Retry on failed initial connect | false | false | When enabled, `connect` returns a handle at once and the reconnect procedure runs in the background from the first attempt. |
| Max reconnects | "3 / none" | 60 | Consecutive failed attempts before giving up and closing. ADR-40 warns that a client that gives up surprises users. |
| Reconnect delay | exponential backoff with jitter | fixed 2 s wait plus random jitter up to 100 ms (1 s with TLS) | ADR and Go client differ here. |
| Custom reconnect delay | none | none | Callback from attempt number to delay. |
| Disconnect buffer | unspecified | 8 MiB | See below. Must be tunable and possible to disable. |
| TLS required | false | false | Forces TLS regardless of `INFO`; a `tls://` URL implies it. |
| Ignore advertised servers | false | false | Section 11.1. |
| Retain servers order | false | false | Disables shuffling. |

**Disconnect buffer.** While disconnected, publishes are collected in a bounded buffer and flushed
after reconnecting; when the buffer is full, publish fails with an error. A reply published while
the requester was disconnected is lost, like any other message.

**Further ADR-40 requirements.**

- *On-demand reconnect:* a `reconnect` method drops the current connection and runs the standard
  procedure, honoring all options. Used to refresh credentials or rebalance.
- *Custom server pool:* a method replaces the whole pool, including advertised servers; it stays
  in force until replaced; same randomization and attempt rules.
- *Reconnect-to-server callback:* an optional callback is invoked on each attempt with the pool
  (including per-server attempt counts) and the latest server info; it returns the server to use,
  which must be in the pool, and optionally a delay.
- *Host name resolution (ADR-11):* resolve the host name, shuffle the returned addresses unless
  randomization is disabled, and try them in order until one connects.
- *Statistics,* kept across reconnects: bytes in and out (as read from and written to the socket),
  messages in (`MSG` + `HMSG`), messages out (`PUB` + `HPUB`), number of connects or reconnects.

**What a reconnect looks like on the wire.** It is a new connection from scratch: new `INFO`, new
`nonce`, possibly new `max_payload` and `headers` values, TLS again, `CONNECT` with freshly
evaluated credentials, `PING`. The server remembers nothing about the old connection.

> **Client rule:** After the new connection's `CONNECT` and `PING`, and before releasing buffered
> publishes, replay state in this order: (1) `SUB` for every live subscription with its original
> sid, subject and queue, including the mux inbox; (2) for each with an auto-unsubscribe limit,
> `UNSUB <sid> <max - delivered>`, or skip the subscription if nothing remains; (3) the buffered
> publishes, re-checked against the new `max_payload`; (4) a `PING` to confirm. Subscriptions that
> are draining are not replayed. Fail pending flushes (queued `PONG` waiters) at the moment of
> disconnect, because their `PING`s died with the old connection. Messages published by others
> while the client was away are lost: core NATS is at-most-once.

---

## 12. WebSocket transport

The server can carry the identical protocol inside WebSocket frames, for browsers and for networks
where only HTTP passes. WebSocket is a transport substitution: same parser, same verbs, same
limits. ADR-40 leaves the WebSocket flow unspecified; everything here is from the server source.

### 12.1 Handshake

The server has a separate listener (`websocket { port }`). TLS is on unless `no_tls: true`, and is
terminated by the HTTP layer: the URL scheme is `wss` or `ws`.

The request path selects the connection type by suffix: a path ending in `/leafnode` is a leaf
node, one ending in `/mqtt` is MQTT, **anything else is a NATS client** (`/` is conventional).

The upgrade request must satisfy, in this order (failure status in parentheses): method `GET`
(405); a non-empty `Host` (400); `Upgrade` containing the token `websocket` (400); `Connection`
containing `Upgrade` (400); `Sec-WebSocket-Key` that base64-decodes to exactly 16 bytes (400);
`Sec-WebSocket-Version` containing `13` (400); the origin check (403). Token comparisons are
case-insensitive. Sending any data before the handshake completes yields 400.

- **No subprotocol.** `Sec-WebSocket-Protocol` is neither required nor echoed for NATS clients.
- **Origin** is checked only if the server configures `same_origin` or `allowed_origins`; a
  request without an `Origin` header passes.
- `X-Forwarded-For` (first value, if a valid IP) is recorded as the client address.
- A `User-Agent` starting with `Mozilla/` marks a browser: server frames to it are capped at 4096
  bytes.

The response is:

```
HTTP/1.1 101 Switching Protocols␍␊
Upgrade: websocket␍␊
Connection: Upgrade␍␊
Sec-WebSocket-Accept: <accept key>␍␊
[Sec-WebSocket-Extensions: permessage-deflate; server_no_context_takeover; client_no_context_takeover␍␊]
[Nats-No-Masking: true␍␊]
[custom headers configured on the server]
␍␊
```

Immediately afterwards the server sends `INFO` in a binary frame, and the normal lifecycle of
section 3 continues inside frames.

### 12.2 Framing

- **Frame boundaries carry no meaning.** The WebSocket payload is the NATS byte stream. The server
  packs whatever is pending into one frame (or 4096-byte frames for browsers) without regard to
  protocol lines, and feeds each received frame's bytes to the same parser as TCP.
- **Client to server:** binary or text data frames are both accepted, as are fragmented messages.
  Frames **must be masked**, or the server reports a protocol error, unless no-masking was
  negotiated: a request header `Nats-No-Masking: true` that the server echoes in its response.
  That option is intended for leaf nodes; browsers cannot use it.
- **Server to client:** always binary frames, unmasked.
- **Control frames:** payload at most 125 bytes, never fragmented, never compressed. The server
  answers a WebSocket Ping with a Pong carrying the same payload and ignores Pongs. WebSocket
  ping/pong is independent of NATS `PING`/`PONG`, which still travel as data.
- **Close:** the server echoes a client Close frame (with its status) and closes. When the server
  closes, it sends a Close frame whose status reflects the reason, with the reason text as body:

| Status | Reasons |
|---|---|
| 1000 | Client closed. |
| 1001 | Server shutdown, stale connection, read or write error. |
| 1002 | Parse error, protocol violation, bad client protocol version, WebSocket framing error. |
| 1008 | Authentication timeout or violation, authentication expired or revoked, slow consumer, connection/control-line/subscription limits, missing account. |
| 1009 | Maximum payload exceeded. |
| 1015 | TLS handshake error. |
| 1011 | Anything else. |

NATS `-ERR` lines are still sent as data ahead of the Close frame.

### 12.3 Compression

Per-message deflate is used only if the server enables `websocket { compression: true }` **and**
the request offers `permessage-deflate` in `Sec-WebSocket-Extensions`. The server does not inspect
the offered parameters and always answers with
`server_no_context_takeover; client_no_context_takeover`: every message is compressed
independently.

- Server to client: a batch of 64 bytes or less is sent uncompressed; larger batches are deflated.
  The compressed flag (RSV1) is set on the first frame of a message only.
- Client to server: the decompressed size of **one WebSocket message** may not exceed
  `max_payload`, and its compressed size may not exceed `min(max_payload × 8, 64 MiB)`. Exceeding
  either closes the connection as a read error, without the `Maximum Payload Violation` `-ERR`.

### 12.4 Differences from TCP

| Aspect | WebSocket behavior |
|---|---|
| Handshake order | HTTP upgrade (and TLS) first, then `INFO`. No TLS-first logic, no TLS sniffing, no PROXY protocol. |
| `INFO` `tls_required`, `tls_available` | Both equal "the WebSocket listener uses TLS". They do **not** ask for a NATS-level TLS upgrade. |
| `INFO` `host`, `port`, `connect_urls` | Describe WebSocket listeners. |
| `INFO` `auth_required` | Also true when the `websocket {}` block defines its own `username`, `token` or `no_auth_user`, which override the global ones for WebSocket clients. |
| Authentication timeout, ping interval | `websocket.auth_timeout` and `websocket.ping_interval` override the global values if set. |
| Cookie authentication | The server can be configured with cookie names (`jwt_cookie`, `user_cookie`, `pass_cookie`, `token_cookie`). A cookie's value from the upgrade request is used **only if the corresponding `CONNECT` field is empty**. A cookie JWT still needs a `sig` unless it is a bearer token. |
| JWT connection type | `WEBSOCKET` instead of `STANDARD`. |
| Parse errors | Closed as a read error; an `-ERR 'Unknown Protocol Operation'` queued just before is not guaranteed to be flushed. Errors that close on their own (`Maximum Payload Violation`, `Authorization Violation`, control line) behave as on TCP. |

> **Client rule:** For a WebSocket transport: request path `/` unless configured; send no
> subprotocol; mask every frame; send data as binary frames; treat received data frames of either
> type, in any fragmentation, as a byte stream feeding the ordinary parser; answer WebSocket Pings;
> on a Close frame record the status and reason, then run the normal disconnect path. Never start
> a TLS upgrade based on `INFO`. If compression is negotiated, compress per message without
> context takeover and keep each message's uncompressed size at or below `max_payload` (a single
> `PUB` frame is `max_payload` plus its control line, so a maximum-size publish must be split
> across WebSocket messages or sent uncompressed).

---

## 13. Other core features visible to clients

### 13.1 Message tracing

Since server 2.11 (ADR-41) a publisher can ask the servers to report the path a message takes.
Tracing is triggered purely by headers on an `HPUB`:

| Header | Value | Effect |
|---|---|---|
| `Nats-Trace-Dest` | a subject | Trace events (JSON) are published to this subject by every server the message passes through. |
| `Nats-Trace-Only` | `1`, `true` or `on` (case-insensitive) | Trace the path but do not deliver the message to subscribers. |
| `Accept-Encoding` | contains `snappy`, `s2` or `gzip` | Compress trace event payloads. |
| `traceparent` | W3C trace context with the sampled flag | Triggers tracing only if the account is configured with a trace destination (`msg_trace`). `Nats-Trace-Dest` takes precedence. The header name is matched case-insensitively. |

Rules a client observes:

- The destination must be a valid literal subject. If it is invalid there is no tracing (and with
  `Nats-Trace-Only`, no delivery). If the publisher is not permitted to publish to it, the server
  sends `-ERR 'Permissions Violation for Publish to "<dest>"'␍␊` and **does not process the
  message**.
- `Nats-Trace-Hop` and `Nats-Trace-Origin-Account` are managed by servers. A client must not set
  them.
- A server can rewrite `Nats-Trace-Dest` to the literal value `trace disabled` (deliberately not a
  valid subject) to switch tracing off further along the path. Subscribers may see that value.
- A trace-only message counts as delivered for no-responders purposes, so it never produces a 503.
- Tracing across an account import or export requires `allow_trace` on it.

Each trace event has the fields `server`, `request` (`header`, `msgsize`), `hops` and `events`,
where every entry of `events` has a `type`: `in` (ingress), `sm` (subject mapping), `se` (stream
export), `si` (service import), `js` (JetStream), `eg` (egress).

A client needs no special support beyond headers: tracing is requested by setting headers and
consumed by subscribing to the destination subject.

### 13.2 Client-usable system subjects

When system events are enabled, these request subjects are available **in every account** (they
are service-imported from the system account). All require a reply subject.

| Subject | Returns |
|---|---|
| `$SYS.REQ.USER.INFO` | "Who am I." A server API response (`server`, `data`, `error`) whose `data` holds `user`, `account`, and optionally `account_name`, `user_name`, `permissions` and `expires`. `expires` is the remaining credential lifetime as a Go duration: an integer number of nanoseconds, truncated to whole seconds. |
| `$SYS.REQ.ACCOUNT.PING.CONNZ` | Connections of the caller's own account. |
| `$SYS.REQ.SERVER.PING.CONNZ` | The same, account-scoped. |
| `$SYS.REQ.ACCOUNT.PING.STATZ` | Statistics of the caller's own account. |

Users of the system account can additionally use `$SYS.REQ.SERVER.PING.<X>` and
`$SYS.REQ.SERVER.<id>.<X>` (with `<X>` among `IDZ`, `STATSZ`, `VARZ`, `SUBSZ`, `CONNZ`, `ROUTEZ`,
`GATEWAYZ`, `LEAFZ`, `ACCOUNTZ`, `JSZ`, `HEALTHZ`, `PROFILEZ`, `EXPVARZ`, `IPQUEUESZ`, `RAFTZ`),
`$SYS.REQ.ACCOUNT.<account>.<X>`, the control requests `$SYS.REQ.SERVER.<id>.KICK`, `.LDM` and
`.RELOAD`, `$SYS.REQ.ACCOUNT.NSUBS`, `$SYS.REQ.CLAIMS.*`, and subscribe to events such as
`$SYS.ACCOUNT.<account>.CONNECT`, `$SYS.ACCOUNT.<account>.DISCONNECT`,
`$SYS.SERVER.<id>.CLIENT.AUTH.ERR`, `$SYS.SERVER.<id>.STATSZ`, `$SYS.SERVER.<id>.LAMEDUCK` and
`$SYS.SERVER.<id>.SHUTDOWN`. The payload schemas of these were not verified for this chapter.

`PING` subjects are answered by every server, so they are request-many subjects
([section 7.5](#75-multiple-replies-request-many)).

### 13.3 Accounts, mappings, imports and exports

An account is an isolated subject namespace. A connection belongs to exactly one account, decided
at authentication (`$G` when none is assigned) and reported in the `connect_info` `INFO`. Subjects
in different accounts never meet unless an operator configures sharing. The client has no protocol
operation for any of this, but it can observe the effects:

- **Subject mapping.** An account may rewrite subjects of messages published by its clients. The
  mapping is applied after the payload is read and before permission checks and routing.
  Subscribers receive the **mapped** subject in `MSG`; publish permissions are evaluated against
  the mapped subject; the `Nats-Subject` of a 503 carries the mapped subject.
- **Stream imports.** When an account imports a stream of messages from another account, a
  client's `SUB` on the local subject is mirrored into the exporting account. The subject
  delivered in `MSG` is the import's local name (or the result of its transform), not necessarily
  what the remote publisher used.
- **Service imports.** A request published to an imported service subject is re-published in the
  exporting account with a server-generated reply subject under `_R_.`. The responder sees that
  subject, not the requester's inbox, and the reply is translated back. The mechanics beyond this
  were not traced.
- **Delivered subject may differ from subscribed pattern for reasons other than wildcards.** This
  is why a mux inbox router needs the single-pending-request fallback of section 7.2.

> **Client rule:** Never assume the subject in a received `MSG` is one the client itself would
> have produced, and never parse meaning out of a reply subject received in a request beyond
> "publish the response here". Route messages by sid first; use the subject only for
> application-level dispatch.

---

## 14. Scenarios

Each transcript shows the exact bytes on the wire. Long JSON documents are shown in full once and
abbreviated with `...` afterwards; identifiers, keys and signatures are illustrative (the
signature in 14.4 is not a valid signature of the nonce shown). The `CONNECT` used throughout,
called "the standard CONNECT" below, is:

```
C: CONNECT {"verbose":false,"pedantic":false,"tls_required":false,"name":"demo","lang":"ruby","version":"0.1.0","protocol":1,"echo":true,"headers":true,"no_responders":true}␍␊
```

### 14.1 Minimal connect, subscribe, publish

```
S: INFO {"server_id":"NZDOCIS2JHTLGMXGEDN73U55XTPLPFT7V4SEH2KVJ72CEUVW75EFR6ED","server_name":"n1","version":"2.16.0-dev","proto":1,"git_commit":"a0385c9","go":"go1.25.4","host":"0.0.0.0","port":4222,"headers":true,"max_payload":1048576,"client_id":7,"client_ip":"127.0.0.1","api_lvl":5,"xkey":"XPZOM7WBBR6QMW4WXFOGO6MVN6A6WFHYM6L3VFZ5ZFKKIBJ5J6WJIBAG"} ␍␊
C: CONNECT {"verbose":false,"pedantic":false,"tls_required":false,"name":"demo","lang":"ruby","version":"0.1.0","protocol":1,"echo":true,"headers":true,"no_responders":true}␍␊
C: PING␍␊
S: PONG␍␊
S: INFO {"server_id":"NZDOCIS2JHTLGMXGEDN73U55XTPLPFT7V4SEH2KVJ72CEUVW75EFR6ED","server_name":"n1","version":"2.16.0-dev","proto":1,"git_commit":"a0385c9","go":"go1.25.4","host":"0.0.0.0","port":4222,"headers":true,"max_payload":1048576,"client_id":7,"client_ip":"127.0.0.1","connect_info":true,"remote_account":"$G","api_lvl":5,"xkey":"XPZOM7WBBR6QMW4WXFOGO6MVN6A6WFHYM6L3VFZ5ZFKKIBJ5J6WJIBAG"} ␍␊
C: SUB greet.* 1␍␊
C: PUB greet.joe 5␍␊hello␍␊
S: MSG greet.joe 1 5␍␊hello␍␊
C: UNSUB 1␍␊
```

1. The server speaks first. No `auth_required`, no `tls_required`: the client may send `CONNECT`
   directly. No `nonce`, no `connect_urls`: a standalone server without nkey or JWT users.
2. `CONNECT` and `PING` go out in one write. Nothing comes back for `CONNECT` because `verbose`
   is false.
3. `PONG` proves the `CONNECT` was accepted. The connection is now established.
4. Because of `"protocol":1` the server follows the first `PONG` with the `connect_info` `INFO`:
   the connection was bound to the global account `$G`.
5. The client subscribes with sid `1`. No acknowledgment.
6. The client publishes 5 bytes to `greet.joe`. Echo is on and the subject matches the client's
   own subscription, so the server delivers the message back: `MSG` with the concrete subject, the
   sid, and the size.
7. `UNSUB 1` removes the subscription. No acknowledgment.

About two seconds after step 2 the server will also send a `PING` (scenario 14.10).

### 14.2 Request/reply through a muxed inbox

Two connections: a requester and a responder, both already connected. Lines are in time order.

Requester, once, before its first request:

```
C: SUB _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.* 2␍␊
```

Responder, at startup:

```
C: SUB svc.time workers 1␍␊
```

Requester sends a request with a zero-length payload:

```
C: PUB svc.time _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.5ZR3qa7y 0␍␊␍␊
```

Responder receives it and answers:

```
S: MSG svc.time 1 _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.5ZR3qa7y 0␍␊␍␊
C: PUB _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.5ZR3qa7y 8␍␊12:00:00␍␊
```

Requester receives the reply:

```
S: MSG _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.5ZR3qa7y 2 8␍␊12:00:00␍␊
```

1. The inbox prefix is `_INBOX.` plus a 22-character NUID; the wildcard subscription on
   `<prefix>.*` with sid `2` serves every request this connection will ever make.
2. The responder joins queue group `workers`, so if several responders run, each request goes to
   exactly one of them.
3. The reply subject carries the per-request token `5ZR3qa7y`. The `PUB` has three arguments:
   subject, reply, size.
4. The responder's `MSG` has four arguments, so the third is a reply subject. It publishes the
   answer there as an ordinary `PUB` with no reply of its own.
5. The reply arrives on sid `2` with the full reply subject. The requester takes the last token,
   finds the waiting request, completes it, and removes the token from its map. The subscription
   stays.

### 14.3 Request with no responders

The requester of 14.2 sends a request to a subject nobody subscribes to:

```
C: PUB svc.missing _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.Eeeby3ab 2␍␊hi␍␊
S: HMSG _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.Eeeby3ab 2 43 43␍␊NATS/1.0 503␍␊Nats-Subject: svc.missing␍␊␍␊␍␊
```

1. All four preconditions hold: there is a reply subject; nothing was delivered; the standard
   CONNECT set `no_responders`; and this same connection holds a plain subscription (sid `2`)
   matching the reply subject.
2. The server answers at once with an `HMSG` addressed to the reply subject on sid `2`. It has
   four arguments, so there is no reply subject. Header size and total size are both 43:
   `NATS/1.0 503␍␊` (14) + `Nats-Subject: ` (14) + `svc.missing` (11) + `␍␊` (2) + `␍␊` (2).
   The payload is empty; the final `␍␊` is the frame terminator.
3. The client sees status 503 with an empty payload and fails request `Eeeby3ab` with a "no
   responders" error instead of waiting for its timeout.

Had the request been sent from a connection that used someone else's inbox as the reply subject,
the server would have sent nothing.

### 14.4 Authentication with an nkey and the nonce

```
S: INFO {"server_id":"NT4SYWB5WKH7DNSIPZZ7FK4ZRI3R2WYOJFLJOOA7LQSAJ2XUID5ZZZZG","server_name":"n1","version":"2.16.0-dev","proto":1,"git_commit":"a0385c9","go":"go1.25.4","host":"0.0.0.0","port":4222,"headers":true,"auth_required":true,"max_payload":1048576,"client_id":12,"client_ip":"127.0.0.1","nonce":"r3yBdGBLEPH1qhT","api_lvl":5,"xkey":"XPZOM7WBBR6QMW4WXFOGO6MVN6A6WFHYM6L3VFZ5ZFKKIBJ5J6WJIBAG"} ␍␊
C: CONNECT {"verbose":false,"pedantic":false,"nkey":"UA6ZDMEN4KHVDGAJGXBENYJQWX6HH7566TFJGVQ6KBNXJBTFQXKWOVOM","sig":"61qtc4xatws8phP9nhFyJfm5di4PzJ59FHz5r1pY4OjE2jBMptUsGr7CmY-uCu3ZR1zTOlUcR64cXQLioDnkHI","tls_required":false,"name":"demo","lang":"ruby","version":"0.1.0","protocol":1,"echo":true,"headers":true,"no_responders":true}␍␊
C: PING␍␊
S: PONG␍␊
S: INFO {...,"client_id":12,"client_ip":"127.0.0.1","connect_info":true,"remote_account":"APP","api_lvl":5,...} ␍␊
```

1. `auth_required` is true and a `nonce` is present.
2. The client signs the 15 bytes `r3yBdGBLEPH1qhT` with the Ed25519 key of its seed and encodes
   the 64-byte signature as 86 characters of unpadded URL-safe base64 (note the `-` in the
   example: the URL alphabet uses `-` and `_`, never `+`, `/` or `=`).
3. `PONG` means the signature verified and the public key is a configured user. The
   `connect_info` `INFO` names the account, and no longer contains the nonce.

With a wrong key, a missing `sig`, or a signature over the decoded nonce bytes instead of the
text, the exchange ends like this, the same as for any other credential failure:

```
C: CONNECT {...}␍␊
C: PING␍␊
S: -ERR 'Authorization Violation'␍␊
S: <connection closed>
```

And if the client sends nothing within the authentication timeout:

```
S: -ERR 'Authentication Timeout'␍␊
S: <connection closed>
```

### 14.5 TLS upgrade

Explicit TLS, the default when the server has a `tls {}` block:

```
S: INFO {"server_id":"NZDOCIS2JHTLGMXGEDN73U55XTPLPFT7V4SEH2KVJ72CEUVW75EFR6ED","server_name":"n1","version":"2.16.0-dev","proto":1,"git_commit":"a0385c9","go":"go1.25.4","host":"0.0.0.0","port":4222,"headers":true,"tls_required":true,"max_payload":1048576,"client_id":21,"client_ip":"192.0.2.10","api_lvl":5,"xkey":"XPZOM7WBBR6QMW4WXFOGO6MVN6A6WFHYM6L3VFZ5ZFKKIBJ5J6WJIBAG"} ␍␊
   <TLS handshake on the same socket; the client sends ClientHello first>
C: CONNECT {"verbose":false,"pedantic":false,"tls_required":true,"name":"demo","lang":"ruby","version":"0.1.0","protocol":1,"echo":true,"headers":true,"no_responders":true}␍␊
C: PING␍␊
S: PONG␍␊
S: INFO {...,"tls_required":true,...,"connect_info":true,"remote_account":"$G",...} ␍␊
```

1. The first `INFO` is in clear text and says `"tls_required":true`.
2. The client hands the socket to its TLS library, which performs the handshake as a client. The
   server allows 2 s by default. The client must have consumed exactly the `INFO` line and not one
   byte more from the socket before starting (the server sends nothing else before the handshake,
   so a line-oriented read is safe).
3. Everything after the handshake is encrypted. The server does not repeat `INFO`; the next one
   is the `connect_info` `INFO` after the first `PONG`, and it is the first server information
   the client receives over the authenticated channel.

TLS-first, when the user enabled the client option, reverses the opening:

```
   <TCP connect; TLS handshake immediately; the client sends ClientHello first>
S: INFO {...,"tls_required":true,...} ␍␊
C: CONNECT {...}␍␊
C: PING␍␊
S: PONG␍␊
```

Against a server with `handshake_first: auto`, both variants work: a client that waits for `INFO`
receives it in clear text after the 50 ms fallback delay and proceeds as in the first transcript.

### 14.6 Auto-unsubscribe

```
C: SUB updates 5␍␊
C: UNSUB 5 2␍␊
```

Another connection now publishes `a`, `b` and `c` to `updates`:

```
S: MSG updates 5 1␍␊a␍␊
S: MSG updates 5 1␍␊b␍␊
```

1. `UNSUB 5 2` arrives while the subscription has delivered 0 messages; 2 > 0, so the threshold
   is stored.
2. `a` is delivered (count 1). `b` is delivered (count 2, equal to the threshold) and the server
   then removes the subscription. `c` finds no subscription.

The count is absolute. Compare:

```
C: SUB updates 6␍␊
S: MSG updates 6 1␍␊a␍␊
S: MSG updates 6 1␍␊b␍␊
C: UNSUB 6 3␍␊
S: MSG updates 6 1␍␊c␍␊
```

Here `UNSUB 6 3` after two deliveries allows exactly one more message, not three. And
`UNSUB 6 2` at that point would have removed the subscription immediately, because 2 is not
greater than the 2 already delivered.

### 14.7 A permissions violation that does not close the connection

The user may publish and subscribe to `orders.>` only.

```
C: PUB admin.shutdown 0␍␊␍␊
S: -ERR 'Permissions Violation for Publish to "admin.shutdown"'␍␊
C: SUB admin.> 3␍␊
S: -ERR 'Permissions Violation for Subscription to "admin.>"'␍␊
C: SUB admin.audit auditors 4␍␊
S: -ERR 'Permissions Violation for Subscription to "admin.audit" using queue "auditors"'␍␊
C: PUB orders.new _R_.x 0␍␊␍␊
S: -ERR 'Permissions Violation for Publish with Reply of "_R_.x"'␍␊
C: PING␍␊
S: PONG␍␊
```

1. The publish is read completely (control line and payload), then rejected and dropped. The
   stream stays in sync.
2. Sids `3` and `4` do not exist on the server. The errors name subject and queue but not the
   sid; the client matches them to its subscriptions and marks those failed.
3. The fourth message is to a permitted subject but uses a reserved reply prefix; it is dropped.
4. The `PONG` shows the connection is alive and well. Each error arrives asynchronously, before
   the `PONG` of a `PING` written after the offending operation, which is how a client can make
   "subscribe and wait for confirmation" synchronous: `SUB`, `PING`, then check for an error
   attributed to the subscription when the `PONG` arrives.

### 14.8 A maximum-payload violation that closes the connection

`max_payload` is 1048576.

```
C: PUB big.blob 1048577␍␊
S: -ERR 'Maximum Payload Violation'␍␊
S: <connection closed>
```

The server decides on the control line alone. It does not read the payload; whatever the client
is still writing goes nowhere, and every subscription, pending request and unflushed publish on
the connection is disrupted by the forced reconnect. For `HPUB` the comparison uses the total
size:

```
C: HPUB big.blob 22 1048577␍␊
S: -ERR 'Maximum Payload Violation'␍␊
S: <connection closed>
```

A client that checks sizes before writing never triggers this.

### 14.9 Lame duck mode and reconnect

A three-server cluster; the client is connected to `10.0.0.1:4222` and holds subscription sid `1`
on `greet.*`, the mux inbox as sid `2`, and sid `5` on `updates` with an auto-unsubscribe limit of
3 of which 2 were delivered.

```
S: INFO {"server_id":"NZDOCIS2JHTLGMXGEDN73U55XTPLPFT7V4SEH2KVJ72CEUVW75EFR6ED","server_name":"n1","version":"2.16.0-dev","proto":1,"git_commit":"a0385c9","go":"go1.25.4","host":"0.0.0.0","port":4222,"headers":true,"max_payload":1048576,"client_id":7,"client_ip":"10.0.0.50","cluster":"c1","connect_urls":["10.0.0.2:4222","10.0.0.3:4222"],"ldm":true,"api_lvl":5,"xkey":"XPZOM7WBBR6QMW4WXFOGO6MVN6A6WFHYM6L3VFZ5ZFKKIBJ5J6WJIBAG"} ␍␊
   <at least the grace period (10 s by default) passes; traffic continues normally>
S: <connection closed>
```

The client picks `10.0.0.2:4222` from its pool and opens a new connection:

```
S: INFO {"server_id":"NT4SYWB5WKH7DNSIPZZ7FK4ZRI3R2WYOJFLJOOA7LQSAJ2XUID5ZZZZG","server_name":"n2",...,"client_id":31,"client_ip":"10.0.0.50","cluster":"c1","connect_urls":["10.0.0.2:4222","10.0.0.3:4222"],"api_lvl":5,...} ␍␊
C: CONNECT {"verbose":false,"pedantic":false,"tls_required":false,"name":"demo","lang":"ruby","version":"0.1.0","protocol":1,"echo":true,"headers":true,"no_responders":true}␍␊
C: PING␍␊
S: PONG␍␊
S: INFO {...,"client_id":31,...,"connect_info":true,"remote_account":"$G",...} ␍␊
C: SUB greet.* 1␍␊
C: SUB _INBOX.Ky9Pf34qY6Nb3wWD25RQ4F.* 2␍␊
C: SUB updates 5␍␊
C: UNSUB 5 1␍␊
C: PUB greet.joe 5␍␊hello␍␊
C: PING␍␊
S: MSG greet.joe 1 5␍␊hello␍␊
S: PONG␍␊
```

1. The lame duck `INFO` has `"ldm":true` and lists only the *other* servers. The client updates
   its pool and tells the user.
2. The close is silent. The client marks itself disconnected, fails pending flushes, starts
   buffering publishes, and makes its first reconnect attempt immediately.
3. The new server sends a fresh `INFO`: different `server_id`, different `client_id`. If
   authentication were in use there would be a new `nonce` to sign.
4. After the first `PONG` the client replays its subscriptions with their original sids. The mux
   inbox keeps its subject, so replies to requests still pending can arrive again.
5. Sid `5` had a limit of 3 and had received 2, and the new server counts from zero, so the
   client sends `UNSUB 5 1`: one more message.
6. The publish that was buffered during the outage is sent after the subscriptions, so the
   client's own subscription sees it (echo). The closing `PING`/`PONG` confirms that the server
   processed everything, and the client announces "reconnected".

### 14.10 Server-initiated PING

About two seconds after `CONNECT`, on every new connection, and afterwards whenever the
connection has been idle from the server's point of view for a ping interval:

```
S: PING␍␊
C: PONG␍␊
```

If the client does not answer (defaults: interval 2 minutes, 2 pings allowed outstanding):

```
S: PING␍␊
   <2 minutes, no PONG and no other activity from the client>
S: PING␍␊
   <2 minutes>
S: -ERR 'Stale Connection'␍␊
S: <connection closed>
```

The mirror image runs in the client: it sends `PING` every ping interval, and after two intervals
without any `PONG` it closes the socket itself and reconnects.

---

## 15. Client implementation checklist

Every client rule of this chapter, grouped by area. Section numbers point to the explanation.

**Parsing and writing (sections 2, 4)**

1. Write only canonical operations: upper case, single spaces, `␍␊`, no blank lines, no trailing
   whitespace.
2. Never send `MSG`, `HMSG`, `+OK` or `-ERR`. There is no reason to send `INFO`.
3. Parse incoming control lines by splitting on space and TAB only; tolerate repeated separators
   and trailing whitespace; accept operation names case-insensitively.
4. Reassemble control lines and payloads across reads; do not assume a read returns whole
   operations.
5. Parse `INFO` JSON to the end of the line with no length limit; ignore unknown keys; absent
   booleans are false.
6. For `MSG`: 3 arguments (subject, sid, size) or 4 (with reply). For `HMSG`: 4 (subject, sid,
   header size, total size) or 5 (with reply).
7. Parse sizes as decimal, accepting leading zeros.
8. After a payload, require `␍␊`; anything else is a fatal protocol error for the connection.
9. Accept `INFO`, `PING`, `PONG`, `+OK`, `-ERR`, `MSG`, `HMSG` at any point after the initial
   `INFO`.
10. Strip one pair of single quotes from `-ERR` text and match it case-insensitively.

**Limits (section 2.2)**

11. Fail a publish locally if payload (for `HPUB`: headers plus payload) exceeds the current
    `max_payload`.
12. Fail locally any operation whose control line arguments would exceed 4096 bytes.
13. Emit sizes with at most 9 digits and no leading zeros.
14. Re-read `max_payload` from every `INFO`, including the `connect_info` one and after
    reconnects.

**Connecting (section 3)**

15. Wait for `INFO` first, unless TLS-first is enabled.
16. TLS modes: upgrade after `INFO` when `tls_required` or when the user requires TLS; upgrade
    opportunistically when `tls_available` and the user asked for TLS; with the TLS-first option,
    handshake before reading anything.
17. Consume exactly the `INFO` line before starting a TLS handshake.
18. Verify the server certificate against the user-supplied host name; present a client
    certificate when configured.
19. Do not use `connect_urls` from an `INFO` received before the TLS handshake.
20. Send `CONNECT` with `verbose`, `pedantic` and `echo` always explicit; `verbose` and
    `pedantic` false.
21. Send `"protocol":1`.
22. Set `headers` and `no_responders` to the server's `headers` value; do not expose `headers` as
    a user option.
23. Send `name`, `lang`, `version`. Never send `account` or `new_account`. Omit unused credential
    keys.
24. Keep the whole `CONNECT` line within the control line limit; report a clear local error when
    a JWT makes it too large.
25. Send `PING` right after `CONNECT`; the connection is established when its `PONG` arrives.
26. While waiting for that `PONG`, handle `-ERR`, EOF, `+OK`, `PING` and `INFO`.
27. Bound the whole handshake with a timeout (dial, `INFO`, TLS, `PONG`).
28. Do not wait for the `connect_info` `INFO`; process it when it comes. Servers older than 2.12
    do not send it.
29. Keep the `nonce` of the initial `INFO` separate from later server info.

**Subjects and subscriptions (sections 4.3, 4.4, 5)**

30. Reject locally: empty subject, reply or queue; any of them containing space, TAB, CR or LF.
31. Reject locally subscription subjects with empty tokens or a `>` that is not the last token.
32. Reject locally publish subjects with empty tokens or whole-token wildcards (optionally
    switchable off).
33. Generate sids from a never-reused per-client counter, as decimal text; keep a subscription's
    sid across reconnects.
34. Route incoming messages by sid; drop messages for unknown sids.
35. Count deliveries per subscription and enforce auto-unsubscribe limits locally.
36. Treat the delivered subject as data: with wildcards, mappings and imports it differs from the
    subscribed pattern.
37. Generate reply subjects only under the inbox prefix; refuse inbox prefixes that are reserved
    (`$JS.`, `_R_.`, `_GR_.`, `$GR.`) or contain wildcards or whitespace.
38. Expose `echo` as a connection option (`no_echo`), default on.

**Headers (section 6)**

39. Use `HPUB` only for messages with headers; never on a server with `"headers":false`.
40. Serialize as `NATS/1.0␍␊`, then `name: value␍␊` per value in order, then `␍␊`.
41. Header size includes the final blank line; total size is header size plus payload size.
42. Reject header names that are empty or contain bytes outside 33 to 126 or `:`; reject values
    containing CR or LF.
43. Preserve header name case; compare names byte-exactly; keep all values of repeated keys;
    "get" returns the first.
44. On receive, require the `NATS/1.0` prefix; parse an optional status code (3 digits) and
    description from the first line; a malformed block is an error for that message, not for the
    connection.
45. Do not set server-owned headers (`Nats-Trace-Hop`, `Nats-Trace-Origin-Account`,
    `Nats-Subject`, and the JetStream ones documented in later chapters).

**Request/reply (section 7)**

46. Use one lazily created wildcard inbox subscription per connection and a token-to-request map.
47. Remove the map entry on reply, timeout and cancellation; drop replies for unknown tokens.
48. If a reply's subject lacks the inbox prefix and exactly one request is pending, give it to
    that request.
49. Treat status `503` with an empty payload as "no responders": fail the request immediately
    with a distinct error.
50. Keep the inbox prefix stable across reconnects; leave in-flight requests pending across a
    reconnect and fail them on final close.
51. For request-many: total timeout always; optional stall timer, maximum count and sentinel; a
    status message is terminal; never count user callback time against timers; always clean up the
    token or subscription.
52. With a dedicated inbox and `UNSUB <sid> <n>`, still count replies locally.

**Authentication (section 8)**

53. Select the mechanism from user configuration: token, user/password, nkey, JWT plus nkey,
    credentials file, TLS certificate.
54. Evaluate credential callbacks on every connect and reconnect attempt; re-read credentials
    files each time.
55. Sign the nonce text exactly as received (15 ASCII bytes); encode the Ed25519 signature as
    unpadded URL-safe base64 in `sig`.
56. With an nkey configured and no `nonce` in `INFO`, fail locally.
57. With a JWT, send `jwt` and `sig` and no `nkey`.
58. From a `.creds` file, take the line after the `BEGIN NATS USER JWT` marker and the line after
    the `BEGIN USER NKEY SEED` marker; do not keep the seed in memory.
59. Never log `CONNECT`. Use TLS when sending tokens or passwords.
60. Treat `Authorization Violation` as a per-server authentication failure; after the same
    authentication error twice in a row from one server, stop reconnecting unless the user opted
    out.
61. On `User Authentication Expired`, `Account Authentication Expired` and `User Authentication
    Revoked`: notify the async error handler and reconnect.
62. Send `CONNECT` promptly: the server's authentication timeout is about 2 s.

**Keepalive and flush (sections 4.6, 9)**

63. Answer every `PING` with `PONG` without waiting for user code.
64. Keep a FIFO of `PONG` waiters shared by the connect handshake, flushes and keepalive pings.
65. Run a ping timer (default 2 min); after more than 2 (configurable) outstanding pings declare
    the connection stale, close the socket and reconnect.
66. Implement flush as `PING` plus waiting for its `PONG`, with a timeout.

**Errors (section 10)**

67. Never rely on an `-ERR` before a close; EOF at any time is a disconnect.
68. Classify `-ERR` text by case-insensitive match against the catalog; unknown texts go to the
    async error handler and do not by themselves close the connection.
69. Subscription permission violation: find the subscription by sid if given, else the first with
    matching subject and queue; fail it; notify it and the async error handler; keep the
    connection.
70. Publish permission violation: notify the async error handler; fail a pending request to that
    subject if there is one.
71. `maximum subscriptions exceeded`: not terminal for the connection.
72. `Stale Connection`, `maximum connections exceeded`, `Connection throttling is active`:
    expect a close and reconnect.
73. Keep the last error and report it when the client finally closes.
74. Never block the socket reader on user handlers; bound per-subscription queues and report
    client-side slow consumers.

**Cluster and reconnect (section 11)**

75. Maintain a server pool of configured and discovered servers; shuffle unless the user retains
    order.
76. Merge `connect_urls` from each `INFO`: add new entries as discovered, remove discovered
    entries no longer listed (never the current or a configured server), ignore empty lists.
77. Offer options: ignore advertised servers, retain order, custom pool replacement, a
    server-selection callback restricted to the pool.
78. Resolve host names and shuffle the resulting addresses unless randomization is off.
79. On `"ldm":true`: update the pool and raise a lame-duck event.
80. Detect disconnect by socket error/EOF or missed `PONG`s.
81. Reconnect: first attempt immediately; then backoff with jitter; random server order; reset on
    success; honor maximum attempts and a custom delay callback.
82. Offer retry-on-failed-initial-connect, a forced `reconnect` method, and state-change
    notifications (disconnected, reconnected, closed, discovered servers, lame duck).
83. Buffer publishes while disconnected in a bounded, configurable, disableable buffer; fail
    publishes when it is full.
84. On reconnect, after the handshake `PONG`: resend every non-draining subscription with its
    original sid; send `UNSUB <sid> <max - delivered>` for auto-unsubscribe subscriptions, or
    drop those with nothing remaining; then flush buffered publishes (re-checked against
    `max_payload`); then `PING`.
85. Fail pending flushes at disconnect.
86. Keep statistics across reconnects: bytes in/out, messages in/out, connects/reconnects.

**WebSocket (section 12)**

87. Path `/` by default, no subprotocol, masked binary frames.
88. Treat received frames as a byte stream; never assume one operation per frame.
89. Never perform a NATS-level TLS upgrade on WebSocket, whatever `INFO` says.
90. With compression: no context takeover; at most `max_payload` uncompressed bytes per
    compressed message.
91. Answer WebSocket Pings; map a Close frame to the ordinary disconnect path, keeping status and
    reason for diagnostics.

---

## 16. Unverified items

The following could not be confirmed from the server source, the Go client or the ADRs and are
flagged where they appear above.

- The exact parsing tolerance of `.creds` files (`nkeys.ParseDecoratedJWT`,
  `nkeys.ParseDecoratedNKey`): the `nkeys` library source was not available. The layout shown is
  the one in the Go client's test fixtures.
- The nkey text encoding (prefix bytes, base32, checksum) is not described; it is defined by the
  `nkeys` library.
- The server version that introduced the `connect_info` `INFO` (2.12) is taken from a version
  check in the Go client, not from server history.
- Whether `-ERR 'Secure Connection - TLS Required'` reaches a plaintext client that never
  completed the TLS handshake.
- Message ordering across routes, gateways and leaf nodes. Only single-server ordering was derived
  from code.
- The full mechanics of service-import request and reply rewriting.
- Payload schemas of `$SYS.REQ.SERVER.*` and `$SYS.REQ.ACCOUNT.*` responses.
- Details of how the `traceparent` sampled flag is parsed (taken from ADR-41).
- What the server does with an `HMSG` sent by a client (the parser does not reject it at the
  operation level; the outcome was not traced).
- On WebSocket, whether an `-ERR 'Unknown Protocol Operation'` is flushed before the close.
- Nothing was built or executed; all server behavior is from reading source.
