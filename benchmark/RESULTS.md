# tether vs nats-pure: performance smoke test

Side-by-side comparison of tether (this checkout) and nats-pure 2.7.0 against a local `nats:alpine` server.

- Ruby 4.0.10 (arm64, macOS), nats-server in Docker, reached through Docker's port forwarding on 127.0.0.1.
- Each scenario runs in a fresh process per library, 3 rounds, alternating which library goes first. Numbers are the
  median of the rounds. Raw data: [results.json](results.json).
- Reproduce: `ruby benchmark/run.rb [ROUNDS] [SCENARIO_REGEX]` (starts and removes its own container; set `NATS_URL`
  to use a running server).
- Smoke test, not a rigorous benchmark: one machine, no CPU pinning, the server shares the machine with the clients.

## Update: after the publish and worker-pool changes

Three commits on `main` (`5fe7e88`, `c3ce987`, `5e7b4d1`): publish subjects that passed validation are remembered, the
flusher is woken only by the frame that starts a new buffer, and subscription blocks run on a shared worker pool
instead of a thread each. The affected scenarios were measured again the same way (3 rounds, medians).

| Scenario | tether before | tether after | nats-pure |
|---|---|---|---|
| Publish 16 B | 709k msg/s | 865k msg/s | 1,155k msg/s |
| Publish 128 B | 621k msg/s | 754k msg/s | 909k msg/s |
| Publish 128 B with 2 headers | 258k msg/s | 282k msg/s | 424k msg/s |
| Subscribe, 1 callback | 444k msg/s | 426k msg/s | 86k msg/s |
| Subscribe, 1000 subjects | 85k msg/s | **312k msg/s** | 124k msg/s |
| Subscribe, fan-out to 10 | 323k msg/s | 392k msg/s | 70k msg/s |
| 5000 callback subscriptions: memory | 358 MB | **5.9 MB** | 4.4 MB |
| 5000 callback subscriptions: threads | 4,997 | **0 until messages arrive** | 0 |

### Why publish is still slower without YJIT

Per call, a 16 B publish costs about 1.1 us in tether. Frame encoding takes about 540 ns, the writer (lock, counter,
append) about 380 ns, and the started/subject checks about 260 ns. nats-pure only checks that the subject is non-empty,
interpolates the frame and pushes it onto a `Thread::Queue`, with no lock or size check of its own. tether still
allocates fewer objects per message (2 vs 3), so the gap is interpreter overhead per call, not allocation or I/O.

With YJIT, which removes most of that overhead, tether is ahead (medians of 3 runs, `RUBY_YJIT_ENABLE=1`; the
"before" and "after" runs were on different occasions, so compare each against its own nats-pure column):

| Scenario | tether before | nats-pure then | tether after | nats-pure now |
|---|---|---|---|---|
| Publish 16 B | 1,492k msg/s | 1,619k msg/s | **2,220k msg/s** | 1,637k msg/s |
| Publish 128 B | 1,004k msg/s | 994k msg/s | **1,263k msg/s** | 1,100k msg/s |

Rejected: leaving all socket writes to the flusher, as nats-pure does, was slower at 128 B. Rewriting the PUB encoder
as one interpolation measured no faster.

### Publish with the same checks as nats-pure

On each publish nats-pure checks only that the subject is non-empty and that the connection is not closed. To compare
like with like, tether was patched for this run to do the same: the fork check, full subject validation and the size
check were turned off, the status check and the writer's closed check stayed. Both libraries ran in the same Ruby mode
in every row; each process reported `RubyVM::YJIT.enabled?` to confirm it. Medians of 5 runs, msg/s:

| Payload | tether, no YJIT | nats-pure, no YJIT | tether, YJIT | nats-pure, YJIT |
|---|---|---|---|---|
| 16 B | 740k | **907k** | **2,005k** | 1,327k |
| 128 B | 723k | 734k | **1,129k** | 945k |
| 1 KB | **173k** | 156k | **165k** | 159k |

- Without YJIT at 16 B nats-pure is still 18% faster. Its publish is one flat method that interpolates a string and
  pushes it onto a queue with no lock. tether's goes through about eight small methods and takes the writer lock, and
  the interpreter charges for every call. YJIT inlines those calls, so tether gains 2.7x from it and nats-pure 1.5x.
- At 1 KB and above tether is ahead in both modes. nats-pure copies the payload into a new string per publish; tether
  appends it to one reused buffer.
- Turning the checks off on shipped tether gained 17% at 16 B (688k to 807k msg/s on the same day).

### What the extra checks are for

nats-pure leaves bad input to the server. What the server does with it (nats-server `server/client.go` at `a0385c9af`):

| Published | Server, and so nats-pure | tether |
|---|---|---|
| Payload over `max_payload` | `-ERR 'Maximum Payload Violation'`, closes the connection (`maxPayloadViolation`) | raises `MaxPayloadError`, connection stays |
| Subject `"foo bar"` | reads it as subject `foo`, reply `bar`; delivered to `foo` with no error (`processPub`) | raises `InvalidSubjectError` |
| Subject `"a b c"` | parse error, closes the connection as a protocol violation | raises |
| Subject containing `\r\n` | the rest is read as new protocol commands | raises |
| Wildcards or empty tokens | accepted; rejected only in pedantic mode, which both clients leave off | raises, unless `validate_subjects: false` |

A dropped connection costs nats-pure whatever it had buffered and every request still waiting for a reply.

- **Fork check** (`Lifecycle#check_fork`): a forked child (Puma, Unicorn, Sidekiq) would otherwise write into the
  parent's socket, interleaving frames until the server closes the connection. The first publish in the child resets
  the client and opens its own connection. Normally one comparison of `Process.pid`.
- **Status check**: raises on a client that was never connected, instead of buffering messages that are never sent.
- **Size check**: compares against the `max_payload` the server announced, so an oversized message raises before it
  costs the connection.

Each check does almost no work; their cost without YJIT is the chain of calls that reaches it (`check_size` goes
through `max_payload`, `server_info`, `@connection.server_info` and `ServerInfo#max_payload`). Caching `max_payload` on
the client when INFO arrives would shorten that chain; it has not been measured.

### Worker pool trade-off

The pool hands a subscription over through a job queue. Measured in-process with no network, raw dispatch on one
subscription went from 1.55M to 1.39M msg/s, about 80 ns more per message. End to end it is within run-to-run noise.

## How it is measured

- **Publish**: time to publish N messages plus a `flush` round trip, so the server has really received them. Payloads are
  `"x" * size`.
- **Subscribe**: a separate raw-socket process (`feeder.rb`) writes N pre-built `PUB` frames. The clock runs from the
  moment it is told to go until the last message reaches the subscriber. This keeps the publisher off the GVL of the client
  under test, and both libraries receive identical bytes. Pending limits are raised on both so nothing is dropped.
- **Request**: an echo responder on a second connection in the same process; latency is per request.
- **allocs/msg**: process-wide `GC.stat(:total_allocated_objects)` delta divided by messages, so it includes background
  threads. It is a relative indicator.

## Results before the changes

Higher is better for rates, lower for latency, allocations and memory. Ratio is tether / nats-pure for rates.

### Publish

| Scenario | tether | nats-pure | Ratio | allocs/msg (tether / nats-pure) |
|---|---|---|---|---|
| 16 B payload | 709k msg/s | 1,137k msg/s | 0.62x | 2.0 / 3.0 |
| 128 B payload | 621k msg/s | 898k msg/s | 0.69x | 2.0 / 3.0 |
| 1 KB payload | 187k msg/s (192 MB/s) | 174k msg/s (179 MB/s) | 1.07x | 2.0 / 3.0 |
| 16 KB payload | 7.3k-12k msg/s (119-197 MB/s) | did not complete | n/a | 2.3 / n/a |
| 128 B with 2 headers | 258k msg/s | 418k msg/s | 0.62x | 10.0 / 10.0 |
| 128 B from 4 threads | 617k msg/s | 898k msg/s | 0.69x | 2.0 / 3.0 |

- nats-pure is faster at publishing small messages by roughly 1.4-1.6x. The gap closes at 1 KB and above, where the
  socket and server dominate.
- On 16 KB payloads nats-pure's `flush` timed out after 30 s in all three rounds, so there is no number for it. Not
  investigated further.
- tether's 16 KB rate varied between rounds (first run 12k msg/s, later runs about 7-8k msg/s).

### Subscribe

| Scenario | tether | nats-pure | Ratio | allocs/msg (tether / nats-pure) |
|---|---|---|---|---|
| 1 subscription, callback, 128 B | 444k msg/s | 86k msg/s | **5.2x** | 5.0 / 18.0 |
| 1 subscription, callback, 1 KB | 98k msg/s | 58k msg/s | 1.7x | 5.2 / 18.2 |
| 1 subscription, sync (`next` / `next_msg`) | 379k msg/s | 301k msg/s | 1.26x | 6.0 / 10.0 |
| 10 subscriptions on one subject (fan-out, 200k deliveries) | 323k-350k msg/s | 39k-40k msg/s | **~8.5x** | 5.0 / 18.0 |
| 1000 subjects, one subscription each | 85k msg/s | 120k msg/s | 0.71x | 5.0 / 18.0 |

- Receiving is where tether is clearly ahead, especially on the callback path: about a quarter of the allocations and
  5x the throughput.
- With 1000 subscriptions tether is slower. Each callback subscription owns a worker thread, and spreading messages
  over 1000 queues and threads costs more than the cheaper per-message path saves.

### Request/reply

| Scenario | tether | nats-pure | allocs/req (tether / nats-pure) |
|---|---|---|---|
| Sequential, 128 B | 1,005 req/s, p50 937 us, p99 2.2 ms | 874 req/s, p50 1,093 us, p99 2.5 ms | 43 / 89 |
| 8 threads, 128 B | 4,349 req/s, p50 1.5 ms, p99 6.6 ms | 3,232 req/s, p50 2.1 ms, p99 7.8 ms | 31 / 77 |

Sequential requests are bound by the Docker round trip (about 1 ms), so the difference is small there. With 8 threads
tether is about 1.35x faster with lower tail latency.

### Connect and memory

| Scenario | tether | nats-pure |
|---|---|---|
| Connect + close (average of 100) | 3.2 ms | 2.9 ms |
| 5000 callback subscriptions: RSS growth | 358 MB | 4.4 MB |
| 5000 callback subscriptions: threads added | 4,997 | 0 |

## Findings from the first run

1. **One thread per callback subscription** (fixed by the worker pool, see the update above). tether starts a worker thread for every subscription with a block. At 5000
   subscriptions that is 358 MB and ~5000 threads. On this machine `Thread.new` fails with `ThreadError` at about 8,190
   subscriptions (the first full run with 10,000 failed this way). nats-pure handles 10,000 with almost no growth. This is
   the one scenario where tether can fail outright, and it is also why the 1000-subject case is slower.
2. **Small-message publish is slower than nats-pure** by 1.4-1.6x (now 1.2-1.3x without YJIT, and ahead with it; see
   the update above).
3. **Large-payload publish**: nats-pure did not complete 16 KB x 40k within the 30 s flush timeout.

## Caveats

- The server runs in Docker on macOS; absolute numbers and request latency include port-forwarding overhead.
- Fan-out was first attempted with 1M deliveries and was invalid: the server disconnects a client that falls more than
  64 MB behind. It was reduced to 200k deliveries.
- nats-pure's default pending limits (65,536 messages) would drop messages at these rates; both libraries ran with the
  limits raised to 100M.
- The publisher for subscribe tests is the same raw-socket feeder for both libraries, so results measure the receive path
  only.
