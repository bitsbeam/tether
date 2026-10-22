# Tether documentation

| Document | What it is |
|---|---|
| [The NATS protocol](protocol/README.md) | A reference for the protocol a client speaks, from the TCP handshake to Key/Value, read from the server source. Four chapters, each ending in a checklist of what a client must implement. |
| [Client design](design.md) | What tether is: the public API, the internal architecture, the concurrency model, and how it was built. Derived from the protocol reference and from a source-level study of `nats.go` and `nats-pure.rb`. |
| [Migrating from nats-pure](migrating-from-nats-pure.md) | A practical guide for code that uses `nats-pure` today: option, method and error renames with before and after examples, how to turn a push subscription into a pull consumer, and the behavior differences that can surprise. |

Start with the [README](../README.md) to use the gem, with the protocol overview if NATS is new to
you, with the design if you want to know why the gem works the way it does or want to change it,
and with the migration guide if you are coming from `nats-pure`.
