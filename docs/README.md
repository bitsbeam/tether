# Tether documentation

| Document | What it is |
|---|---|
| [The NATS protocol](protocol/README.md) | A reference for the protocol a client speaks, from the TCP handshake to Key/Value, read from the server source. Four chapters, each ending in a checklist of what a client must implement. |
| [Client design](design.md) | What tether is when finished: the public API, the internal architecture, the concurrency model, and the order it gets built in. Derived from the protocol reference and from a source-level study of `nats.go` and `nats-pure.rb`. |

Start with the protocol overview if NATS is new to you, and with the design if you want to know
how to use or change the gem.
