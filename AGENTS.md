# AGENTS.md

Tether is a pure-Ruby NATS client: core pub/sub, request/reply, JetStream with pull consumers, Key/Value, Object
Store and Services. No runtime dependencies. Push consumers are deliberately unsupported, don't add them.

## Layout

- `lib/tether/` - the library. `Client` is the public connection object and delegates to `Connection::Lifecycle`,
  which owns the `Writer`, the `Protocol::Registry`, the `Timer` and the `ServerPool`.
- `sig/` - RBS signatures mirroring `lib/`. Keep them in sync with every public or private method you add or change.
- `spec/` - RSpec suite, see [Tests](#tests).
- `docs/protocol/` - the NATS protocol as read from the server source. `docs/design.md` - the client design.
  Protocol facts come from these or from the server source, never from memory.

## Commands

```sh
bundle exec rake                              # specs, rubocop and rbs validate
bundle exec rspec spec/client_spec.rb         # one file
bundle exec rspec spec/client_spec.rb:42      # one example
bundle exec rubocop                           # lint, must be clean
```

The suite starts `nats:alpine` in Docker (`spec/support/nats_server.rb`). Set `NATS_URL` to use a server that is
already running. Specs that need a particular server configuration start their own with `NatsServer.run(config)`.

## Rules for library code

- Every blocking call takes a `Tether::Deadline`. Public methods accept `timeout:` (seconds or a `Deadline`) and
  convert it once with `Deadline.coerce`; internal methods take `deadline` and never start a clock of their own.
- No `Timeout.timeout`, `Thread#raise` or `Thread#kill` anywhere in `lib/`. Block only on `Thread::Queue`,
  `Mutex`, `ConditionVariable` and socket readiness.
- The reader thread never runs user code and never blocks. A `Connection::Resolver` block runs on the reader
  thread and may only look something up, push to a queue, or append a frame.
- Lock order: lifecycle, writer I/O, writer buffer, registry. Never call user code with a lock held.

## Tests

- **Prefer system/feature specs**: drive a real server and assert on observable behavior. They live in `spec/`
  next to the feature, e.g. `spec/client_spec.rb`, `spec/jetstream/consumer_spec.rb`.
- **Unit specs go in `spec/unit/` and nowhere else.** A unit spec tests one class in isolation or its internals,
  with no server: the parser, the encoder, headers, `Deadline`, `Options`. A spec that talks to the server and
  asserts observable behavior stays a feature spec even if it stubs one call. Mirror the `lib/` path:
  `lib/tether/protocol/parser.rb` -> `spec/unit/protocol/parser_spec.rb`. Never add them to the feature spec files.
- Don't test private methods; cover them through the public API.
- Structure every spec file in one of two shapes, nothing else:

  ```ruby
  describe Tether::Client do
    describe "#publish" do
      it "delivers the payload to a subscriber" do
  ```

  ```ruby
  describe Tether::Connection::Subscription do
    describe "#each" do
      context "when a block is given" do
        it "yields each message" do
  ```

  The top `describe` is the class, and the file path mirrors that class's path in `lib/tether/`, for feature and
  unit specs alike: `Tether::Connection::Subscription` -> `spec/connection/subscription_spec.rb`,
  `Tether::Protocol::Parser` -> `spec/unit/protocol/parser_spec.rb`. Each nested `describe` names one public
  method (`"#method"`, `".method"` for class methods), never a topic like `"reconnect"`.
- Use contexts only for contrasting groups, at least two that oppose each other (`"when a block is given"` /
  `"when no block is given"`). A single condition isn't a context: keep the examples flat and put the condition in
  the example name. The exception is a context that carries `let`, hooks (`before`/`after`/`around`) or tags its
  examples share; it may sit next to plain examples. Never nest contexts.
- Every example must be runnable alone with `rspec spec/file_spec.rb:LINE`, so each one gets its own `it` on its
  own line. No `shared_examples`/`include_examples` and no loops that generate examples. Share the steps and
  assertions through a helper method in the `describe` instead, and call it from one-line examples, one per
  variant.
- Start every example name with a verb in the third person: "delivers the payload", "raises when the server is
  gone". Never "must ...", "should ..." or a bare noun.
- Never `sleep` to wait for something. Wait on a queue with a timeout, or poll with a deadline through the
  `eventually` helper.
- Almost no comments in specs; the example name says what is checked.
- For a bug fix, make sure the new spec fails without the fix and passes with it.

## Code style

- Ruby >= 3.2, `# frozen_string_literal: true`, double quotes, RuboCop config in `.rubocop.yml`.
- Prefer self-documenting code over comments. Document public methods with YARD (`@param`, `@return`, `@raise`).
- Add or update RBS types in `sig/` for anything you touch.
- American English everywhere: code, comments, docs, errors, commits.

## Git

- Conventional commits: `<type>(<scope>): <subject>`, type one of `feat|fix|docs|style|ref|test|chore|perf`,
  imperative mood, no trailing period, subject <= 100 chars. Add a body for non-trivial changes.
- One logical change per commit.
- No AI attribution of any kind: no `Co-Authored-By`, no session links.
- Work on a branch, commit locally. Never push or open a PR unless asked.
