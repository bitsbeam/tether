# frozen_string_literal: true

RSpec.describe Tether::JetStream do
  include ClientHelper
  include PublishingHelper

  let(:nc) { connect }
  let(:js) { nc.jetstream }

  let(:name) { "JS#{Tether::Connection::NUID.next}" }

  after { made.each { |stream| error_from { js.delete_stream(stream) } } }

  def made
    @made ||= []
  end

  def make(stream = name, **config)
    made << stream
    js.create_stream(name: stream, storage: :memory, **config)
  end

  describe ".new" do
    it "uses the default API prefix" do
      expect(described_class.new(nc).api.prefix).to eq("$JS.API")
    end

    it "uses the prefix of a domain" do
      expect(described_class.new(nc, domain: "hub").api.prefix).to eq("$JS.hub.API")
    end

    it "uses a custom prefix without its trailing dot" do
      expect(described_class.new(nc, api_prefix: "acme.js.").api.prefix).to eq("acme.js")
    end

    it "reaches the server through a custom prefix" do
      expect(described_class.new(nc, api_prefix: "$JS.API.").account_info.streams).to be >= 0
    end

    it "raises ServiceUnavailableError from a call when nothing serves the domain" do
      expect { described_class.new(nc, domain: "nowhere").account_info }
        .to raise_error(Tether::JetStream::ServiceUnavailableError)
    end

    it "defaults the timeout of its calls to 5 seconds" do
      expect(described_class.new(nc).timeout).to eq(5)
    end

    it "takes the default timeout of its calls" do
      expect(described_class.new(nc, timeout: 0.5).timeout).to eq(0.5)
    end

    it "raises ArgumentError when both a domain and a prefix are given" do
      expect { described_class.new(nc, domain: "hub", api_prefix: "acme.js") }
        .to raise_error(ArgumentError, /cannot be combined/)
    end

    it "raises ArgumentError for an empty domain" do
      expect { described_class.new(nc, domain: "") }.to raise_error(ArgumentError, /invalid domain/)
    end

    it "raises ArgumentError for a domain that is more than one subject token" do
      expect { described_class.new(nc, domain: "hub.eu") }.to raise_error(ArgumentError, /invalid domain/)
    end

    it "raises ArgumentError for an empty prefix" do
      expect { described_class.new(nc, api_prefix: "") }.to raise_error(ArgumentError, /invalid api_prefix/)
    end

    it "raises ArgumentError for a prefix with a wildcard" do
      expect { described_class.new(nc, api_prefix: "acme.*") }.to raise_error(ArgumentError, /invalid api_prefix/)
    end
  end

  describe "#create_stream" do
    before { made << name }

    it "creates a stream from keywords and returns a handle to it" do
      stream = js.create_stream(name: name, subjects: ["#{name}.>"], storage: :memory, max_age: 3600)

      expect(stream).to be_a(Tether::JetStream::Stream).and have_attributes(name: name)
      expect(js.stream(name).config).to have_attributes(subjects: ["#{name}.>"], storage: :memory, max_age: 3600)
    end

    it "creates a stream from a StreamConfig" do
      config = Tether::JetStream::StreamConfig.new(name: name, storage: :memory, retention: :work_queue)

      expect(js.create_stream(config).config.retention).to eq(:work_queue)
    end

    it "creates a stream from a hash, with keywords overriding it" do
      stream = js.create_stream({ name: name, storage: :memory, max_messages: 5 }, max_messages: 9)

      expect(stream.config).to have_attributes(storage: :memory, max_messages: 9)
    end

    it "returns a handle that carries the configuration with the server's defaults" do
      config = js.create_stream(name: name, storage: :memory).config

      expect(config).to have_attributes(subjects: [name], retention: :limits, discard: :old, replicas: 1,
                                        max_messages: -1, duplicate_window: 120)
    end

    it "sends nested structures and durations in the server's form" do
      stream = js.create_stream(name: name, storage: :memory, subjects: ["#{name}.>"], duplicate_window: 0.5,
                                republish: { source: ">", destination: "copy.#{name}.>" },
                                consumer_limits: { max_ack_pending: 50 })

      expect(stream.config.duplicate_window).to eq(0.5)
      expect(stream.config.republish).to have_attributes(source: ">", destination: "copy.#{name}.>")
      expect(stream.config.consumer_limits.max_ack_pending).to eq(50)
    end

    it "returns the existing stream when it is created again with the same configuration" do
      js.create_stream(name: name, storage: :memory, max_messages: 5)

      expect(js.create_stream(name: name, storage: :memory, max_messages: 5).config.max_messages).to eq(5)
    end

    it "raises StreamNameInUseError when the name is taken by a stream configured differently" do
      js.create_stream(name: name, storage: :memory, max_messages: 5)

      expect { js.create_stream(name: name, storage: :memory, max_messages: 6) }
        .to raise_error(Tether::JetStream::StreamNameInUseError)
    end

    it "creates a stream with a feature of a newer API level" do
      stream = js.create_stream(name: name, storage: :memory, allow_message_ttl: true)

      expect(stream.config.allow_message_ttl).to be(true)
    end

    it "requires the API level of the features the configuration uses" do
      allow(js.api).to receive(:request).and_call_original
      js.create_stream(name: name, storage: :memory, allow_message_ttl: true)

      expect(js.api).to have_received(:request).with("STREAM.CREATE.#{name}", anything, timeout: nil, level: 1)
    end

    it "raises the server's error for a configuration the server refuses" do
      expect { js.create_stream(name: name, storage: :memory, max_age: 0.001) }
        .to raise_error(Tether::JetStream::APIError, /max age/)
    end

    it "raises ArgumentError for a field that does not exist" do
      expect { js.create_stream(name: name, subject: "a") }.to raise_error(ArgumentError, /StreamConfig field: subject/)
    end

    it "raises ArgumentError for a name that cannot be part of a subject" do
      expect { js.create_stream(name: "a.b") }.to raise_error(ArgumentError, /invalid name/)
    end

    it "raises ArgumentError without a name" do
      expect { js.create_stream(subjects: ["a.>"]) }.to raise_error(ArgumentError, /invalid name/)
    end

    it "raises TimeoutError when the deadline it is given has passed" do
      expect { js.create_stream(name: name, timeout: Tether::Deadline.after(0)) }.to raise_error(Tether::TimeoutError)
    end
  end

  describe "#update_stream" do
    it "replaces the configuration of the stream" do
      make(max_messages: 5)
      stream = js.update_stream(name: name, storage: :memory, max_messages: 9, description: "changed")

      expect(stream.config).to have_attributes(max_messages: 9, description: "changed")
      expect(js.stream(name).config.max_messages).to eq(9)
    end

    it "resets a field the new configuration leaves out" do
      make(max_messages: 5)

      expect(js.update_stream(name: name, storage: :memory).config.max_messages).to eq(-1)
    end

    it "takes a configuration read from the server with one field changed" do
      config = make(max_messages: 5).config

      expect(js.update_stream(config.with(max_age: 3600)).config).to have_attributes(max_messages: 5, max_age: 3600)
    end

    it "raises StreamNotFoundError when the stream does not exist" do
      expect { js.update_stream(name: name, storage: :memory) }.to raise_error(Tether::JetStream::StreamNotFoundError)
    end

    it "raises the server's error for a change the server refuses" do
      make

      expect { js.update_stream(name: name, storage: :file) }.to raise_error(Tether::JetStream::APIError, /storage/)
    end
  end

  describe "#create_or_update_stream" do
    before { made << name }

    it "creates the stream when it does not exist" do
      stream = js.create_or_update_stream(name: name, storage: :memory, max_messages: 5)

      expect(stream).to be_a(Tether::JetStream::Stream)
      expect(js.stream(name).config.max_messages).to eq(5)
    end

    it "updates the stream when it exists with another configuration" do
      js.create_stream(name: name, storage: :memory, max_messages: 5)

      expect(js.create_or_update_stream(name: name, storage: :memory, max_messages: 9).config.max_messages).to eq(9)
    end

    it "asks for an update first and creates only when the stream is missing" do
      allow(js.api).to receive(:request).and_call_original
      js.create_or_update_stream(name: name, storage: :memory)

      expect(js.api).to have_received(:request).with("STREAM.UPDATE.#{name}", any_args).ordered
      expect(js.api).to have_received(:request).with("STREAM.CREATE.#{name}", any_args).ordered
    end

    it "does not ask to create a stream that exists" do
      js.create_stream(name: name, storage: :memory)
      allow(js.api).to receive(:request).and_call_original
      js.create_or_update_stream(name: name, storage: :memory, max_messages: 9)

      expect(js.api).not_to have_received(:request).with("STREAM.CREATE.#{name}", any_args)
    end

    it "spends one deadline on the update and the create" do
      deadlines = []
      allow(js.api).to receive(:request).and_wrap_original do |original, *arguments, **options|
        deadlines << options[:timeout] if arguments.first.match?(/\ASTREAM\.(UPDATE|CREATE)\./)
        original.call(*arguments, **options)
      end
      js.create_or_update_stream(name: name, storage: :memory, timeout: 3)

      expect(deadlines).to match([a_kind_of(Tether::Deadline), be(deadlines.first)])
    end

    it "raises the server's error for a change the server refuses" do
      js.create_stream(name: name, storage: :memory)

      expect { js.create_or_update_stream(name: name, storage: :file) }
        .to raise_error(Tether::JetStream::APIError, /storage/)
    end
  end

  describe "#stream" do
    it "returns a handle with the stream's info" do
      make(subjects: ["#{name}.>"])
      js.publish("#{name}.a", "one")
      stream = js.stream(name)

      expect(stream).to be_a(Tether::JetStream::Stream).and have_attributes(name: name)
      expect(stream.cached_info.state.messages).to eq(1)
      expect(stream.config.subjects).to eq(["#{name}.>"])
    end

    it "raises StreamNotFoundError when the stream does not exist" do
      expect { js.stream(name) }.to raise_error(Tether::JetStream::StreamNotFoundError)
    end

    it "raises ArgumentError for a name that cannot be part of a subject" do
      expect { js.stream("a b") }.to raise_error(ArgumentError, /invalid name/)
    end
  end

  describe "#stream?" do
    it "is true for a stream that exists" do
      make

      expect(js.stream?(name)).to be(true)
    end

    it "is false for a stream that does not exist" do
      expect(js.stream?(name)).to be(false)
    end
  end

  describe "#delete_stream" do
    it "deletes the stream" do
      make

      expect(js.delete_stream(name)).to be(true)
      expect(js.stream?(name)).to be(false)
    end

    it "raises StreamNotFoundError when the stream does not exist" do
      expect { js.delete_stream(name) }.to raise_error(Tether::JetStream::StreamNotFoundError)
    end
  end

  describe "#streams" do
    it "lists the streams as handles with their info" do
      make("#{name}A", subjects: ["#{name}.a"])
      make("#{name}B", subjects: ["#{name}.b"])
      streams = js.streams(subject: "#{name}.*").to_a

      expect(streams).to all(be_a(Tether::JetStream::Stream))
      expect(streams.map(&:name)).to eq(["#{name}A", "#{name}B"])
      expect(streams.first.config.subjects).to eq(["#{name}.a"])
    end

    it "lists every stream when no subject is given" do
      make

      expect(js.streams.map(&:name).to_a).to include(name)
    end

    it "lists only the streams that capture a subject matching the filter" do
      make("#{name}A", subjects: ["#{name}.a"])
      make("#{name}B", subjects: ["#{name}.b"])

      expect(js.streams(subject: "#{name}.b").map(&:name).to_a).to eq(["#{name}B"])
    end

    it "lists nothing when no stream captures the subject" do
      expect(js.streams(subject: "#{name}.none").to_a).to eq([])
    end

    it "fetches nothing until the listing is consumed" do
      allow(js.api).to receive(:request).and_call_original
      listing = js.streams

      expect(js.api).not_to have_received(:request)
      expect(listing).to be_a(Enumerator::Lazy)
    end
  end

  describe "#stream_names" do
    it "lists the names of the streams that capture a subject matching the filter" do
      make("#{name}A", subjects: ["#{name}.a"])
      make("#{name}B", subjects: ["#{name}.b"])

      expect(js.stream_names(subject: "#{name}.>").to_a).to eq(["#{name}A", "#{name}B"])
    end

    it "lists every name when no subject is given" do
      make

      expect(js.stream_names.to_a).to include(name)
    end

    it "lists nothing when no stream captures the subject" do
      expect(js.stream_names(subject: "#{name}.none").to_a).to eq([])
    end
  end

  describe "#stream_name_for" do
    it "returns the name of the stream that captures the subject" do
      make(subjects: ["#{name}.>"])

      expect(js.stream_name_for("#{name}.orders.created")).to eq(name)
    end

    it "returns nil when no stream captures the subject" do
      expect(js.stream_name_for("#{name}.orders.created")).to be_nil
    end
  end

  describe "#account_info" do
    it "returns the usage and limits of the account" do
      make
      info = js.account_info

      expect(info).to be_a(Tether::JetStream::AccountInfo)
      expect(info.streams).to be >= 1
      expect(info).to have_attributes(memory: a_kind_of(Integer), storage: a_kind_of(Integer),
                                      consumers: a_kind_of(Integer))
    end

    it "returns the limits as their own structure" do
      expect(js.account_info.limits).to be_a(Tether::JetStream::AccountLimits)
        .and have_attributes(max_streams: -1, max_bytes_required: false)
    end

    it "returns the API level the server implements" do
      expect(js.account_info.api["level"]).to be >= 1
    end

    it "raises ServiceUnavailableError when JetStream is not enabled on the server" do
      NatsServer.run do |plain|
        expect { connect(plain.uri).jetstream.account_info }
          .to raise_error(Tether::JetStream::ServiceUnavailableError)
      end
    end
  end

  describe "#publish" do
    let(:schedules) do
      require_api_level(js, 2)
      ingest_stream(js, allow_message_schedules: true, allow_message_ttl: true, allow_rollup: true)
    end

    def schedule_error(subject = "#{schedules}.schedule", **schedule)
      js.publish(subject, "body", schedule: schedule)
      nil
    rescue ArgumentError, Tether::Error => e
      e
    end

    def basics(**config)
      @basics ||= ingest_stream(js, **config)
    end

    def publish_error(subject, **options)
      error_from { js.publish(subject, "x", **options) }
    end

    it "returns an acknowledgment with the stream and the sequence of the message" do
      stream = basics
      js.publish("#{stream}.a", "one")
      ack = js.publish("#{stream}.a", "two")

      expect(ack).to be_a(Tether::JetStream::PubAck).and have_attributes(stream: stream, sequence: 2, domain: nil)
      expect(ack).not_to be_duplicate
    end

    it "stores the payload and the caller's headers" do
      js.publish("#{basics}.a", "body", headers: { "X-Kind" => "order" })

      expect(last_stored(js, basics, "#{basics}.a")).to eq(data: "body", headers: { "X-Kind" => "order" })
    end

    it "stores a message without a payload" do
      js.publish("#{basics}.a")

      expect(last_stored(js, basics, "#{basics}.a")).to eq(data: "", headers: {})
    end

    it "leaves the caller's headers as they were" do
      headers = Tether::Protocol::Headers["X-Kind" => "order"]
      js.publish("#{basics}.a", "body", headers: headers, msg_id: "one")

      expect(headers.to_h).to eq("X-Kind" => ["order"])
    end

    it "stores a message once when it is published twice with the same message id" do
      first = js.publish("#{basics}.a", "body", msg_id: "order-1")
      second = js.publish("#{basics}.a", "body", msg_id: "order-1")

      expect(first).not_to be_duplicate
      expect(second).to be_duplicate.and have_attributes(sequence: first.sequence)
      expect(stored_messages(js, basics)).to eq(1)
    end

    it "stores both messages when their message ids differ" do
      js.publish("#{basics}.a", "body", msg_id: "order-1")

      expect(js.publish("#{basics}.a", "body", msg_id: "order-2")).to have_attributes(sequence: 2, duplicate: nil)
    end

    it "stores the message when it reaches the expected stream" do
      expect(js.publish("#{basics}.a", "x", expect: { stream: basics }).sequence).to eq(1)
    end

    it "raises StreamMismatchError when the message reaches another stream than expected" do
      expect(publish_error("#{basics}.a", expect: { stream: "OTHER" }))
        .to be_a(Tether::JetStream::StreamMismatchError).and have_attributes(err_code: 10_060)
    end

    it "stores the message when the stream's last sequence is the expected one" do
      js.publish("#{basics}.a", "x", expect: { last_sequence: 0 })

      expect(js.publish("#{basics}.a", "x", expect: { last_sequence: 1 }).sequence).to eq(2)
    end

    it "raises WrongLastSequenceError when the stream's last sequence is not the expected one" do
      js.publish("#{basics}.a", "x")

      expect(publish_error("#{basics}.a", expect: { last_sequence: 0 }))
        .to be_a(Tether::JetStream::WrongLastSequenceError).and have_attributes(err_code: 10_071)
      expect(stored_messages(js, basics)).to eq(1)
    end

    it "stores the message when the subject's last sequence is the expected one" do
      js.publish("#{basics}.a", "x")
      js.publish("#{basics}.b", "x")

      expect(js.publish("#{basics}.a", "x", expect: { last_subject_sequence: 1 }).sequence).to eq(3)
    end

    it "raises WrongLastSequenceError when the subject's last sequence is not the expected one" do
      js.publish("#{basics}.a", "x")

      expect(publish_error("#{basics}.a", expect: { last_subject_sequence: 0 }))
        .to be_a(Tether::JetStream::WrongLastSequenceError)
    end

    it "stores the message when the last sequence of the expected subjects matches" do
      js.publish("#{basics}.orders.1", "x")
      js.publish("#{basics}.other", "x")

      ack = js.publish("#{basics}.orders.2", "x", expect: { last_subject_sequence: 1, subject: "#{basics}.orders.*" })

      expect(ack.sequence).to eq(3)
    end

    it "raises WrongLastSequenceError when the last sequence of the expected subjects does not match" do
      js.publish("#{basics}.orders.1", "x")

      expect(publish_error("#{basics}.orders.2", expect: { last_subject_sequence: 0, subject: "#{basics}.orders.*" }))
        .to be_a(Tether::JetStream::WrongLastSequenceError)
    end

    it "stores the message when the last message id is the expected one" do
      js.publish("#{basics}.a", "x", msg_id: "one")

      expect(js.publish("#{basics}.a", "x", msg_id: "two", expect: { last_msg_id: "one" }).sequence).to eq(2)
    end

    it "raises WrongLastSequenceError when the last message id is not the expected one" do
      js.publish("#{basics}.a", "x", msg_id: "one")

      expect(publish_error("#{basics}.a", expect: { last_msg_id: "zero" }))
        .to be_a(Tether::JetStream::WrongLastSequenceError).and have_attributes(err_code: 10_070)
    end

    it "raises ArgumentError for an expectation that does not exist" do
      expect { js.publish("#{basics}.a", "x", expect: { last_seq: 1 }) }
        .to raise_error(ArgumentError, /unknown expectation: :last_seq/)
    end

    it "raises ArgumentError for an empty expected subject" do
      expect { js.publish("#{basics}.a", "x", expect: { last_subject_sequence: 1, subject: "" }) }
        .to raise_error(ArgumentError, /cannot be empty/)
    end

    it "sends a time to live in whole seconds" do
      js.publish("#{basics(allow_message_ttl: true)}.a", "x", ttl: 90)

      expect(last_stored(js, basics, "#{basics}.a")[:headers]).to eq("Nats-TTL" => "90s")
    end

    it "sends a fractional time to live in milliseconds" do
      js.publish("#{basics(allow_message_ttl: true)}.a", "x", ttl: 1.5)

      expect(last_stored(js, basics, "#{basics}.a")[:headers]).to eq("Nats-TTL" => "1500ms")
    end

    it "sends a time to live that never ends" do
      js.publish("#{basics(allow_message_ttl: true)}.a", "x", ttl: :never)

      expect(last_stored(js, basics, "#{basics}.a")[:headers]).to eq("Nats-TTL" => "never")
    end

    it "raises the server's error for a time to live on a stream that does not allow one" do
      expect(publish_error("#{basics}.a", ttl: 90)).to be_a(Tether::JetStream::APIError)
        .and have_attributes(err_code: 10_166)
    end

    it "raises the server's error for a time to live under a second" do
      expect(publish_error("#{basics(allow_message_ttl: true)}.a", ttl: 0.5))
        .to be_a(Tether::JetStream::APIError).and have_attributes(err_code: 10_165)
    end

    it "raises ArgumentError for a time to live that is not a number" do
      expect { js.publish("#{basics}.a", "x", ttl: "soon") }.to raise_error(ArgumentError, /ttl/)
    end

    it "replaces the earlier messages on the subject with a subject rollup" do
      stream = basics(allow_rollup: true)
      2.times { js.publish("#{stream}.a", "old") }
      js.publish("#{stream}.b", "kept")
      js.publish("#{stream}.a", "new", rollup: :subject)

      expect(stored_subjects(js, stream)).to eq("#{stream}.a" => 1, "#{stream}.b" => 1)
      expect(last_stored(js, stream, "#{stream}.a")).to eq(data: "new", headers: { "Nats-Rollup" => "sub" })
    end

    it "replaces every earlier message in the stream with a rollup of all" do
      stream = basics(allow_rollup: true)
      js.publish("#{stream}.a", "old")
      js.publish("#{stream}.b", "old")
      js.publish("#{stream}.c", "new", rollup: :all)

      expect(stored_subjects(js, stream)).to eq("#{stream}.c" => 1)
    end

    it "raises the server's error for a rollup on a stream that does not allow one" do
      expect(publish_error("#{basics}.a", rollup: :all)).to be_a(Tether::JetStream::APIError)
        .and have_attributes(description: /rollup not permitted/)
    end

    it "raises ArgumentError for a rollup that does not exist" do
      expect { js.publish("#{basics}.a", "x", rollup: :everything) }.to raise_error(ArgumentError, /rollup/)
    end

    it "raises NoStreamResponseError after every attempt when no stream captures the subject" do
      allow(nc).to receive(:request).and_call_original

      expect { js.publish("uncaptured.#{name}", "x", retry_wait: 0.01) }
        .to raise_error(Tether::JetStream::NoStreamResponseError, /uncaptured/)
      expect(nc).to have_received(:request).exactly(3).times
    end

    it "makes as many extra attempts as it is told to" do
      allow(nc).to receive(:request).and_call_original

      expect(publish_error("uncaptured.#{name}", retries: 4, retry_wait: 0.01)).to be_retryable
      expect(nc).to have_received(:request).exactly(5).times
    end

    it "makes a single attempt when told not to retry" do
      allow(nc).to receive(:request).and_call_original
      publish_error("uncaptured.#{name}", retries: 0)

      expect(nc).to have_received(:request).once
    end

    it "stores the message when a stream starts capturing the subject between attempts" do
      attempts = 0
      allow(nc).to receive(:request).and_wrap_original do |original, *arguments, **options|
        make(subjects: ["late.#{name}"]) if arguments.first == "late.#{name}" && (attempts += 1) == 2
        original.call(*arguments, **options)
      end

      expect(js.publish("late.#{name}", "x", retry_wait: 0.01)).to have_attributes(stream: name, sequence: 1)
    end

    it "raises TimeoutError when the deadline passes while it waits to retry" do
      expect { js.publish("uncaptured.#{name}", "x", retry_wait: 30, timeout: 0.05) }
        .to raise_error(Tether::TimeoutError)
    end

    it "raises TimeoutError when the deadline it is given has passed" do
      expect { js.publish("#{basics}.a", "x", timeout: Tether::Deadline.after(0)) }
        .to raise_error(Tether::TimeoutError)
    end

    it "raises a JetStream error when the reply is not an acknowledgment" do
      nc.subscribe("service.#{name}") { |msg| msg.respond("{}") }

      expect { js.publish("service.#{name}", "x") }
        .to raise_error(Tether::JetStream::Error, /not a JetStream acknowledgment/)
    end

    it "delivers the message to the target when a schedule with at: comes due" do
      js.publish("#{schedules}.schedule", "report", schedule: { at: Time.now, target: "#{schedules}.out" })
      eventually(timeout: 5) { stored_subjects(js, schedules).key?("#{schedules}.out") }

      expect(last_stored(js, schedules, "#{schedules}.out")).to match(
        data: "report", headers: { "Nats-Scheduler" => "#{schedules}.schedule", "Nats-Schedule-Next" => "purge" }
      )
    end

    it "stores a repeating schedule with its target, source and ttl" do
      js.publish("#{schedules}.schedule", "body",
                 schedule: { every: 90, target: "#{schedules}.out", source: "#{schedules}.source", ttl: 1.5 })

      expect(last_stored(js, schedules, "#{schedules}.schedule")[:headers]).to include(
        "Nats-Schedule" => "@every 90s", "Nats-Schedule-Target" => "#{schedules}.out",
        "Nats-Schedule-Source" => "#{schedules}.source", "Nats-Schedule-TTL" => "1500ms"
      )
    end

    it "stores a cron schedule with its time zone and rollup" do
      js.publish("#{schedules}.schedule", "body", schedule: { cron: "0 0 9 * * *", target: "#{schedules}.out",
                                                              time_zone: "Europe/Berlin", rollup: true })

      expect(last_stored(js, schedules, "#{schedules}.schedule")[:headers]).to include(
        "Nats-Schedule" => "0 0 9 * * *", "Nats-Schedule-Time-Zone" => "Europe/Berlin",
        "Nats-Schedule-Rollup" => "sub"
      )
    end

    it "stores a schedule whose produced messages never expire" do
      js.publish("#{schedules}.schedule", "body", schedule: { cron: "@daily", target: "#{schedules}.out",
                                                              ttl: :never })

      expect(last_stored(js, schedules, "#{schedules}.schedule")[:headers]).to include("Nats-Schedule-TTL" => "never")
    end

    it "sends a schedule time with its fraction of a second in UTC" do
      at = Time.new(2099, 1, 2, 3, 4, 5.25, "+02:00")
      js.publish("#{schedules}.schedule", "body", schedule: { at: at, target: "#{schedules}.out" })

      expect(last_stored(js, schedules, "#{schedules}.schedule")[:headers])
        .to include("Nats-Schedule" => "@at 2099-01-02T01:04:05.250000000Z")
    end

    it "replaces the schedule on a subject when another is published to it" do
      js.publish("#{schedules}.schedule", "old", schedule: { every: 60, target: "#{schedules}.out" })
      js.publish("#{schedules}.schedule", "new", schedule: { every: 120, target: "#{schedules}.out" })

      expect(stored_subjects(js, schedules)).to eq("#{schedules}.schedule" => 1)
    end

    it "removes a schedule when cancel_schedule: names its subject" do
      js.publish("#{schedules}.schedule", "body", schedule: { every: 60, target: "#{schedules}.out" })
      js.publish("#{schedules}.out", "final", cancel_schedule: "#{schedules}.schedule")

      expect(stored_subjects(js, schedules)).to eq("#{schedules}.out" => 1)
    end

    it "raises SchedulesDisabledError when the stream does not allow schedules" do
      require_api_level(js, 2)
      plain = ingest_stream(js)

      expect(schedule_error("#{plain}.schedule", every: 60, target: "#{plain}.out"))
        .to be_a(Tether::JetStream::SchedulesDisabledError)
    end

    it "raises InvalidScheduleError when the target is the schedule's own subject" do
      expect(schedule_error(every: 60, target: "#{schedules}.schedule"))
        .to be_a(Tether::JetStream::InvalidScheduleError).and have_attributes(err_code: 10_190)
    end

    it "raises InvalidScheduleError when the server cannot parse a cron expression" do
      expect(schedule_error(cron: "every now and then", target: "#{schedules}.out"))
        .to be_a(Tether::JetStream::InvalidScheduleError).and have_attributes(err_code: 10_189)
    end

    it "raises InvalidScheduleError when the server does not know the time zone" do
      expect(schedule_error(cron: "@daily", target: "#{schedules}.out", time_zone: "Mars/Olympus"))
        .to be_a(Tether::JetStream::InvalidScheduleError).and have_attributes(err_code: 10_223)
    end

    it "raises InvalidScheduleError when cancel_schedule: names the subject published to" do
      error = error_from { js.publish("#{schedules}.out", "x", cancel_schedule: "#{schedules}.out") }

      expect(error).to be_a(Tether::JetStream::InvalidScheduleError).and have_attributes(err_code: 10_212)
    end

    it "raises ArgumentError when a schedule has no target" do
      expect(schedule_error(every: 60)).to be_a(ArgumentError).and have_attributes(message: /target/)
    end

    it "raises ArgumentError when a schedule has an empty target" do
      expect(schedule_error(every: 60, target: " ")).to be_a(ArgumentError).and have_attributes(message: /target/)
    end

    it "raises ArgumentError when a schedule has no time" do
      expect(schedule_error(target: "#{schedules}.out")).to be_a(ArgumentError)
    end

    it "raises ArgumentError when a schedule has more than one time" do
      expect(schedule_error(every: 60, cron: "@daily", target: "#{schedules}.out")).to be_a(ArgumentError)
    end

    it "raises ArgumentError when every: is under a second" do
      expect(schedule_error(every: 0.5, target: "#{schedules}.out"))
        .to be_a(ArgumentError).and have_attributes(message: /at least 1 second/)
    end

    it "raises ArgumentError when at: is not a time" do
      expect(schedule_error(at: "tomorrow", target: "#{schedules}.out")).to be_a(ArgumentError)
    end

    it "raises ArgumentError when a schedule has an empty cron expression" do
      expect(schedule_error(cron: "", target: "#{schedules}.out")).to be_a(ArgumentError)
    end

    it "raises ArgumentError when a schedule has an empty source" do
      expect(schedule_error(every: 60, target: "#{schedules}.out", source: "")).to be_a(ArgumentError)
    end

    it "raises ArgumentError when a schedule has an empty time zone" do
      expect(schedule_error(cron: "@daily", target: "#{schedules}.out", time_zone: "")).to be_a(ArgumentError)
    end

    it "raises ArgumentError when the ttl of a schedule is not positive" do
      expect(schedule_error(every: 60, target: "#{schedules}.out", ttl: 0)).to be_a(ArgumentError)
    end

    it "raises ArgumentError for an unknown schedule option" do
      expect(schedule_error(every: 60, target: "#{schedules}.out", jitter: 5))
        .to be_a(ArgumentError).and have_attributes(message: /jitter/)
    end

    it "raises ArgumentError when cancel_schedule: is empty" do
      expect { js.publish("#{schedules}.out", "x", cancel_schedule: "") }.to raise_error(ArgumentError)
    end
  end

  describe "#publish_async" do
    include ForkHelper

    let(:stream) { ingest_stream(js) }
    let(:silent) { ingest_stream(js, no_ack: true) }

    it "returns a future that resolves to the acknowledgment" do
      future = js.publish_async("#{stream}.a", "one")

      expect(future.value(timeout: 2)).to have_attributes(stream: stream, sequence: 1)
    end

    it "stores the messages in the order they were published" do
      futures = Array.new(200) { |index| js.publish_async("#{stream}.a", index.to_s) }

      expect(futures.map { |future| future.value(timeout: 5).sequence }).to eq((1..200).to_a)
    end

    it "sends the publish options with the message" do
      js.publish_async("#{stream}.a", "one", msg_id: "id-1").value(timeout: 2)
      again = js.publish_async("#{stream}.a", "one", msg_id: "id-1", headers: { "X-Trace" => "1" })

      expect(again.value(timeout: 2)).to be_duplicate.and have_attributes(sequence: 1)
    end

    it "surfaces a failed expectation through the future" do
      future = js.publish_async("#{stream}.a", "one", expect: { last_sequence: 7 })

      expect { future.value(timeout: 2) }
        .to raise_error(Tether::JetStream::WrongLastSequenceError, /wrong last sequence: 0/)
    end

    it "fails the future with NoStreamResponseError when no stream captures the subject" do
      future = js.publish_async(topic, "one")
      future.wait(timeout: 2)

      expect(future.error).to be_a(Tether::JetStream::NoStreamResponseError)
    end

    it "waits for room when the window of pending publishes is full" do
      windowed = nc.jetstream(publish_async_max_pending: 1)
      futures = Array.new(20) { windowed.publish_async("#{stream}.a", "x", stall: 5) }

      expect(futures.map { |future| future.value(timeout: 2).sequence }).to eq((1..20).to_a)
    end

    it "raises TooManyStalledMessagesError when the window stays full" do
      windowed = nc.jetstream(publish_async_max_pending: 2)
      2.times { windowed.publish_async("#{silent}.a", "x") }

      expect { windowed.publish_async("#{silent}.a", "x", stall: 0.05) }
        .to raise_error(Tether::JetStream::TooManyStalledMessagesError)
    end

    it "does not send the message that found the window full" do
      windowed = nc.jetstream(publish_async_max_pending: 1)
      windowed.publish_async("#{silent}.a", "x")
      error_from { windowed.publish_async("#{silent}.a", "y", stall: 0.05) }
      nc.flush

      expect(stored_messages(js, silent)).to eq(1)
    end

    it "fails the future with TimeoutError when publish_async_timeout passes without an acknowledgment" do
      impatient = nc.jetstream(publish_async_timeout: 0.05)
      future = impatient.publish_async("#{silent}.a", "x")

      expect { future.value(timeout: 2) }.to raise_error(Tether::TimeoutError, /no acknowledgment within 0.05s/)
    end

    it "frees the window when publish_async_timeout fails a future" do
      impatient = nc.jetstream(publish_async_timeout: 0.05, publish_async_max_pending: 1)
      impatient.publish_async("#{silent}.a", "x")
      impatient.publish_async("#{silent}.a", "y", stall: 2).wait(timeout: 2)

      expect(impatient.pending_publishes).to eq(0)
    end

    it "leaves acknowledged futures alone when publish_async_timeout passes" do
      impatient = nc.jetstream(publish_async_timeout: 0.05)
      acknowledged = impatient.publish_async("#{stream}.a", "x")
      acknowledged.wait(timeout: 2)
      impatient.publish_async("#{silent}.a", "y").wait(timeout: 2)

      expect(acknowledged.error).to be_nil
    end

    it "fails in-flight futures with DisconnectedError when the connection drops" do
      remote = connect(proxy.uri, reconnect: ClientHelper::FAST_RECONNECT).jetstream
      future = remote.publish_async("#{silent}.a", "x")
      remote.client.flush
      proxy.stop

      expect { future.value(timeout: 2) }.to raise_error(Tether::DisconnectedError)
    end

    it "acknowledges publishes made after a reconnect" do
      remote = connect(proxy.uri, reconnect: ClientHelper::FAST_RECONNECT).jetstream
      remote.publish_async("#{stream}.a", "before").value(timeout: 2)
      proxy.stop
      eventually { remote.client.reconnecting? }
      proxy.start
      eventually { remote.client.connected? }

      expect(remote.publish_async("#{stream}.a", "after").value(timeout: 2).sequence).to eq(2)
    end

    it "fails in-flight futures with ConnectionClosedError when the client is closed" do
      closing = connect.jetstream
      future = closing.publish_async("#{silent}.a", "x")
      closing.client.close

      expect { future.value(timeout: 2) }.to raise_error(Tether::ConnectionClosedError)
    end

    it "raises ConnectionClosedError on a closed client" do
      closed = connect.jetstream
      closed.client.close

      expect { closed.publish_async("#{stream}.a", "x") }.to raise_error(Tether::ConnectionClosedError)
    end

    it "fails futures inherited across a fork with ForkedError" do
      skip "fork is not available" unless Process.respond_to?(:fork)
      future = js.publish_async("#{silent}.a", "x")
      nc.flush

      expect(in_child { [future.done?, future.error.class.name, js.pending_publishes] })
        .to eq([true, "Tether::ForkedError", 0])
    end

    it "publishes from a forked child over a connection of its own" do
      skip "fork is not available" unless Process.respond_to?(:fork)
      js.publish_async("#{stream}.a", "parent").value(timeout: 2)

      expect(in_child { js.publish_async("#{stream}.a", "child").value(timeout: 5).sequence }).to eq(2)
    end

    it "raises ArgumentError for an option it does not know" do
      expect { js.publish_async("#{stream}.a", "x", retries: 3) }.to raise_error(ArgumentError, /retries/)
    end

    it "raises InvalidSubjectError for a subject that cannot be published to" do
      expect { js.publish_async("bad subject", "x") }.to raise_error(Tether::InvalidSubjectError)
    end

    it "forgets the future of a message that could not be sent" do
      error_from { js.publish_async("bad subject", "x") }

      expect(js.pending_publishes).to eq(0)
    end
  end

  describe "#pending_publishes" do
    it "returns zero before anything was published" do
      expect(js.pending_publishes).to eq(0)
    end

    it "counts the publishes whose acknowledgment has not arrived" do
      silent = ingest_stream(js, no_ack: true)
      3.times { js.publish_async("#{silent}.a", "x") }

      expect(js.pending_publishes).to eq(3)
    end

    it "returns to zero once every acknowledgment arrived" do
      stream = ingest_stream(js)
      10.times { js.publish_async("#{stream}.a", "x") }
      js.wait_for_publishes(timeout: 2)

      expect(js.pending_publishes).to eq(0)
    end

    it "counts each context on its own" do
      silent = ingest_stream(js, no_ack: true)
      js.publish_async("#{silent}.a", "x")

      expect(nc.jetstream.pending_publishes).to eq(0)
    end
  end

  describe "#wait_for_publishes" do
    it "returns true at once when nothing is pending" do
      expect(js.wait_for_publishes(timeout: 0)).to be(true)
    end

    it "returns true once every publish is acknowledged" do
      stream = ingest_stream(js)
      futures = Array.new(500) { js.publish_async("#{stream}.a", "x") }

      expect([js.wait_for_publishes(timeout: 5), futures.all?(&:done?)]).to eq([true, true])
    end

    it "returns true when the pending publishes failed" do
      js.publish_async(topic, "x")

      expect(js.wait_for_publishes(timeout: 2)).to be(true)
    end

    it "returns false when acknowledgments are still outstanding at the timeout" do
      silent = ingest_stream(js, no_ack: true)
      js.publish_async("#{silent}.a", "x")

      expect(js.wait_for_publishes(timeout: 0.05)).to be(false)
    end

    it "accepts a deadline" do
      silent = ingest_stream(js, no_ack: true)
      js.publish_async("#{silent}.a", "x")

      expect(js.wait_for_publishes(timeout: Tether::Deadline.after(0.05))).to be(false)
    end
  end

  describe "#on_publish_error" do
    let(:failed) { Thread::Queue.new }

    it "calls the block with each future that failed" do
      js.on_publish_error { |future| failed.push(future) }
      js.publish_async(topic, "x")

      expect(failed.pop(timeout: 2)).to have_attributes(subject: topic,
                                                        error: a_kind_of(Tether::JetStream::NoStreamResponseError))
    end

    it "calls the block with a future the server refused" do
      stream = ingest_stream(js)
      js.on_publish_error { |future| failed.push(future.error) }
      js.publish_async("#{stream}.a", "x", expect: { last_sequence: 3 })

      expect(failed.pop(timeout: 2)).to be_a(Tether::JetStream::WrongLastSequenceError)
    end

    it "does not call the block for acknowledged publishes" do
      stream = ingest_stream(js)
      js.on_publish_error { |future| failed.push(future.subject) }
      js.publish_async("#{stream}.a", "x").wait(timeout: 2)
      js.publish_async(topic, "x")

      expect(failed.pop(timeout: 2)).to eq(topic)
    end

    it "calls the block with futures failed by a lost connection" do
      silent = ingest_stream(js, no_ack: true)
      remote = connect(proxy.uri, reconnect: ClientHelper::FAST_RECONNECT).jetstream
      remote.on_publish_error { |future| failed.push(future.error) }
      remote.publish_async("#{silent}.a", "x")
      remote.client.flush
      proxy.stop

      expect(failed.pop(timeout: 2)).to be_a(Tether::DisconnectedError)
    end

    it "calls the block with futures failed by publish_async_timeout" do
      silent = ingest_stream(js, no_ack: true)
      impatient = nc.jetstream(publish_async_timeout: 0.05)
      impatient.on_publish_error { |future| failed.push(future.error) }
      impatient.publish_async("#{silent}.a", "x")

      expect(failed.pop(timeout: 2)).to be_a(Tether::TimeoutError)
    end

    it "lets the block call back into the client" do
      stream = ingest_stream(js)
      js.on_publish_error { failed.push(js.publish("#{stream}.a", "from the handler").sequence) }
      js.publish_async(topic, "x")

      expect(failed.pop(timeout: 2)).to eq(1)
    end

    it "keeps reporting after the block raised and reports what it raised as a handler error" do
      errors = record(nc, :error)
      js.on_publish_error { |future| failed.push(future) && raise("handler failed") }
      2.times { js.publish_async(topic, "x") }
      take(failed, 2)

      expect(errors.pop(timeout: 2).last).to be_a(Tether::HandlerError)
    end

    it "replaces the block registered before" do
      js.on_publish_error { failed.push(:first) }
      js.on_publish_error { failed.push(:second) }
      js.publish_async(topic, "x")

      expect(failed.pop(timeout: 2)).to eq(:second)
    end

    it "returns the context" do
      expect(js.on_publish_error { nil }).to be(js)
    end
  end

  describe "#batch" do
    let(:stream) { ingest_stream(js, allow_atomic: true) }

    before { require_api_level(js, 2) }

    it "stores every message and returns the acknowledgment of the commit" do
      ack = js.batch do |batch|
        batch.publish("#{stream}.a", "one")
        batch.publish("#{stream}.b", "two")
        batch.publish("#{stream}.a", "three")
      end

      expect(ack).to have_attributes(stream: stream, sequence: 3, count: 3, batch_id: a_string_matching(/\A\S{1,64}\z/))
    end

    it "stores nothing before the block returns" do
      during = nil
      js.batch do |batch|
        batch.publish("#{stream}.a", "one").publish("#{stream}.a", "two")
        during = stored_messages(js, stream)
      end

      expect([during, stored_messages(js, stream)]).to eq([0, 2])
    end

    it "returns nil when the block published nothing" do
      expect(js.batch { nil }).to be_nil
    end

    it "raises what the block raised" do
      expect { js.batch { |batch| batch.publish("#{stream}.a", "one") && raise(IOError, "disk gone") } }
        .to raise_error(IOError, "disk gone")
    end

    it "stores nothing when the block raises" do
      begin
        js.batch { |batch| batch.publish("#{stream}.a", "one") && raise("abandon") }
      rescue RuntimeError
        nil
      end
      js.publish("#{stream}.a", "marker")

      expect(stored_messages(js, stream)).to eq(1)
    end

    it "frees the batch on the server when the block raises, so abandoned batches never exhaust it" do
      60.times do
        js.batch { |batch| batch.publish("#{stream}.a", "one") && raise("abandon") }
      rescue RuntimeError
        nil
      end

      expect(js.batch { |batch| batch.publish("#{stream}.a", "kept") }).to have_attributes(count: 1, sequence: 1)
    end

    it "raises the failed expectation of any message at the commit and stores nothing" do
      error = error_from do
        js.batch do |batch|
          batch.publish("#{stream}.a", "one")
          batch.publish("#{stream}.b", "two", expect: { last_subject_sequence: 3 })
        end
      end

      expect([error, stored_messages(js, stream)]).to match([a_kind_of(Tether::JetStream::WrongLastSequenceError), 0])
    end

    it "raises BatchPublishDisabledError when the stream does not allow atomic batches" do
      plain = ingest_stream(js)

      expect { js.batch { |batch| batch.publish("#{plain}.a", "one") } }
        .to raise_error(Tether::JetStream::BatchPublishDisabledError)
    end

    it "raises NoStreamResponseError when no stream captures the first subject" do
      expect { js.batch { |batch| batch.publish(topic, "one") } }
        .to raise_error(Tether::JetStream::NoStreamResponseError)
    end

    it "raises IncompleteBatchError at the commit when the server gave up on an idle batch" do
      NatsServer.run(%(jetstream { limits { batch { timeout: "100ms" } } })) do |dedicated|
        remote = connect(dedicated.uri)
        idle = remote.jetstream
        idle.create_stream(name: "IDLE", subjects: ["idle.>"], storage: :memory, allow_atomic: true)
        abandoned = collect(remote, "$JS.EVENT.ADVISORY.STREAM.BATCH_ABANDONED.IDLE")

        expect { idle.batch { |batch| batch.publish("idle.a", "one") && abandoned.pop(timeout: 5) } }
          .to raise_error(Tether::JetStream::IncompleteBatchError)
      end
    end

    it "raises TimeoutError when the stream does not acknowledge" do
      silent = ingest_stream(js, allow_atomic: true, no_ack: true)

      expect { js.batch(timeout: 0.1) { |batch| batch.publish("#{silent}.a", "one") } }
        .to raise_error(Tether::TimeoutError)
    end
  end

  describe "#fast_batch" do
    let(:stream) { ingest_stream(js, allow_batched: true) }

    before { require_api_level(js, 4) }

    it "stores every message and returns the final acknowledgment" do
      ack = js.fast_batch { |batch| 250.times { |index| batch.publish("#{stream}.rows", index.to_s) } }

      expect([ack, stored_messages(js, stream)])
        .to match([have_attributes(stream: stream, count: 250, sequence: 250, batch_id: a_kind_of(String)), 250])
    end

    it "stores the messages in the order they were published" do
      js.fast_batch(flow: 3) { |batch| 10.times { |index| batch.publish("#{stream}.rows", index.to_s) } }

      expect(last_stored(js, stream, "#{stream}.rows")[:data]).to eq("9")
    end

    it "returns nil when the block published nothing" do
      expect(js.fast_batch { nil }).to be_nil
    end

    it "sends the publish options with each message" do
      ack = js.fast_batch do |batch|
        batch.publish("#{stream}.rows", "one", msg_id: "row-1")
        batch.publish("#{stream}.rows", "one again", msg_id: "row-1")
      end

      expect([ack.count, stored_messages(js, stream)]).to eq([2, 1])
    end

    it "raises the server's error for a refused message with gap: :fail" do
      expect do
        js.fast_batch do |batch|
          batch.publish("#{stream}.rows", "one")
          batch.publish("#{stream}.rows", "two", expect: { last_sequence: 9 })
        end
      end.to raise_error(Tether::JetStream::WrongLastSequenceError)
    end

    it "keeps what the server stored before a refused message with gap: :fail" do
      error_from do
        js.fast_batch do |batch|
          batch.publish("#{stream}.rows", "one")
          batch.publish("#{stream}.rows", "two", expect: { last_sequence: 9 })
        end
      end

      expect(stored_messages(js, stream)).to eq(1)
    end

    it "raises MessageGapError when the server missed a message with gap: :fail" do
      expect do
        js.fast_batch do |batch|
          batch.publish("#{stream}.rows", "one")
          batch.publish(topic, "lost")
          batch.publish("#{stream}.rows", "three")
        end
      end.to raise_error(Tether::JetStream::MessageGapError, /did not receive messages 2 to 2/)
    end

    it "continues past refused and missed messages with gap: :ok" do
      ack = js.fast_batch(gap: :ok) do |batch|
        batch.publish("#{stream}.rows", "one")
        batch.publish("#{stream}.rows", "two", expect: { last_sequence: 9 })
        batch.publish(topic, "lost")
        batch.publish("#{stream}.rows", "four")
      end

      expect([ack.count, stored_messages(js, stream)]).to eq([4, 2])
    end

    it "keeps what was stored when the block raises" do
      begin
        js.fast_batch(flow: 1) { |batch| batch.publish("#{stream}.rows", "one") && raise("stop") }
      rescue RuntimeError
        nil
      end

      expect(stored_messages(js, stream)).to eq(1)
    end

    it "raises what the block raised" do
      expect { js.fast_batch { |batch| batch.publish("#{stream}.rows", "one") && raise(IOError, "disk gone") } }
        .to raise_error(IOError, "disk gone")
    end

    it "raises BatchPublishDisabledError when the stream does not allow fast-ingest batches" do
      plain = ingest_stream(js)

      expect { js.fast_batch { |batch| batch.publish("#{plain}.rows", "one") } }
        .to raise_error(Tether::JetStream::BatchPublishDisabledError)
    end

    it "raises NoStreamResponseError when no stream captures the first subject" do
      expect { js.fast_batch { |batch| batch.publish(topic, "one") } }
        .to raise_error(Tether::JetStream::NoStreamResponseError)
    end

    it "raises DisconnectedError when the connection drops during the batch" do
      remote = connect(proxy.uri, reconnect: ClientHelper::FAST_RECONNECT).jetstream

      expect do
        remote.fast_batch do |batch|
          batch.publish("#{stream}.rows", "one")
          proxy.stop
          eventually { remote.client.reconnecting? }
          batch.publish("#{stream}.rows", "two")
        end
      end.to raise_error(Tether::DisconnectedError)
    end

    it "raises TimeoutError when the stream does not acknowledge" do
      silent = ingest_stream(js, allow_batched: true, no_ack: true)

      expect { js.fast_batch(timeout: 0.1) { |batch| batch.publish("#{silent}.rows", "one") } }
        .to raise_error(Tether::TimeoutError)
    end

    it "raises ArgumentError for a gap mode it does not know" do
      expect { js.fast_batch(gap: :ignore) { nil } }.to raise_error(ArgumentError, /gap/)
    end

    it "raises ArgumentError for a flow outside 1 to 65,535" do
      expect { js.fast_batch(flow: 0) { nil } }.to raise_error(ArgumentError, /flow/)
    end

    it "raises ArgumentError for more than three windows" do
      expect { js.fast_batch(windows: 4) { nil } }.to raise_error(ArgumentError, /windows/)
    end
  end

  describe "#counter" do
    it "returns a counter for the subject" do
      expect(js.counter("hits.home")).to be_a(Tether::JetStream::Counter).and have_attributes(subject: "hits.home")
    end

    it "raises InvalidSubjectError for a wildcard subject" do
      expect { js.counter("hits.*") }.to raise_error(Tether::InvalidSubjectError)
    end

    it "raises ArgumentError for a stream name that cannot be one" do
      expect { js.counter("hits.home", stream: "no.dots") }.to raise_error(ArgumentError)
    end
  end

  describe "#restore_stream" do
    let(:payloads) { Array.new(12) { Random.bytes(8192) } }
    let(:backup) do
      made << name
      stream = js.create_stream(name: name, subjects: ["#{name}.>"], allow_direct: true)
      payloads.each { |payload| js.publish("#{name}.data", payload) }
      bytes = "".b
      config, state = stream.snapshot { |chunk| bytes << chunk }
      stream.delete
      { config: config, state: state, bytes: bytes }
    end

    def restore(**options, &block)
      block ||= ->(writer) { writer.write(backup[:bytes]) }
      js.restore_stream(backup[:config], backup[:state], **options, &block)
    end

    it "recreates the stream with its messages and returns a handle to it" do
      restored = restore

      expect(restored).to be_a(Tether::JetStream::Stream).and have_attributes(name: name)
      expect(restored.cached_info.state).to have_attributes(messages: 12, last_sequence: 12)
      expect(restored.each_message.map(&:data)).to eq(payloads)
    end

    it "takes the snapshot in pieces of any size" do
      restored = restore do |writer|
        backup[:bytes].each_char.each_slice(1000) { |piece| writer.write(piece.join) }
      end

      expect(restored.info.state.messages).to eq(12)
    end

    it "takes pieces appended with the shovel operator" do
      restored = restore { |writer| writer << backup[:bytes] }

      expect(restored.info.state.messages).to eq(12)
    end

    it "uploads the snapshot in chunks the server takes one at a time" do
      allow(nc).to receive(:request).and_call_original
      restore

      chunks = backup[:bytes].bytesize.fdiv(Tether::JetStream::RestoreWriter::CHUNK_SIZE).ceil
      expect(nc).to have_received(:request).with(/\A\$JS\.SNAPSHOT\.RESTORE\./, any_args).exactly(chunks + 1).times
    end

    it "returns the number of bytes taken from each write" do
      taken = nil
      restore { |writer| taken = writer.write(backup[:bytes]) }

      expect(taken).to eq(backup[:bytes].bytesize)
    end

    it "takes the configuration and state as hashes of keywords" do
      backup
      state = { messages: 12, bytes: backup[:state].bytes, first_sequence: 1, last_sequence: 12 }
      restored = js.restore_stream({ name: name, subjects: ["#{name}.>"] }, state) do |writer|
        writer.write(backup[:bytes])
      end

      expect(restored.info.state.messages).to eq(12)
    end

    it "raises StreamNameInUseError when a stream with the name exists" do
      backup
      js.create_stream(name: name, storage: :memory)

      expect { restore }.to raise_error(Tether::JetStream::StreamNameInUseError)
    end

    it "raises the server's error for bytes that are not a snapshot" do
      expect { restore { |writer| writer.write("not a snapshot" * 100) } }
        .to raise_error(Tether::JetStream::APIError) { |error| expect(error.err_code).to eq(10_062) }
    end

    it "leaves no stream behind when the restore fails" do
      error_from { restore { |writer| writer.write("not a snapshot" * 100) } }

      expect(js.stream?(name)).to be(false)
    end

    it "raises ArgumentError without a block" do
      expect { js.restore_stream(backup[:config], backup[:state]) }.to raise_error(ArgumentError, /block/)
    end

    it "raises ArgumentError for a configuration without a name" do
      expect { js.restore_stream({ subjects: ["a"] }, {}) { nil } }.to raise_error(ArgumentError, /invalid name/)
    end

    it "raises TimeoutError when a deadline for the whole restore has passed" do
      backup

      expect { restore(timeout: Tether::Deadline.after(0)) }.to raise_error(Tether::TimeoutError)
    end
  end

  describe "#advisories" do
    def watch(*kind)
      Thread::Queue.new.tap do |advisories|
        @watching = js.advisories(*kind) { |advisory| advisories.push(advisory) }
        nc.flush
      end
    end

    def subjects_of(advisories, count)
      take(advisories, count).map { |advisory| advisory.subject.delete_prefix("$JS.EVENT.ADVISORY.") }
    end

    it "yields an advisory with its type, subject and decoded event" do
      advisories = watch("STREAM.CREATED.#{name}")
      make
      advisory = advisories.pop(timeout: 2)

      expect(advisory).to be_a(Tether::JetStream::Advisory).and be_frozen
      expect(advisory).to have_attributes(type: "io.nats.jetstream.advisory.v1.stream_action",
                                          subject: "$JS.EVENT.ADVISORY.STREAM.CREATED.#{name}")
      expect(advisory.to_h).to include("stream" => name, "action" => "create")
    end

    it "yields advisories whose keys can be read by name, with an id and a time" do
      advisories = watch("STREAM.CREATED.#{name}")
      make
      advisory = advisories.pop(timeout: 2)

      expect(advisory[:action]).to eq("create")
      expect(advisory["stream"]).to eq(name)
      expect(advisory.id).to be_a(String)
      expect(advisory.time).to be_within(60).of(Time.now)
    end

    it "yields only the advisories of the subject it is given" do
      advisories = watch("STREAM.*.#{name}")
      make
      js.update_stream(name: name, storage: :memory, max_messages: 5)
      js.delete_stream(name)

      expect(subjects_of(advisories, 3))
        .to eq(["STREAM.CREATED.#{name}", "STREAM.UPDATED.#{name}", "STREAM.DELETED.#{name}"])
    end

    it "yields the advisories of a family named by a symbol" do
      advisories = watch(:stream)
      make

      expect(eventually { subjects_of(advisories, 1).include?("STREAM.CREATED.#{name}") }).to be(true)
    end

    it "yields the audit advisory of an API call when asked for every advisory" do
      advisories = watch
      make
      audit = eventually do
        advisory = advisories.pop(timeout: 2)
        advisory if advisory&.subject == "$JS.EVENT.ADVISORY.API" && advisory["subject"].end_with?(".CREATE.#{name}")
      end

      expect(audit.type).to eq("io.nats.jetstream.advisory.v1.api_audit")
    end

    it "returns the subscription, which ends the advisories when it is unsubscribed" do
      advisories = watch("STREAM.*.#{name}")
      make
      take(advisories, 1)
      @watching.unsubscribe
      nc.flush
      js.delete_stream(name)

      expect(@watching).to be_a(Tether::Connection::Subscription).and be_closed
      expect(advisories).to be_empty
    end

    it "raises ArgumentError without a block" do
      expect { js.advisories }.to raise_error(ArgumentError, /block/)
    end

    it "raises ArgumentError for a family that does not exist" do
      expect { js.advisories(:streams) { nil } }.to raise_error(ArgumentError, /unknown advisory kind: :streams/)
    end
  end

  describe "#create_consumer" do
    it "creates a durable consumer and returns a handle to it" do
      make

      consumer = js.create_consumer(name, durable: "billing", max_deliver: 5, ack_wait: 30)

      expect(consumer).to have_attributes(class: Tether::JetStream::Consumer, name: "billing", stream: name)
      expect(consumer.config).to have_attributes(durable: "billing", name: "billing", max_deliver: 5, ack_wait: 30)
    end

    it "sends explicit acknowledgment, deliver all and instant replay unless told otherwise" do
      make

      expect(js.create_consumer(name, durable: "billing").config)
        .to have_attributes(ack_policy: :explicit, deliver_policy: :all, replay_policy: :instant)
    end

    it "creates an ephemeral consumer with a generated name when none is given" do
      make

      consumer = js.create_consumer(name, filter_subjects: ["#{name}.a", "#{name}.b"])

      expect(consumer.name).to match(/\A[A-Za-z0-9]{22}\z/)
      expect(consumer.config).to have_attributes(durable: nil, inactive_threshold: 5,
                                                 filter_subjects: ["#{name}.a", "#{name}.b"])
    end

    it "creates an ephemeral consumer with the name given as name:" do
      make

      expect(js.create_consumer(name, name: "reader").config).to have_attributes(name: "reader", durable: nil)
    end

    it "accepts a ConsumerConfig" do
      make
      config = Tether::JetStream::ConsumerConfig.new(durable: "billing", description: "invoices")

      expect(js.create_consumer(name, config).config.description).to eq("invoices")
    end

    it "derives the deliver policy from start_sequence:" do
      make

      expect(js.create_consumer(name, durable: "billing", start_sequence: 3).config)
        .to have_attributes(deliver_policy: :by_start_sequence, start_sequence: 3)
    end

    it "puts a single literal filter in the request subject" do
      make(subjects: ["#{name}.>"])
      allow(js.api).to receive(:request).and_call_original

      js.create_consumer(name, durable: "billing", filter_subject: "#{name}.eu")

      expect(js.api).to have_received(:request)
        .with("CONSUMER.CREATE.#{name}.billing.#{name}.eu", a_kind_of(Hash), any_args)
    end

    it "keeps a wildcard filter out of the request subject" do
      make(subjects: ["#{name}.>"])

      expect(js.create_consumer(name, durable: "billing", filter_subject: "#{name}.*").config.filter_subject)
        .to eq("#{name}.*")
    end

    it "sends the required API level for a configuration that needs one" do
      make
      allow(js.api).to receive(:request).and_call_original

      js.create_consumer(name, durable: "billing", priority_policy: :overflow, priority_groups: ["jobs"])

      expect(js.api).to have_received(:request).with(/CONSUMER\.CREATE/, a_kind_of(Hash), timeout: nil, level: 1)
    end

    it "returns the existing consumer when it is created again with the same configuration" do
      make
      js.create_consumer(name, durable: "billing", max_deliver: 5)

      expect(js.create_consumer(name, durable: "billing", max_deliver: 5).name).to eq("billing")
    end

    it "raises ConsumerExistsError when it exists with another configuration" do
      make
      js.create_consumer(name, durable: "billing", max_deliver: 5)

      expect { js.create_consumer(name, durable: "billing", max_deliver: 6) }
        .to raise_error(Tether::JetStream::ConsumerExistsError) { |error| expect(error.err_code).to eq(10_148) }
    end

    it "raises StreamNotFoundError when the stream does not exist" do
      expect { js.create_consumer(name, durable: "billing") }.to raise_error(Tether::JetStream::StreamNotFoundError)
    end

    it "raises ArgumentError for a name that cannot be a subject token" do
      expect { js.create_consumer(name, durable: "bill.ing") }.to raise_error(ArgumentError, /invalid name/)
    end

    it "raises ArgumentError when name: and durable: differ" do
      expect { js.create_consumer(name, name: "a", durable: "b") }.to raise_error(ArgumentError, /must be equal/)
    end

    it "raises ArgumentError for a field that would make a push consumer" do
      expect { js.create_consumer(name, deliver_subject: "deliver.x") }
        .to raise_error(ArgumentError, /unknown ConsumerConfig field: deliver_subject/)
    end
  end

  describe "#update_consumer" do
    it "replaces the configuration of an existing consumer" do
      make
      js.create_consumer(name, durable: "billing", max_deliver: 5)

      expect(js.update_consumer(name, durable: "billing", max_deliver: 7).config.max_deliver).to eq(7)
    end

    it "raises ConsumerNotFoundError when the consumer does not exist" do
      make

      expect { js.update_consumer(name, durable: "billing") }
        .to raise_error(Tether::JetStream::ConsumerNotFoundError) { |error| expect(error.err_code).to eq(10_149) }
    end

    it "raises APIError for a field that cannot be changed" do
      make
      js.create_consumer(name, durable: "billing")

      expect { js.update_consumer(name, durable: "billing", ack_policy: :none) }
        .to raise_error(Tether::JetStream::APIError, /ack policy can not be updated/)
    end
  end

  describe "#create_or_update_consumer" do
    it "creates the consumer when it does not exist" do
      make

      expect(js.create_or_update_consumer(name, durable: "billing").name).to eq("billing")
    end

    it "replaces the configuration when it exists" do
      make
      js.create_consumer(name, durable: "billing", max_deliver: 5)

      expect(js.create_or_update_consumer(name, durable: "billing", max_deliver: 7).config.max_deliver).to eq(7)
    end
  end

  describe "#consumer" do
    it "looks an existing consumer up" do
      make
      js.create_consumer(name, durable: "billing", description: "invoices")

      expect(js.consumer(name, "billing").config.description).to eq("invoices")
    end

    it "raises ConsumerNotFoundError when the consumer does not exist, and creates nothing" do
      make

      expect { js.consumer(name, "billing") }.to raise_error(Tether::JetStream::ConsumerNotFoundError)
      expect(js.consumer_names(name).to_a).to be_empty
    end

    it "raises StreamNotFoundError when the stream does not exist" do
      expect { js.consumer(name, "billing") }.to raise_error(Tether::JetStream::StreamNotFoundError)
    end
  end

  describe "#consumer?" do
    it "is true for a consumer that exists" do
      make
      js.create_consumer(name, durable: "billing")

      expect(js.consumer?(name, "billing")).to be(true)
    end

    it "is false for a consumer that does not exist" do
      make

      expect(js.consumer?(name, "billing")).to be(false)
    end

    it "is false when the stream does not exist" do
      expect(js.consumer?(name, "billing")).to be(false)
    end
  end

  describe "#consumers" do
    it "returns a lazy enumerator of handles to every consumer, sorted by name" do
      make
      %w[b a].each { |consumer| js.create_consumer(name, durable: consumer) }

      consumers = js.consumers(name)

      expect(consumers).to be_a(Enumerator::Lazy)
      expect(consumers.map(&:name).to_a).to eq(%w[a b])
    end

    it "walks every page" do
      make
      260.times { |index| js.create_consumer(name, durable: format("c%03d", index)) }

      expect(js.consumers(name).count).to eq(260)
    end

    it "raises StreamNotFoundError when the stream does not exist" do
      expect { js.consumers(name).to_a }.to raise_error(Tether::JetStream::StreamNotFoundError)
    end
  end

  describe "#consumer_names" do
    it "returns a lazy enumerator of every consumer name, sorted" do
      make
      %w[b a].each { |consumer| js.create_consumer(name, durable: consumer) }

      expect(js.consumer_names(name).to_a).to eq(%w[a b])
    end

    it "is empty for a stream without consumers" do
      make

      expect(js.consumer_names(name).to_a).to be_empty
    end
  end

  describe "#delete_consumer" do
    it "removes the consumer" do
      make
      js.create_consumer(name, durable: "billing")

      expect(js.delete_consumer(name, "billing")).to be(true)
      expect(js.consumer?(name, "billing")).to be(false)
    end

    it "raises ConsumerNotFoundError when the consumer does not exist" do
      make

      expect { js.delete_consumer(name, "billing") }.to raise_error(Tether::JetStream::ConsumerNotFoundError)
    end
  end

  describe "#ordered_consumer" do
    it "returns an ordered consumer over the stream without creating anything" do
      make

      expect(js.ordered_consumer(name)).to have_attributes(class: Tether::JetStream::Ordered, stream: name)
      expect(js.consumer_names(name).to_a).to be_empty
    end

    it "raises ArgumentError for an option it does not have" do
      expect { js.ordered_consumer(name, durable: "billing") }.to raise_error(ArgumentError, /unknown keyword/)
    end

    it "raises ArgumentError when start_sequence: and start_time: are combined" do
      expect { js.ordered_consumer(name, start_sequence: 1, start_time: Time.now) }
        .to raise_error(ArgumentError, /cannot be combined/)
    end
  end

  describe "#create_key_value" do
    it "creates the stream behind the bucket and returns the bucket" do
      made << "KV_#{name}"
      bucket = js.create_key_value(bucket: name, history: 5, storage: :memory)

      expect(bucket).to have_attributes(class: Tether::KeyValue, bucket: name)
      expect(js.stream("KV_#{name}").config).to have_attributes(
        subjects: ["$KV.#{name}.>"], max_messages_per_subject: 5, discard: :new, allow_rollup: true,
        deny_delete: true, allow_direct: true, max_messages: -1, max_consumers: -1, duplicate_window: 120
      )
    end

    it "raises BucketExistsError when the bucket exists with other options" do
      made << "KV_#{name}"
      js.create_key_value(bucket: name, storage: :memory)

      expect { js.create_key_value(bucket: name, storage: :memory, history: 9) }
        .to raise_error(Tether::KeyValue::BucketExistsError)
    end

    it "raises InvalidBucketNameError for a name that cannot be a bucket" do
      expect { js.create_key_value(bucket: "no dots.allowed") }
        .to raise_error(Tether::KeyValue::InvalidBucketNameError)
    end

    it "raises ArgumentError without a bucket name" do
      expect { js.create_key_value(history: 5) }.to raise_error(ArgumentError, /bucket/)
    end
  end

  describe "#key_value" do
    it "returns the bucket with the given name" do
      made << "KV_#{name}"
      js.create_key_value(bucket: name, storage: :memory).put("color", "blue")

      expect(js.key_value(name)["color"]).to eq("blue")
    end

    it "raises BucketNotFoundError when the bucket does not exist" do
      expect { js.key_value(name) }.to raise_error(Tether::KeyValue::BucketNotFoundError) do |error|
        expect(error.cause).to be_a(Tether::JetStream::StreamNotFoundError)
      end
    end

    it "does not take a stream that lacks the bucket prefix for a bucket" do
      make

      expect { js.key_value(name) }.to raise_error(Tether::KeyValue::BucketNotFoundError)
    end
  end

  describe "#update_key_value" do
    it "replaces the options of the bucket" do
      made << "KV_#{name}"
      js.create_key_value(bucket: name, storage: :memory)

      expect(js.update_key_value(bucket: name, storage: :memory, history: 10).status.history).to eq(10)
    end

    it "raises BucketNotFoundError when the bucket does not exist" do
      expect { js.update_key_value(bucket: name, history: 10) }
        .to raise_error(Tether::KeyValue::BucketNotFoundError)
    end
  end

  describe "#create_or_update_key_value" do
    it "creates the bucket when it does not exist" do
      made << "KV_#{name}"

      expect(js.create_or_update_key_value(bucket: name, storage: :memory, history: 3).status.history).to eq(3)
    end

    it "updates the bucket when it exists" do
      made << "KV_#{name}"
      js.create_key_value(bucket: name, storage: :memory)

      expect(js.create_or_update_key_value(bucket: name, storage: :memory, history: 8).status.history).to eq(8)
    end
  end

  describe "#delete_key_value" do
    it "deletes the bucket" do
      js.create_key_value(bucket: name, storage: :memory)

      expect(js.delete_key_value(name)).to be(true)
      expect { js.key_value(name) }.to raise_error(Tether::KeyValue::BucketNotFoundError)
    end

    it "raises BucketNotFoundError when the bucket does not exist" do
      expect { js.delete_key_value(name) }.to raise_error(Tether::KeyValue::BucketNotFoundError)
    end
  end

  describe "#key_value_names" do
    it "lists the names of the buckets and of nothing else" do
      made << "KV_#{name}"
      js.create_key_value(bucket: name, storage: :memory)
      make("#{name}_plain", subjects: ["#{name}.>"])

      expect(js.key_value_names.to_a).to include(name)
      expect(js.key_value_names.to_a).not_to include("#{name}_plain")
    end
  end

  describe "#key_values" do
    it "returns a handle for every bucket" do
      made << "KV_#{name}"
      js.create_key_value(bucket: name, storage: :memory)

      expect(js.key_values.map(&:bucket).to_a).to include(name)
      expect(js.key_values.first).to be_a(Tether::KeyValue)
    end
  end

  describe "#create_object_store" do
    def create_request(**options)
      requests = collect(connect, "$JS.API.STREAM.CREATE.OBJ_#{name}")
      made << "OBJ_#{name}"
      error_from { js.create_object_store(bucket: name, **options) }
      JSON.parse(requests.pop(timeout: 2).data)
    end

    it "returns the bucket" do
      made << "OBJ_#{name}"

      expect(js.create_object_store(bucket: name, storage: :memory))
        .to have_attributes(class: Tether::ObjectStore, name: name)
    end

    it "sends exactly the stream configuration of an object store" do
      expect(create_request(storage: :memory))
        .to eq("name" => "OBJ_#{name}", "subjects" => ["$O.#{name}.C.>", "$O.#{name}.M.>"], "max_age" => 0,
               "max_bytes" => -1, "storage" => "memory", "num_replicas" => 1, "discard" => "new",
               "allow_rollup_hdrs" => true, "allow_direct" => true)
    end

    it "defaults to file storage" do
      expect(create_request["storage"]).to eq("file")
    end

    it "sends every option in the stream configuration" do
      request = create_request(description: "build artifacts", ttl: 3600, max_bytes: 5_000_000, storage: :file,
                               replicas: 3, compression: true, placement: { cluster: "east", tags: ["ssd"] },
                               metadata: { "team" => "ops" })

      expect(request)
        .to eq("name" => "OBJ_#{name}", "description" => "build artifacts",
               "subjects" => ["$O.#{name}.C.>", "$O.#{name}.M.>"], "max_age" => 3_600_000_000_000,
               "max_bytes" => 5_000_000, "storage" => "file", "num_replicas" => 3,
               "placement" => { "cluster" => "east", "tags" => ["ssd"] }, "discard" => "new",
               "allow_rollup_hdrs" => true, "allow_direct" => true, "compression" => "s2",
               "metadata" => { "team" => "ops" })
    end

    it "creates the stream behind the bucket with the configuration other clients expect" do
      made << "OBJ_#{name}"
      js.create_object_store(bucket: name, storage: :memory)

      expect(js.stream("OBJ_#{name}").config).to have_attributes(
        name: "OBJ_#{name}", subjects: ["$O.#{name}.C.>", "$O.#{name}.M.>"], retention: :limits, max_age: 0,
        max_bytes: -1, max_messages: -1, max_messages_per_subject: -1, max_message_size: -1, max_consumers: -1,
        storage: :memory, replicas: 1, discard: :new, duplicate_window: 120, allow_rollup: true,
        allow_direct: true, deny_delete: false, deny_purge: false, sealed: false, compression: :none
      )
    end

    it "creates the stream with the options the server keeps" do
      made << "OBJ_#{name}"
      js.create_object_store(bucket: name, description: "build artifacts", ttl: 3600, max_bytes: 5_000_000,
                             storage: :file, compression: true, metadata: { "team" => "ops" })

      expect(js.stream("OBJ_#{name}").config).to have_attributes(
        description: "build artifacts", max_age: 3600, max_bytes: 5_000_000, storage: :file, compression: :s2,
        metadata: include("team" => "ops"), discard: :new, allow_rollup: true, allow_direct: true
      )
    end

    it "returns the bucket when it exists with the same options" do
      made << "OBJ_#{name}"
      js.create_object_store(bucket: name, storage: :memory)

      expect(js.create_object_store(bucket: name, storage: :memory).name).to eq(name)
    end

    it "raises BucketExistsError when the bucket exists with other options" do
      made << "OBJ_#{name}"
      js.create_object_store(bucket: name, storage: :memory)

      expect { js.create_object_store(bucket: name, storage: :memory, max_bytes: 1_000_000) }
        .to raise_error(Tether::ObjectStore::BucketExistsError)
    end

    it "raises InvalidBucketNameError for a name with a dot" do
      expect { js.create_object_store(bucket: "no.dots") }
        .to raise_error(Tether::ObjectStore::InvalidBucketNameError)
    end

    it "raises InvalidBucketNameError for an empty name" do
      expect { js.create_object_store(bucket: "") }.to raise_error(Tether::ObjectStore::InvalidBucketNameError)
    end

    it "raises InvalidBucketNameError for a name longer than 251 characters" do
      expect { js.create_object_store(bucket: "a" * 252) }
        .to raise_error(Tether::ObjectStore::InvalidBucketNameError)
    end

    it "raises ArgumentError for an unknown option" do
      expect { js.create_object_store(bucket: name, history: 5) }.to raise_error(ArgumentError, /history/)
    end
  end

  describe "#object_store" do
    it "binds to an existing bucket" do
      made << "OBJ_#{name}"
      js.create_object_store(bucket: name, storage: :memory).put("object", "hello world")

      expect(js.object_store(name).get("object").read).to eq("hello world")
    end

    it "raises BucketNotFoundError for a bucket that does not exist" do
      expect { js.object_store(name) }.to raise_error(Tether::ObjectStore::BucketNotFoundError, /#{name}/)
    end

    it "raises InvalidBucketNameError for a name that cannot be a bucket" do
      expect { js.object_store("no spaces") }.to raise_error(Tether::ObjectStore::InvalidBucketNameError)
    end
  end

  describe "#delete_object_store" do
    it "deletes the stream behind the bucket" do
      js.create_object_store(bucket: name, storage: :memory)

      expect(js.delete_object_store(name)).to be(true)
      expect(js.stream?("OBJ_#{name}")).to be(false)
    end

    it "raises BucketNotFoundError for a bucket that does not exist" do
      expect { js.delete_object_store(name) }.to raise_error(Tether::ObjectStore::BucketNotFoundError)
    end
  end

  describe "#object_store_names" do
    it "returns the names of the object store buckets" do
      made << "OBJ_#{name}"
      js.create_object_store(bucket: name, storage: :memory)

      expect(js.object_store_names.to_a).to include(name)
    end

    it "leaves streams that are not object stores out" do
      make(subjects: ["#{name}.>"])

      expect(js.object_store_names.to_a).not_to include(name)
    end
  end

  describe "#object_stores" do
    it "returns a handle for every bucket" do
      made << "OBJ_#{name}"
      js.create_object_store(bucket: name, storage: :memory).put("object", "hello world")

      found = js.object_stores.find { |store| store.name == name }

      expect(found.get("object").read).to eq("hello world")
    end
  end
end
