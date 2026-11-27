# frozen_string_literal: true

RSpec.describe Tether::JetStream::Consumer do
  include ConsumerHelper

  def waiting(consumer, count = 1)
    eventually { consumer.info.waiting_count == count }
  end

  def consume(consumer, **options, &block)
    block ||= lambda do |message|
      received.push(message)
      message.ack
    end
    consumer.consume(on_error: ->(error) { errors.push(error) }, **options, &block)
  end

  def data_until(last, timeout: 10)
    seen = []
    eventually(timeout: timeout) { (seen << received.pop(timeout: 0.05)&.data).last == last }
    seen.compact.uniq
  end

  describe "#name" do
    it "returns the name the consumer was created with" do
      expect(durable.name).to eq("worker")
    end

    it "returns a generated name for a consumer created without one" do
      expect(stream.create_consumer.name).to match(/\A[A-Za-z0-9]{22}\z/)
    end
  end

  describe "#stream" do
    it "returns the name of the stream the consumer reads from" do
      expect(durable.stream).to eq(name)
    end
  end

  describe "#jetstream" do
    it "returns the context the consumer was created through" do
      expect(durable.jetstream).to be(js)
    end
  end

  describe "#info" do
    it "reads the consumer's state from the server" do
      consumer = durable
      store(3)

      expect(consumer.info).to have_attributes(name: "worker", stream: name, pending_count: 3, ack_pending_count: 0,
                                               created: a_kind_of(Time))
    end

    it "replaces the cached info" do
      consumer = durable
      store(2)

      expect { consumer.info }.to change { consumer.cached_info.pending_count }.from(0).to(2)
    end

    it "raises ConsumerNotFoundError once the consumer was deleted" do
      consumer = durable
      consumer.delete

      expect { consumer.info }.to raise_error(Tether::JetStream::ConsumerNotFoundError)
    end
  end

  describe "#cached_info" do
    it "returns what the server answered when the consumer was created, without a request" do
      consumer = durable
      store(2)

      expect(consumer.cached_info.pending_count).to eq(0)
    end
  end

  describe "#config" do
    it "returns the configuration with the server's defaults applied" do
      expect(durable(max_deliver: 3).config)
        .to have_attributes(durable: "worker", ack_policy: :explicit, deliver_policy: :all, max_deliver: 3,
                            ack_wait: 30)
    end
  end

  describe "#push?" do
    it "is false for a pull consumer" do
      expect(durable).not_to be_push
    end

    it "is true for a push consumer made by another client" do
      expect(push_consumer).to be_push
    end
  end

  describe "#pause_until" do
    it "suspends delivery until the given time" do
      consumer = durable
      store(1)

      pause = consumer.pause_until(Time.now + 60)

      expect(pause).to have_attributes(paused: true, pause_until: be_within(1).of(Time.now + 60))
      expect(consumer.fetch(1, timeout: 0.3)).to be_empty
      expect(consumer.info.paused).to be(true)
    end
  end

  describe "#resume" do
    it "resumes delivery of a paused consumer" do
      consumer = durable
      store(1)
      consumer.pause_until(Time.now + 60)

      expect(consumer.resume).to have_attributes(paused: false, pause_until: nil)
      expect(data_of(consumer.fetch(1, timeout: 2))).to eq(["m1"])
    end
  end

  describe "#reset" do
    it "rewinds to the acknowledgment floor when no sequence is given" do
      consumer = durable
      store(3)
      consumer.fetch(3, timeout: 2).first.ack
      eventually { consumer.info.ack_floor.stream_sequence == 1 }

      expect(consumer.reset).to be(consumer)
      expect(data_of(consumer.fetch(3, timeout: 2))).to eq(%w[m2 m3])
    end

    it "moves the consumer to the given stream sequence and restarts the consumer sequence" do
      consumer = durable
      store(5)
      consumer.fetch(5, timeout: 2)

      consumer.reset(sequence: 4)

      expect(consumer.cached_info.delivered).to have_attributes(consumer_sequence: 0, stream_sequence: 3)
      expect(consumer.fetch(5, timeout: 0.5).map { |message| message.metadata.consumer_sequence }).to eq([1, 2])
    end

    it "raises when the sequence is not allowed by the deliver policy" do
      store(2)
      consumer = durable(deliver_policy: :new)

      expect { consumer.reset(sequence: 1) }.to raise_error(Tether::JetStream::APIError, /invalid reset/)
    end
  end

  describe "#unpin" do
    it "makes the server choose a new pinned client" do
      require_priority_groups
      consumer = durable(priority_policy: :pinned_client, priority_groups: ["jobs"])
      store(2)
      consumer.fetch(1, group: "jobs", timeout: 2)

      expect(consumer.unpin("jobs")).to be(consumer)
      expect { consumer.fetch(1, group: "jobs", timeout: 2) }.to raise_error(Tether::JetStream::PinIdMismatchError)
    end

    it "raises for a group the consumer does not have" do
      require_priority_groups
      consumer = durable(priority_policy: :pinned_client, priority_groups: ["jobs"])

      expect { consumer.unpin("other") }.to raise_error(Tether::JetStream::APIError, /group does not exist/)
    end
  end

  describe "#step_down" do
    it "raises on a server that is not clustered" do
      expect { durable.step_down }.to raise_error(Tether::JetStream::APIError, /clustering support required/)
    end
  end

  describe "#delete" do
    it "removes the consumer from the server" do
      consumer = durable

      expect(consumer.delete).to be(true)
      expect(stream.consumer?("worker")).to be(false)
    end

    it "removes a push consumer made by another client" do
      expect(push_consumer.delete).to be(true)
    end

    it "raises ConsumerNotFoundError when it is already gone" do
      consumer = durable
      consumer.delete

      expect { consumer.delete }.to raise_error(Tether::JetStream::ConsumerNotFoundError)
    end
  end

  describe "#fetch" do
    it "returns a full batch as soon as it is complete" do
      store(10)

      batch = durable.fetch(5, timeout: 5)

      expect(batch).to be_a(Tether::JetStream::Batch)
      expect(data_of(batch)).to eq(expected(5))
    end

    it "continues where the previous fetch ended" do
      store(6)
      consumer = durable
      consumer.fetch(3, timeout: 2)

      expect(data_of(consumer.fetch(3, timeout: 2))).to eq(expected(3, from: 4))
    end

    it "returns what arrived when the timeout passes first" do
      store(2)

      batch = durable.fetch(5, timeout: 0.4)

      expect(data_of(batch)).to eq(expected(2))
      expect(batch.error).to be_nil
    end

    it "returns an empty batch when nothing arrives in time" do
      started = Tether::Deadline.now

      expect(durable.fetch(5, timeout: 0.3)).to be_empty
      expect(Tether::Deadline.now - started).to be_between(0.25, 1)
    end

    it "returns at once when the timeout is a deadline that has passed" do
      store(1)

      expect(durable.fetch(1, timeout: Tether::Deadline.after(0))).to be_empty
    end

    it "returns what is available now with wait: false" do
      store(3)

      expect(data_of(durable.fetch(10, wait: false))).to eq(expected(3))
    end

    it "returns an empty batch at once with wait: false on a drained consumer" do
      started = Tether::Deadline.now

      expect(durable.fetch(10, wait: false)).to be_empty
      expect(Tether::Deadline.now - started).to be < 1
    end

    it "waits for the first messages with wait: false and a timeout" do
      consumer = durable
      fetching = Thread.new { consumer.fetch(10, wait: false, timeout: 5) }
      waiting(consumer)
      store(1)

      expect(data_of(fetching.value)).to eq(["m1"])
    end

    it "ends the batch before it exceeds max_bytes" do
      stream
      5.times { nc.publish("#{name}.a", "x" * 1000) }
      nc.flush

      batch = durable.fetch(5, timeout: 2, max_bytes: 2500)

      expect(batch.size).to eq(2)
    end

    it "fills the batch up to max_bytes when no batch is given" do
      stream
      5.times { nc.publish("#{name}.a", "x" * 1000) }
      nc.flush

      expect(durable.fetch(timeout: 2, max_bytes: 2500).size).to eq(2)
    end

    it "raises ArgumentError when neither a batch nor max_bytes is given" do
      expect { durable.fetch(timeout: 1) }.to raise_error(ArgumentError, /batch or max_bytes/)
    end

    it "yields each message to a block and returns the batch" do
      store(3)
      yielded = []

      batch = durable.fetch(3, timeout: 2) { |message| yielded << message.data }

      expect(yielded).to eq(expected(3))
      expect(batch.size).to eq(3)
    end

    it "gives concurrent fetches on one consumer disjoint messages" do
      store(200)
      consumer = durable

      batches = Array.new(8) { Thread.new { consumer.fetch(25, timeout: 5).map(&:data) } }.map(&:value)

      expect(batches.map(&:size)).to all(eq(25))
      expect(batches.flatten.sort).to eq(expected(200).sort)
    end

    it "raises ConsumerDeletedError when the consumer is deleted while it waits" do
      consumer = durable
      fetching = Thread.new { error_from { consumer.fetch(5, timeout: 10) } }
      waiting(consumer)
      stream.delete_consumer("worker")

      expect(fetching.value).to be_a(Tether::JetStream::ConsumerDeletedError)
    end

    it "keeps what was delivered and reports the deletion on the batch" do
      consumer = durable
      fetching = Thread.new { consumer.fetch(5, timeout: 10) }
      waiting(consumer)
      store(2)
      eventually { consumer.info.delivered.consumer_sequence == 2 }
      stream.delete_consumer("worker")

      expect(fetching.value).to have_attributes(size: 2, error: a_kind_of(Tether::JetStream::ConsumerDeletedError))
    end

    it "raises ServiceUnavailableError when the consumer no longer exists" do
      consumer = durable
      consumer.delete

      expect { consumer.fetch(1, timeout: 2) }.to raise_error(Tether::JetStream::ServiceUnavailableError)
    end

    it "raises PullError when the batch exceeds the consumer's max_batch" do
      consumer = durable(max_batch: 2)

      expect { consumer.fetch(3, timeout: 2) }
        .to raise_error(Tether::JetStream::PullError, "409 Exceeded MaxRequestBatch of 2")
    end

    it "raises PushConsumerError for a push consumer before anything is sent" do
      consumer = push_consumer
      allow(nc).to receive(:publish).and_call_original

      expect { consumer.fetch(1, timeout: 1) }.to raise_error(Tether::JetStream::PushConsumerError)
      expect(nc).not_to have_received(:publish)
    end

    it "raises ArgumentError when a group is given for a consumer without priority groups" do
      expect { durable.fetch(1, group: "jobs") }.to raise_error(ArgumentError, /no priority groups/)
    end

    it "raises ArgumentError when the heartbeat exceeds half of the timeout" do
      expect { durable.fetch(1, timeout: 2, heartbeat: 1.5) }.to raise_error(ArgumentError, /at most half/)
    end

    it "raises ArgumentError for a batch below one" do
      expect { durable.fetch(0) }.to raise_error(ArgumentError, /at least 1/)
    end

    it "asks for a heartbeat when the timeout is ten seconds or more" do
      store(1)
      consumer = durable
      allow(nc).to receive(:publish).and_call_original

      consumer.fetch(1, timeout: 12)

      expect(nc).to have_received(:publish).with(/MSG\.NEXT/, /"idle_heartbeat":5000000000/, any_args)
    end

    it "serves an overflow group only once the backlog reaches min_pending" do
      require_priority_groups
      consumer = durable(priority_policy: :overflow, priority_groups: ["jobs"])
      store(5)

      expect(consumer.fetch(1, group: "jobs", min_pending: 100, timeout: 0.4)).to be_empty
      expect(data_of(consumer.fetch(1, group: "jobs", min_pending: 5, timeout: 2))).to eq(["m1"])
    end

    it "remembers the pin id of a pinned client and keeps other handles on standby" do
      require_priority_groups
      consumer = durable(priority_policy: :pinned_client, priority_groups: ["jobs"])
      store(3)

      pinned = consumer.fetch(1, group: "jobs", timeout: 2).first

      expect(consumer.pin_id).to eq(pinned.headers["Nats-Pin-Id"])
      expect(stream.consumer("worker").fetch(1, group: "jobs", timeout: 0.4)).to be_empty
      expect(data_of(consumer.fetch(1, group: "jobs", timeout: 2))).to eq(["m2"])
    end

    it "forgets the pin id when the server reports a mismatch" do
      require_priority_groups
      consumer = durable(priority_policy: :pinned_client, priority_groups: ["jobs"])
      store(2)
      consumer.fetch(1, group: "jobs", timeout: 2)
      consumer.unpin("jobs")

      expect(error_from { consumer.fetch(1, group: "jobs", timeout: 2) })
        .to be_a(Tether::JetStream::PinIdMismatchError)
      expect(consumer.pin_id).to be_nil
    end
  end

  describe "#next" do
    it "returns one message" do
      store(2)

      expect(durable.next(timeout: 2).data).to eq("m1")
    end

    it "raises TimeoutError when nothing arrives in time" do
      expect { durable.next(timeout: 0.3) }.to raise_error(Tether::TimeoutError)
    end
  end

  describe "#consume" do
    it "delivers messages to the block in order" do
      store(20)

      consume(durable)

      expect(data_of(take(received, 20))).to eq(expected(20))
    end

    it "delivers messages published after it started" do
      consume(durable)
      store(3)

      expect(data_of(take(received, 3))).to eq(expected(3))
    end

    it "keeps pulling past several batches" do
      store(3000)

      consume(durable(max_ack_pending: 5000), max_messages: 100)

      expect(data_of(take(received, 3000))).to eq(expected(3000))
    end

    it "keeps pulling within a byte budget" do
      stream
      40.times { |index| nc.publish("#{name}.a", "#{index}:#{"x" * 500}") }
      nc.flush

      consume(durable, max_bytes: 4000)

      expect(take(received, 40).map { |message| message.data.to_i }).to eq(Array.new(40) { |index| index })
    end

    it "runs the block on a thread named after the consumer" do
      store(1)

      consume(durable) { |_message| received.push(Thread.current.name) }

      expect(received.pop(timeout: 2)).to eq("tether:consume:worker")
    end

    it "does not take a slow block for a silent server" do
      store(4)

      consume(durable, max_messages: 2, expires: 1, heartbeat: 0.5) do |message|
        gate.pop(timeout: 1.6) if message.data == "m1"
        received.push(message)
        message.ack
      end

      expect(data_of(take(received, 4, timeout: 5))).to eq(expected(4))
      expect(errors).to be_empty
    end

    it "reports an exception of the block to on_error and keeps delivering" do
      store(3)

      consume(durable) do |message|
        raise "boom" if message.data == "m2"

        received.push(message)
      end

      expect(data_of(take(received, 2))).to eq(%w[m1 m3])
      expect(errors.pop(timeout: 2)).to have_attributes(class: RuntimeError, message: "boom")
    end

    it "reports an exception of the block to the client's error event" do
      store(1)
      reported = record(nc, :error)

      consume(durable) { |_message| raise "boom" }

      expect(reported.pop(timeout: 2).last).to have_attributes(class: Tether::HandlerError, message: /boom/)
    end

    it "runs the block on several threads with concurrency:" do
      store(8)
      threads = Thread::Queue.new

      consume(durable, concurrency: 4) do |message|
        threads.push(Thread.current)
        gate.pop(timeout: 2)
        message.ack
      end

      expect(take(threads, 4).uniq.size).to eq(4)
    end

    it "delivers every message once across concurrent workers" do
      store(200)

      consume(durable, concurrency: 4, max_messages: 20)

      expect(data_of(take(received, 200)).sort).to eq(expected(200).sort)
    end

    it "ends with ConsumerDeletedError when the consumer is deleted" do
      consumer = durable
      loop = consume(consumer)
      waiting(consumer)
      consumer.delete

      expect { loop.wait(timeout: 5) }.to raise_error(Tether::JetStream::ConsumerDeletedError)
      expect(errors.pop(timeout: 1)).to be_a(Tether::JetStream::ConsumerDeletedError)
      expect(loop).to be_stopped
    end

    it "pulls again after the connection was lost and re-established" do
      client = connect(proxy.uri, reconnect: ClientHelper::FAST_RECONNECT)
      reconnected = record(client, :reconnect)
      consumer = client.jetstream.stream(stream.name).create_consumer(durable: "worker", ack_wait: 1)
      consume(consumer)
      store(2)
      take(received, 2)

      proxy.drop
      reconnected.pop(timeout: 5)
      store(3, from: 3)

      expect(data_of(take(received, 3, timeout: 5))).to eq(expected(3, from: 3))
    end

    it "resumes after the server was restarted" do
      NatsServer.run("jetstream { store_dir: /data }") do |restarting|
        client = connect(restarting.uri, reconnect: ClientHelper::FAST_RECONNECT)
        reconnected = record(client, :reconnect)
        context = client.jetstream
        stored = context.create_stream(name: name, subjects: ["#{name}.>"], storage: :file)
        consume(stored.create_consumer(durable: "worker"), expires: 2)
        2.times { |index| context.publish("#{name}.a", "m#{index + 1}") }
        take(received, 2)

        restarting.restart
        reconnected.pop(timeout: 10)
        2.times { |index| context.publish("#{name}.a", "m#{index + 3}", timeout: 10, retries: 20) }

        expect(data_until("m4")).to end_with("m3", "m4")
      end
    end

    it "warns with NoHeartbeatError when the server goes silent and resumes when it is back" do
      NatsServer.run("jetstream {}") do |silent|
        client = connect(silent.uri)
        context = client.jetstream
        stored = context.create_stream(name: name, subjects: ["#{name}.>"], storage: :memory)
        consume(stored.create_consumer(durable: "worker"), expires: 1, heartbeat: 0.5)
        context.publish("#{name}.a", "m1")
        take(received, 1)

        silent.pause
        warning = errors.pop(timeout: 5)
        silent.unpause
        context.publish("#{name}.a", "m2", timeout: 10)

        expect(warning).to be_a(Tether::JetStream::NoHeartbeatError)
        expect(data_of(take(received, 1, timeout: 5))).to eq(["m2"])
      end
    end

    it "raises PushConsumerError for a push consumer" do
      expect { push_consumer.consume { |message| received.push(message) } }
        .to raise_error(Tether::JetStream::PushConsumerError)
    end

    it "raises ArgumentError without a block" do
      expect { durable.consume }.to raise_error(ArgumentError, /needs a block/)
    end

    it "leaves no thread behind once stopped" do
      consumer = durable
      before = tether_threads
      consume(consumer).stop.wait(timeout: 2)

      expect(tether_threads - before).to be_empty
    end
  end

  describe "#messages" do
    it "returns an iterator the caller takes messages from" do
      store(3)

      iterator = durable.messages

      expect(Array.new(3) { iterator.next(timeout: 2).data }).to eq(expected(3))
    end

    it "starts no worker thread" do
      consumer = durable
      before = tether_threads

      consumer.messages

      expect(tether_threads - before).to be_empty
    end

    it "raises PushConsumerError for a push consumer" do
      expect { push_consumer.messages }.to raise_error(Tether::JetStream::PushConsumerError)
    end
  end

  describe "#inspect" do
    it "shows the stream and the name" do
      expect(durable.inspect).to eq(%(#<Tether::JetStream::Consumer stream="#{name}" name="worker">))
    end
  end
end
