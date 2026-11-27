# frozen_string_literal: true

RSpec.describe Tether::JetStream::Ordered do
  include ConsumerHelper

  let(:ordered) { stream.ordered_consumer }

  def consume(source = ordered, **options)
    source.consume(on_error: ->(error) { errors.push(error) }, **options) { |message| received.push(message) }
  end

  def stream_sequences(messages)
    messages.map { |message| message&.metadata&.stream_sequence }
  end

  def server_consumers
    stream.consumer_names.to_a
  end

  describe ".new" do
    it "raises ArgumentError for max_reset_attempts that is not positive" do
      expect { stream.ordered_consumer(max_reset_attempts: 0) }.to raise_error(ArgumentError, /max_reset_attempts/)
    end
  end

  describe "#stream" do
    it "returns the name of the stream read from" do
      expect(ordered.stream).to eq(name)
    end
  end

  describe "#jetstream" do
    it "returns the context it was made through" do
      expect(ordered.jetstream).to be(js)
    end
  end

  describe "#consume" do
    it "delivers every message in stream order" do
      store(300)

      consume(max_messages: 50)

      expect(stream_sequences(take(received, 300))).to eq(sequences(300))
    end

    it "delivers messages published after it started" do
      consume
      store(3)

      expect(data_of(take(received, 3))).to eq(expected(3))
    end

    it "reads through an ephemeral memory consumer without acknowledgments" do
      loop = consume

      expect(loop.consumer.config).to have_attributes(durable: nil, ack_policy: :none, memory_storage: true,
                                                      replicas: 1, inactive_threshold: 300)
    end

    it "creates nothing on the server until it is called" do
      ordered

      expect(server_consumers).to be_empty
    end

    it "hands out messages whose acknowledgment methods do nothing" do
      store(1)
      consume

      expect(received.pop(timeout: 2).ack.ack).not_to be_acked
    end

    it "continues without a gap or a duplicate when the server consumer is deleted" do
      store(50)
      loop = consume(max_messages: 10)
      take(received, 50)

      stream.delete_consumer(loop.consumer.name)
      store(50, from: 51)

      expect(stream_sequences(take(received, 50, timeout: 5))).to eq(sequences(50, from: 51))
      expect(errors.pop(timeout: 1)).to be_a(Tether::JetStream::ConsumerDeletedError)
    end

    it "continues without a gap or a duplicate when the server consumer is deleted mid-stream" do
      store(2000)
      deleted = false
      loop = nil
      loop = ordered.consume(max_messages: 100, expires: 2, heartbeat: 1) do |message|
        if message.metadata.stream_sequence == 500 && !deleted
          deleted = true
          stream.delete_consumer(loop.consumer.name)
        end
        received.push(message)
      end

      expect(stream_sequences(take(received, 2000, timeout: 5))).to eq(sequences(2000))
    end

    it "replaces the server consumer after a consumer sequence gap" do
      store(20)
      loop = consume(max_messages: 5)
      take(received, 20)
      first = loop.consumer.name

      stream.consumer(first).reset(sequence: 15)
      store(1, from: 21)

      expect(stream_sequences(take(received, 1, timeout: 5))).to eq([21])
      expect(loop.consumer.name).not_to eq(first)
    end

    it "names each server consumer with the prefix and a serial" do
      loop = consume(stream.ordered_consumer(name_prefix: "watch"))
      eventually { stream.consumer("watch_1").info.waiting_count == 1 }
      stream.delete_consumer("watch_1")

      eventually { loop.consumer.name == "watch_2" }
      eventually { server_consumers == ["watch_2"] }
    end

    it "continues without a gap after the connection was lost and re-established" do
      client = connect(proxy.uri, reconnect: ClientHelper::FAST_RECONNECT)
      reconnected = record(client, :reconnect)
      consume(client.jetstream.ordered_consumer(stream.name))
      store(10)
      take(received, 10)

      proxy.drop
      store(10, from: 11)
      reconnected.pop(timeout: 5)

      expect(stream_sequences(take(received, 10, timeout: 10))).to eq(sequences(10, from: 11))
    end

    it "continues without a gap after the server was restarted" do
      NatsServer.run("jetstream { store_dir: /data }") do |restarting|
        client = connect(restarting.uri, reconnect: ClientHelper::FAST_RECONNECT)
        reconnected = record(client, :reconnect)
        context = client.jetstream
        context.create_stream(name: name, subjects: ["#{name}.>"], storage: :file)
        consume(context.ordered_consumer(name))
        5.times { context.publish("#{name}.a", "before") }
        take(received, 5)

        restarting.restart
        reconnected.pop(timeout: 10)
        5.times { context.publish("#{name}.a", "after", timeout: 10, retries: 20) }

        expect(stream_sequences(take(received, 5, timeout: 15))).to eq(sequences(5, from: 6))
      end
    end

    it "ends with the last error once max_reset_attempts recreations have failed" do
      limited = js.create_stream(name: name, subjects: ["#{name}.>"], storage: :memory, max_consumers: 1)
      loop = consume(limited.ordered_consumer(max_reset_attempts: 1), expires: 2)
      limited.delete_consumer(loop.consumer.name)
      limited.create_consumer(durable: "blocker")

      expect { loop.wait(timeout: 15) }.to raise_error(Tether::JetStream::APIError, /maximum consumers/i)
    end

    it "ends with StreamNotFoundError when the stream is deleted" do
      loop = consume
      eventually { stream.consumer(loop.consumer.name).info.waiting_count == 1 }
      stream.delete

      expect { loop.wait(timeout: 10) }.to raise_error(Tether::JetStream::StreamNotFoundError)
    end

    it "starts at start_sequence" do
      store(10)

      consume(stream.ordered_consumer(start_sequence: 7))

      expect(stream_sequences(take(received, 4))).to eq([7, 8, 9, 10])
    end

    it "starts at the first message stored at or after start_time" do
      store(2)
      later = stream.info.state.last_time + 0.001
      store(2, from: 3)

      consume(stream.ordered_consumer(start_time: later))

      expect(stream_sequences(take(received, 2))).to eq([3, 4])
    end

    it "delivers only new messages with deliver_policy: :new" do
      store(3)
      loop = consume(stream.ordered_consumer(deliver_policy: :new))
      eventually { stream.consumer(loop.consumer.name).info.waiting_count == 1 }
      store(1, from: 4)

      expect(stream_sequences(take(received, 1))).to eq([4])
    end

    it "delivers the last message of each subject with deliver_policy: :last_per_subject" do
      store(2, subject: "#{name}.a")
      store(2, subject: "#{name}.b", from: 3)

      consume(stream.ordered_consumer(deliver_policy: :last_per_subject))

      expect(stream_sequences(take(received, 2))).to eq([2, 4])
    end

    it "delivers headers and the payload size without the payload with headers_only" do
      store(1)

      consume(stream.ordered_consumer(headers_only: true))

      message = received.pop(timeout: 2)

      expect(message.data).to be_empty
      expect(message.headers["Nats-Msg-Size"]).to eq("2")
    end

    it "delivers only subjects matching one filter" do
      store(2, subject: "#{name}.a")
      store(2, subject: "#{name}.b", from: 3)

      consume(stream.ordered_consumer(filter_subjects: "#{name}.b"))

      expect(stream_sequences(take(received, 2))).to eq([3, 4])
    end

    it "delivers subjects matching any of several filters" do
      store(1, subject: "#{name}.a")
      store(1, subject: "#{name}.b", from: 2)
      store(1, subject: "#{name}.c", from: 3)

      consume(stream.ordered_consumer(filter_subjects: ["#{name}.a", "#{name}.c"]))

      expect(stream_sequences(take(received, 2))).to eq([1, 3])
    end

    it "calls on_caught_up at once when nothing existed at creation" do
      stream

      consume(on_caught_up: -> { received.push(:caught_up) })

      expect(received.pop(timeout: 2)).to eq(:caught_up)
    end

    it "calls on_caught_up once, after the block handled the last message that existed at creation" do
      store(5)

      consume(on_caught_up: -> { received.push(:caught_up) })
      taken = take(received, 6)
      store(2, from: 6)

      expect(taken.last).to eq(:caught_up)
      expect(data_of(take(received, 2))).to eq(expected(2, from: 6))
    end

    it "calls on_caught_up on the worker thread" do
      stream

      consume(on_caught_up: -> { received.push(Thread.current.name) })

      expect(received.pop(timeout: 2)).to start_with("tether:consume:")
    end

    it "raises ArgumentError for concurrency: 2" do
      expect { consume(concurrency: 2) }.to raise_error(ArgumentError, /concurrency: must be 1/)
    end

    it "raises StreamNotFoundError when the stream does not exist" do
      missing = js.ordered_consumer("MISSING#{name}")

      expect { consume(missing) }.to raise_error(Tether::JetStream::StreamNotFoundError)
    end

    it "deletes its server consumer when stopped" do
      loop = consume
      eventually { server_consumers.size == 1 }
      loop.stop

      eventually { server_consumers.empty? }
    end
  end

  describe "#messages" do
    it "returns an iterator over every message in stream order" do
      store(100)

      iterator = ordered.messages(max_messages: 20)

      expect(stream_sequences(Array.new(100) { iterator.next(timeout: 2) })).to eq(sequences(100))
    end

    it "continues without a gap or a duplicate when the server consumer is deleted" do
      store(10)
      iterator = ordered.messages(on_error: ->(error) { errors.push(error) })
      taken = Array.new(10) { iterator.next(timeout: 2) }
      stream.delete_consumer(iterator.consumer.name)
      store(10, from: 11)

      taken += Array.new(10) { iterator.next(timeout: 5) }

      expect(stream_sequences(taken)).to eq(sequences(20))
    end

    it "raises TimeoutError from next when nothing arrives in time" do
      iterator = ordered.messages

      expect { iterator.next(timeout: 0.2) }.to raise_error(Tether::TimeoutError)
    end

    it "calls on_caught_up before next returns the last message that existed at creation" do
      store(2)
      iterator = ordered.messages(on_caught_up: -> { received.push(:caught_up) })

      iterator.next(timeout: 2)
      expect(received).to be_empty
      iterator.next(timeout: 2)

      expect(received.pop(timeout: 1)).to eq(:caught_up)
    end

    it "deletes its server consumer when stopped" do
      iterator = ordered.messages
      eventually { server_consumers.size == 1 }
      iterator.stop

      eventually { server_consumers.empty? }
    end
  end

  describe "#fetch" do
    it "returns a batch in stream order" do
      store(5)

      expect(stream_sequences(ordered.fetch(3, timeout: 2))).to eq([1, 2, 3])
    end

    it "continues after the last message of the previous fetch" do
      store(5)
      ordered.fetch(3, timeout: 2)

      expect(stream_sequences(ordered.fetch(3, timeout: 0.5))).to eq([4, 5])
    end

    it "continues after the last message when its server consumer was deleted in between" do
      store(5)
      ordered.fetch(3, timeout: 2)
      stream.delete_consumer(server_consumers.first)

      expect(stream_sequences(ordered.fetch(3, timeout: 0.5))).to eq([4, 5])
    end

    it "returns what is available now with wait: false" do
      store(2)

      expect(stream_sequences(ordered.fetch(5, wait: false))).to eq([1, 2])
    end

    it "yields each message to a block" do
      store(2)

      ordered.fetch(2, timeout: 2) { |message| received.push(message) }

      expect(data_of(take(received, 2))).to eq(expected(2))
    end

    it "starts at start_sequence" do
      store(5)

      expect(stream_sequences(stream.ordered_consumer(start_sequence: 4).fetch(5, timeout: 0.5))).to eq([4, 5])
    end
  end

  describe "#close" do
    it "deletes the server consumer kept between fetches" do
      store(1)
      ordered.fetch(1, timeout: 2)

      expect(ordered.close).to be(ordered)
      eventually { server_consumers.empty? }
    end
  end

  describe "#inspect" do
    it "shows the stream and the name prefix" do
      expect(stream.ordered_consumer(name_prefix: "watch").inspect)
        .to eq(%(#<Tether::JetStream::Ordered stream="#{name}" prefix="watch">))
    end
  end
end
