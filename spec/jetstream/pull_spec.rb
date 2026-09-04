# frozen_string_literal: true

RSpec.describe Tether::JetStream::Pull do
  include ConsumerHelper
  include ForkHelper

  let(:consumer) { durable }
  let(:iterator) { consumer.messages }

  def consume(**options, &block)
    block ||= lambda do |message|
      received.push(message)
      message.ack
    end
    consumer.consume(on_error: ->(error) { errors.push(error) }, **options, &block)
  end

  def held(**options)
    consume(**options) do |message|
      received.push(message)
      gate.pop
    end
  end

  describe "#consumer" do
    it "returns the consumer the loop reads from" do
      expect(iterator.consumer).to be(consumer)
    end
  end

  describe "#next" do
    it "returns the next message" do
      store(2)

      expect(iterator.next(timeout: 2).data).to eq("m1")
    end

    it "waits for a message that is published later" do
      taking = Thread.new { iterator.next(timeout: 5) }
      eventually { consumer.info.waiting_count == 1 }
      store(1)

      expect(taking.value.data).to eq("m1")
    end

    it "raises TimeoutError when nothing arrives in time" do
      started = Tether::Deadline.now

      expect { iterator.next(timeout: 0.2) }.to raise_error(Tether::TimeoutError)
      expect(Tether::Deadline.now - started).to be_between(0.15, 1)
    end

    it "accepts a deadline shared across calls" do
      store(1)
      deadline = Tether::Deadline.after(0.3)
      iterator.next(timeout: deadline)

      expect { iterator.next(timeout: deadline) }.to raise_error(Tether::TimeoutError)
    end

    it "keeps pulling past several batches" do
      store(500)
      reader = consumer.messages(max_messages: 50)

      expect(Array.new(500) { reader.next(timeout: 2).data }).to eq(expected(500))
    end

    it "raises StopIteration after stop" do
      iterator.stop

      expect { iterator.next(timeout: 1) }.to raise_error(StopIteration)
    end

    it "raises ConsumerDeletedError once the consumer is deleted" do
      iterator
      eventually { consumer.info.waiting_count == 1 }
      consumer.delete

      expect { iterator.next(timeout: 5) }.to raise_error(Tether::JetStream::ConsumerDeletedError)
    end

    it "releases a caller blocked in it when the loop is stopped" do
      taking = Thread.new do
        iterator.next
      rescue StopIteration => e
        e
      end
      eventually { taking.status == "sleep" }
      iterator.stop

      expect(taking.value).to be_a(StopIteration)
    end

    it "raises ConnectionClosedError when the connection is closed" do
      iterator
      nc.close

      expect { iterator.next(timeout: 2) }.to raise_error(Tether::ConnectionClosedError)
    end

    it "raises ForkedError in a forked child" do
      iterator

      expect(in_child { iterator.next(timeout: 1) }).to eq(["Tether::ForkedError",
                                                            "the loop belongs to the parent process"])
    end

    it "raises ArgumentError on a loop that delivers to a block" do
      expect { consume.next }.to raise_error(ArgumentError, /delivers to a block/)
    end
  end

  describe "#each" do
    context "when a block is given" do
      it "yields messages until the loop is stopped and returns the loop" do
        store(3)
        seen = []

        result = iterator.each do |message|
          seen << message.data
          iterator.stop if seen.size == 3
        end

        expect(seen).to eq(expected(3))
        expect(result).to be(iterator)
      end

      it "raises the error that ended the loop" do
        iterator
        eventually { consumer.info.waiting_count == 1 }
        consumer.delete

        expect { iterator.each { |message| received.push(message) } }
          .to raise_error(Tether::JetStream::ConsumerDeletedError)
      end
    end

    context "when no block is given" do
      it "returns an enumerator over the messages" do
        store(3)

        expect(iterator.each.first(3).map(&:data)).to eq(expected(3))
      end
    end
  end

  describe "#stop" do
    it "ends delivery and discards what is buffered" do
      store(5)
      loop = held

      received.pop(timeout: 2)
      loop.stop
      gate.push(true)
      loop.wait(timeout: 2)

      expect(received).to be_empty
    end

    it "returns the loop without waiting for the block" do
      store(1)
      loop = held
      received.pop(timeout: 2)

      expect(loop.stop).to be(loop)
      gate.push(true)
    end

    it "leaves unacknowledged messages for the next reader" do
      store(3)
      consumer = durable(ack_wait: 0.5)
      loop = consumer.consume { |message| received.push(message) }
      take(received, 3)
      loop.stop.wait(timeout: 2)

      expect(data_of(consumer.fetch(3, timeout: 5)).sort).to eq(expected(3))
    end

    it "removes the pull requests' interest from the server" do
      loop = consume
      eventually { consumer.info.waiting_count == 1 }
      loop.stop
      store(1)

      expect(consumer.fetch(1, timeout: 2).first.metadata.delivered).to eq(1)
    end

    it "may be called more than once" do
      loop = consume

      expect(loop.stop.stop).to be_stopped
    end

    it "may be called from the block" do
      store(2)
      loop = nil
      loop = consume do |message|
        received.push(message)
        loop.stop
      end

      expect(loop.wait(timeout: 2)).to be_stopped
      expect(received.size).to eq(1)
    end
  end

  describe "#drain" do
    it "finishes the messages already buffered before it returns" do
      store(5)
      loop = held
      received.pop(timeout: 2)
      eventually { consumer.info.delivered.consumer_sequence == 5 }
      draining = Thread.new { loop.drain(timeout: 5) }
      eventually { draining.status == "sleep" }
      5.times { gate.push(true) }
      draining.value

      expect(received.size).to eq(4)
      expect(loop).to be_stopped
    end

    it "stops pulling, so later messages are left on the server" do
      loop = consume
      eventually { consumer.info.waiting_count == 1 }
      loop.drain(timeout: 2)
      store(1)

      expect(consumer.fetch(1, timeout: 2).first.metadata.delivered).to eq(1)
      expect(received).to be_empty
    end

    it "raises TimeoutError and stops when the block does not finish in time" do
      store(2)
      loop = held
      received.pop(timeout: 2)

      expect { loop.drain(timeout: 0.2) }.to raise_error(Tether::TimeoutError)
      expect(loop).to be_stopped
      gate.push(true)
    end

    it "returns at once when called from the block, and the loop ends when the buffer is empty" do
      store(3)
      loop = nil
      loop = consume do |message|
        received.push(message)
        loop.drain
      end

      loop.wait(timeout: 2)

      expect(data_of(take(received, 3))).to eq(expected(3))
    end

    it "lets an iterator take what is buffered and then ends it" do
      store(3)
      reader = consumer.messages
      first = reader.next(timeout: 2)
      eventually { consumer.info.delivered.consumer_sequence == 3 }

      reader.drain

      expect([first.data, reader.next(timeout: 1).data, reader.next(timeout: 1).data]).to eq(expected(3))
      expect { reader.next(timeout: 1) }.to raise_error(StopIteration)
    end
  end

  describe "#wait" do
    it "blocks until the loop is stopped and returns the loop" do
      loop = consume
      waiting = Thread.new { loop.wait(timeout: 5) }
      eventually { waiting.status == "sleep" }
      loop.stop

      expect(waiting.value).to be(loop)
    end

    it "waits for a block that is still running" do
      store(1)
      loop = held
      received.pop(timeout: 2)
      loop.stop
      waiting = Thread.new { loop.wait(timeout: 5) }
      eventually { waiting.status == "sleep" }
      gate.push(true)

      expect(waiting.value).to be(loop)
    end

    it "raises TimeoutError when the loop is still running at the deadline" do
      loop = consume

      expect { loop.wait(timeout: 0.1) }.to raise_error(Tether::TimeoutError)
    end

    it "raises the error that ended the loop" do
      loop = consume
      eventually { consumer.info.waiting_count == 1 }
      consumer.delete

      expect { loop.wait(timeout: 5) }.to raise_error(Tether::JetStream::ConsumerDeletedError)
    end

    it "raises ConnectionClosedError when the connection was closed under the loop" do
      loop = consume
      nc.close

      expect { loop.wait(timeout: 5) }.to raise_error(Tether::ConnectionClosedError)
    end

    it "raises ForkedError in a forked child" do
      loop = consume

      expect(in_child { loop.wait(timeout: 1) }.first).to eq("Tether::ForkedError")
    end

    it "returns at once when called from the block" do
      store(1)
      loop = nil
      loop = consume { |_message| received.push(loop.wait) }

      expect(received.pop(timeout: 2)).to be(loop)
    end
  end

  describe "#stopped?" do
    it "is false while the loop runs" do
      expect(consume).not_to be_stopped
    end

    it "is true after stop" do
      expect(consume.stop).to be_stopped
    end

    it "is true in a forked child, while the parent's loop keeps running" do
      loop = consume

      expect(in_child { loop.stop.stopped? }).to be(true)
      expect(loop).not_to be_stopped
    end

    it "is true once the consumer was deleted" do
      loop = consume
      eventually { consumer.info.waiting_count == 1 }
      consumer.delete

      eventually { loop.stopped? }
    end
  end

  describe "#error" do
    it "is nil for a loop that was stopped" do
      expect(consume.stop.error).to be_nil
    end

    it "holds the error that ended the loop" do
      loop = consume
      eventually { consumer.info.waiting_count == 1 }
      consumer.delete

      eventually { loop.stopped? }
      expect(loop.error).to be_a(Tether::JetStream::ConsumerDeletedError)
    end
  end

  describe "#on_error" do
    it "registers another handler for warnings and the terminal error" do
      loop = consume
      loop.on_error { |error| received.push(error) }
      eventually { consumer.info.waiting_count == 1 }
      consumer.delete

      expect(received.pop(timeout: 5)).to be_a(Tether::JetStream::ConsumerDeletedError)
      expect(errors.pop(timeout: 1)).to be_a(Tether::JetStream::ConsumerDeletedError)
    end

    it "returns the loop" do
      loop = consume

      expect(loop.on_error { |error| errors.push(error) }).to be(loop)
    end

    it "receives a warning the server sends for a pull and the loop keeps running" do
      limited = stream.create_consumer(durable: "limited", max_batch: 5)
      loop = limited.consume(max_messages: 10, on_error: ->(error) { errors.push(error) }, &:ack)

      expect(errors.pop(timeout: 2)).to have_attributes(class: Tether::JetStream::PullError,
                                                        message: "409 Exceeded MaxRequestBatch of 5")
      expect(loop).not_to be_stopped
    end

    it "survives a handler that raises" do
      loop = consume
      loop.on_error { |_error| raise "handler bug" }
      eventually { consumer.info.waiting_count == 1 }
      consumer.delete

      expect { loop.wait(timeout: 5) }.to raise_error(Tether::JetStream::ConsumerDeletedError)
    end
  end

  describe "#inspect" do
    it "shows the consumer and whether the loop has stopped" do
      expect(iterator.inspect).to eq(%(#<Tether::JetStream::Pull consumer="worker" stopped=false>))
    end
  end
end
