# frozen_string_literal: true

RSpec.describe Tether::JetStream::Ordered::Pull do
  include ConsumerHelper

  let(:ordered) { stream.ordered_consumer }

  def consume(**options)
    ordered.consume(**options) do |message|
      received.push(message)
      gate.pop
    end
  end

  describe "#caught_up?" do
    it "is true at once when nothing existed at creation" do
      stream

      expect(ordered.messages).to be_caught_up
    end

    it "is true at once for a loop that only wants new messages" do
      store(3)

      expect(stream.ordered_consumer(deliver_policy: :new).messages).to be_caught_up
    end

    it "turns true once next has returned the last message that existed at creation" do
      store(2)
      iterator = ordered.messages
      iterator.next(timeout: 2)

      expect { iterator.next(timeout: 2) }.to change(iterator, :caught_up?).from(false).to(true)
    end

    it "turns true only after the block has handled the last message that existed at creation" do
      store(1)
      loop = consume
      received.pop(timeout: 2)

      expect(loop).not_to be_caught_up
      gate.push(true)
      eventually { loop.caught_up? }
    end

    it "counts the last message of each subject for deliver_policy: :last_per_subject" do
      store(3, subject: "#{name}.a")
      store(3, subject: "#{name}.b", from: 4)
      iterator = stream.ordered_consumer(deliver_policy: :last_per_subject).messages
      iterator.next(timeout: 2)

      expect { iterator.next(timeout: 2) }.to change(iterator, :caught_up?).from(false).to(true)
    end

    it "stays true when the server consumer is replaced" do
      stream
      iterator = ordered.messages
      eventually { iterator.consumer.info.waiting_count == 1 }
      stream.delete_consumer(iterator.consumer.name)
      store(1)
      iterator.next(timeout: 15)

      expect(iterator).to be_caught_up
    end
  end

  describe "#stream_sequence" do
    it "is zero before the first message" do
      expect(ordered.messages.stream_sequence).to eq(0)
    end

    it "returns the stream sequence of the last message handed out" do
      store(3)
      iterator = ordered.messages
      2.times { iterator.next(timeout: 2) }

      expect(iterator.stream_sequence).to eq(2)
    end
  end

  describe "#consumer" do
    it "returns the server consumer currently read from" do
      iterator = stream.ordered_consumer(name_prefix: "reader").messages

      expect(iterator.consumer.name).to eq("reader_1")
    end
  end

  describe "#next" do
    it "raises StreamNotFoundError once the stream is gone, and the loop has ended" do
      iterator = ordered.messages(expires: 1, heartbeat: 0.5)
      stream.delete

      expect { iterator.next(timeout: 10) }.to raise_error(Tether::JetStream::StreamNotFoundError)
      expect(iterator).to be_stopped
    end

    it "keeps the caller's deadline while the server consumer cannot be replaced" do
      client = connect(proxy.uri, reconnect: ClientHelper::FAST_RECONNECT)
      iterator = client.jetstream.ordered_consumer(stream.name).messages(expires: 1, heartbeat: 0.5)
      proxy.stop
      started = Tether::Deadline.now

      expect { iterator.next(timeout: 0.5) }.to raise_error(Tether::TimeoutError)
      expect(Tether::Deadline.now - started).to be < 2
    end
  end
end
