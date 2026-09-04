# frozen_string_literal: true

RSpec.describe Tether::JetStream::Batch do
  include ConsumerHelper

  let(:batch) do
    store(3)
    durable.fetch(3, timeout: 2)
  end

  describe "#each" do
    context "when a block is given" do
      it "yields the messages in the order delivered" do
        batch.each { |message| received.push(message.data) }

        expect(take(received, 3)).to eq(expected(3))
      end

      it "returns the batch" do
        expect(batch.each { |message| message }).to be(batch)
      end
    end

    context "when no block is given" do
      it "returns an enumerator that knows its size" do
        expect(batch.each).to have_attributes(class: Enumerator, size: 3)
      end
    end
  end

  describe "#map" do
    it "is available because a batch is Enumerable" do
      expect(batch.map(&:data)).to eq(expected(3))
    end
  end

  describe "#size" do
    it "counts the messages delivered" do
      expect(batch.size).to eq(3)
    end

    it "is zero for a fetch that found nothing" do
      expect(durable.fetch(3, wait: false).size).to eq(0)
    end
  end

  describe "#empty?" do
    it "is false when messages were delivered" do
      expect(batch).not_to be_empty
    end

    it "is true for a fetch that found nothing" do
      expect(durable.fetch(3, wait: false)).to be_empty
    end
  end

  describe "#to_a" do
    it "returns the messages as an array the caller may change" do
      messages = batch.to_a
      messages.clear

      expect(batch.to_a.map(&:data)).to eq(expected(3))
    end
  end

  describe "#error" do
    it "is nil when the pull ended normally" do
      expect(batch.error).to be_nil
    end

    it "is nil when the timeout cut the batch short" do
      store(1)

      expect(durable.fetch(3, timeout: 0.3).error).to be_nil
    end

    it "holds why the pull ended early after delivering something" do
      consumer = durable
      fetching = Thread.new { consumer.fetch(5, timeout: 10) }
      eventually { consumer.info.waiting_count == 1 }
      store(1)
      eventually { consumer.info.delivered.consumer_sequence == 1 }
      consumer.delete

      expect(fetching.value.error).to be_a(Tether::JetStream::ConsumerDeletedError)
    end
  end

  describe "#inspect" do
    it "shows the size and the error without the messages" do
      expect(batch.inspect).to eq("#<Tether::JetStream::Batch size=3 error=nil>")
    end
  end
end
