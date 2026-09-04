# frozen_string_literal: true

RSpec.describe Tether::JetStream::FastBatch do
  include ClientHelper
  include PublishingHelper

  let(:nc) { connect }
  let(:js) { nc.jetstream }
  let(:stream) { ingest_stream(js, allow_batched: true) }

  before { require_api_level(js, 4) }

  def run_batch(**options)
    kept = nil
    error = error_from { js.fast_batch(**options) { |batch| yield(kept = batch) } }
    [kept, error]
  end

  describe "#publish" do
    it "returns the batch, so publishes chain" do
      js.fast_batch { |batch| expect(batch.publish("#{stream}.rows", "one")).to be(batch) }
    end

    it "stores the payload and headers of every message" do
      js.fast_batch do |batch|
        batch.publish("#{stream}.a", "one", headers: { "X-Kind" => "first" })
        batch.publish("#{stream}.b", "two")
      end

      expect([last_stored(js, stream, "#{stream}.a"), last_stored(js, stream, "#{stream}.b")]).to match(
        [{ data: "one", headers: include("X-Kind" => "first") }, { data: "two", headers: {} }]
      )
    end

    it "carries the flow, gap mode, batch sequence and operation in the reply subject of each message" do
      wire = collect(nc, "#{stream}.>")
      batch, = run_batch(flow: 7, gap: :ok) { |open| open.publish("#{stream}.a", "one").publish("#{stream}.b", "two") }
      replies = take(wire, 3).map { |msg| msg.reply_to.split(".").drop(2).join(".") }

      expect(replies).to eq(["#{batch.id}.7.ok.1.0.$FI", "#{batch.id}.7.ok.2.1.$FI", "#{batch.id}.7.ok.3.3.$FI"])
    end

    it "requires API level 4 of the server on the first message" do
      wire = collect(nc, "#{stream}.>")
      js.fast_batch { |batch| batch.publish("#{stream}.a", "one").publish("#{stream}.a", "two") }

      expect(take(wire, 2).map { |msg| msg.headers&.[]("Nats-Required-Api-Level") }).to eq(["4", nil])
    end

    it "pings with the highest sequence it sent when acknowledgments stop arriving" do
      stub_const("#{described_class}::PING_INTERVAL", 0.02)
      silent = ingest_stream(js, allow_batched: true, no_ack: true)
      wire = collect(nc, "#{silent}.>")
      batch, = run_batch(timeout: 0.2) { |open| open.publish("#{silent}.a", "one") }

      expect(take(wire, 2).last.reply_to).to end_with("#{batch.id}.100.fail.1.4.$FI")
    end

    it "never gets further ahead of the server than the acknowledgment windows allow" do
      ahead = []
      js.fast_batch(flow: 2, windows: 1) do |batch|
        20.times { ahead << (batch.publish("#{stream}.rows", "x").size - batch.acknowledged) }
      end

      expect(ahead.max).to be <= 2
    end

    it "publishes with one window of one message outstanding" do
      ack = js.fast_batch(flow: 1, windows: 1) { |batch| 10.times { batch.publish("#{stream}.rows", "x") } }

      expect(ack.count).to eq(10)
    end

    it "publishes with three windows outstanding" do
      ack = js.fast_batch(flow: 5, windows: 3) { |batch| 100.times { batch.publish("#{stream}.rows", "x") } }

      expect(ack.count).to eq(100)
    end

    it "raises the server's error from a later publish after a refused message with gap: :fail" do
      batch, error = run_batch do |open|
        open.publish("#{stream}.rows", "one")
        open.publish("#{stream}.rows", "two", expect: { last_sequence: 9 })
        loop { open.publish("#{stream}.rows", "more") }
      end

      expect([error, batch.ack.count]).to match([a_kind_of(Tether::JetStream::WrongLastSequenceError), 1])
    end

    it "raises ArgumentError for an option it does not know" do
      expect { js.fast_batch { |batch| batch.publish("#{stream}.rows", "x", retries: 2) } }
        .to raise_error(ArgumentError, /retries/)
    end

    it "raises once the batch is committed" do
      batch, = run_batch { |open| open.publish("#{stream}.rows", "one") }

      expect { batch.publish("#{stream}.rows", "late") }.to raise_error(Tether::JetStream::Error, /closed/)
    end
  end

  describe "#errors" do
    it "is empty when the server refused nothing" do
      batch, = run_batch { |open| open.publish("#{stream}.rows", "one") }

      expect(batch.errors).to be_empty
    end

    it "holds the batch sequence and error of each refused message with gap: :ok" do
      batch, = run_batch(gap: :ok) do |open|
        open.publish("#{stream}.rows", "one")
        open.publish("#{stream}.rows", "two", expect: { last_sequence: 9 })
        open.publish("#{stream}.rows", "three", expect: { stream: "ANOTHER" })
      end

      expect(batch.errors).to match(
        [have_attributes(sequence: 2, error: a_kind_of(Tether::JetStream::WrongLastSequenceError)),
         have_attributes(sequence: 3, error: a_kind_of(Tether::JetStream::StreamMismatchError))]
      )
    end

    it "holds the refused message that ended a batch with gap: :fail" do
      batch, error = run_batch do |open|
        open.publish("#{stream}.rows", "one")
        open.publish("#{stream}.rows", "two", expect: { last_sequence: 9 })
      end

      expect(batch.errors).to match([have_attributes(sequence: 2, error: error)])
    end
  end

  describe "#gaps" do
    it "is empty when the server received everything" do
      batch, = run_batch { |open| open.publish("#{stream}.rows", "one") }

      expect(batch.gaps).to be_empty
    end

    it "holds the batch sequences the server did not receive with gap: :ok" do
      batch, = run_batch(gap: :ok) do |open|
        open.publish("#{stream}.rows", "one")
        2.times { open.publish(topic, "lost") }
        open.publish("#{stream}.rows", "four")
        open.publish(topic, "lost")
        open.publish("#{stream}.rows", "six")
      end

      expect(batch.gaps).to eq([2..3, 5..5])
    end

    it "holds the gap that ended a batch with gap: :fail" do
      batch, = run_batch do |open|
        open.publish("#{stream}.rows", "one")
        open.publish(topic, "lost")
        open.publish("#{stream}.rows", "three")
      end

      expect(batch.gaps).to eq([2..2])
    end
  end

  describe "#ack" do
    it "is nil while the batch is open" do
      js.fast_batch { |batch| expect(batch.publish("#{stream}.rows", "one").ack).to be_nil }
    end

    it "returns the final acknowledgment once the batch is committed" do
      batch, = run_batch { |open| 3.times { open.publish("#{stream}.rows", "x") } }

      expect(batch.ack).to have_attributes(stream: stream, count: 3, sequence: 3, batch_id: batch.id)
    end

    it "tells how far the server got when a batch with gap: :fail ended early" do
      batch, = run_batch do |open|
        2.times { open.publish("#{stream}.rows", "kept") }
        open.publish("#{stream}.rows", "refused", expect: { last_sequence: 9 })
      end

      expect(batch.ack).to have_attributes(count: 2, sequence: 2)
    end
  end

  describe "#acknowledged" do
    it "returns the highest batch sequence the server confirmed" do
      batch, = run_batch(flow: 5) { |open| 12.times { open.publish("#{stream}.rows", "x") } }

      expect(batch.acknowledged).to be_between(10, 12)
    end
  end

  describe "#size" do
    it "counts the messages published into the batch" do
      batch, = run_batch { |open| 7.times { open.publish("#{stream}.rows", "x") } }

      expect(batch.size).to eq(7)
    end
  end
end
