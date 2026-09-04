# frozen_string_literal: true

RSpec.describe Tether::JetStream::Counter do
  include ClientHelper
  include PublishingHelper

  let(:nc) { connect }
  let(:js) { nc.jetstream }
  let(:stream) { ingest_stream(js, allow_message_counter: true) }
  let(:counter) { js.counter("#{stream}.hits") }

  before { require_api_level(js, 2) }

  describe "#increment" do
    it "adds one by default and returns the new total" do
      expect([counter.increment, counter.increment]).to eq([1, 2])
    end

    it "adds the given delta" do
      counter.increment(5)

      expect(counter.increment(37)).to eq(42)
    end

    it "subtracts a negative delta, below zero if need be" do
      counter.increment(3)

      expect(counter.increment(-10)).to eq(-7)
    end

    it "accepts a zero delta" do
      counter.increment(4)

      expect(counter.increment(0)).to eq(4)
    end

    it "keeps integers too big for 64 bits exact" do
      counter.increment(2**70)

      expect(counter.increment(10**30)).to eq((2**70) + (10**30))
    end

    it "keeps one total per subject" do
      counter.increment(5)

      expect(js.counter("#{stream}.misses").increment(2)).to eq(2)
    end

    it "publishes the delta in a header with an empty payload" do
      wire = collect(nc, "#{stream}.>")
      counter.increment(-3)

      expect(wire.pop(timeout: 2))
        .to have_attributes(data: "", headers: have_attributes(to_h: { "Nats-Incr" => ["-3"] }))
    end

    it "raises ArgumentError for a delta that is not an integer" do
      expect { counter.increment(1.5) }.to raise_error(ArgumentError, /Integer/)
    end

    it "raises the server's error when the stream does not allow counters" do
      plain = ingest_stream(js)

      expect { js.counter("#{plain}.hits").increment }
        .to raise_error(Tether::JetStream::APIError) { |error| expect(error.err_code).to eq(10_168) }
    end

    it "raises NoStreamResponseError when no stream captures the subject" do
      expect { js.counter(topic).increment(timeout: 2) }.to raise_error(Tether::JetStream::NoStreamResponseError)
    end
  end

  describe "#value" do
    it "returns nil when the counter was never incremented" do
      expect(counter.value).to be_nil
    end

    it "returns the total" do
      counter.increment(5)
      counter.increment(-2)

      expect(counter.value).to eq(3)
    end

    it "returns a total too big for 64 bits exactly" do
      counter.increment(10**30)

      expect(counter.value).to eq(10**30)
    end

    it "reads from the stream it was given without looking it up" do
      counter.increment(8)

      expect(js.counter("#{stream}.hits", stream: stream).value).to eq(8)
    end

    it "raises NoStreamResponseError when no stream captures the subject" do
      expect { js.counter(topic).value }.to raise_error(Tether::JetStream::NoStreamResponseError)
    end

    it "raises StreamNotFoundError when the stream it was given does not exist" do
      expect { js.counter(topic, stream: "MISSING").value }.to raise_error(Tether::JetStream::StreamNotFoundError)
    end

    it "raises an error when the last message on the subject is not a counter" do
      plain = ingest_stream(js)
      js.publish("#{plain}.hits", "not a counter")

      expect { js.counter("#{plain}.hits").value }.to raise_error(Tether::JetStream::Error, /not a counter/)
    end
  end

  describe "#sources" do
    it "returns an empty hash for a counter that aggregates nothing" do
      counter.increment

      expect(counter.sources).to eq({})
    end

    it "returns an empty hash when the counter was never incremented" do
      expect(counter.sources).to eq({})
    end

    it "returns the last total seen per origin stream and subject on an aggregating stream" do
      total = "AGG#{stream}"
      js.create_stream(name: total, storage: :memory, allow_message_counter: true, sources: [{ name: stream }])
      counter.increment(5)
      counter.increment(7)
      aggregate = js.counter("#{stream}.hits", stream: total)
      eventually { aggregate.value == 12 }

      expect(aggregate.sources).to eq(stream => { "#{stream}.hits" => 12 })
    ensure
      js.delete_stream(total)
    end
  end

  describe "#subject" do
    it "returns the subject the counter lives on" do
      expect(counter.subject).to eq("#{stream}.hits")
    end
  end
end
