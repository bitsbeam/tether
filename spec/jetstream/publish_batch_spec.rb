# frozen_string_literal: true

RSpec.describe Tether::JetStream::PublishBatch do
  include ClientHelper
  include PublishingHelper

  let(:nc) { connect }
  let(:js) { nc.jetstream }
  let(:stream) { ingest_stream(js, allow_atomic: true, allow_message_ttl: true, allow_rollup: true) }

  before { require_api_level(js, 2) }

  describe "#publish" do
    it "returns the batch, so publishes chain" do
      js.batch { |batch| expect(batch.publish("#{stream}.a", "one")).to be(batch) }
    end

    it "stores the payload and headers of every message" do
      js.batch do |batch|
        batch.publish("#{stream}.a", "one", headers: { "X-Kind" => "first" })
        batch.publish("#{stream}.b", "two", headers: { "X-Kind" => "second" })
      end

      expect([last_stored(js, stream, "#{stream}.a"), last_stored(js, stream, "#{stream}.b")]).to match(
        [{ data: "one", headers: include("X-Kind" => "first") },
         { data: "two", headers: include("X-Kind" => "second") }]
      )
    end

    it "numbers the messages from 1 under one batch id and commits with an empty end-of-batch marker" do
      wire = collect(nc, "#{stream}.>")
      ack = js.batch { |batch| batch.publish("#{stream}.a", "one").publish("#{stream}.b", "two") }
      sent = take(wire, 3).map do |msg|
        [msg.subject, msg.data, msg.headers["Nats-Batch-Id"], msg.headers["Nats-Batch-Sequence"],
         msg.headers["Nats-Batch-Commit"]]
      end

      expect(sent).to eq([["#{stream}.a", "one", ack.batch_id, "1", nil],
                          ["#{stream}.b", "two", ack.batch_id, "2", nil],
                          ["#{stream}.b", "", ack.batch_id, "3", "eob"]])
    end

    it "asks for a reply to the first message and the commit only" do
      wire = collect(nc, "#{stream}.>")
      js.batch { |batch| batch.publish("#{stream}.a", "one").publish("#{stream}.a", "two") }

      expect(take(wire, 3).map { |msg| !msg.reply_to.nil? }).to eq([true, false, true])
    end

    it "requires API level 2 of the server on every message" do
      wire = collect(nc, "#{stream}.>")
      js.batch { |batch| batch.publish("#{stream}.a", "one").publish("#{stream}.a", "two") }

      expect(take(wire, 3).map { |msg| msg.headers["Nats-Required-Api-Level"] }).to eq(%w[2 2 2])
    end

    it "does not store the header that asks for an API level" do
      js.batch { |batch| batch.publish("#{stream}.a", "one") }

      expect(last_stored(js, stream, "#{stream}.a")[:headers]).not_to include("Nats-Required-Api-Level")
    end

    it "leaves the caller's headers alone" do
      headers = { "X-Kind" => "first" }
      js.batch { |batch| batch.publish("#{stream}.a", "one", headers: headers) }

      expect(headers).to eq("X-Kind" => "first")
    end

    it "sends the publish options with the message" do
      js.publish("#{stream}.b", "replaced")
      js.batch do |batch|
        batch.publish("#{stream}.a", "one", msg_id: "id-1", ttl: 60)
        batch.publish("#{stream}.b", "two", rollup: :subject)
      end

      expect(stored_subjects(js, stream)).to eq("#{stream}.a" => 1, "#{stream}.b" => 1)
    end

    it "checks the expectations of every message against the stream as it is at the commit" do
      js.publish("#{stream}.a", "before")
      ack = js.batch do |batch|
        batch.publish("#{stream}.a", "one", expect: { last_sequence: 1 })
        batch.publish("#{stream}.b", "two", expect: { last_subject_sequence: 0 })
      end

      expect(ack).to have_attributes(sequence: 3, count: 2)
    end

    it "raises the server's error when a message id repeats within the batch" do
      error = error_from do
        js.batch do |batch|
          batch.publish("#{stream}.a", "one", msg_id: "same")
          batch.publish("#{stream}.a", "two", msg_id: "same")
        end
      end

      expect(error).to be_a(Tether::JetStream::APIError).and have_attributes(err_code: 10_201)
    end

    it "raises ArgumentError for expect: { last_msg_id: }, which the server rejects in a batch" do
      expect { js.batch { |batch| batch.publish("#{stream}.a", "one", expect: { last_msg_id: "id-0" }) } }
        .to raise_error(ArgumentError, /last_msg_id/)
    end

    it "raises ArgumentError for an option it does not know" do
      expect { js.batch { |batch| batch.publish("#{stream}.a", "one", retries: 2) } }
        .to raise_error(ArgumentError, /retries/)
    end

    it "raises BatchTooLargeError past the server's limit of 1,000 messages and stores nothing" do
      error = error_from do
        js.batch { |batch| 1001.times { |index| batch.publish("#{stream}.a", index.to_s) } }
      end

      expect([error, stored_messages(js, stream)])
        .to match([a_kind_of(Tether::JetStream::BatchTooLargeError).and(have_attributes(err_code: 10_199)), 0])
    end

    it "commits a batch of exactly 1,000 messages" do
      ack = js.batch { |batch| 1000.times { |index| batch.publish("#{stream}.a", index.to_s) } }

      expect(ack).to have_attributes(count: 1000, sequence: 1000)
    end

    it "raises once the batch is committed" do
      kept = nil
      js.batch { |batch| (kept = batch).publish("#{stream}.a", "one") }

      expect { kept.publish("#{stream}.a", "late") }.to raise_error(Tether::JetStream::Error, /closed/)
    end

    it "raises once the server refused to open the batch" do
      plain = ingest_stream(js)
      kept = nil
      error_from { js.batch { |batch| (kept = batch).publish("#{plain}.a", "one") } }

      expect { kept.publish("#{plain}.a", "again") }.to raise_error(Tether::JetStream::Error, /closed/)
    end
  end

  describe "#size" do
    it "counts the messages published into the batch" do
      sizes = []
      js.batch do |batch|
        sizes << batch.size
        batch.publish("#{stream}.a", "one").publish("#{stream}.a", "two")
        sizes << batch.size
      end

      expect(sizes).to eq([0, 2])
    end
  end

  describe "#id" do
    it "returns the id the commit is acknowledged with" do
      id = nil
      ack = js.batch { |batch| (id = batch.id) && batch.publish("#{stream}.a", "one") }

      expect(ack.batch_id).to eq(id)
    end

    it "differs from one batch to the next" do
      ids = Array.new(2) { js.batch { |batch| batch.publish("#{stream}.a", "one") }.batch_id }

      expect(ids.uniq.size).to eq(2)
    end
  end
end
