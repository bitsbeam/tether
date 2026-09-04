# frozen_string_literal: true

RSpec.describe Tether::JetStream::PublishFuture do
  include ClientHelper
  include PublishingHelper

  let(:nc) { connect }
  let(:js) { nc.jetstream }
  let(:stream) { ingest_stream(js) }
  let(:silent) { ingest_stream(js, no_ack: true) }

  describe "#value" do
    it "returns the acknowledgment" do
      future = js.publish_async("#{stream}.a", "one")

      expect(future.value(timeout: 2)).to be_a(Tether::JetStream::PubAck).and have_attributes(stream: stream)
    end

    it "returns the same acknowledgment every time" do
      future = js.publish_async("#{stream}.a", "one")

      expect(future.value(timeout: 2)).to be(future.value)
    end

    it "raises what the server answered" do
      future = js.publish_async("#{stream}.a", "one", expect: { stream: "ANOTHER" })

      expect { future.value(timeout: 2) }.to raise_error(Tether::JetStream::StreamMismatchError)
    end

    it "raises NoStreamResponseError when no stream captures the subject" do
      future = js.publish_async(topic, "one")

      expect { future.value(timeout: 2) }.to raise_error(Tether::JetStream::NoStreamResponseError, /#{topic}/)
    end

    it "raises an error when something other than a stream answered" do
      nc.subscribe(topic) { |msg| msg.respond("not an acknowledgment") }
      nc.flush
      future = js.publish_async(topic, "one")

      expect { future.value(timeout: 2) }.to raise_error(Tether::JetStream::Error, /not a JetStream acknowledgment/)
    end

    it "raises TimeoutError when no acknowledgment arrives in time and leaves the future pending" do
      future = js.publish_async("#{silent}.a", "one")

      expect { future.value(timeout: 0.05) }.to raise_error(Tether::TimeoutError)
      expect(future).not_to be_done
    end

    it "wakes every thread waiting for the acknowledgment" do
      gate = Thread::Queue.new
      nc.subscribe(topic) { |msg| gate.pop && msg.respond(%({"stream":"S","seq":9})) }
      nc.flush
      future = js.publish_async(topic, "one")
      waiters = Array.new(3) { Thread.new { future.value(timeout: 5).sequence } }
      eventually { waiters.all? { |waiter| waiter.status == "sleep" } }
      gate.push(true)

      expect(waiters.map(&:value)).to eq([9, 9, 9])
    end
  end

  describe "#wait" do
    it "returns true once the publish is acknowledged" do
      expect(js.publish_async("#{stream}.a", "one").wait(timeout: 2)).to be(true)
    end

    it "returns true without raising when the publish failed" do
      expect(js.publish_async(topic, "one").wait(timeout: 2)).to be(true)
    end

    it "returns false when no acknowledgment arrives in time" do
      expect(js.publish_async("#{silent}.a", "one").wait(timeout: 0.05)).to be(false)
    end

    it "returns false for an expired deadline while the publish is in flight" do
      expect(js.publish_async("#{silent}.a", "one").wait(timeout: Tether::Deadline.after(0))).to be(false)
    end

    it "waits for the context's timeout by default" do
      future = nc.jetstream(timeout: 0.05).publish_async("#{silent}.a", "one")

      expect(future.wait).to be(false)
    end
  end

  describe "#done?" do
    it "returns false while the publish is in flight" do
      expect(js.publish_async("#{silent}.a", "one")).not_to be_done
    end

    it "returns true once the publish is acknowledged" do
      future = js.publish_async("#{stream}.a", "one")
      future.wait(timeout: 2)

      expect(future).to be_done
    end

    it "returns true once the publish failed" do
      future = js.publish_async(topic, "one")
      future.wait(timeout: 2)

      expect(future).to be_done
    end
  end

  describe "#error" do
    it "returns nil while the publish is in flight" do
      expect(js.publish_async("#{silent}.a", "one").error).to be_nil
    end

    it "returns nil once the publish is acknowledged" do
      future = js.publish_async("#{stream}.a", "one")
      future.wait(timeout: 2)

      expect(future.error).to be_nil
    end

    it "returns what the server answered once the publish was refused" do
      future = js.publish_async("#{stream}.a", "one", expect: { last_subject_sequence: 4 })
      future.wait(timeout: 2)

      expect(future.error).to be_a(Tether::JetStream::WrongLastSequenceError).and have_attributes(err_code: 10_071)
    end
  end

  describe "#subject" do
    it "returns the subject the message was published to" do
      expect(js.publish_async("#{stream}.a", "one").subject).to eq("#{stream}.a")
    end
  end

  describe "#inspect" do
    it "shows the subject and whether the outcome is known" do
      expect(js.publish_async("#{silent}.a", "one").inspect).to include("#{silent}.a", "done=false")
    end
  end
end
