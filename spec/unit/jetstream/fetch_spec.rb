# frozen_string_literal: true

RSpec.describe Tether::JetStream::Fetch do
  let(:client) { FakePullClient.new }
  let(:js) { Tether::JetStream.new(client) }
  let(:config) { { "name" => "worker", "ack_policy" => "explicit" } }
  let(:consumer) do
    info = Tether::JetStream::ConsumerInfo.from_h("stream_name" => "ORDERS", "name" => "worker", "config" => config)
    Tether::JetStream::Consumer.new(js, info)
  end

  def fetch(batch = 3, timeout: 0.05, **options)
    deadline = timeout && Tether::Deadline.after(timeout)
    described_class.new(consumer, batch, deadline: deadline, **options).call
  end

  def answer(*lines, messages: 0)
    client.on_pull do
      messages.times { |index| client.message("m#{index}", sequence: index + 1) }
      lines.each { |line, headers| client.status(line, headers || {}) }
    end
  end

  def request
    client.pulls.last
  end

  describe "#initialize" do
    it "raises ArgumentError for a batch below one" do
      expect { fetch(0) }.to raise_error(ArgumentError, /at least 1/)
    end

    it "raises ArgumentError for a heartbeat without a timeout" do
      expect { fetch(1, timeout: nil, wait: false, heartbeat: 1) }.to raise_error(ArgumentError, /needs a timeout/)
    end

    it "raises PushConsumerError for a push consumer" do
      config["deliver_subject"] = "deliver.orders"

      expect { fetch }.to raise_error(Tether::JetStream::PushConsumerError)
    end
  end

  describe "#call" do
    it "asks for the batch with an expiry a tenth below the timeout" do
      fetch(3, timeout: 0.1)

      expect(request).to include("batch" => 3, "expires" => be_between(80_000_000, 90_000_000))
      expect(request.keys).to contain_exactly("batch", "expires")
    end

    it "keeps the expiry at most one second below a long timeout and asks for a heartbeat of five seconds" do
      answer("404 No Messages")
      fetch(3, timeout: 30)

      expect(request).to include("expires" => be_between(28_900_000_000, 29_000_000_000),
                                 "idle_heartbeat" => 5_000_000_000)
    end

    it "asks only for what is available now with wait: false and no timeout" do
      answer("404 No Messages")
      fetch(3, timeout: nil, wait: false)

      expect(request).to eq("batch" => 3, "no_wait" => true)
    end

    it "sends no_wait with an expiry for wait: false and a timeout" do
      fetch(3, timeout: 0.05, wait: false)

      expect(request).to include("no_wait" => true, "expires" => a_kind_of(Integer))
    end

    it "sends the byte budget" do
      fetch(3, max_bytes: 4096)

      expect(request).to include("max_bytes" => 4096)
    end

    it "sends the consumer's pin id and the priority group fields" do
      config["priority_groups"] = ["jobs"]
      consumer.pin_id = "pin-1"
      fetch(3, group: "jobs", min_pending: 5)

      expect(request).to include("id" => "pin-1", "group" => "jobs", "min_pending" => 5)
    end

    it "replies to an inbox of its own and unsubscribes when it ends" do
      fetch

      expect(client.published.last.reply_to).to start_with("_INBOX.fake.")
      expect(client).not_to be_subscribed
    end

    it "sends nothing when the deadline has already passed" do
      expect(fetch(3, timeout: 0)).to be_empty
      expect(client.published).to be_empty
    end

    it "returns as soon as the batch is complete" do
      answer(messages: 3)

      expect(fetch(3, timeout: 30).map(&:data)).to eq(%w[m0 m1 m2])
    end

    it "returns once the received bytes reach the byte budget" do
      answer(messages: 3)

      expect(fetch(10, timeout: 30, max_bytes: 60).size).to eq(1)
    end

    it "skips heartbeats" do
      answer(["100 Idle Heartbeat"], ["404 No Messages"], messages: 1)

      expect(fetch(3, timeout: 30).size).to eq(1)
    end

    it "returns what arrived on 404 No Messages" do
      answer(["404 No Messages"], messages: 2)

      expect(fetch(3, timeout: 30)).to have_attributes(size: 2, error: nil)
    end

    it "returns what arrived on 408 Request Timeout" do
      answer(["408 Request Timeout", { "Nats-Pending-Messages" => 3, "Nats-Pending-Bytes" => 0 }])

      expect(fetch(3, timeout: 30)).to be_empty
    end

    it "returns an empty batch on 408 Requests Pending" do
      answer("408 Requests Pending")

      expect(fetch(3, timeout: 30)).to be_empty
    end

    it "returns what arrived on 409 Message Size Exceeds MaxBytes" do
      answer(["409 Message Size Exceeds MaxBytes"], messages: 1)

      expect(fetch(3, timeout: 30, max_bytes: 4096)).to have_attributes(size: 1, error: nil)
    end

    it "raises ConsumerDeletedError on 409 Consumer Deleted when nothing arrived" do
      answer("409 Consumer Deleted")

      expect { fetch(3, timeout: 30) }.to raise_error(Tether::JetStream::ConsumerDeletedError)
    end

    it "leaves the error on the batch when messages arrived before it" do
      answer(["409 Leadership Change"], messages: 1)

      expect(fetch(3, timeout: 30)).to have_attributes(size: 1, error: a_kind_of(Tether::JetStream::PullError))
    end

    it "raises PullError on 400 Bad Request" do
      answer("400 Bad Request - Priority Group missing")

      expect { fetch(3, timeout: 30) }
        .to raise_error(Tether::JetStream::PullError, "400 Bad Request - Priority Group missing")
    end

    it "raises ServiceUnavailableError on 503" do
      answer("503")

      expect { fetch(3, timeout: 30) }.to raise_error(Tether::JetStream::ServiceUnavailableError)
    end

    it "clears the pin id and raises PinIdMismatchError on 423" do
      consumer.pin_id = "stale"
      answer("423 Nats-Pin-Id mismatch")

      expect { fetch(3, timeout: 30) }.to raise_error(Tether::JetStream::PinIdMismatchError)
      expect(consumer.pin_id).to be_nil
    end

    it "raises NoHeartbeatError after two silent heartbeat intervals" do
      expect { fetch(3, timeout: 30, heartbeat: 0.02) }.to raise_error(Tether::JetStream::NoHeartbeatError)
    end

    it "yields each message as it arrives" do
      answer(messages: 2)
      yielded = []

      described_class.new(consumer, 2, deadline: Tether::Deadline.after(30)).call { |message| yielded.push(message) }

      expect(yielded.map(&:data)).to eq(%w[m0 m1])
    end
  end
end
