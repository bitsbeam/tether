# frozen_string_literal: true

RSpec.describe Tether::JetStream::Pull do
  let(:client) { FakePullClient.new }
  let(:js) { Tether::JetStream.new(client) }
  let(:config) { { "name" => "worker", "ack_policy" => "explicit" } }
  let(:consumer) do
    info = Tether::JetStream::ConsumerInfo.from_h("stream_name" => "ORDERS", "name" => "worker", "config" => config)
    Tether::JetStream::Consumer.new(js, info)
  end
  let(:warnings) { [] }
  let(:options) { { max_messages: 10 } }
  let(:pull) { described_class.new(consumer, nil, on_error: ->(error) { warnings << error }, **options) }

  def idle
    pull.next(timeout: 0)
  rescue Tether::TimeoutError
    nil
  end

  def silence(seconds)
    allow(Tether::Deadline).to receive(:now).and_return(Tether::Deadline.now + seconds)
    client.tick
    allow(Tether::Deadline).to receive(:now).and_call_original
  end

  def take(count)
    count.times { |index| client.message("m#{index}", sequence: index + 1) }
    Array.new(count) { pull.next(timeout: 0) }
  end

  describe "#initialize" do
    it "subscribes to an inbox and pulls a full batch with an expiry and a heartbeat of half of it" do
      pull

      expect(client.pulls).to eq([{ "batch" => 10, "expires" => 30_000_000_000, "idle_heartbeat" => 15_000_000_000 }])
      expect(client.published.first).to have_attributes(subject: "$JS.API.CONSUMER.MSG.NEXT.ORDERS.worker",
                                                        reply_to: client.inbox)
    end

    it "caps the heartbeat at 30 seconds" do
      described_class.new(consumer, nil, expires: 300)

      expect(client.pulls.first).to include("idle_heartbeat" => 30_000_000_000)
    end

    it "asks for a million messages within the byte budget when max_bytes is given" do
      described_class.new(consumer, nil, max_bytes: 4096)

      expect(client.pulls.first).to include("batch" => 1_000_000, "max_bytes" => 4096)
    end

    it "sends the priority group fields and the required API level with every pull" do
      config["priority_groups"] = ["jobs"]
      described_class.new(consumer, nil, group: "jobs", min_pending: 5, min_ack_pending: 2, priority: 3)

      expect(client.pulls.first).to include("group" => "jobs", "min_pending" => 5, "min_ack_pending" => 2,
                                            "priority" => 3)
      expect(client.published.first.headers).to eq("Nats-Required-Api-Level" => "1")
    end

    it "raises when a group is given for a consumer without priority groups" do
      expect { described_class.new(consumer, nil, group: "jobs") }.to raise_error(ArgumentError, /no priority groups/)
    end

    it "raises when the consumer has priority groups and no group is given" do
      config["priority_groups"] = ["jobs"]

      expect { described_class.new(consumer, nil) }.to raise_error(ArgumentError, /group: is required/)
    end

    it "raises for a push consumer before anything is sent" do
      config["deliver_subject"] = "deliver.orders"

      expect { pull }.to raise_error(Tether::JetStream::PushConsumerError)
      expect(client.published).to be_empty
    end

    it "raises when the heartbeat exceeds half of the expiry" do
      expect { described_class.new(consumer, nil, expires: 2, heartbeat: 1.5) }
        .to raise_error(ArgumentError, /at most half/)
    end

    it "raises when the expiry is below one second" do
      expect { described_class.new(consumer, nil, expires: 0.5) }.to raise_error(ArgumentError, /at least 1 second/)
    end

    it "raises for an unknown option" do
      expect { described_class.new(consumer, nil, batch: 5) }.to raise_error(ArgumentError, /unknown option: batch/)
    end

    it "does not pull while the connection is down, and pulls once it is back" do
      client.connected = false
      pull
      expect(client.pulls).to be_empty

      client.connected = true
      client.transition(:reconnect)
      idle

      expect(client.pulls.size).to eq(1)
    end
  end

  describe "#next" do
    it "returns messages in the order they arrived" do
      pull

      expect(take(3).map(&:data)).to eq(%w[m0 m1 m2])
    end

    it "raises TimeoutError when nothing arrives in time" do
      expect { pull.next(timeout: 0.01) }.to raise_error(Tether::TimeoutError)
    end

    it "pulls the difference once fewer than half of the batch is outstanding" do
      pull
      take(5)
      expect(client.pulls.size).to eq(1)

      take(1)

      expect(client.pulls.last).to include("batch" => 6)
    end

    it "pulls again when the outstanding count falls below a custom threshold" do
      options[:threshold] = 9
      pull

      take(2)

      expect(client.pulls.map { |request| request["batch"] }).to eq([10, 2])
    end

    it "pulls the bytes taken once less than half of the byte budget is outstanding" do
      options.replace(max_bytes: 1000)
      pull
      client.message("x" * 600)

      size = pull.next(timeout: 0).then { |message| message.subject.bytesize + message.reply_to.bytesize + 600 }

      expect(client.pulls.last).to include("batch" => 1_000_000, "max_bytes" => size)
    end

    it "never hands a status to the caller" do
      pull
      client.status("100 Idle Heartbeat", "Nats-Last-Consumer" => 0, "Nats-Last-Stream" => 0)
      client.status("408 Request Timeout", "Nats-Pending-Messages" => 1, "Nats-Pending-Bytes" => 0)

      expect { pull.next(timeout: 0) }.to raise_error(Tether::TimeoutError)
    end

    it "treats an empty message without a status as a message" do
      pull
      client.message("", headers: { "Nats-Msg-Size" => 5 })

      expect(pull.next(timeout: 0).headers["Nats-Msg-Size"]).to eq("5")
    end

    it "does nothing but note activity on 100 Idle Heartbeat" do
      pull
      client.status("100 Idle Heartbeat")
      idle

      expect(client.pulls.size).to eq(1)
      expect(warnings).to be_empty
    end

    it "subtracts the pending headers of 408 Request Timeout and pulls the difference without a warning" do
      pull
      client.status("408 Request Timeout", "Nats-Pending-Messages" => 10, "Nats-Pending-Bytes" => 0)
      idle

      expect(client.pulls.last).to include("batch" => 10)
      expect(warnings).to be_empty
    end

    it "keeps what is buffered in the outstanding count when a pull ends" do
      pull
      3.times { |index| client.message("m#{index}", sequence: index + 1) }
      client.status("408 Request Timeout", "Nats-Pending-Messages" => 7, "Nats-Pending-Bytes" => 0)
      idle

      expect(client.pulls.last).to include("batch" => 7)
    end

    it "subtracts the pending headers of 409 Batch Completed" do
      options.replace(max_bytes: 1000)
      pull
      client.status("409 Batch Completed", "Nats-Pending-Messages" => 0, "Nats-Pending-Bytes" => 800)
      idle

      expect(client.pulls.last).to include("max_bytes" => 800)
    end

    it "subtracts the pending headers of 409 Message Size Exceeds MaxBytes" do
      options.replace(max_bytes: 1000)
      pull
      client.message("x" * 300)
      client.status("409 Message Size Exceeds MaxBytes", "Nats-Pending-Messages" => 9, "Nats-Pending-Bytes" => 620)
      pull.next(timeout: 0)

      expect(client.pulls.last).to include("max_bytes" => 620)
      expect(warnings).to be_empty
    end

    it "warns and waits for the next silence when a full byte budget cannot hold the next message" do
      options.replace(max_bytes: 1000)
      pull
      client.status("409 Message Size Exceeds MaxBytes", "Nats-Pending-Messages" => 1, "Nats-Pending-Bytes" => 1000)
      idle

      expect(client.pulls.size).to eq(1)
      expect(warnings).to contain_exactly(have_attributes(class: Tether::JetStream::PullError, status: 409))
    end

    it "ends the pull silently on 404 No Messages" do
      pull
      client.status("404 No Messages")
      idle

      expect(client.pulls.size).to eq(1)
      expect(warnings).to be_empty
    end

    it "warns on 408 Requests Pending" do
      pull
      client.status("408 Requests Pending")
      idle

      expect(warnings).to contain_exactly(have_attributes(class: Tether::JetStream::PullError, status: 408))
    end

    it "warns on 409 Exceeded MaxRequestBatch and keeps going" do
      pull
      client.status("409 Exceeded MaxRequestBatch of 5")
      idle

      expect(warnings.map(&:message)).to eq(["409 Exceeded MaxRequestBatch of 5"])
      expect(pull).not_to be_stopped
    end

    it "warns on 409 Exceeded MaxRequestExpires" do
      pull
      client.status("409 Exceeded MaxRequestExpires of 10s")
      idle

      expect(warnings.map(&:message)).to eq(["409 Exceeded MaxRequestExpires of 10s"])
    end

    it "warns on 409 Exceeded MaxRequestMaxBytes" do
      pull
      client.status("409 Exceeded MaxRequestMaxBytes of 64")
      idle

      expect(warnings.map(&:message)).to eq(["409 Exceeded MaxRequestMaxBytes of 64"])
    end

    it "warns on 409 Exceeded MaxWaiting" do
      pull
      client.status("409 Exceeded MaxWaiting")
      idle

      expect(warnings.map(&:message)).to eq(["409 Exceeded MaxWaiting"])
    end

    it "voids every pull on 409 Leadership Change, pulls a full batch and warns" do
      pull
      client.status("409 Leadership Change")
      idle

      expect(client.pulls.map { |request| request["batch"] }).to eq([10, 10])
      expect(warnings.map(&:message)).to eq(["409 Leadership Change"])
    end

    it "subtracts the pending headers of 409 Server Shutdown, pulls again and warns" do
      pull
      client.status("409 Server Shutdown", "Nats-Pending-Messages" => 10, "Nats-Pending-Bytes" => 0)
      idle

      expect(client.pulls.map { |request| request["batch"] }).to eq([10, 10])
      expect(warnings.map(&:message)).to eq(["409 Server Shutdown"])
    end

    it "remembers the pin id of a delivered message and sends it with the next pulls" do
      config.merge!("priority_groups" => ["jobs"], "priority_policy" => "pinned_client")
      options[:group] = "jobs"
      pull
      6.times { |index| client.message("m#{index}", sequence: index + 1, headers: { "Nats-Pin-Id" => "pin-1" }) }
      6.times { pull.next(timeout: 0) }

      expect(client.pulls.last).to include("id" => "pin-1", "group" => "jobs")
    end

    it "clears the pin id on 423 Nats-Pin-Id mismatch, pulls again without it and warns" do
      config.merge!("priority_groups" => ["jobs"], "priority_policy" => "pinned_client")
      options[:group] = "jobs"
      consumer.pin_id = "stale"
      pull
      client.status("423 Nats-Pin-Id mismatch")
      idle

      expect(client.pulls.map { |request| request["id"] }).to eq(["stale", nil])
      expect(warnings).to contain_exactly(a_kind_of(Tether::JetStream::PinIdMismatchError))
    end

    it "clears the pin id on 423 Nats-Wrong-Pin-Id" do
      consumer.pin_id = "stale"
      pull
      client.status("423 Nats-Wrong-Pin-Id", "Nats-Pending-Messages" => 10, "Nats-Pending-Bytes" => 0)
      idle

      expect(consumer.pin_id).to be_nil
      expect(client.pulls.last).not_to include("id")
    end

    it "ends the loop with ConsumerDeletedError on 503 when the server says the consumer is gone" do
      client.response = { "error" => { "code" => 404, "err_code" => 10_014, "description" => "consumer not found" } }
      pull
      client.status("503")

      expect { pull.next(timeout: 0) }.to raise_error(Tether::JetStream::ConsumerDeletedError)
      expect(warnings).to contain_exactly(a_kind_of(Tether::JetStream::ConsumerDeletedError))
    end

    it "reports 503 without pulling when the consumer still exists, and pulls again at the next silence" do
      client.response = { "stream_name" => "ORDERS", "name" => "worker", "config" => config }
      pull
      client.status("503")
      idle
      expect(client.pulls.size).to eq(1)

      silence(31)
      idle

      expect(client.pulls.size).to eq(2)
      expect(warnings).to contain_exactly(a_kind_of(Tether::JetStream::ServiceUnavailableError))
    end

    it "ends the loop with ConsumerDeletedError on 409 Consumer Deleted" do
      pull
      client.status("409 Consumer Deleted")

      expect { pull.next(timeout: 0) }.to raise_error(Tether::JetStream::ConsumerDeletedError)
      expect(pull).to be_stopped
      expect(client).not_to be_subscribed
    end

    it "ends the loop with PushConsumerError on 409 Consumer is push based" do
      pull
      client.status("409 Consumer is push based")

      expect { pull.next(timeout: 0) }.to raise_error(Tether::JetStream::PushConsumerError)
    end

    it "ends the loop with PullError on 400 Bad Request" do
      pull
      client.status("400 Bad Request - heartbeat value too large")

      expect { pull.next(timeout: 0) }
        .to raise_error(Tether::JetStream::PullError, "400 Bad Request - heartbeat value too large")
    end

    it "ends the loop with RequiredApiLevelError on 412 Required Api Level" do
      pull
      client.status("412 Required Api Level")

      expect { pull.next(timeout: 0) }.to raise_error(Tether::JetStream::RequiredApiLevelError)
    end

    it "warns on an unknown 409 and keeps going" do
      pull
      client.status("409 Something New")
      idle

      expect(warnings.map(&:message)).to eq(["409 Something New"])
      expect(pull).not_to be_stopped
    end

    it "warns on an unknown status code and keeps going" do
      pull
      client.status("499 From The Future")
      idle

      expect(warnings.map(&:message)).to eq(["499 From The Future"])
      expect(pull).not_to be_stopped
    end

    it "reports the terminal error to the error handlers as well" do
      pull
      client.status("409 Consumer Deleted")
      error_class = Tether::JetStream::ConsumerDeletedError

      expect { pull.next(timeout: 0) }.to raise_error(error_class)
      expect(warnings).to contain_exactly(a_kind_of(error_class))
    end

    it "warns with NoHeartbeatError and pulls a full batch after two silent heartbeat intervals" do
      pull
      silence(31)
      idle

      expect(warnings).to contain_exactly(a_kind_of(Tether::JetStream::NoHeartbeatError))
      expect(client.pulls.map { |request| request["batch"] }).to eq([10, 10])
    end

    it "stays quiet while frames keep arriving" do
      pull
      silence(29)
      idle

      expect(client.pulls.size).to eq(1)
      expect(warnings).to be_empty
    end

    it "ignores silence while the connection is down" do
      pull
      client.connected = false
      silence(31)
      client.connected = true
      idle

      expect(warnings).to be_empty
    end

    it "pulls again without a warning when the silence follows the end of every pull" do
      pull
      client.status("408 Request Timeout", "Nats-Pending-Messages" => 10, "Nats-Pending-Bytes" => 0)
      idle
      client.connected = false
      client.status("408 Request Timeout", "Nats-Pending-Messages" => 10, "Nats-Pending-Bytes" => 0)
      idle
      client.connected = true
      silence(31)
      idle

      expect(client.pulls.size).to eq(3)
      expect(warnings).to be_empty
    end

    it "does not take a slow reader for a silent server once the batch has arrived" do
      pull
      10.times { |index| client.message("m#{index}", sequence: index + 1) }
      silence(31)
      pull.next(timeout: 0)

      expect(client.pulls.size).to eq(1)
      expect(warnings).to be_empty
    end

    it "pulls a full batch after a reconnect, less what is still buffered" do
      pull
      2.times { |index| client.message("m#{index}", sequence: index + 1) }
      client.transition(:reconnect)
      pull.next(timeout: 0)

      expect(client.pulls.last).to include("batch" => 8)
    end

    it "ends the loop with ConnectionClosedError when the connection closes" do
      pull
      client.transition(:close)

      expect { pull.next(timeout: 0) }.to raise_error(Tether::ConnectionClosedError)
    end

    it "sends warnings to the client's error event when the loop has no error handler" do
      loop = described_class.new(consumer)
      client.status("409 Exceeded MaxWaiting")

      expect { loop.next(timeout: 0) }.to raise_error(Tether::TimeoutError)
      expect(client.errors.map(&:message)).to eq(["409 Exceeded MaxWaiting"])
    end

    it "raises StopIteration once the loop was stopped" do
      pull.stop

      expect { pull.next(timeout: 0) }.to raise_error(StopIteration)
    end

    it "raises when the loop delivers to a block" do
      loop = described_class.new(consumer, ->(_message) {})

      expect { loop.next }.to raise_error(ArgumentError, /delivers to a block/)
      loop.stop
    end
  end

  describe "#drain" do
    it "hands out what is buffered without pulling again, then ends" do
      pull
      6.times { |index| client.message("m#{index}", sequence: index + 1) }
      pull.drain

      expect(Array.new(6) { pull.next(timeout: 0).data }).to eq(%w[m0 m1 m2 m3 m4 m5])
      expect { pull.next(timeout: 0) }.to raise_error(StopIteration)
      expect(client.pulls.size).to eq(1)
    end
  end

  describe "#stop" do
    it "unsubscribes, cancels the heartbeat monitor and discards what is buffered" do
      pull
      client.message("m0")
      pull.stop
      client.tick

      expect(client).not_to be_subscribed
      expect { pull.next(timeout: 0) }.to raise_error(StopIteration)
    end
  end

  describe "#next_request" do
    it "builds a pull for the loop's inbox and counts it as outstanding" do
      pull

      inbox, request = pull.next_request(3)

      expect(inbox).to eq(client.inbox)
      expect(JSON.parse(request)).to eq("batch" => 3, "expires" => 30_000_000_000, "idle_heartbeat" => 15_000_000_000)
    end
  end
end
