# frozen_string_literal: true

RSpec.describe Tether::JetStream::PullStatus do
  def frame(line, payload: "", headers: {})
    block = "NATS/1.0 #{line}".strip + "\r\n#{headers.map { |key, value| "#{key}: #{value}\r\n" }.join}\r\n"
    Tether::Protocol::Msg.new(subject: "_INBOX.x", data: payload, header_bytes: block)
  end

  def kind_of(line)
    described_class.of(frame(line)).kind
  end

  def error_of(line)
    described_class.of(frame(line)).error
  end

  describe ".of" do
    it "returns nil for a message without headers" do
      expect(described_class.of(Tether::Protocol::Msg.new(subject: "a", data: ""))).to be_nil
    end

    it "returns nil for a message with headers and no status" do
      expect(described_class.of(frame("", headers: { "Nats-Msg-Size" => 5 }))).to be_nil
    end

    it "returns nil for a message with a status line and a payload" do
      expect(described_class.of(frame("409 Consumer Deleted", payload: "data"))).to be_nil
    end

    it "classifies 100 Idle Heartbeat as a heartbeat" do
      expect(kind_of("100 Idle Heartbeat")).to eq(:heartbeat)
    end

    it "classifies 404 No Messages as a normal end" do
      expect(kind_of("404 No Messages")).to eq(:end)
    end

    it "classifies 408 Request Timeout as a normal end" do
      expect(kind_of("408 Request Timeout")).to eq(:end)
    end

    it "classifies 408 Interest Expired as a normal end" do
      expect(kind_of("408 Interest Expired")).to eq(:end)
    end

    it "classifies 408 Requests Pending as a warning" do
      expect(kind_of("408 Requests Pending")).to eq(:warning)
    end

    it "classifies 409 Batch Completed as a normal end" do
      expect(kind_of("409 Batch Completed")).to eq(:end)
    end

    it "classifies 409 Message Size Exceeds MaxBytes as too large" do
      expect(kind_of("409 Message Size Exceeds MaxBytes")).to eq(:too_large)
    end

    it "classifies 409 Exceeded MaxWaiting as a warning carrying a PullError" do
      expect(kind_of("409 Exceeded MaxWaiting")).to eq(:warning)
      expect(error_of("409 Exceeded MaxWaiting")).to have_attributes(class: Tether::JetStream::PullError, status: 409)
    end

    it "classifies 409 Server Shutdown as a warning" do
      expect(kind_of("409 Server Shutdown")).to eq(:warning)
    end

    it "classifies 409 Leadership Change as a reset" do
      expect(kind_of("409 Leadership Change")).to eq(:reset)
    end

    it "classifies 409 Consumer Deleted as terminal with ConsumerDeletedError" do
      expect(kind_of("409 Consumer Deleted")).to eq(:terminal)
      expect(error_of("409 Consumer Deleted")).to be_a(Tether::JetStream::ConsumerDeletedError)
    end

    it "matches the description of a 409 without regard to case" do
      expect(kind_of("409 consumer deleted")).to eq(:terminal)
    end

    it "classifies 409 Consumer is push based as terminal with PushConsumerError" do
      expect(error_of("409 Consumer is push based")).to be_a(Tether::JetStream::PushConsumerError)
    end

    it "classifies 423 as a pin mismatch with PinIdMismatchError" do
      expect(kind_of("423 Nats-Pin-Id mismatch")).to eq(:pin)
      expect(error_of("423 Nats-Wrong-Pin-Id")).to be_a(Tether::JetStream::PinIdMismatchError)
    end

    it "classifies a bare 503 as unavailable with a retryable error" do
      expect(kind_of("503")).to eq(:unavailable)
      expect(error_of("503")).to be_retryable
    end

    it "classifies 400 as terminal with the server's text" do
      expect(error_of("400 Bad Request - Priority Group missing").message)
        .to eq("400 Bad Request - Priority Group missing")
    end

    it "classifies 412 as terminal with RequiredApiLevelError" do
      expect(error_of("412 Required Api Level")).to have_attributes(class: Tether::JetStream::RequiredApiLevelError,
                                                                    err_code: 10_185)
    end

    it "classifies an unknown 409 as a warning" do
      expect(kind_of("409 Something New")).to eq(:warning)
    end

    it "classifies an unknown code as a warning" do
      expect(kind_of("499 From The Future")).to eq(:warning)
    end

    it "reads the pending headers" do
      status = described_class.of(frame("408 Request Timeout",
                                        headers: { "Nats-Pending-Messages" => 7, "Nats-Pending-Bytes" => 512 }))

      expect(status).to have_attributes(code: 408, pending_messages: 7, pending_bytes: 512)
    end

    it "reads absent pending headers as zero" do
      expect(described_class.of(frame("409 Consumer Deleted")))
        .to have_attributes(pending_messages: 0, pending_bytes: 0)
    end
  end

  describe "#heartbeat?" do
    it "is true for 100" do
      expect(described_class.of(frame("100 Idle Heartbeat"))).to be_heartbeat
    end

    it "is false for a status that ends a pull" do
      expect(described_class.of(frame("408 Request Timeout"))).not_to be_heartbeat
    end
  end

  describe "#normal_end?" do
    it "is true for 404" do
      expect(described_class.of(frame("404 No Messages"))).to be_normal_end
    end

    it "is true for every 408" do
      expect(described_class.of(frame("408 Requests Pending"))).to be_normal_end
    end

    it "is true for 409 Message Size Exceeds MaxBytes" do
      expect(described_class.of(frame("409 Message Size Exceeds MaxBytes"))).to be_normal_end
    end

    it "is false for 409 Leadership Change" do
      expect(described_class.of(frame("409 Leadership Change"))).not_to be_normal_end
    end

    it "is false for 503" do
      expect(described_class.of(frame("503"))).not_to be_normal_end
    end
  end
end
