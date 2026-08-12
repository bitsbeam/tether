# frozen_string_literal: true

RSpec.describe Tether::Protocol::Dispatcher do
  subject(:dispatcher) do
    described_class.new(connection: connection, registry: registry, writer: writer, counters: counters)
  end

  let(:connection) do
    instance_double(Tether::Connection::Lifecycle, info_received: nil, error_received: nil, pong_received: nil)
  end
  let(:registry) { Tether::Protocol::Registry.new }
  let(:writer) { instance_double(Tether::Connection::Writer, pong: nil, resolve_pong: nil) }
  let(:counters) { Tether::Counters.new }
  let(:entry) { instance_double(Tether::Connection::Subscription, sid: "7", deliver: true) }

  describe "#on_msg" do
    it "hands the entry a message attached to it" do
      registry.add(entry)
      dispatcher.on_msg("orders", "7", "_INBOX.r", "NATS/1.0\r\nK: v\r\n\r\n", "body")

      expect(entry).to have_received(:deliver).with(
        have_attributes(subject: "orders", data: "body", reply_to: "_INBOX.r", subscription: entry,
                        headers: have_attributes(to_h: { "K" => ["v"] }))
      )
    end

    it "drops a message whose entry is already gone" do
      expect { dispatcher.on_msg("orders", "9", nil, nil, "body") }.not_to raise_error
    end

    it "counts every message, routed or not" do
      dispatcher.on_msg("orders", "9", nil, nil, "body")

      expect(counters.messages_in).to eq(1)
    end
  end

  describe "#on_ping" do
    it "answers through the writer without touching the socket" do
      dispatcher.on_ping

      expect(writer).to have_received(:pong)
    end
  end

  describe "#on_pong" do
    it "resets the keepalive and releases the oldest flush" do
      dispatcher.on_pong

      expect(connection).to have_received(:pong_received)
      expect(writer).to have_received(:resolve_pong)
    end
  end

  describe "#on_ok" do
    it "ignores the acknowledgement" do
      expect(dispatcher.on_ok).to be_nil
    end
  end

  describe "#on_info" do
    it "passes the decoded document to the connection" do
      dispatcher.on_info(%({"server_id":"x","ldm":true}))

      expect(connection).to have_received(:info_received).with("server_id" => "x", "ldm" => true)
    end

    it "reports a malformed document as a protocol error" do
      dispatcher.on_info("{nope")

      expect(connection).to have_received(:error_received).with(an_instance_of(Tether::ProtocolError))
    end
  end

  describe "#on_error" do
    it "reports the server's error as its typed class" do
      dispatcher.on_error("Authorization Violation")

      expect(connection).to have_received(:error_received).with(an_instance_of(Tether::AuthorizationError))
    end
  end

  describe "#on_protocol_error" do
    it "reports a protocol error carrying the parser's message" do
      dispatcher.on_protocol_error("unknown protocol operation")

      expect(connection).to have_received(:error_received)
        .with(an_instance_of(Tether::ProtocolError).and(having_attributes(message: "unknown protocol operation")))
    end
  end
end
