# frozen_string_literal: true

RSpec.describe Tether::Protocol::Msg do
  include ClientHelper

  let(:nc) { connect }
  let(:subscription) { nc.subscribe(topic) }

  def receive(payload = nil, **options)
    subscription
    nc.publish(topic, payload, **options)
    subscription.next(timeout: 2)
  end

  describe "#respond" do
    it "publishes the payload to the reply subject" do
      replies = nc.subscribe("#{topic}.reply")
      receive("question", reply_to: "#{topic}.reply").respond("answer")

      expect(replies.next(timeout: 2).data).to eq("answer")
    end

    it "sends an empty reply without a payload" do
      replies = nc.subscribe("#{topic}.reply")
      receive("question", reply_to: "#{topic}.reply").respond

      expect(replies.next(timeout: 2).data).to eq("")
    end

    it "sends the given headers and none of the request's" do
      replies = nc.subscribe("#{topic}.reply")
      receive("question", reply_to: "#{topic}.reply", headers: { "Request-Only" => "1" })
        .respond("answer", headers: { "Reply-Only" => "2" })

      expect(replies.next(timeout: 2).headers.to_h).to eq("Reply-Only" => ["2"])
    end

    it "answers a request" do
      nc.subscribe("#{topic}.service") { |msg| msg.respond(msg.data.upcase) }

      expect(nc.request("#{topic}.service", "shout").data).to eq("SHOUT")
    end

    it "raises when the message has no reply subject" do
      expect { receive("statement").respond("nope") }.to raise_error(Tether::Error, /no reply subject/)
    end

    it "raises when the message is not attached to a connection" do
      detached = described_class.new(subject: "a", data: "b", reply_to: "c")

      expect { detached.respond("nope") }.to raise_error(Tether::Error, /not attached to a connection/)
    end
  end

  describe "#headers" do
    it "returns nil for a message published without headers" do
      expect(receive("plain").headers).to be_nil
    end

    it "returns the headers the publisher sent" do
      headers = receive("body", headers: { "Nats-Msg-Id" => "m-1", "Tags" => %w[a b] }).headers

      expect(headers).to be_a(Tether::Headers)
      expect(headers.to_h).to eq("Nats-Msg-Id" => ["m-1"], "Tags" => %w[a b])
    end

    it "returns the same object each time" do
      msg = receive("body", headers: { "A" => "b" })

      expect(msg.headers).to be(msg.headers)
    end

    it "raises ProtocolError for a header block that is not NATS headers" do
      malformed = described_class.new(subject: "a", data: "", header_bytes: "HTTP/1.1 200 OK\r\n\r\n")

      expect { malformed.headers }.to raise_error(Tether::ProtocolError)
    end
  end

  describe "#status" do
    it "returns nil for an ordinary message" do
      expect(receive("plain").status).to be_nil
    end

    it "returns 503 on the message the server sends when a request reaches nobody" do
      inbox = nc.subscribe("#{topic}.inbox")
      nc.publish("#{topic}.nobody", "anyone?", reply_to: "#{topic}.inbox")

      expect(inbox.next(timeout: 2).status).to eq(503)
    end
  end

  describe "#description" do
    it "returns nil for an ordinary message" do
      expect(receive("plain").description).to be_nil
    end

    it "returns the text after the status code" do
      msg = described_class.new(subject: "a", data: "", header_bytes: "NATS/1.0 408 Request Timeout\r\n\r\n")

      expect(msg.description).to eq("Request Timeout")
    end
  end

  describe "#no_responders?" do
    it "returns true on the message the server sends when a request reaches nobody" do
      inbox = nc.subscribe("#{topic}.inbox")
      nc.publish("#{topic}.nobody", "anyone?", reply_to: "#{topic}.inbox")

      expect(inbox.next(timeout: 2)).to be_no_responders.and be_empty
    end

    it "returns false for an ordinary message" do
      expect(receive("plain")).not_to be_no_responders
    end
  end

  describe "#size" do
    it "returns the bytes of the payload" do
      expect(receive("héllo").size).to eq(6)
    end

    it "includes the header block, which is what the server counts" do
      expect(receive("12345", headers: { "A" => "b" }).size).to eq(5 + "NATS/1.0\r\nA: b\r\n\r\n".bytesize)
    end
  end

  describe "#empty?" do
    it "returns true for a message without a payload" do
      expect(receive).to be_empty
    end

    it "returns false for a message with a payload" do
      expect(receive("x")).not_to be_empty
    end
  end

  describe "#data" do
    it "returns the payload as binary" do
      expect(receive("héllo").data.encoding).to eq(Encoding::BINARY)
    end
  end

  describe "#subject" do
    it "returns the subject published to, not the pattern subscribed to" do
      wildcard = nc.subscribe("#{topic}.*")
      nc.publish("#{topic}.concrete", "x")

      expect(wildcard.next(timeout: 2).subject).to eq("#{topic}.concrete")
    end
  end

  describe "#subscription" do
    it "returns the subscription that received the message" do
      expect(receive("x").subscription).to be(subscription)
    end
  end

  describe "#inspect" do
    it "shows the payload's size rather than its bytes" do
      expect(receive("secret").inspect)
        .to eq(%(#<Tether::Protocol::Msg subject="#{topic}" reply_to=nil bytes=6>))
    end
  end
end
