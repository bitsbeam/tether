# frozen_string_literal: true

RSpec.describe Tether::Connection::Keepalive do
  subject(:keepalive) do
    described_class.new(timer: timer, writer: writer, interval: interval, max_out: 2,
                        on_stale: -> { stale.push(true) })
  end

  let(:timer) { Tether::Connection::Timer.new }
  let(:writer) { instance_double(Tether::Connection::Writer) }
  let(:pings) { Thread::Queue.new }
  let(:stale) { Thread::Queue.new }
  let(:interval) { 0.01 }

  before { allow(writer).to receive(:keepalive_ping) { pings.push(true) } }

  after { timer.stop }

  describe "#start" do
    it "sends a PING every interval" do
      keepalive.start

      expect([pings.pop(timeout: 2), pings.pop(timeout: 2)]).to eq([true, true])
    end

    it "reports the connection stale once max_out PINGs are unanswered" do
      keepalive.start

      expect(stale.pop(timeout: 2)).to be(true)
      expect(pings.size).to eq(2)
    end

    it "forgets unanswered PINGs when started again for a fresh connection" do
      keepalive.start
      2.times { pings.pop(timeout: 2) }
      keepalive.start

      expect(pings.pop(timeout: 2)).to be(true)
    end

    context "when the interval is nil" do
      let(:interval) { nil }

      it "sends no PING" do
        keepalive.start

        expect(pings.pop(timeout: 0.05)).to be_nil
      end
    end
  end

  describe "#pong" do
    it "resets the count, so an answering server is never stale" do
      allow(writer).to receive(:keepalive_ping) do
        pings.push(true)
        keepalive.pong
      end
      keepalive.start
      5.times { pings.pop(timeout: 2) }

      expect(stale).to be_empty
    end
  end

  describe "#stop" do
    it "stops pinging" do
      keepalive.start
      pings.pop(timeout: 2)
      keepalive.stop

      expect(eventually { pings.pop(timeout: 0.03).nil? }).to be(true)
    end
  end
end
