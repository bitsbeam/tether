# frozen_string_literal: true

RSpec.describe Tether::Protocol::Dispatcher do
  subject(:dispatcher) { described_class.new(connection) }

  let(:transport) { instance_double(Tether::Connection::Transport, write: 6) }
  let(:connection) { instance_double(Tether::Client, transport: transport) }

  def ping_in_background(dispatcher, seconds = 2)
    started = Thread::Queue.new
    thread = Thread.new do
      started << :queued
      dispatcher.ping(Tether::Deadline.after(seconds))
    end
    started.pop
    thread
  end

  describe "#ping" do
    it "returns once the server answers" do
      thread = ping_in_background(dispatcher)
      sleep 0.05
      dispatcher.on_pong

      expect(thread.value).to be(true)
    end

    it "writes the PING itself, so nothing can answer before a waiter exists" do
      expect(transport).to receive(:write).with(Tether::Protocol::PING, flush: true)
      thread = ping_in_background(dispatcher)
      sleep 0.05
      dispatcher.on_pong
      thread.join
    end

    it "answers the oldest waiter first, since a PONG only proves progress up to its own PING" do
      order = Thread::Queue.new
      first = Thread.new do
        dispatcher.ping(Tether::Deadline.after(2))
        order << :first
      end
      sleep 0.05
      second = Thread.new do
        dispatcher.ping(Tether::Deadline.after(2))
        order << :second
      end
      sleep 0.05

      dispatcher.on_pong
      expect(order.pop(timeout: 2)).to eq(:first)
      expect(order).to be_empty

      dispatcher.on_pong
      expect(order.pop(timeout: 2)).to eq(:second)
      [first, second].each(&:join)
    end

    it "raises a timeout when no answer arrives" do
      expect { dispatcher.ping(Tether::Deadline.after(0.1)) }
        .to raise_error(Tether::TimeoutError, /did not answer/)
    end

    it "stops waiting when the connection closes, and says so" do
      thread = Thread.new { dispatcher.ping(Tether::Deadline.after(5)) }
      sleep 0.1
      dispatcher.close

      expect { thread.value }.to raise_error(Tether::ConnectionClosedError, /closed while waiting/)
    end

    it "refuses once closed" do
      dispatcher.close

      expect { dispatcher.ping(Tether::Deadline.after(1)) }
        .to raise_error(Tether::ConnectionClosedError, /is closed/)
    end

    it "leaves no waiter behind once it settles" do
      thread = ping_in_background(dispatcher)
      dispatcher.on_pong
      thread.join
      begin
        dispatcher.ping(Tether::Deadline.after(0.05))
      rescue Tether::TimeoutError
        nil
      end

      expect(dispatcher.instance_variable_get(:@pongs)).to be_empty
    end

    it "does not block the reader thread answering a waiter that already gave up" do
      Thread.new do
        Thread.current.report_on_exception = false
        dispatcher.ping(Tether::Deadline.after(0.05))
      end
      sleep 0.2

      expect { dispatcher.on_pong }.not_to raise_error
    end
  end

  describe "#on_msg" do
    it "drops a message whose subscription is already gone" do
      expect { dispatcher.on_msg("orders", "9", nil, nil, "body") }.not_to raise_error
    end
  end

  describe "#on_error" do
    it "records the server's error as a typed class" do
      dispatcher.on_error("Authorization Violation")

      expect(dispatcher.errors.last).to be_a(Tether::AuthorizationError)
    end
  end

  describe "#on_info" do
    it "keeps the announced INFO" do
      dispatcher.on_info(%({"server_id":"x","max_payload":1048576}))

      expect(dispatcher.server_info).to include("max_payload" => 1_048_576)
    end
  end
end
