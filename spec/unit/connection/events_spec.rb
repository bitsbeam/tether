# frozen_string_literal: true

require "logger"
require "stringio"

RSpec.describe Tether::Connection::Events do
  subject(:events) { described_class.new(logger: Logger.new(log)) }

  let(:log) { StringIO.new }
  let(:seen) { Thread::Queue.new }

  after { events.shutdown }

  describe "#on" do
    it "raises for an event that does not exist" do
      expect { events.on(:explode) { nil } }.to raise_error(ArgumentError, "unknown event: :explode")
    end

    it "raises without a block" do
      expect { events.on(:error) }.to raise_error(ArgumentError, "a block is required")
    end

    it "calls the handler on the event thread" do
      events.on(:connect) { seen.push(Thread.current.name) }
      events.emit(:connect, :server)

      expect(seen.pop(timeout: 2)).to eq("tether:events")
    end

    it "calls every handler registered for the event" do
      events.on(:close) { seen.push(:first) }
      events.on(:close) { seen.push(:second) }
      events.emit(:close)

      expect([seen.pop(timeout: 2), seen.pop(timeout: 2)]).to eq(%i[first second])
    end

    it "returns a registration whose cancel removes the handler" do
      events.on(:status) { |status| seen.push([:canceled, status]) }.cancel
      events.on(:status) { |status| seen.push([:kept, status]) }
      events.emit(:status, :connected)

      expect(seen.pop(timeout: 2)).to eq(%i[kept connected])
      expect(seen.pop(timeout: 0.05)).to be_nil
    end
  end

  describe "#emit" do
    it "delivers events in the order they were emitted" do
      events.on(:status) { |status| seen.push(status) }
      %i[connecting connected closed].each { |status| events.emit(:status, status) }

      expect(Array.new(3) { seen.pop(timeout: 2) }).to eq(%i[connecting connected closed])
    end

    it "keeps delivering after a handler raises, and logs the exception" do
      events.on(:connect) { raise "handler bug" }
      events.on(:connect) { seen.push(:delivered) }
      events.emit(:connect)

      expect(seen.pop(timeout: 2)).to eq(:delivered)
      expect(log.string).to include("RuntimeError: handler bug")
    end

    it "lets a handler emit from inside itself without deadlocking" do
      events.on(:connect) { events.emit(:close) }
      events.on(:close) { seen.push(:nested) }
      events.emit(:connect)

      expect(seen.pop(timeout: 2)).to eq(:nested)
    end

    it "ignores an event emitted after shutdown" do
      events.on(:close) { seen.push(:late) }
      events.shutdown

      expect { events.emit(:close) }.not_to raise_error
      expect(seen.pop(timeout: 0.05)).to be_nil
    end
  end

  describe "#error" do
    it "delivers the error to the error handlers" do
      events.on(:error) { |error| seen.push(error) }
      events.error(Tether::SlowConsumerError.new("slow"))

      expect(seen.pop(timeout: 2)).to be_a(Tether::SlowConsumerError)
    end

    it "logs the error when nobody handles it" do
      events.error(Tether::SlowConsumerError.new("slow"))

      expect(log.string).to include("Tether::SlowConsumerError: slow")
    end
  end

  describe "#last_error" do
    it "returns nil before any error" do
      expect(events.last_error).to be_nil
    end

    it "returns the most recent error, handled or not" do
      events.error(Tether::Error.new("first"))
      events.error(Tether::Error.new("second"))

      expect(events.last_error.message).to eq("second")
    end
  end

  describe "#listen" do
    it "registers a listener that notify calls with the event and its detail" do
      events.listen { |event, detail| seen.push([event, detail]) }
      events.notify(:disconnect, :why)

      expect(seen.pop(timeout: 2)).to eq(%i[disconnect why])
    end

    it "returns a registration whose cancel removes the listener" do
      events.listen { |event, _| seen.push(event) }.cancel
      events.notify(:close)

      expect(seen).to be_empty
    end
  end

  describe "#notify" do
    it "calls listeners on the calling thread" do
      events.listen { seen.push(Thread.current) }
      events.notify(:reconnect)

      expect(seen.pop(timeout: 2)).to be(Thread.current)
    end

    it "keeps notifying after a listener raises" do
      events.listen { raise "listener bug" }
      events.listen { |event, _| seen.push(event) }
      events.notify(:reconnect)

      expect(seen.pop(timeout: 2)).to eq(:reconnect)
    end
  end

  describe "#shutdown" do
    it "delivers everything queued before the thread ends" do
      before = tether_threads
      gate = Thread::Queue.new
      events.on(:status) do |status|
        gate.pop(timeout: 2)
        seen.push(status)
      end
      events.emit(:status, :draining)
      events.emit(:status, :closed)
      gate.push(true).push(true)
      events.shutdown

      expect(seen.size).to eq(2)
      expect(tether_threads - before).to be_empty
    end

    it "returns at the deadline when a handler is stuck" do
      gate = Thread::Queue.new
      events.on(:close) { gate.pop(timeout: 5) }
      events.emit(:close)
      started = Tether::Deadline.now
      events.shutdown(Tether::Deadline.after(0.05))

      expect(Tether::Deadline.now - started).to be < 2
      gate.push(true)
    end
  end

  describe "#reset" do
    it "keeps the handlers and delivers to them on a fresh thread" do
      events.on(:connect) { seen.push(:after_reset) }
      events.reset
      events.emit(:connect)

      expect(seen.pop(timeout: 2)).to eq(:after_reset)
    end
  end
end
