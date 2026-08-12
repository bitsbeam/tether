# frozen_string_literal: true

RSpec.describe Tether::Connection::Timer do
  subject(:timer) { described_class.new(on_error: ->(error) { errors.push(error) }) }

  let(:errors) { Thread::Queue.new }
  let(:fired) { Thread::Queue.new }

  after { timer.stop }

  describe "#after" do
    it "runs the block once, on the timer thread" do
      timer.after(0.01) { fired.push(Thread.current.name) }

      expect(fired.pop(timeout: 2)).to eq("tether:timer")
      expect(fired.pop(timeout: 0.05)).to be_nil
    end

    it "runs entries in the order they are due, not the order they were added" do
      timer.after(0.06) { fired.push(:late) }
      timer.after(0.01) { fired.push(:early) }

      expect([fired.pop(timeout: 2), fired.pop(timeout: 2)]).to eq(%i[early late])
    end

    it "reports an exception from a callback and keeps running" do
      timer.after(0.01) { raise "boom" }
      timer.after(0.02) { fired.push(:still_running) }

      expect(errors.pop(timeout: 2).message).to eq("boom")
      expect(fired.pop(timeout: 2)).to eq(:still_running)
    end

    it "returns an entry whose cancel prevents the call" do
      timer.after(0.03) { fired.push(:canceled) }.cancel
      timer.after(0.05) { fired.push(:kept) }

      expect(fired.pop(timeout: 2)).to eq(:kept)
    end

    it "returns an entry whose reset postpones the call" do
      entry = timer.after(0.05) { fired.push(Tether::Deadline.now) }
      started = Tether::Deadline.now
      entry.reset(0.15)

      expect(fired.pop(timeout: 2) - started).to be >= 0.15
    end
  end

  describe "#every" do
    it "runs the block repeatedly until canceled" do
      entry = timer.every(0.01) { fired.push(:tick) }
      3.times { fired.pop(timeout: 2) }
      entry.cancel

      expect(entry).to be_canceled
      expect(eventually { fired.pop(timeout: 0.03).nil? }).to be(true)
    end

    it "lets the callback cancel its own entry" do
      entry = timer.every(0.01) do
        fired.push(:once)
        entry.cancel
      end

      expect(fired.pop(timeout: 2)).to eq(:once)
      expect(fired.pop(timeout: 0.05)).to be_nil
    end
  end

  describe "#remove" do
    it "drops an entry before it fires" do
      entry = timer.after(0.03) { fired.push(:removed) }
      timer.remove(entry)

      expect(fired.pop(timeout: 0.08)).to be_nil
    end
  end

  describe "#reschedule" do
    it "moves an entry to a new delay from now" do
      entry = timer.after(30) { fired.push(:moved) }
      timer.reschedule(entry, 0.01)

      expect(fired.pop(timeout: 2)).to eq(:moved)
    end

    it "revives a one-shot entry that already fired" do
      entry = timer.after(0.01) { fired.push(:fired) }
      fired.pop(timeout: 2)
      timer.reschedule(entry, 0.01)

      expect(fired.pop(timeout: 2)).to eq(:fired)
    end

    it "leaves a canceled entry canceled" do
      entry = timer.after(30) { fired.push(:never) }
      entry.cancel
      timer.reschedule(entry, 0.01)

      expect(fired.pop(timeout: 0.05)).to be_nil
    end
  end

  describe "#stop" do
    it "ends the timer thread" do
      before = tether_threads
      timer.after(30) { fired.push(:never) }
      timer.stop

      expect(tether_threads - before).to be_empty
    end

    it "drops every entry and refuses new ones" do
      timer.after(0.02) { fired.push(:dropped) }
      timer.stop
      timer.after(0.01) { fired.push(:refused) }

      expect(fired.pop(timeout: 0.08)).to be_nil
    end
  end
end
