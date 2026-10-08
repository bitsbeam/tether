# frozen_string_literal: true

RSpec.describe Tether::KeyValue::Watcher do
  include KeyValueHelper

  let(:fired) { Thread::Queue.new }

  after { @watchers&.each(&:stop) }

  def iterate(*keys, **options)
    kv.watch(*keys, **options).tap { |watcher| (@watchers ||= []) << watcher }
  end

  def blocked
    kv.watch do |entry|
      entries.push(entry)
      gate.pop
    end
  end

  describe "#ready?" do
    it "is true as soon as a watcher on an empty bucket is created" do
      expect(collecting).to be_ready
    end

    it "is true at once for a filter that matches no key" do
      kv.put("color", "blue")

      expect(collecting("shape")).to be_ready
    end

    it "is true at once for a watcher of updates only" do
      kv.put("color", "blue")

      expect(collecting(updates_only: true)).to be_ready
    end

    it "is false while the block still handles the last initial entry" do
      kv.put("color", "blue")
      watcher = blocked
      entries.pop(timeout: 2)

      expect(watcher).not_to be_ready
      gate.push(:go)
      expect(eventually { watcher.ready? }).to be(true)
    end

    it "turns true once next has returned the last initial entry" do
      kv.put("color", "blue")
      kv.put("shape", "round")
      watcher = iterate
      watcher.next(timeout: 2)

      expect { watcher.next(timeout: 2) }.to change(watcher, :ready?).from(false).to(true)
    end

    it "stays true after the server consumer was replaced" do
      watcher = collecting(on_ready: -> { fired.push(:ready) })
      watcher.wait_until_ready(timeout: 2)
      js.delete_consumer("KV_#{bucket}", server_consumers.first)
      kv.put("color", "blue")
      taken(1, timeout: 15)

      expect(watcher).to be_ready
      expect(fired.size).to eq(1)
    end
  end

  describe "#on_ready" do
    it "calls the block once the initial values were delivered, on the watcher's worker" do
      kv.put("color", "blue")
      watcher = blocked
      watcher.on_ready { fired.push(Thread.current) }
      entries.pop(timeout: 2)
      gate.push(:go)

      expect(fired.pop(timeout: 2)).not_to eq(Thread.current)
      expect(fired).to be_empty
    end

    it "calls the block at once, on the calling thread, when the watcher is ready already" do
      watcher = collecting
      watcher.wait_until_ready(timeout: 2)
      watcher.on_ready { fired.push(Thread.current) }

      expect(fired.pop(timeout: 0)).to eq(Thread.current)
    end

    it "calls every registered block once" do
      kv.put("color", "blue")
      watcher = kv.watch(on_ready: -> { fired.push(:first) }) do |entry|
        entries.push(entry)
        gate.pop
      end
      watcher.on_ready { fired.push(:second) }
      gate.push(:go)
      kv.put("shape", "round")
      gate.push(:go)
      taken(2)

      expect(Array.new(2) { fired.pop(timeout: 2) }).to eq(%i[first second])
      expect(fired).to be_empty
    end

    it "calls the block given at creation for an empty bucket" do
      collecting(on_ready: -> { fired.push(:ready) })

      expect(fired.pop(timeout: 2)).to eq(:ready)
    end

    it "calls the block before next returns the last initial entry of a watcher without a block" do
      kv.put("color", "blue")
      watcher = iterate(on_ready: -> { fired.push(:ready) })

      expect(fired).to be_empty
      watcher.next(timeout: 2)
      expect(fired.pop(timeout: 0)).to eq(:ready)
    end

    it "returns the watcher" do
      watcher = collecting

      expect(watcher.on_ready { nil }).to equal(watcher)
    end

    it "raises ArgumentError without a block" do
      expect { collecting.on_ready }.to raise_error(ArgumentError, /block/)
    end
  end

  describe "#wait_until_ready" do
    it "returns true once the initial values were delivered" do
      3.times { |index| kv.put("k#{index}", "v") }
      watcher = collecting

      expect(watcher.wait_until_ready(timeout: 2)).to be(true)
      expect(entries.size).to eq(3)
    end

    it "returns false when the time runs out first" do
      kv.put("color", "blue")
      watcher = blocked

      expect(watcher.wait_until_ready(timeout: 0.05)).to be(false)
    end

    it "returns false when the watcher is stopped first" do
      kv.put("color", "blue")
      watcher = blocked
      entries.pop(timeout: 2)
      waiter = Thread.new { watcher.wait_until_ready }
      watcher.stop
      gate.push(:go)

      expect(waiter.join(2)&.value).to be(false)
    end

    it "answers at once for a watcher without a block" do
      kv.put("color", "blue")
      watcher = iterate

      expect(watcher.wait_until_ready).to be(false)
      watcher.next(timeout: 2)
      expect(watcher.wait_until_ready).to be(true)
    end
  end

  describe "#next" do
    it "returns the entries in the order they were written" do
      kv.put("color", "blue")
      kv.put("shape", "round")
      watcher = iterate
      kv.put("size", "big")

      expect(summary(Array.new(3) { watcher.next(timeout: 2) }))
        .to eq([["color", "blue", :put], ["shape", "round", :put], ["size", "big", :put]])
    end

    it "raises TimeoutError when no entry arrives in time, and returns the next entry afterwards" do
      watcher = iterate

      expect { watcher.next(timeout: 0.05) }.to raise_error(Tether::TimeoutError)
      kv.put("color", "blue")
      expect(watcher.next(timeout: 2)).to have_attributes(key: "color", revision: 1)
    end

    it "skips tombstones with ignore_deletes" do
      kv.put("color", "blue")
      kv.delete("color")
      watcher = iterate(ignore_deletes: true)
      kv.put("shape", "round")

      expect(watcher.next(timeout: 2)).to have_attributes(key: "shape")
    end

    it "raises StopIteration once the watcher was stopped" do
      watcher = iterate.stop

      expect { watcher.next(timeout: 2) }.to raise_error(StopIteration)
    end

    it "raises ArgumentError on a watcher that delivers to a block" do
      expect { collecting.next(timeout: 1) }.to raise_error(ArgumentError, /block/)
    end
  end

  describe "#each" do
    it "yields entries until the watcher is stopped" do
      kv.put("color", "blue")
      kv.put("shape", "round")
      watcher = iterate
      yielded = []
      watcher.each do |entry|
        yielded << entry.key
        watcher.stop if yielded.size == 2
      end

      expect(yielded).to eq(%w[color shape])
    end

    it "returns an enumerator without a block" do
      kv.put("color", "blue")
      kv.put("shape", "round")

      expect(iterate.each.first(2).map(&:key)).to eq(%w[color shape])
    end
  end

  describe "#stop" do
    it "ends delivery" do
      watcher = collecting
      watcher.wait_until_ready(timeout: 2)
      watcher.stop.wait(timeout: 2)
      kv.put("color", "blue")

      expect(entries.pop(timeout: 0.1)).to be_nil
    end

    it "removes the server consumer" do
      watcher = collecting
      eventually { server_consumers.size == 1 }
      watcher.stop

      expect(eventually { server_consumers.empty? }).to be(true)
    end

    it "may be called from the watcher's own block" do
      kv.put("color", "blue")
      kv.put("shape", "round")
      watcher = kv.watch do |entry|
        entries.push(entry)
        gate.pop.stop
      end
      gate.push(watcher)

      expect(watcher.wait(timeout: 2)).to be_stopped
      expect(entries.size).to eq(1)
    end

    it "does nothing the second time" do
      watcher = collecting.stop

      expect(watcher.stop).to equal(watcher)
    end
  end

  describe "#stopped?" do
    it "is false while the watcher runs" do
      expect(collecting).not_to be_stopped
    end

    it "is true once the watcher was stopped" do
      expect(collecting.stop).to be_stopped
    end

    it "turns true when the bucket is deleted under the watcher" do
      watcher = collecting(on_error: ->(error) { fired.push(error) })
      watcher.wait_until_ready(timeout: 2)
      js.delete_key_value(bucket)

      expect(eventually(timeout: 20) { watcher.stopped? }).to be(true)
      expect(watcher.error).to be_a(Tether::Error)
    end
  end

  describe "#wait" do
    it "returns once the watcher has stopped and its block has returned" do
      kv.put("color", "blue")
      watcher = blocked
      entries.pop(timeout: 2)
      watcher.stop
      gate.push(:go)

      expect(watcher.wait(timeout: 2)).to equal(watcher)
    end

    it "raises TimeoutError while the watcher runs" do
      expect { collecting.wait(timeout: 0.05) }.to raise_error(Tether::TimeoutError)
    end
  end

  describe "#on_error" do
    it "receives what a handler given at creation would" do
      watcher = collecting
      watcher.on_error { |error| fired.push(error) }
      watcher.wait_until_ready(timeout: 2)
      js.delete_key_value(bucket)

      expect(fired.pop(timeout: 20)).to be_a(Tether::Error)
    end
  end

  describe "#error" do
    it "is nil for a watcher that was stopped" do
      expect(collecting.stop.error).to be_nil
    end
  end

  describe "#revision" do
    it "is zero before the first entry" do
      expect(iterate.revision).to eq(0)
    end

    it "returns the revision of the last entry received" do
      kv.put("color", "blue")
      kv.put("shape", "round")
      watcher = iterate
      2.times { watcher.next(timeout: 2) }

      expect(watcher.revision).to eq(2)
    end
  end

  describe "#filters" do
    it "returns the whole bucket for a watcher without keys" do
      expect(collecting.filters).to eq([">"])
    end

    it "returns the keys the watcher was given" do
      expect(collecting("config.>", "region").filters).to eq(["config.>", "region"])
    end
  end

  describe "#key_value" do
    it "returns the bucket being watched" do
      expect(collecting.key_value).to equal(kv)
    end
  end

  describe "#inspect" do
    it "names the bucket, the filters and the state" do
      expect(collecting("region").stop.inspect).to eq(
        %(#<Tether::KeyValue::Watcher bucket="#{bucket}" filters=["region"] ready=true stopped=true>)
      )
    end
  end
end
