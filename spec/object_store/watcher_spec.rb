# frozen_string_literal: true

RSpec.describe Tether::ObjectStore::Watcher do
  include ObjectStoreHelper

  let(:proxied) { connect(proxy.uri, reconnect: ClientHelper::FAST_RECONNECT) }

  def seed(*names)
    names.each { |name| store.put(name, name) }
  end

  describe "#on_ready" do
    it "calls the block once the initial entries were delivered" do
      seed("a", "b")
      watcher = collecting
      watcher.on_ready { entries.push(:ready) }
      watcher.wait_until_ready(timeout: 5)

      expect(taken(3).last).to eq(:ready)
    end

    it "calls the block at once on a watcher that is ready" do
      watcher = collecting
      watcher.wait_until_ready(timeout: 5)
      called = false

      watcher.on_ready { called = true }

      expect(called).to be(true)
    end

    it "calls a block given when the watch started on an empty bucket" do
      store.watch(on_ready: -> { entries.push(:ready) }) { |info| entries.push(info) }

      expect(taken(1)).to eq([:ready])
    end

    it "calls the block once, however many updates follow" do
      seed("a")
      watcher = store.watch(on_ready: -> { entries.push(:ready) }) { |info| entries.push(info.name) }
      watcher.wait_until_ready(timeout: 5)
      seed("b", "c")

      expect(taken(4)).to eq(["a", :ready, "b", "c"])
    end

    it "returns the watcher" do
      watcher = collecting

      expect(watcher.on_ready { nil }).to be(watcher)
    end
  end

  describe "#on_error" do
    it "returns the watcher" do
      watcher = collecting

      expect(watcher.on_error { |error| error }).to be(watcher)
    end

    it "receives the loss of the watcher's consumer" do
      watcher = collecting
      watcher.on_error { |error| entries.push(error) }
      watcher.wait_until_ready(timeout: 5)
      server_consumers.each { |name| js.delete_consumer("OBJ_#{bucket}", name) }

      expect(entries.pop(timeout: 15)).to be_a(Tether::JetStream::ConsumerDeletedError)
    end

    it "keeps delivering after the watcher's consumer was lost" do
      seed("a")
      lost = Thread::Queue.new
      watcher = collecting
      watcher.on_error { |error| lost.push(error) }
      watcher.wait_until_ready(timeout: 5)
      server_consumers.each { |name| js.delete_consumer("OBJ_#{bucket}", name) }
      lost.pop(timeout: 15)
      seed("b")

      expect(names_of(taken(2, timeout: 10))).to eq([["a", false], ["b", false]])
    end
  end

  describe "#ready?" do
    it "returns true at once on an empty bucket" do
      expect(collecting.ready?).to be(true)
    end

    it "returns true at once for a watcher of updates only" do
      seed("a")

      expect(collecting(updates_only: true).ready?).to be(true)
    end

    it "returns true once the initial entries were delivered" do
      seed("a", "b")
      watcher = collecting

      eventually { watcher.ready? }
      expect(entries.size).to eq(2)
    end

    it "returns true once the only entries are tombstones that are left out" do
      seed("a")
      store.delete("a")
      watcher = collecting(ignore_deletes: true)

      eventually { watcher.ready? }
      expect(entries).to be_empty
    end

    it "returns false while a handler still works on the initial entries" do
      seed("a")
      watcher = store.watch { |_info| gate.pop }

      expect(watcher.ready?).to be(false)
    end
  end

  describe "#wait_until_ready" do
    it "returns the watcher once it is ready" do
      seed("a")
      watcher = collecting

      expect(watcher.wait_until_ready(timeout: 5)).to be(watcher)
    end

    it "raises TimeoutError when the watcher is not ready in time" do
      seed("a")
      watcher = store.watch { |_info| gate.pop }

      expect { watcher.wait_until_ready(timeout: 0.05) }.to raise_error(Tether::TimeoutError, /not ready/)
    end
  end

  describe "#next" do
    it "returns the entries in the order they were stored" do
      seed("a", "b")
      watcher = store.watch

      expect([watcher.next(timeout: 5).name, watcher.next(timeout: 5).name]).to eq(%w[a b])
    end

    it "returns entries with the metadata of the object" do
      store.put("a", "hello world", chunk_size: 4, description: "a thing")

      expect(store.watch.next(timeout: 5)).to have_attributes(size: 11, chunks: 3, description: "a thing")
    end

    it "sets the modification time from the stored message" do
      seed("a")

      expect(store.watch.next(timeout: 5).modified_at).to be_within(60).of(backing_stream.info.state.last_time)
    end

    it "makes the watcher ready with the last initial entry" do
      seed("a", "b")
      watcher = store.watch
      watcher.next(timeout: 5)
      before = watcher.ready?
      watcher.next(timeout: 5)

      expect([before, watcher.ready?]).to eq([false, true])
    end

    it "returns updates stored after the watch started" do
      watcher = store.watch
      seed("a")

      expect(watcher.next(timeout: 5).name).to eq("a")
    end

    it "skips tombstones with ignore_deletes" do
      seed("a")
      watcher = store.watch(ignore_deletes: true, updates_only: true)
      store.delete("a")
      seed("b")

      expect(watcher.next(timeout: 5).name).to eq("b")
    end

    it "skips metadata that cannot be decoded" do
      watcher = store.watch
      js.publish(meta_subject("broken"), "not json")
      seed("a")

      expect(watcher.next(timeout: 5).name).to eq("a")
    end

    it "raises TimeoutError when no entry arrives in time" do
      expect { store.watch.next(timeout: 0.05) }.to raise_error(Tether::TimeoutError)
    end

    it "raises StopIteration after the watcher was stopped" do
      watcher = store.watch
      watcher.stop

      expect { watcher.next(timeout: 1) }.to raise_error(StopIteration)
    end

    it "raises ArgumentError on a watcher that delivers to a block" do
      expect { collecting.next(timeout: 1) }.to raise_error(ArgumentError, /block/)
    end

    it "returns every entry once when the connection drops while watching" do
      seed("a")
      watcher = proxied.jetstream.object_store(bucket).watch
      watcher.next(timeout: 5)
      proxy.drop
      seed("b", "c")

      expect([watcher.next(timeout: 10).name, watcher.next(timeout: 10).name]).to eq(%w[b c])
    end
  end

  describe "#each" do
    context "when a block is given" do
      it "yields entries until the watcher is stopped" do
        seed("a", "b")
        watcher = store.watch
        seen = []

        watcher.each do |info|
          seen << info.name
          watcher.stop if watcher.ready?
        end

        expect(seen).to eq(%w[a b])
      end

      it "returns the watcher" do
        watcher = store.watch
        watcher.stop

        expect(watcher.each { |info| info }).to be(watcher)
      end
    end

    context "when no block is given" do
      it "returns an enumerator over the entries" do
        seed("a", "b")

        expect(store.watch.each.first(2).map(&:name)).to eq(%w[a b])
      end
    end
  end

  describe "#stop" do
    it "removes the consumer" do
      watcher = collecting
      watcher.wait_until_ready(timeout: 5)
      watcher.stop

      eventually { server_consumers.empty? }
    end

    it "returns the watcher" do
      watcher = collecting

      expect(watcher.stop).to be(watcher)
    end

    it "stops a watcher from inside its own block" do
      seed("a")
      watcher = nil
      watcher = store.watch do |info|
        entries.push(info.name)
        watcher.stop
      end
      seed("b")

      expect([entries.pop(timeout: 5), entries.pop(timeout: 0.2)]).to eq(["a", nil])
    end
  end

  describe "#stopped?" do
    it "returns false while watching" do
      expect(collecting.stopped?).to be(false)
    end

    it "returns true after the watcher was stopped" do
      expect(collecting.stop.stopped?).to be(true)
    end
  end

  describe "#error" do
    it "returns nil for a watcher that was stopped" do
      expect(collecting.stop.error).to be_nil
    end

    it "returns the error that ended the watcher when the bucket was deleted" do
      watcher = collecting
      watcher.wait_until_ready(timeout: 5)
      js.delete_object_store(bucket)

      eventually(timeout: 20) { watcher.stopped? }
      expect(watcher.error).to be_a(Tether::Error)
    end
  end

  describe "#inspect" do
    it "shows the bucket and state" do
      watcher = collecting
      watcher.wait_until_ready(timeout: 5)

      expect(watcher.inspect)
        .to eq(%(#<Tether::ObjectStore::Watcher bucket="#{bucket}" ready=true stopped=false>))
    end
  end
end
