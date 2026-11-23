# frozen_string_literal: true

RSpec.describe Tether::Connection::WorkerPool do
  subject(:pool) { described_class.new(max_threads: 2, idle_timeout: 0.05) }

  let(:gate) { Thread::Queue.new }

  after do
    gate.close
    pool.close
  end

  def job(&block)
    Struct.new(:block) { def run = block.call }.new(block)
  end

  describe "#schedule" do
    it "runs the job on a worker thread" do
      names = Thread::Queue.new
      pool.schedule(job { names.push(Thread.current.name) && false })

      expect(names.pop(timeout: 2)).to eq("tether:worker")
    end

    it "runs the job again while it returns true" do
      runs = Thread::Queue.new
      count = 0
      pool.schedule(job { runs.push(count += 1) && count < 3 })

      expect(take(runs, 3)).to eq([1, 2, 3])
      expect(runs.pop(timeout: 0.05)).to be_nil
    end

    it "starts no more than max_threads workers" do
      entered = Thread::Queue.new
      5.times { pool.schedule(job { entered.push(true) && gate.pop && false }) }
      take(entered, 2)

      expect(pool.threads).to eq(2)
      expect(entered.pop(timeout: 0.05)).to be_nil
    end

    it "returns false once the pool is closed" do
      pool.close

      expect(pool.schedule(job { false })).to be(false)
    end
  end

  describe "#threads" do
    it "drops to zero once the workers have sat idle" do
      done = Thread::Queue.new
      pool.schedule(job { done.push(true) && false })
      done.pop(timeout: 2)

      expect(eventually { pool.threads.zero? }).to be(true)
    end
  end

  describe "#close" do
    it "runs the jobs already queued" do
      runs = Thread::Queue.new
      entered = Thread::Queue.new
      2.times { pool.schedule(job { entered.push(true) && gate.pop && false }) }
      take(entered, 2)
      pool.schedule(job { runs.push(:queued) && false })
      pool.close
      2.times { gate.push(true) }

      expect(runs.pop(timeout: 2)).to eq(:queued)
    end

    it "ends the workers" do
      before = tether_threads
      pool.schedule(job { false })
      pool.close

      expect(eventually { (tether_threads - before).empty? }).to be(true)
    end
  end

  def take(queue, count)
    Array.new(count) { queue.pop(timeout: 2) }
  end
end
