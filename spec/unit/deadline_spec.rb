# frozen_string_literal: true

RSpec.describe Tether::Deadline do
  let(:clock) { 1_000.0 }

  before { allow(described_class).to receive(:now).and_return(clock) }

  describe ".after" do
    it "counts down from the monotonic clock" do
      deadline = described_class.after(5)

      expect(deadline.remaining).to eq(5.0)
      expect(deadline).not_to be_expired
    end

    it "clamps a passed deadline to zero rather than going negative" do
      deadline = described_class.after(5)
      allow(described_class).to receive(:now).and_return(clock + 9)

      expect(deadline.remaining).to eq(0.0)
      expect(deadline).to be_expired
    end

    it "builds an infinite deadline from nil" do
      deadline = described_class.after(nil)

      expect(deadline).to be_infinite
      expect(deadline.remaining).to be_nil
      expect(deadline).not_to be_expired
    end
  end

  describe ".coerce" do
    it "passes a deadline through untouched" do
      deadline = described_class.after(1)

      expect(described_class.coerce(deadline)).to be(deadline)
    end

    it "reads a number of seconds" do
      expect(described_class.coerce(7).remaining).to eq(7.0)
    end

    it "reads nil as infinite" do
      expect(described_class.coerce(nil)).to be_infinite
    end
  end

  describe "#with_at_most" do
    it "narrows a deadline" do
      expect(described_class.after(10).with_at_most(2).remaining).to eq(2.0)
    end

    it "keeps a deadline that is already sooner" do
      expect(described_class.after(1).with_at_most(30).remaining).to eq(1.0)
    end

    it "makes an infinite deadline finite" do
      expect(described_class.infinite.with_at_most(3).remaining).to eq(3.0)
    end

    it "leaves the deadline alone when given nil" do
      deadline = described_class.after(4)

      expect(deadline.with_at_most(nil)).to be(deadline)
    end
  end

  describe "#earliest" do
    it "returns the sooner of two deadlines" do
      expect(described_class.after(9).earliest(described_class.after(2)).remaining).to eq(2.0)
      expect(described_class.after(2).earliest(described_class.after(9)).remaining).to eq(2.0)
    end

    it "prefers any finite deadline over an infinite one" do
      expect(described_class.infinite.earliest(described_class.after(1)).remaining).to eq(1.0)
      expect(described_class.after(1).earliest(described_class.infinite).remaining).to eq(1.0)
    end
  end

  describe "#check!" do
    it "stays quiet while time remains" do
      expect { described_class.after(1).check!("foo") }.not_to raise_error
    end

    it "raises a timeout naming the subject once expired" do
      deadline = described_class.after(1)
      allow(described_class).to receive(:now).and_return(clock + 2)

      expect { deadline.check!("$JS.API.STREAM.INFO.orders") }
        .to raise_error(Tether::TimeoutError, /orders/)
    end
  end

  describe ".infinite" do
    it "builds a deadline that never expires" do
      expect(described_class.infinite).to be_infinite
    end
  end

  describe ".now" do
    it "reads the monotonic clock" do
      allow(described_class).to receive(:now).and_call_original

      expect(described_class.now).to be_within(1).of(Process.clock_gettime(Process::CLOCK_MONOTONIC))
    end
  end

  describe "#initialize" do
    it "freezes the deadline so it cannot drift after being handed down a call stack" do
      expect(described_class.new(clock + 1)).to be_frozen
    end
  end

  describe "#infinite?" do
    it "returns false for a deadline with an instant" do
      expect(described_class.after(1)).not_to be_infinite
    end
  end

  describe "#remaining" do
    it "returns nil for an infinite deadline, which blocking primitives read as forever" do
      expect(described_class.infinite.remaining).to be_nil
    end

    it "returns zero once the instant has passed" do
      expect(described_class.new(clock - 1).remaining).to eq(0.0)
    end
  end

  describe "#expired?" do
    it "returns false while time remains" do
      expect(described_class.after(1)).not_to be_expired
    end

    it "returns true at the instant itself" do
      expect(described_class.new(clock)).to be_expired
    end

    it "returns false for an infinite deadline" do
      expect(described_class.infinite).not_to be_expired
    end
  end

  describe "#inspect" do
    it "shows the time remaining" do
      expect(described_class.after(1.5).inspect).to eq("#<Tether::Deadline remaining=1.500s>")
    end

    it "says when the deadline is infinite" do
      expect(described_class.infinite.inspect).to eq("#<Tether::Deadline infinite>")
    end
  end
end
