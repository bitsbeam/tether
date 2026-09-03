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

  describe "#with_at_most" do
    it "narrows a deadline" do
      expect(described_class.after(10).with_at_most(2).remaining).to eq(2.0)
    end

    it "never widens a deadline" do
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

  it "is frozen so it cannot drift after being handed down a call stack" do
    expect(described_class.after(1)).to be_frozen
  end
end
