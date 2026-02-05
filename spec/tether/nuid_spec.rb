# frozen_string_literal: true

RSpec.describe Tether::NUID do
  describe ".next" do
    it "generates 22 character base62 identifiers" do
      expect(described_class.next).to match(/\A[0-9A-Za-z]{22}\z/)
    end

    it "keeps the prefix stable within a thread and advances the sequence" do
      ids = Array.new(1_000) { described_class.next }

      expect(ids.map { _1[0, described_class::PREFIX_LENGTH] }.uniq.size).to eq(1)
      expect(ids.map { _1[described_class::PREFIX_LENGTH..] }).to eq(ids.map { _1[12..] }.sort)
    end

    it "does not repeat itself" do
      ids = Array.new(50_000) { described_class.next }

      expect(ids.uniq.size).to eq(50_000)
    end

    it "gives each thread its own generator, so nothing is shared to race over" do
      ids = 8.times.map { Thread.new { Array.new(20_000) { described_class.next } } }.flat_map(&:value)

      expect(ids.uniq.size).to eq(160_000)
      expect(ids.map { _1[0, 12] }.uniq.size).to eq(8)
    end

    it "shares a thread's generator across its fibers, which never run at once" do
      thread_prefix = described_class.next[0, 12]
      fiber_prefix = Fiber.new { described_class.next[0, 12] }.resume

      expect(fiber_prefix).to eq(thread_prefix)
    end
  end

  describe "the generator itself" do
    subject(:nuid) { described_class.send(:new) }

    it "is not constructible from outside, since a shared one would race" do
      expect { described_class.new }.to raise_error(NoMethodError)
    end

    it "uses a different prefix per generator" do
      prefixes = Array.new(50) { described_class.send(:new).next[0, 12] }

      expect(prefixes.uniq.size).to eq(50)
    end

    it "rolls the prefix over when the sequence overflows" do
      before = nuid.next[0, 12]
      nuid.instance_variable_set(:@sequence, described_class::MAX_SEQUENCE - 1)

      expect(nuid.next[0, 12]).not_to eq(before)
    end
  end
end
