# frozen_string_literal: true

RSpec.describe Tether::NUID do
  subject(:nuid) { described_class.new }

  it "generates 22 character base62 identifiers" do
    expect(nuid.next).to match(/\A[0-9A-Za-z]{22}\z/)
  end

  it "keeps the prefix stable and advances the sequence" do
    ids = Array.new(1_000) { nuid.next }

    expect(ids.map { _1[0, described_class::PREFIX_LENGTH] }.uniq.size).to eq(1)
    expect(ids.map { _1[described_class::PREFIX_LENGTH..] }).to eq(ids.map { _1[12..] }.sort)
  end

  it "does not repeat itself" do
    ids = Array.new(50_000) { nuid.next }

    expect(ids.uniq.size).to eq(50_000)
  end

  it "uses a different prefix per instance" do
    prefixes = Array.new(50) { described_class.new.next[0, 12] }

    expect(prefixes.uniq.size).to eq(50)
  end

  it "rolls the prefix over when the sequence overflows" do
    before = nuid.next[0, 12]
    nuid.instance_variable_set(:@sequence, described_class::MAX_SEQUENCE - 1)

    expect(nuid.next[0, 12]).not_to eq(before)
  end

  describe ".next" do
    it "serves a process wide generator" do
      ids = 4.times.map { Thread.new { Array.new(500) { described_class.next } } }.flat_map(&:value)

      expect(ids.uniq.size).to eq(2_000)
    end
  end
end
