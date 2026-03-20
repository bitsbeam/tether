# frozen_string_literal: true

RSpec.describe Tether::Connection::NUID do
  it "is a module with no instances to share or mis-share" do
    expect(described_class).to be_an_instance_of(Module)
    expect(described_class).not_to respond_to(:new)
  end

  describe ".next" do
    it "generates 22 character base62 identifiers" do
      expect(described_class.next).to match(/\A[0-9A-Za-z]{22}\z/)
    end

    it "keeps one prefix for the process and advances the sequence" do
      ids = Array.new(1_000) { described_class.next }

      expect(ids.map { _1[0, described_class::PREFIX_LENGTH] }.uniq.size).to eq(1)
      expect(ids.map { _1[described_class::PREFIX_LENGTH..] }).to eq(ids.map { _1[12..] }.sort)
    end

    it "does not repeat itself" do
      ids = Array.new(50_000) { described_class.next }

      expect(ids.uniq.size).to eq(50_000)
    end

    it "hands out no duplicates when threads generate at once" do
      ids = 8.times.map { Thread.new { Array.new(20_000) { described_class.next } } }.flat_map(&:value)

      expect(ids.uniq.size).to eq(160_000)
    end

    it "rolls the prefix over when the sequence wraps" do
      before = described_class.next[0, 12]
      described_class.instance_variable_set(:@sequence, described_class::MAX_SEQUENCE - 1)

      expect(described_class.next[0, 12]).not_to eq(before)
    end

    it "reseeds after a fork, so a child does not reissue the parent's identifiers" do
      skip "fork is unavailable" unless Process.respond_to?(:fork)

      reader, writer = IO.pipe
      pid = fork do
        reader.close
        writer.write(described_class.next)
        writer.close
        exit!
      end
      writer.close
      child = reader.read
      reader.close
      Process.wait(pid)

      expect(child[0, 12]).not_to eq(described_class.next[0, 12])
    end
  end
end
