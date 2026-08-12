# frozen_string_literal: true

RSpec.describe Tether::Counters do
  subject(:counters) { described_class.new }

  describe "#initialize" do
    it "starts every counter at zero" do
      expect(counters.snapshot.to_h.values).to all(eq(0))
    end
  end

  describe "#snapshot" do
    it "returns a frozen copy that later traffic does not change" do
      counters.messages_out += 2
      snapshot = counters.snapshot
      counters.messages_out += 1

      expect(snapshot).to be_frozen
      expect(snapshot.messages_out).to eq(2)
    end

    it "carries every counter" do
      counters.messages_in = 1
      counters.bytes_in = 2
      counters.bytes_out = 3
      counters.reconnects = 4

      expect(counters.snapshot)
        .to eq(Tether::Stats.new(messages_in: 1, messages_out: 0, bytes_in: 2, bytes_out: 3, reconnects: 4))
    end
  end
end
