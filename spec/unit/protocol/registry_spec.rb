# frozen_string_literal: true

RSpec.describe Tether::Protocol::Registry do
  subject(:registry) { described_class.new }

  let(:entry) { entry_double("1") }

  def entry_double(sid, frames = "")
    instance_double(Tether::Connection::Subscription, sid: sid, close: nil, abandon: nil, replay_frames: frames)
  end

  describe "#next_sid" do
    it "returns a different string each time" do
      expect([registry.next_sid, registry.next_sid]).to eq(%w[1 2])
    end

    it "hands out no duplicates when threads ask at once" do
      sids = 8.times.map { Thread.new { Array.new(200) { registry.next_sid } } }.flat_map(&:value)

      expect(sids.uniq.size).to eq(1600)
    end
  end

  describe "#add" do
    it "registers an entry under its sid" do
      registry.add(entry)

      expect(registry["1"]).to be(entry)
    end

    it "returns the entry so a caller can keep it" do
      expect(registry.add(entry)).to be(entry)
    end

    it "survives threads registering and removing at the same time" do
      threads = 8.times.map do |thread|
        Thread.new do
          50.times do |index|
            registry.add(entry_double("#{thread}-#{index}"))
            registry.remove("#{thread}-#{index}")
          end
        end
      end
      threads.each(&:join)

      expect(registry.size).to eq(0)
    end
  end

  describe "#remove" do
    it "stops routing to the entry and returns it" do
      registry.add(entry)

      expect(registry.remove("1")).to be(entry)
      expect(registry["1"]).to be_nil
    end

    it "returns nil for a sid that was never registered" do
      expect(registry.remove("404")).to be_nil
    end
  end

  describe "#[]" do
    it "returns nil for a message whose entry is already gone" do
      expect(registry["404"]).to be_nil
    end
  end

  describe "#to_a" do
    it "returns a snapshot that survives later changes" do
      registry.add(entry)
      snapshot = registry.to_a
      registry.remove("1")

      expect(snapshot).to eq([entry])
    end
  end

  describe "#size" do
    it "counts the registered entries" do
      registry.add(entry)
      registry.add(entry_double("2"))

      expect(registry.size).to eq(2)
    end
  end

  describe "#replay_frames" do
    it "joins the frames of every entry in registration order" do
      registry.add(entry_double("1", "SUB a 1\r\n"))
      registry.add(entry_double("2", "SUB b 2\r\nUNSUB 2 3\r\n"))

      expect(registry.replay_frames).to eq("SUB a 1\r\nSUB b 2\r\nUNSUB 2 3\r\n")
    end
  end

  describe "#close" do
    it "closes every entry with the error and empties itself" do
      error = Tether::DisconnectedError.new("gone")
      registry.add(entry)
      registry.close(error)

      expect(entry).to have_received(:close).with(error)
      expect(registry.size).to eq(0)
    end
  end

  describe "#abandon" do
    it "abandons every entry without closing it and empties itself" do
      registry.add(entry)
      registry.abandon

      expect(entry).to have_received(:abandon)
      expect(entry).not_to have_received(:close)
      expect(registry.size).to eq(0)
    end
  end
end
