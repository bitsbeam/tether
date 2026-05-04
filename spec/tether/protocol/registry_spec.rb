# frozen_string_literal: true

RSpec.describe Tether::Protocol::Registry do
  subject(:registry) { described_class.new }

  let(:subscription) { instance_double(Tether::Connection::Subscription, sid: "1", close: nil) }

  describe "#add" do
    it "registers a subscription under its sid" do
      registry.add(subscription)

      expect(registry["1"]).to be(subscription)
    end

    it "returns the subscription so a caller can keep it" do
      expect(registry.add(subscription)).to be(subscription)
    end
  end

  describe "#remove" do
    it "stops routing to a subscription" do
      registry.add(subscription)
      registry.remove("1")

      expect(registry["1"]).to be_nil
    end

    it "is quiet about an sid that was never registered" do
      expect(registry.remove("nope")).to be_nil
    end
  end

  describe "#[]" do
    it "returns nil for a message whose subscription is already gone" do
      expect(registry["7"]).to be_nil
    end
  end

  describe "#to_a" do
    it "hands back a snapshot that survives later changes" do
      registry.add(subscription)
      snapshot = registry.to_a
      registry.remove("1")

      expect(snapshot).to eq([subscription])
    end
  end

  describe "#clear" do
    it "closes every subscription and empties itself" do
      expect(subscription).to receive(:close)
      registry.add(subscription)
      registry.clear

      expect(registry.size).to eq(0)
    end
  end

  it "survives threads registering and removing at the same time" do
    threads = 8.times.map do |t|
      Thread.new do
        50.times do |i|
          sid = "#{t}-#{i}"
          registry.add(instance_double(Tether::Connection::Subscription, sid: sid, close: nil))
          registry.remove(sid)
        end
      end
    end
    threads.each(&:join)

    expect(registry.size).to eq(0)
  end
end
