# frozen_string_literal: true

RSpec.describe Tether::Options do
  describe "#initialize" do
    it "applies the defaults" do
      options = described_class.new

      expect(options.to_h).to eq(described_class::DEFAULTS)
    end

    it "raises naming an unknown option" do
      expect { described_class.new(pingg_interval: 1) }.to raise_error(ArgumentError, "unknown option: :pingg_interval")
    end

    it "raises naming an unknown reconnect option" do
      expect { described_class.new(reconnect: { waits: 1 }) }
        .to raise_error(ArgumentError, "unknown reconnect option: :waits")
    end

    it "raises naming an unknown tls option" do
      expect { described_class.new(tls: { ca: "x" }) }.to raise_error(ArgumentError, "unknown tls option: :ca")
    end

    it "raises naming an unknown websocket option" do
      expect { described_class.new(websocket: { deflate: true }) }
        .to raise_error(ArgumentError, "unknown websocket option: :deflate")
    end

    it "raises for a reconnect value that is neither a boolean nor a hash" do
      expect { described_class.new(reconnect: 3) }
        .to raise_error(ArgumentError, /reconnect: must be true, false, or a Hash/)
    end

    it "raises for a connect_timeout that is not positive" do
      expect { described_class.new(connect_timeout: 0) }
        .to raise_error(ArgumentError, /connect_timeout: must be a positive number/)
    end

    it "raises for a max_pending that is not a number" do
      expect { described_class.new(max_pending: "many") }
        .to raise_error(ArgumentError, /max_pending: must be a positive number/)
    end

    it "raises for a worker_threads that is not positive" do
      expect { described_class.new(worker_threads: 0) }
        .to raise_error(ArgumentError, /worker_threads: must be a positive number/)
    end

    it "raises for two authentication mechanisms at once" do
      expect { described_class.new(token: "t", user: "u") }
        .to raise_error(ArgumentError, "conflicting authentication options: :token, :user")
    end

    it "accepts a jwt together with the seed that signs for it" do
      expect(described_class.new(jwt: "j", nkey_seed: "s").jwt).to eq("j")
    end

    it "raises for an empty inbox_prefix" do
      expect { described_class.new(inbox_prefix: "") }.to raise_error(ArgumentError, /inbox_prefix/)
    end

    it "raises for an inbox_prefix with a wildcard" do
      expect { described_class.new(inbox_prefix: "_INBOX.*") }.to raise_error(ArgumentError, /not a literal subject/)
    end

    it "raises for an inbox_prefix ending in a dot" do
      expect { described_class.new(inbox_prefix: "_INBOX.") }.to raise_error(ArgumentError, /not a literal subject/)
    end

    it "freezes itself" do
      expect(described_class.new).to be_frozen
    end
  end

  describe "#reconnect" do
    it "returns the defaults for true" do
      expect(described_class.new(reconnect: true).reconnect).to eq(described_class::RECONNECT_DEFAULTS)
    end

    it "returns nil for false" do
      expect(described_class.new(reconnect: false).reconnect).to be_nil
    end

    it "merges a hash over the defaults" do
      settings = described_class.new(reconnect: { wait: 5 }).reconnect

      expect(settings).to include(wait: 5, max_attempts: 60)
      expect(settings).to be_frozen
    end
  end

  describe "#reconnect?" do
    it "returns true by default" do
      expect(described_class.new).to be_reconnect
    end

    it "returns false when reconnect is off" do
      expect(described_class.new(reconnect: false)).not_to be_reconnect
    end
  end

  describe "#tls" do
    it "returns nil when TLS was not asked for" do
      expect(described_class.new.tls).to be_nil
    end

    it "returns the defaults for true" do
      expect(described_class.new(tls: true).tls).to eq(described_class::TLS_DEFAULTS)
    end

    it "merges a hash over the defaults" do
      expect(described_class.new(tls: { verify: false }).tls).to include(verify: false, first: false)
    end
  end

  describe "#websocket" do
    it "returns the defaults when nothing was given" do
      expect(described_class.new.websocket).to eq(described_class::WEBSOCKET_DEFAULTS)
    end
  end

  describe "#to_h" do
    it "returns every top-level option with the given values applied" do
      expect(described_class.new(name: "billing").to_h).to include(name: "billing", ping_interval: 120)
    end
  end

  describe "#ping_interval" do
    it "returns the value given" do
      expect(described_class.new(ping_interval: 5).ping_interval).to eq(5)
    end

    it "accepts nil, which disables keepalive" do
      expect(described_class.new(ping_interval: nil).ping_interval).to be_nil
    end
  end
end
