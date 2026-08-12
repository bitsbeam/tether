# frozen_string_literal: true

RSpec.describe Tether::Connection::ServerPool do
  subject(:pool) { described_class.new(%w[nats://a:4222 nats://b:4222 nats://c:4222], randomize: false) }

  def urls(pool)
    pool.to_a.map(&:url)
  end

  describe "#initialize" do
    it "splits a comma separated list" do
      expect(urls(described_class.new("nats://a:1, nats://b:2", randomize: false))).to eq(%w[nats://a:1 nats://b:2])
    end

    it "defaults the scheme and the port" do
      expect(urls(described_class.new("localhost"))).to eq(["nats://localhost:4222"])
    end

    it "defaults a tls URL to the client port" do
      expect(urls(described_class.new("tls://secure.example.com"))).to eq(["tls://secure.example.com:4222"])
    end

    it "keeps the same servers when it shuffles them" do
      shuffled = described_class.new(%w[nats://a:1 nats://b:1 nats://c:1], randomize: true)

      expect(urls(shuffled)).to match_array(%w[nats://a:1 nats://b:1 nats://c:1])
    end

    it "raises when no URL is given" do
      expect { described_class.new("") }.to raise_error(ArgumentError, "no server URL given")
    end

    it "raises for a scheme it cannot speak" do
      expect { described_class.new("http://a:1") }.to raise_error(ArgumentError, /unsupported URL scheme/)
    end

    it "raises for a URL that does not parse" do
      expect { described_class.new("nats://a b") }.to raise_error(ArgumentError, /invalid server URL/)
    end

    it "raises when WebSocket and TCP URLs are mixed" do
      expect { described_class.new(%w[nats://a:1 ws://b:2]) }
        .to raise_error(ArgumentError, "WebSocket and TCP URLs cannot be mixed")
    end
  end

  describe "#to_a" do
    it "returns a copy, so a caller cannot reorder the pool" do
      pool.to_a.clear

      expect(pool.size).to eq(3)
    end

    it "hides credentials from a server's url" do
      server = described_class.new("nats://user:secret@a:4222").to_a.first

      expect(server.url).to eq("nats://a:4222")
      expect(server.inspect).not_to include("secret")
    end
  end

  describe "#size" do
    it "counts the servers" do
      expect(pool.size).to eq(3)
    end
  end

  describe "#replace" do
    it "swaps in the new servers" do
      pool.replace("nats://x:1,nats://y:2")

      expect(urls(pool)).to eq(%w[nats://x:1 nats://y:2])
    end

    it "keeps the old servers when the new list is invalid" do
      expect { pool.replace("") }.to raise_error(ArgumentError)
      expect(pool.size).to eq(3)
    end
  end

  describe "#next_server" do
    it "rotates through the servers" do
      expect(Array.new(4) { pool.next_server.url }).to eq(%w[nats://a:4222 nats://b:4222 nats://c:4222 nats://a:4222])
    end

    it "drops a server that failed max_attempts times in a row" do
      limited = described_class.new(%w[nats://a:1 nats://b:1], randomize: false, max_attempts: 2)
      flaky = limited.to_a.first
      2.times { limited.failed(flaky, Tether::ConnectionError.new("refused")) }

      expect(Array.new(2) { limited.next_server.url }).to eq(%w[nats://b:1 nats://b:1])
    end

    it "returns nil once every server has been dropped" do
      limited = described_class.new("nats://a:1", max_attempts: 1)
      limited.failed(limited.to_a.first, Tether::ConnectionError.new("refused"))

      expect(limited.next_server).to be_nil
    end

    it "keeps a failing server forever with a negative max_attempts" do
      stubborn = described_class.new("nats://a:1", max_attempts: -1)
      5.times { stubborn.failed(stubborn.to_a.first, Tether::ConnectionError.new("refused")) }

      expect(stubborn.next_server.url).to eq("nats://a:1")
    end
  end

  describe "#connected" do
    it "clears the failure record and moves the server to the back" do
      first = pool.to_a.first
      pool.failed(first, Tether::ConnectionError.new("refused"))
      pool.connected(first)

      expect(first).to have_attributes(attempts: 0, auth_failures: 0, last_error: nil)
      expect(urls(pool)).to eq(%w[nats://b:4222 nats://c:4222 nats://a:4222])
    end
  end

  describe "#failed" do
    it "counts consecutive attempts and remembers the error" do
      server = pool.to_a.first
      error = Tether::ConnectionError.new("refused")
      2.times { pool.failed(server, error) }

      expect(server).to have_attributes(attempts: 2, last_error: error)
    end

    it "counts the same server error repeating" do
      server = pool.to_a.first
      2.times { pool.failed(server, Tether::AuthorizationError.new("Authorization Violation")) }

      expect(server.auth_failures).to eq(2)
    end

    it "restarts the count when a different error interrupts" do
      server = pool.to_a.first
      pool.failed(server, Tether::AuthorizationError.new("Authorization Violation"))
      pool.failed(server, Tether::ConnectionError.new("refused"))
      pool.failed(server, Tether::AuthorizationError.new("Authorization Violation"))

      expect(server.auth_failures).to eq(1)
    end
  end

  describe "#reset_attempts" do
    it "makes every server eligible again" do
      limited = described_class.new("nats://a:1", max_attempts: 1)
      limited.failed(limited.to_a.first, Tether::ConnectionError.new("refused"))
      limited.reset_attempts

      expect(limited.next_server.url).to eq("nats://a:1")
    end
  end

  describe "#discover" do
    let(:seed) { pool.to_a.first }

    it "adds advertised servers it does not know and returns them" do
      added = pool.discover(["10.0.0.9:4222", "b:4222"], seed)

      expect(added.map(&:url)).to eq(["nats://10.0.0.9:4222"])
      expect(added.first).to be_discovered
      expect(pool.size).to eq(4)
    end

    it "removes a discovered server the cluster stopped advertising" do
      pool.discover(["10.0.0.9:4222"], seed)
      pool.discover([], seed)

      expect(urls(pool)).to eq(%w[nats://a:4222 nats://b:4222 nats://c:4222])
    end

    it "keeps a seed the cluster does not advertise" do
      pool.discover([], seed)

      expect(pool.size).to eq(3)
    end

    it "gives a discovered server the scheme and credentials of the one that advertised it" do
      secure = described_class.new("tls://user:secret@nats.example.com:4222")
      added = secure.discover(["10.0.0.9:4222"], secure.to_a.first).first

      expect(added.uri.to_s).to eq("tls://user:secret@10.0.0.9:4222")
    end

    it "verifies a bare address against the hostname of the server that advertised it" do
      secure = described_class.new("tls://nats.example.com:4222")
      added = secure.discover(["10.0.0.9:4222"], secure.to_a.first).first

      expect(added.tls_hostname).to eq("nats.example.com")
    end

    it "leaves a discovered hostname to be verified as itself" do
      secure = described_class.new("tls://nats.example.com:4222")
      added = secure.discover(["other.example.com:4222"], secure.to_a.first).first

      expect(added.tls_hostname).to be_nil
    end
  end
end
