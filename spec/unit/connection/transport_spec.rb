# frozen_string_literal: true

RSpec.describe Tether::Connection::Transport do
  subject(:transport) { described_class.new(URI.parse("nats://127.0.0.1:#{port}")) }

  let(:listener) { TCPServer.new("127.0.0.1", 0) }
  let(:port) { listener.addr[1] }
  let(:peer) { listener.accept }

  after do
    transport.close
    listener.close
  end

  def open
    transport.open(Tether::Deadline.after(2))
    peer
  end

  describe ".for" do
    it "builds a TCP transport for a nats URL" do
      expect(described_class.for(URI.parse("nats://127.0.0.1:4222"), Tether::Options.new))
        .to be_an_instance_of(described_class)
    end
  end

  describe "#open" do
    it "connects and returns itself" do
      expect(transport.open(Tether::Deadline.after(2))).to be(transport)
      expect(transport).not_to be_closed
    end

    it "raises ConnectionError naming the server when nothing is listening" do
      port
      listener.close

      expect { transport.open(Tether::Deadline.after(2)) }
        .to raise_error(Tether::ConnectionError, /could not connect to 127.0.0.1:#{port}/)
    end

    it "raises ConnectionError for a host that does not resolve" do
      unknown = described_class.new(URI.parse("nats://host.invalid:4222"))

      expect { unknown.open(Tether::Deadline.after(2)) }.to raise_error(Tether::ConnectionError)
    end
  end

  describe "#read" do
    it "returns the bytes the peer sent" do
      open.write("INFO {}\r\n")

      expect(transport.read(Tether::Deadline.after(2))).to eq("INFO {}\r\n")
    end

    it "returns nil at the end of the stream" do
      open.close

      expect(transport.read(Tether::Deadline.after(2))).to be_nil
    end

    it "raises TimeoutError when nothing arrives by the deadline" do
      open

      expect { transport.read(Tether::Deadline.after(0.02)) }.to raise_error(Tether::TimeoutError, /reading from/)
    end

    it "raises ConnectionClosedError once closed" do
      open
      transport.close

      expect { transport.read }.to raise_error(Tether::ConnectionClosedError)
    end

    it "is released when another thread closes the transport" do
      open
      reader = Thread.new do
        transport.read
      rescue Tether::ConnectionClosedError
        nil
      end
      eventually { reader.status == "sleep" }
      transport.close

      expect(reader.join(2)).to be(reader)
    end
  end

  describe "#write" do
    it "writes every byte and returns the count" do
      remote = open

      expect(transport.write("PING\r\n", Tether::Deadline.after(2))).to eq(6)
      expect(remote.readpartial(16)).to eq("PING\r\n")
    end

    it "raises TimeoutError when the peer stops reading" do
      open

      expect { transport.write("x" * (32 * 1024 * 1024), Tether::Deadline.after(0.05)) }
        .to raise_error(Tether::TimeoutError, /writing to/)
    end

    it "raises ConnectionClosedError once closed" do
      open
      transport.close

      expect { transport.write("PING\r\n") }.to raise_error(Tether::ConnectionClosedError)
    end
  end

  describe "#tls?" do
    it "returns false for a plain socket" do
      open

      expect(transport).not_to be_tls
    end
  end

  describe "#close" do
    it "ends the connection for the peer" do
      remote = open
      transport.close

      expect(remote.read).to eq("")
    end

    it "tolerates being called twice" do
      open
      transport.close

      expect { transport.close }.not_to raise_error
    end
  end

  describe "#abandon" do
    it "closes the descriptor" do
      open
      transport.abandon

      expect(transport).to be_closed
    end
  end

  describe "#closed?" do
    it "returns true before the transport is opened" do
      expect(transport).to be_closed
    end
  end

  describe "#upgrade_to_tls" do
    let(:certificates) { TlsCertificates.instance }
    let(:server_context) do
      OpenSSL::SSL::SSLContext.new.tap do |context|
        context.cert = OpenSSL::X509::Certificate.new(certificates.server_pem)
        context.key = OpenSSL::PKey.read(certificates.server_key_pem)
      end
    end

    def serve_tls
      Thread.new do
        OpenSSL::SSL::SSLSocket.new(listener.accept, server_context).accept.tap { |tls| tls.write("INFO {}\r\n") }
      rescue OpenSSL::SSL::SSLError, SystemCallError
        nil
      end
    end

    def client_context(**settings)
      Tether::Connection::TLS.context(Tether::Options::TLS_DEFAULTS.merge(settings))
    end

    it "encrypts the socket and verifies the certificate against the hostname" do
      serve_tls
      transport.open(Tether::Deadline.after(2))
      transport.upgrade_to_tls(client_context(ca_file: certificates.ca_file), "localhost", Tether::Deadline.after(2))

      expect(transport).to be_tls
      expect(transport.read(Tether::Deadline.after(2))).to eq("INFO {}\r\n")
    end

    it "raises TLSError for a certificate from an unknown authority" do
      serve_tls
      transport.open(Tether::Deadline.after(2))

      expect { transport.upgrade_to_tls(client_context, "localhost", Tether::Deadline.after(2)) }
        .to raise_error(Tether::TLSError, /certificate verify failed/)
    end

    it "raises TLSError when the certificate is for another hostname" do
      serve_tls
      transport.open(Tether::Deadline.after(2))
      context = client_context(ca_file: certificates.ca_file)

      expect { transport.upgrade_to_tls(context, "nats.example.com", Tether::Deadline.after(2)) }
        .to raise_error(Tether::TLSError)
    end

    it "skips verification when asked to" do
      serve_tls
      transport.open(Tether::Deadline.after(2))
      transport.upgrade_to_tls(client_context(verify: false), "nats.example.com", Tether::Deadline.after(2))

      expect(transport).to be_tls
    end

    it "raises TimeoutError when the peer never answers the handshake" do
      transport.open(Tether::Deadline.after(2))

      expect { transport.upgrade_to_tls(client_context, "localhost", Tether::Deadline.after(0.05)) }
        .to raise_error(Tether::TimeoutError, /TLS handshake/)
    end
  end
end
