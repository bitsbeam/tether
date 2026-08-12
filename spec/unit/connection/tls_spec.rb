# frozen_string_literal: true

RSpec.describe Tether::Connection::TLS do
  def context(**settings)
    described_class.context(Tether::Options::TLS_DEFAULTS.merge(settings))
  end

  describe ".context" do
    it "verifies the peer by default" do
      expect(context.verify_mode).to eq(OpenSSL::SSL::VERIFY_PEER)
    end

    it "turns verification off when asked to" do
      expect(context(verify: false).verify_mode).to eq(OpenSSL::SSL::VERIFY_NONE)
    end

    it "returns a ready context untouched" do
      ready = OpenSSL::SSL::SSLContext.new

      expect(context(context: ready)).to be(ready)
    end

    it "loads a client certificate and its key" do
      certificates = TlsCertificates.instance
      cert_file = File.join(File.dirname(certificates.ca_file), "client.pem")
      File.write(cert_file, certificates.server_pem + certificates.server_key_pem)

      expect(context(cert_file: cert_file).cert.subject.to_s).to eq("/CN=localhost")
    end

    it "raises TLSError for a CA file that does not exist" do
      expect { context(ca_file: "/nonexistent/ca.pem") }
        .to raise_error(Tether::TLSError, /could not build the TLS context/)
    end

    it "raises TLSError for a certificate file that does not exist" do
      expect { context(cert_file: "/nonexistent/client.pem") }
        .to raise_error(Tether::TLSError, /could not build the TLS context/)
    end
  end
end
