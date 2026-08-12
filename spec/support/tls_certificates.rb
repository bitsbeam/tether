# frozen_string_literal: true

require "openssl"
require "tmpdir"

# A throwaway certificate authority and a server certificate for 127.0.0.1.
class TlsCertificates
  attr_reader :ca_pem, :server_pem, :server_key_pem

  def self.instance
    @instance ||= new
  end

  def initialize
    ca_key = OpenSSL::PKey::RSA.new(2048)
    ca = certificate("tether-spec-ca", ca_key, authority: true)
    ca.sign(ca_key, OpenSSL::Digest.new("SHA256"))
    server_key = OpenSSL::PKey::RSA.new(2048)
    server = certificate("localhost", server_key, issuer: ca, authority: false)
    server.sign(ca_key, OpenSSL::Digest.new("SHA256"))
    @ca_pem = ca.to_pem
    @server_pem = server.to_pem
    @server_key_pem = server_key.to_pem
  end

  def ca_file
    @ca_file ||= File.join(Dir.mktmpdir("tether-spec"), "ca.pem").tap { |path| File.write(path, @ca_pem) }
  end

  private

  def certificate(name, key, authority:, issuer: nil)
    cert = OpenSSL::X509::Certificate.new
    cert.version = 2
    cert.serial = rand(1..1_000_000)
    cert.subject = OpenSSL::X509::Name.parse("/CN=#{name}")
    cert.issuer = issuer ? issuer.subject : cert.subject
    cert.public_key = key.public_key
    cert.not_before = Time.now - 3600
    cert.not_after = Time.now + 86_400
    extend_certificate(cert, issuer || cert, authority)
    cert
  end

  def extend_certificate(cert, issuer, authority)
    factory = OpenSSL::X509::ExtensionFactory.new
    factory.subject_certificate = cert
    factory.issuer_certificate = issuer
    if authority
      cert.add_extension(factory.create_extension("basicConstraints", "CA:TRUE", true))
      cert.add_extension(factory.create_extension("keyUsage", "keyCertSign,cRLSign", true))
    else
      cert.add_extension(factory.create_extension("subjectAltName", "DNS:localhost,IP:127.0.0.1"))
    end
  end
end
