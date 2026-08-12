# frozen_string_literal: true

require "tether"

Dir[File.join(__dir__, "support", "**", "*.rb")].each { |file| require file }

# The servers the suite shares. Each is started on first use and removed when
# the suite ends.
module SharedNatsServer
  External = Struct.new(:uri) do
    def port = URI.parse(uri).port
  end

  USERS = <<~CONFIG
    authorization {
      users = [
        { user: "alice", password: "wonderland" },
        { user: "limited", password: "wonderland",
          permissions: { publish: { allow: ["allowed.>", "$SYS.REQ.USER.INFO"] },
                         subscribe: { allow: ["allowed.>", "_INBOX.>"] } } },
        { nkey: "%<nkey>s" }
      ]
    }
  CONFIG

  TLS = <<~CONFIG
    tls {
      cert_file: "/certs/server.pem"
      key_file: "/certs/server.key"
      %<extra>s
    }
  CONFIG

  NKEY_SEED = "SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY"
  NKEY_PUBLIC = "UDXU4RCSJNZOIQHZNWXHXORDPRTGNJAHAHFRGZNEEJCPQTT2M7NLCNF4"

  @servers = {}

  class << self
    def instance
      fetch(:default) do
        ENV["NATS_URL"] ? External.new(ENV.fetch("NATS_URL")) : NatsServer.new(config: "jetstream {}").start
      end
    end

    def users
      fetch(:users) { NatsServer.new(config: format(USERS, nkey: NKEY_PUBLIC)).start }
    end

    def token
      fetch(:token) { NatsServer.new(config: %(authorization { token: "s3cr3t" })).start }
    end

    def tls
      fetch(:tls) { NatsServer.new(config: format(TLS, extra: ""), files: certificate_files).start }
    end

    def tls_first
      fetch(:tls_first) do
        NatsServer.new(config: format(TLS, extra: "handshake_first: true"), files: certificate_files,
                       tls_first: true).start
      end
    end

    def stop
      @servers.each_value { |server| server.stop if server.respond_to?(:stop) }
      @servers.clear
    end

    private

    def fetch(name)
      @servers[name] ||= yield
    end

    def certificate_files
      certificates = TlsCertificates.instance
      { "/certs/server.pem" => certificates.server_pem, "/certs/server.key" => certificates.server_key_pem }
    end
  end
end

RSpec.configure do |config|
  config.include Eventually
  config.include ForkHelper

  config.after(:suite) { SharedNatsServer.stop }

  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = ".rspec_status"

  # Disable RSpec exposing methods globally on `Module` and `main`
  config.disable_monkey_patching!

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end
end
