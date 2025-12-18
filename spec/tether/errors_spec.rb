# frozen_string_literal: true

RSpec.describe Tether::ServerError do
  describe ".for" do
    from_server_source = {
      "Failed Account Registration" => Tether::FailedAccountRegistrationError,
      "invalid client protocol" => Tether::InvalidClientProtocolError,
      "no responders requires headers support" => Tether::NoRespondersRequiresHeadersError,
      "Authentication Timeout" => Tether::AuthenticationTimeoutError,
      "User Authentication Expired" => Tether::AuthenticationExpiredError,
      "Account Authentication Expired" => Tether::AuthenticationExpiredError,
      "User Authentication Revoked" => Tether::AuthenticationRevokedError,
      "Authorization Violation" => Tether::AuthorizationError,
      "maximum account active connections exceeded" => Tether::MaxAccountConnectionsError,
      "maximum connections exceeded" => Tether::MaxConnectionsError,
      "maximum subscriptions exceeded" => Tether::MaxSubscriptionsError,
      "Maximum Payload Violation" => Tether::MaxPayloadViolationError,
      "maximum control line exceeded" => Tether::MaxControlLineError,
      "Invalid Publish Subject" => Tether::InvalidPublishSubjectError,
      "Invalid Subject" => Tether::ServerInvalidSubjectError,
      "Stale Connection" => Tether::StaleConnectionError,
      "Unknown Protocol Operation" => Tether::UnknownProtocolError,
      "Secure Connection - TLS Required" => Tether::TLSRequiredError,
      "Connection throttling is active. Please try again later." => Tether::ConnectionThrottledError,
      "attempted to connect to route port" => Tether::WrongPortError,
      "attempted to connect to leaf node port" => Tether::WrongPortError,
      "attempted to connect to gateway port" => Tether::WrongPortError,
      %(Permissions Violation for Publish to "orders.new") => Tether::PermissionsViolationError,
      %(Permissions Violation for Subscription to "orders.>") => Tether::PermissionsViolationError,
      %(Permissions Violation for Subscription to "orders.>" using queue "workers") =>
        Tether::PermissionsViolationError,
      %(Permissions Violation for Publish with Reply of "_INBOX.x") => Tether::PermissionsViolationError,
      %(Permissions Violation for Subscription to "a.b.c", too many tokens) => Tether::PermissionsViolationError,
      %(Permissions Violation for Subscription to "orders.>" (sid "7")) => Tether::PermissionsViolationError
    }

    from_server_source.each do |description, klass|
      it "maps #{description.inspect} to #{klass}" do
        expect(described_class.for(description)).to be_a(klass)
      end
    end

    it "matches whatever case the server chose, which is not consistent" do
      expect(described_class.for("MAXIMUM CONTROL LINE EXCEEDED")).to be_a(Tether::MaxControlLineError)
      expect(described_class.for("Maximum Control Line Exceeded")).to be_a(Tether::MaxControlLineError)
    end

    it "does not let the account connection limit fall through to the global one" do
      expect(described_class.for("maximum account active connections exceeded"))
        .not_to be_a(Tether::MaxConnectionsError)
    end

    it "is not fooled by a subject embedded in a permissions violation" do
      expect(described_class.for(%(Permissions Violation for Publish to "invalid subject")))
        .to be_a(Tether::PermissionsViolationError)
    end

    it "does not map slow consumer, which the server logs and closes without sending" do
      expect(described_class.for("Slow Consumer").class).to eq(described_class)
    end

    it "falls back to the generic error for anything unrecognized" do
      expect(described_class.for("Some Future Error").class).to eq(described_class)
    end
  end

  describe "#fatal?" do
    it "reports the errors the server sends without closing the connection" do
      survivable = [
        %(Permissions Violation for Publish to "orders.new"),
        "Invalid Subject",
        "Invalid Publish Subject",
        "Failed Account Registration",
        "maximum subscriptions exceeded"
      ]

      expect(survivable.map { described_class.for(_1).fatal? }).to all(be(false))
    end

    it "reports the errors the server follows with closeConnection" do
      terminal = [
        "Unknown Protocol Operation",
        "Maximum Payload Violation",
        "maximum control line exceeded",
        "Authorization Violation",
        "Stale Connection",
        "User Authentication Expired",
        "User Authentication Revoked",
        "maximum account active connections exceeded",
        "no responders requires headers support",
        "attempted to connect to route port"
      ]

      expect(terminal.map { described_class.for(_1).fatal? }).to all(be(true))
    end
  end
end
