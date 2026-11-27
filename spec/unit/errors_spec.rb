# frozen_string_literal: true

RSpec.describe Tether::Error do
  describe "#retryable?" do
    it "returns false, since repeating a failed call unchanged rarely helps" do
      expect(described_class.new).not_to be_retryable
    end
  end
end

RSpec.describe Tether::DisconnectedError do
  describe "#retryable?" do
    it "returns true, since the call may succeed after the reconnect" do
      expect(described_class.new).to be_retryable
    end
  end
end

RSpec.describe Tether::HandlerError do
  subject(:error) { described_class.new(original, :the_message) }

  let(:original) do
    raise ArgumentError, "boom"
  rescue ArgumentError => e
    e
  end

  describe "#initialize" do
    it "describes what the handler raised" do
      expect(error.message).to eq("handler raised ArgumentError: boom")
    end

    it "takes the backtrace of the original, which points at the user's code" do
      expect(error.backtrace).to eq(original.backtrace)
    end
  end

  describe "#msg" do
    it "returns the message the handler was given" do
      expect(error.msg).to eq(:the_message)
    end
  end

  describe "#cause" do
    it "returns the exception the handler raised" do
      expect(error.cause).to be(original)
    end
  end
end

RSpec.describe Tether::ServerError do
  describe ".for" do
    def expect_mapping(description, klass)
      expect(described_class.for(description)).to be_an_instance_of(klass)
    end

    it("maps a failed account registration") do
      expect_mapping("Failed Account Registration", Tether::FailedAccountRegistrationError)
    end
    it("maps an invalid client protocol") do
      expect_mapping("invalid client protocol", Tether::InvalidClientProtocolError)
    end
    it("maps no responders without headers") do
      expect_mapping("no responders requires headers support", Tether::NoRespondersRequiresHeadersError)
    end
    it("maps an authentication timeout") do
      expect_mapping("Authentication Timeout", Tether::AuthenticationTimeoutError)
    end
    it("maps an expired user") { expect_mapping("User Authentication Expired", Tether::AuthenticationExpiredError) }
    it("maps an expired account") do
      expect_mapping("Account Authentication Expired", Tether::AuthenticationExpiredError)
    end
    it("maps a revoked user") { expect_mapping("User Authentication Revoked", Tether::AuthenticationRevokedError) }
    it("maps an authorization violation") { expect_mapping("Authorization Violation", Tether::AuthorizationError) }
    it("maps the account connection limit") do
      expect_mapping("maximum account active connections exceeded", Tether::MaxAccountConnectionsError)
    end
    it("maps the global connection limit") do
      expect_mapping("maximum connections exceeded", Tether::MaxConnectionsError)
    end
    it("maps the subscription limit") do
      expect_mapping("maximum subscriptions exceeded", Tether::MaxSubscriptionsError)
    end
    it("maps a payload violation") { expect_mapping("Maximum Payload Violation", Tether::MaxPayloadViolationError) }
    it("maps an oversized control line") do
      expect_mapping("maximum control line exceeded", Tether::MaxControlLineError)
    end
    it("maps an invalid publish subject") do
      expect_mapping("Invalid Publish Subject", Tether::InvalidPublishSubjectError)
    end
    it("maps an invalid subject") { expect_mapping("Invalid Subject", Tether::ServerInvalidSubjectError) }
    it("maps a stale connection") { expect_mapping("Stale Connection", Tether::StaleConnectionError) }
    it("maps an unknown operation") { expect_mapping("Unknown Protocol Operation", Tether::UnknownProtocolError) }
    it("maps a TLS requirement") { expect_mapping("Secure Connection - TLS Required", Tether::TLSRequiredError) }
    it("maps connection throttling") do
      expect_mapping("Connection throttling is active. Please try again later.", Tether::ConnectionThrottledError)
    end
    it("maps the route port") { expect_mapping("attempted to connect to route port", Tether::WrongPortError) }
    it("maps the leaf node port") { expect_mapping("attempted to connect to leaf node port", Tether::WrongPortError) }
    it("maps the gateway port") { expect_mapping("attempted to connect to gateway port", Tether::WrongPortError) }
    it("maps a publish permissions violation") do
      expect_mapping(%(Permissions Violation for Publish to "orders.new"), Tether::PermissionsViolationError)
    end
    it("maps a subscription permissions violation") do
      expect_mapping(%(Permissions Violation for Subscription to "orders.>"), Tether::PermissionsViolationError)
    end
    it("maps a queue subscription permissions violation") do
      expect_mapping(%(Permissions Violation for Subscription to "orders.>" using queue "workers"), Tether::PermissionsViolationError)
    end
    it("maps a reply permissions violation") do
      expect_mapping(%(Permissions Violation for Publish with Reply of "_INBOX.x"), Tether::PermissionsViolationError)
    end
    it("maps a permissions violation with a suffix") do
      expect_mapping(%(Permissions Violation for Subscription to "a.b.c", too many tokens), Tether::PermissionsViolationError)
    end
    it("maps a permissions violation naming the sid") do
      expect_mapping(%(Permissions Violation for Subscription to "orders.>" (sid "7")), Tether::PermissionsViolationError)
    end

    it "matches whatever case the server chose, which is not consistent" do
      expect(described_class.for("MAXIMUM CONTROL LINE EXCEEDED")).to be_a(Tether::MaxControlLineError)
      expect(described_class.for("Maximum Control Line Exceeded")).to be_a(Tether::MaxControlLineError)
    end

    it "does not let the account connection limit fall through to the global one" do
      expect(described_class.for("maximum account active connections exceeded"))
        .not_to be_a(Tether::MaxConnectionsError)
    end

    it "ignores a subject embedded in a permissions violation" do
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
    it "returns false for the errors the server sends without closing the connection" do
      survivable = [
        %(Permissions Violation for Publish to "orders.new"),
        "Invalid Subject",
        "Invalid Publish Subject",
        "Failed Account Registration",
        "maximum subscriptions exceeded"
      ]

      expect(survivable.map { |text| described_class.for(text).fatal? }).to all(be(false))
    end

    it "returns true for the errors the server follows with closeConnection" do
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

      expect(terminal.map { |text| described_class.for(text).fatal? }).to all(be(true))
    end
  end
end

RSpec.describe Tether::PermissionsViolationError do
  describe "#subscription_subject" do
    it "returns the subject of a refused subscription" do
      expect(described_class.new(%(Permissions Violation for Subscription to "orders.>")).subscription_subject)
        .to eq("orders.>")
    end

    it "returns the subject of a refused queue subscription that names the sid" do
      error = described_class.new(%(Permissions Violation for Subscription to "orders.>" using queue "q" (sid "7")))

      expect(error.subscription_subject).to eq("orders.>")
    end

    it "returns nil for a refused publish" do
      expect(described_class.new(%(Permissions Violation for Publish to "orders.new")).subscription_subject).to be_nil
    end
  end
end
