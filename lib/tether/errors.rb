# frozen_string_literal: true

module Tether
  # Base class for every error raised by tether.
  class Error < StandardError; end

  # Raised when the server sends bytes that violate the client protocol.
  class ProtocolError < Error; end

  # Raised when a {Deadline} expires before an operation completes.
  class TimeoutError < Error; end

  # Raised when a request reaches a subject that has no subscribers.
  class NoRespondersError < Error; end

  # Raised when a payload exceeds the server's advertised max_payload.
  class MaxPayloadError < Error; end

  # Raised when a subscription's pending queue overflows.
  class SlowConsumerError < Error; end

  # Raised when a subject or queue group is not a valid NATS token sequence.
  class InvalidSubjectError < Error; end

  # Base class for errors about the state of the connection itself.
  class ConnectionError < Error; end

  # Raised when an operation is attempted on a closed connection.
  class ConnectionClosedError < ConnectionError; end

  # Raised when an operation is attempted on a connection that is draining.
  class ConnectionDrainingError < ConnectionError; end

  # Raised when an operation cannot be served while reconnecting.
  class ConnectionReconnectingError < ConnectionError; end

  # Raised when no server in the pool could be reached.
  class NoServersError < ConnectionError; end

  # An `-ERR` line received from the server.
  #
  # Every entry below is taken from the nats-server source (v2.14) rather than
  # from documentation. The server writes `-ERR '<description>'` from exactly
  # three places (`sendErr`, and two raw writes for a stale connection), and its
  # casing is inconsistent — `Authorization Violation` is title case while
  # `maximum control line exceeded` is not — so lookup is case insensitive.
  # Several descriptions embed context (`Permissions Violation for Publish to
  # "orders.new"`), so lookup is by substring, ordered specific first.
  class ServerError < Error
    MAPPING = {
      "permissions violation" => :PermissionsViolationError,
      "authorization violation" => :AuthorizationError,
      "authentication expired" => :AuthenticationExpiredError,
      "authentication revoked" => :AuthenticationRevokedError,
      "authentication timeout" => :AuthenticationTimeoutError,
      "stale connection" => :StaleConnectionError,
      "maximum account active connections exceeded" => :MaxAccountConnectionsError,
      "maximum connections exceeded" => :MaxConnectionsError,
      "maximum subscriptions exceeded" => :MaxSubscriptionsError,
      "maximum payload violation" => :MaxPayloadViolationError,
      "maximum control line exceeded" => :MaxControlLineError,
      "invalid publish subject" => :InvalidPublishSubjectError,
      "invalid subject" => :ServerInvalidSubjectError,
      "failed account registration" => :FailedAccountRegistrationError,
      "no responders requires headers support" => :NoRespondersRequiresHeadersError,
      "connection throttling is active" => :ConnectionThrottledError,
      "secure connection - tls required" => :TLSRequiredError,
      "invalid client protocol" => :InvalidClientProtocolError,
      "unknown protocol operation" => :UnknownProtocolError,
      "attempted to connect to route port" => :WrongPortError,
      "attempted to connect to leaf node port" => :WrongPortError,
      "attempted to connect to gateway port" => :WrongPortError
    }.freeze

    # Builds the most specific {ServerError} subclass for an `-ERR` description.
    #
    # @param description [String] the text between the quotes of an `-ERR` line
    # @return [ServerError]
    def self.for(description)
      normalized = description.to_s.downcase
      name = MAPPING[normalized] || MAPPING.find { |text, _| normalized.include?(text) }&.last
      klass = name ? Tether.const_get(name) : self
      klass.new(description)
    end

    # Whether the server is expected to close the connection after this error.
    #
    # Advisory only, and never a substitute for the connection state itself.
    # The server sends `maximum subscriptions exceeded` from two paths that are
    # indistinguishable on the wire: one leaves the connection up, the other
    # closes it 20ms later. Reconnect must therefore be driven by the socket
    # reaching EOF; this predicate only says whether to expect that.
    #
    # @return [Boolean]
    def self.fatal?
      true
    end

    # @return [Boolean] whether the server is expected to close the connection
    #   after this error; see {ServerError.fatal?} for why it is advisory
    def fatal?
      self.class.fatal?
    end
  end

  # Raised when the account presented at connect time is not authorized.
  class AuthorizationError < ServerError; end

  # Raised when the credentials presented at connect time have expired.
  # Covers both the user scoped and account scoped descriptions.
  class AuthenticationExpiredError < ServerError; end

  # Raised when the credentials presented at connect time have been revoked.
  # Distinct from expiry: retrying with the same credentials cannot succeed.
  class AuthenticationRevokedError < ServerError; end

  # Raised when the server times out waiting for authentication.
  class AuthenticationTimeoutError < ServerError; end

  # Raised when the server drops a connection that stopped answering PING.
  class StaleConnectionError < ServerError; end

  # Raised when the server's global connection limit is reached.
  class MaxConnectionsError < ServerError; end

  # Raised when the account's active connection limit is reached.
  class MaxAccountConnectionsError < ServerError; end

  # Raised when the server rejects a payload larger than max_payload.
  class MaxPayloadViolationError < ServerError; end

  # Raised when a control line exceeds the server's limit.
  class MaxControlLineError < ServerError; end

  # Raised when CONNECT asks for no_responders without headers support.
  class NoRespondersRequiresHeadersError < ServerError; end

  # Raised when the server is shedding load and the client should back off.
  class ConnectionThrottledError < ServerError; end

  # Raised when the server requires TLS and the client did not upgrade.
  class TLSRequiredError < ServerError; end

  # Raised when the CONNECT payload is rejected by the server.
  class InvalidClientProtocolError < ServerError; end

  # Raised when the server cannot parse a command the client sent.
  class UnknownProtocolError < ServerError; end

  # Raised when the client connected to a route, leafnode, or gateway port
  # instead of the client port. Reconnecting cannot help.
  class WrongPortError < ServerError; end

  # Raised when a subscription names a subject the server rejects.
  # The server does not close the connection for this error: its parser
  # deliberately swallows the error so a bad subject is not a parse failure.
  class ServerInvalidSubjectError < ServerError
    # @return [Boolean] false: the server's parser deliberately swallows this so a bad
    #   subject is not treated as a parse failure
    def self.fatal?
      false
    end
  end

  # Raised when a publish names a malformed subject, in pedantic mode.
  # The server does not close the connection for this error.
  class InvalidPublishSubjectError < ServerError
    # @return [Boolean] false: the server returns without closing
    def self.fatal?
      false
    end
  end

  # Raised when the server fails to register the connection's account.
  # The server does not close the connection for this error.
  class FailedAccountRegistrationError < ServerError
    # @return [Boolean] false: the server reports it and carries on
    def self.fatal?
      false
    end
  end

  # Raised when the subscription limit is reached.
  #
  # Reported as non fatal because the ordinary over-limit SUB leaves the
  # connection usable, but the account limit path sends the same text and then
  # closes. See {ServerError.fatal?}.
  class MaxSubscriptionsError < ServerError
    # @return [Boolean] false for the ordinary over-limit SUB, but see {ServerError.fatal?}:
    #   the account limit path sends the same text and then closes
    def self.fatal?
      false
    end
  end

  # Raised when publishing to or subscribing to a subject the user cannot access.
  # The server does not close the connection for any of its seven variants.
  class PermissionsViolationError < ServerError
    # @return [Boolean] false for all seven variants the server can send
    def self.fatal?
      false
    end
  end
end
