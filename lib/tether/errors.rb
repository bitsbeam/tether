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
  class ServerError < Error
    FATAL = true
    private_constant :FATAL

    MAPPING = {
      "authorization violation" => :AuthorizationError,
      "authentication expired" => :AuthenticationExpiredError,
      "authentication timeout" => :AuthenticationTimeoutError,
      "stale connection" => :StaleConnectionError,
      "maximum connections exceeded" => :MaxConnectionsError,
      "maximum payload violation" => :MaxPayloadViolationError,
      "maximum control line exceeded" => :MaxControlLineError,
      "slow consumer" => :ServerSlowConsumerError,
      "permissions violation" => :PermissionsViolationError,
      "invalid subject" => :ServerInvalidSubjectError,
      "secure connection - tls required" => :TLSRequiredError,
      "invalid client protocol" => :InvalidClientProtocolError,
      "unknown protocol operation" => :UnknownProtocolError
    }.freeze

    # Builds the most specific {ServerError} subclass for an `-ERR` description.
    #
    # @param description [String] the text between the quotes of an `-ERR` line
    # @return [ServerError]
    def self.for(description)
      normalized = description.to_s.downcase
      name = MAPPING[normalized] || MAPPING.find { |text, _| normalized.start_with?(text) }&.last
      klass = name ? Tether.const_get(name) : self
      klass.new(description)
    end

    # Whether the server closes the connection after sending this error.
    #
    # @return [Boolean]
    def fatal?
      FATAL
    end
  end

  # Raised when the connection is not authorized for the requested operation.
  class AuthorizationError < ServerError; end

  # Raised when the credentials presented at connect time have expired.
  class AuthenticationExpiredError < ServerError; end

  # Raised when the server times out waiting for authentication.
  class AuthenticationTimeoutError < ServerError; end

  # Raised when the server drops a connection that stopped answering PING.
  class StaleConnectionError < ServerError; end

  # Raised when the account's connection limit is reached.
  class MaxConnectionsError < ServerError; end

  # Raised when the server rejects a payload larger than max_payload.
  class MaxPayloadViolationError < ServerError; end

  # Raised when a control line exceeds the server's limit.
  class MaxControlLineError < ServerError; end

  # Raised when a subject or queue group is rejected by the server.
  class ServerInvalidSubjectError < ServerError; end

  # Raised when the server requires TLS and the client did not upgrade.
  class TLSRequiredError < ServerError; end

  # Raised when the CONNECT payload is rejected by the server.
  class InvalidClientProtocolError < ServerError; end

  # Raised when the server cannot parse a command the client sent.
  class UnknownProtocolError < ServerError; end

  # Raised when the server signals that this client is not keeping up.
  # The server does not close the connection for this error.
  class ServerSlowConsumerError < ServerError
    # @return [Boolean]
    def fatal?
      false
    end
  end

  # Raised when publishing to or subscribing to a subject the user cannot access.
  # The server does not close the connection for this error.
  class PermissionsViolationError < ServerError
    # @return [Boolean]
    def fatal?
      false
    end
  end
end
