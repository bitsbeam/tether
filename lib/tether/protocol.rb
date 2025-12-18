# frozen_string_literal: true

module Tether
  # The NATS client protocol: a line based text protocol over TCP where every
  # control line ends in CRLF and payloads are length prefixed.
  module Protocol
    CRLF = "\r\n"
    CRLF_SIZE = 2
    EMPTY_PAYLOAD = ""

    PING = "PING#{CRLF}".freeze
    PONG = "PONG#{CRLF}".freeze

    DEFAULT_MAX_CONTROL_LINE = 1024 * 1024

    BINARY = Encoding::BINARY
  end
end
