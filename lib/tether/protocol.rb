# frozen_string_literal: true

require "tether/protocol/subject"
require "tether/protocol/headers"
require "tether/protocol/registry"
require "tether/protocol/msg"
require "tether/protocol/encoder"
require "tether/protocol/parser"
require "tether/protocol/dispatcher"

module Tether
  # The NATS client protocol: a line based text protocol over TCP where every
  # control line ends in CRLF and payloads are length prefixed.
  module Protocol
    # Terminates every control line, and follows every payload.
    CRLF = "\r\n"

    # Bytes in a CRLF, added to a payload's length to find the next frame.
    CRLF_SIZE = 2

    # Sent when a message carries no body.
    EMPTY_PAYLOAD = ""

    # Answered by the server with PONG, once it has processed everything sent
    # before it.
    PING = "PING#{CRLF}".freeze

    # Sent in answer to the server's PING, promptly, or it drops the connection.
    PONG = "PONG#{CRLF}".freeze

    # A sanity bound on a single control line, not a protocol limit. The
    # server's own 4096 byte limit applies to what a client sends, never to
    # what it emits, and an INFO listing a large cluster can be far larger.
    DEFAULT_MAX_CONTROL_LINE = 1024 * 1024

    # Every frame is assembled and parsed as bytes, never as text.
    BINARY = Encoding::BINARY
  end
end
