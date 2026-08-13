# frozen_string_literal: true

require "tether/connection/nuid"
require "tether/connection/transport"
require "tether/connection/tls"
require "tether/connection/websocket"
require "tether/connection/writer"
require "tether/connection/server_pool"
require "tether/connection/timer"
require "tether/connection/events"
require "tether/connection/keepalive"
require "tether/connection/handshake"
require "tether/connection/subscription"
require "tether/connection/inbox"
require "tether/connection/lifecycle"

module Tether
  # Everything that makes up one live connection: the socket, the write
  # buffer, the state machine, subscriptions and timers.
  module Connection
  end
end
