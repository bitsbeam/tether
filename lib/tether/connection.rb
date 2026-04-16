# frozen_string_literal: true

require "tether/connection/nuid"
require "tether/connection/transport"
require "tether/connection/subscription"
require "tether/connection/inbox"

module Tether
  # The moving parts of one live connection: the socket it reads and writes,
  # the subscriptions it delivers to, and the inbox that matches replies to
  # the requests waiting on them.
  module Connection
  end
end
