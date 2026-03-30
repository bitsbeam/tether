# frozen_string_literal: true

require_relative "tether/version"
require_relative "tether/errors"
require_relative "tether/deadline"

require_relative "tether/protocol"
require_relative "tether/protocol/headers"
require_relative "tether/protocol/msg"
require_relative "tether/protocol/encoder"
require_relative "tether/protocol/parser"
require_relative "tether/protocol/dispatcher"

require_relative "tether/connection"
require_relative "tether/connection/nuid"
require_relative "tether/connection/transport"
require_relative "tether/connection/subscription"
require_relative "tether/connection/inbox"

require_relative "tether/client"

# A NATS client for Ruby.
module Tether
  # Connects to a NATS server.
  #
  # @param uri [String, nil] defaults to NATS_URL, then nats://127.0.0.1:4222
  # @param options [Hash{Symbol => Object}] CONNECT overrides plus :connect_timeout
  # @return [Client, Object] the client, or the block's value when a block is given
  def self.connect(uri = nil, **options, &)
    Client.connect(uri, **options, &)
  end
end
