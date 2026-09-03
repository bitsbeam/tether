# frozen_string_literal: true

require_relative "tether/version"
require_relative "tether/errors"
require_relative "tether/deadline"
require_relative "tether/nuid"
require_relative "tether/headers"
require_relative "tether/protocol"
require_relative "tether/protocol/encoder"
require_relative "tether/protocol/parser"
require_relative "tether/msg"
require_relative "tether/transport"
require_relative "tether/subscription"
require_relative "tether/dispatcher"
require_relative "tether/client"

# A NATS client for Ruby.
module Tether
  # Connects to a NATS server.
  #
  # @param uri [String, nil] defaults to NATS_URL, then nats://127.0.0.1:4222
  # @param options [Hash] CONNECT overrides plus :connect_timeout
  # @return [Client, Object] the client, or the block's value when a block is given
  def self.connect(uri = nil, **, &)
    Client.connect(uri, **, &)
  end
end
