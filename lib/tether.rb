# frozen_string_literal: true

require "tether/version"
require "tether/errors"
require "tether/deadline"
require "tether/protocol"
require "tether/connection"
require "tether/client"

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
