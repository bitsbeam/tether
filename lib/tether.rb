# frozen_string_literal: true

require "tether/version"
require "tether/errors"
require "tether/deadline"
require "tether/options"
require "tether/server_info"
require "tether/stats"
require "tether/auth"
require "tether/protocol"
require "tether/connection"
require "tether/client"
require "tether/service"
require "tether/jetstream"
require "tether/key_value"
require "tether/object_store"

# A NATS client for Ruby.
module Tether
  # Connects to a NATS server.
  #
  # @param urls [String, Array<String>, nil] defaults to NATS_URL, then nats://127.0.0.1:4222
  # @param options [Hash{Symbol => Object}] see {Options::DEFAULTS}
  # @return [Client, Object] the client, or the block's value when a block is given
  def self.connect(urls = nil, **options, &)
    Client.connect(urls, **options, &)
  end

  Msg = Protocol::Msg
  Headers = Protocol::Headers
  Subscription = Connection::Subscription
end
