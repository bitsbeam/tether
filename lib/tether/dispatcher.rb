# frozen_string_literal: true

module Tether
  # Routes parsed frames to subscriptions and to connection level handlers.
  #
  # This is the parser's sink. It knows nothing about sockets or reconnection,
  # so the parser stays testable without a client.
  class Dispatcher
    # Wires up the connection level events the parser will report.
    #
    # @param handlers [Hash{Symbol => Proc}] :ping, :pong, :info, :error, :protocol_error
    def initialize(**handlers)
      @handlers = handlers
      @subscriptions = {}
      @lock = Mutex.new
    end

    # Registers a subscription so messages carrying its sid reach it.
    #
    # @param subscription [Subscription] the subscription to route messages to
    # @return [Subscription] the subscription that was registered
    def add(subscription)
      @lock.synchronize { @subscriptions[subscription.sid] = subscription }
      subscription
    end

    # Stops routing to a subscription. Messages already in flight for it are
    # dropped when they arrive.
    #
    # @param sid [String] the token the subscription was registered under
    # @return [Subscription, nil] the subscription that was registered, if any
    def remove(sid)
      @lock.synchronize { @subscriptions.delete(sid) }
    end

    # Looks up a subscription by the token the server echoed back.
    #
    # @param sid [String] the token echoed back by the server
    # @return [Subscription, nil] nil once the subscription has been removed
    def [](sid)
      @lock.synchronize { @subscriptions[sid] }
    end

    # @return [Array<Subscription>] a snapshot, safe to iterate while others subscribe
    def subscriptions
      @lock.synchronize { @subscriptions.values }
    end

    # @return [Integer] how many subscriptions are registered
    def size
      @lock.synchronize { @subscriptions.size }
    end

    # Closes every subscription and empties the registry.
    #
    # @return [void] nothing
    def clear
      @lock.synchronize { @subscriptions.values.tap { @subscriptions.clear } }.each(&:close)
    end

    # Routes a message to its subscription, on the reader thread.
    #
    # A message whose sid is unknown, because the subscription was cancelled
    # while it was in flight, is dropped rather than raised over.
    #
    # @return [void] nothing
    def on_msg(subject, sid, reply_to, header_bytes, payload)
      subscription = self[sid] or return

      subscription.deliver(
        Msg.new(subject: subject, data: payload, reply_to: reply_to,
                header_bytes: header_bytes, subscription: subscription)
      )
      nil
    end

    # The connection must answer promptly or the server drops it as stale.
    #
    # @return [void] nothing
    def on_ping = @handlers[:ping]&.call

    # Completes a {Client#flush}.
    #
    # @return [void] nothing
    def on_pong = @handlers[:pong]&.call

    # Ignored: tether turns off verbose mode, so the server sends these only if
    # a caller asked for them explicitly.
    #
    # @return [void] nothing
    def on_ok = nil

    # Arrives once at connect and again whenever cluster topology changes.
    #
    # @return [void] nothing
    def on_info(json) = @handlers[:info]&.call(json)

    # An `-ERR` line, which may or may not be followed by the server closing
    # the connection. See {ServerError.fatal?}.
    #
    # @return [void] nothing
    def on_error(description) = @handlers[:error]&.call(description)

    # The parser gave up on the stream; the connection is no longer usable.
    #
    # @return [void] nothing
    def on_protocol_error(message) = @handlers[:protocol_error]&.call(message)
  end
end
