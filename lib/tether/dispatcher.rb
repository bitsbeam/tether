# frozen_string_literal: true

module Tether
  # Routes parsed frames to subscriptions and to connection level handlers.
  #
  # This is the parser's sink. It knows nothing about sockets or reconnection,
  # so the parser stays testable without a client.
  class Dispatcher
    # @param handlers [Hash{Symbol => Proc}] :ping, :pong, :info, :error, :protocol_error
    def initialize(**handlers)
      @handlers = handlers
      @subscriptions = {}
      @lock = Mutex.new
    end

    # @param subscription [Subscription]
    # @return [Subscription]
    def add(subscription)
      @lock.synchronize { @subscriptions[subscription.sid] = subscription }
      subscription
    end

    # @param sid [String]
    # @return [Subscription, nil]
    def remove(sid)
      @lock.synchronize { @subscriptions.delete(sid) }
    end

    # @param sid [String]
    # @return [Subscription, nil]
    def [](sid)
      @lock.synchronize { @subscriptions[sid] }
    end

    # @return [Array<Subscription>]
    def subscriptions
      @lock.synchronize { @subscriptions.values }
    end

    # @return [Integer]
    def size
      @lock.synchronize { @subscriptions.size }
    end

    # Closes every subscription and empties the registry.
    #
    # @return [void]
    def clear
      @lock.synchronize { @subscriptions.values.tap { @subscriptions.clear } }.each(&:close)
    end

    # @return [void]
    def on_msg(subject, sid, reply_to, header_bytes, payload)
      subscription = self[sid] or return

      subscription.deliver(
        Msg.new(subject: subject, data: payload, reply_to: reply_to,
                header_bytes: header_bytes, subscription: subscription)
      )
      nil
    end

    # @return [void]
    def on_ping = @handlers[:ping]&.call

    # @return [void]
    def on_pong = @handlers[:pong]&.call

    # @return [void]
    def on_ok = nil

    # @return [void]
    def on_info(json) = @handlers[:info]&.call(json)

    # @return [void]
    def on_error(description) = @handlers[:error]&.call(description)

    # @return [void]
    def on_protocol_error(message) = @handlers[:protocol_error]&.call(message)
  end
end
