# frozen_string_literal: true

module Tether
  module Protocol
    # The connection's subscriptions, keyed by the token the server echoes back
    # on every message.
    #
    # Registration happens on whichever thread called subscribe, and lookup
    # happens on the reader thread for every frame, so the map is guarded.
    class Registry
      def initialize
        @subscriptions = {}
        @lock = Mutex.new
      end

      # Registers a subscription so messages carrying its sid reach it.
      #
      # @param subscription [Connection::Subscription] the subscription to route messages to
      # @return [Connection::Subscription] the subscription that was registered
      def add(subscription)
        @lock.synchronize { @subscriptions[subscription.sid] = subscription }
        subscription
      end

      # Stops routing to a subscription. Messages already in flight for it are
      # dropped when they arrive.
      #
      # @param sid [String] the token the subscription was registered under
      # @return [Connection::Subscription, nil] the subscription that was registered, if any
      def remove(sid)
        @lock.synchronize { @subscriptions.delete(sid) }
      end

      # Looks up a subscription by the token the server echoed back.
      #
      # @param sid [String] the token echoed back by the server
      # @return [Connection::Subscription, nil] nil once the subscription has been removed
      def [](sid)
        @lock.synchronize { @subscriptions[sid] }
      end

      # @return [Array<Connection::Subscription>] a snapshot, safe to iterate while others subscribe
      def to_a
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
    end
  end
end
