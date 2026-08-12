# frozen_string_literal: true

module Tether
  module Protocol
    # The live subscriptions of a connection, by sid.
    #
    # An entry is anything that responds to `sid`, `deliver`, `replay_frames`,
    # `close` and `abandon`: a {Connection::Subscription} or a
    # {Connection::Resolver}.
    class Registry
      def initialize
        @entries = {}
        @lock = Mutex.new
        @sid = 0
      end

      # @return [String] a sid no entry of this connection has used
      def next_sid
        @lock.synchronize { (@sid += 1).to_s }
      end

      # @param entry [Object] the entry to route messages to
      # @return [Object] the entry
      def add(entry)
        @lock.synchronize { @entries[entry.sid] = entry }
        entry
      end

      # @param sid [String] the sid to stop routing
      # @return [Object, nil] the entry that was removed
      def remove(sid)
        @lock.synchronize { @entries.delete(sid) }
      end

      # @param sid [String] the sid a message arrived for
      # @return [Object, nil] the entry, if it is still registered
      def [](sid)
        @lock.synchronize { @entries[sid] }
      end

      # @return [Array<Object>] every entry
      def to_a
        @lock.synchronize { @entries.values }
      end

      # @return [Integer] how many entries are registered
      def size
        @lock.synchronize { @entries.size }
      end

      # @return [String] the SUB and UNSUB frames that restore every entry on a new connection
      def replay_frames
        @lock.synchronize { @entries.each_value.map(&:replay_frames).join }
      end

      # Removes and closes every entry.
      #
      # @param error [Exception, nil] why, when it was not requested
      # @return [void] nothing
      def close(error = nil)
        take.each { |entry| entry.close(error) }
        nil
      end

      # Removes every entry without running any of their callbacks, for a
      # forked child that inherited them.
      #
      # @return [void] nothing
      def abandon
        take.each(&:abandon)
        nil
      end

      private

      def take
        @lock.synchronize { @entries.values.tap { @entries.clear } }
      end
    end
  end
end
