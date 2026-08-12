# frozen_string_literal: true

module Tether
  module Connection
    # Sends a PING on an interval and reports the connection as stale when
    # too many go unanswered.
    class Keepalive
      # @param timer [Timer] the connection's timer
      # @param writer [Writer] where the PING is appended
      # @param interval [Numeric, nil] seconds between PINGs; nil disables
      # @param max_out [Integer] unanswered PINGs that mark the connection stale
      # @param on_stale [Proc] called with no arguments when the connection is stale
      def initialize(timer:, writer:, interval:, max_out:, on_stale:)
        @timer = timer
        @writer = writer
        @interval = interval
        @max_out = max_out
        @on_stale = on_stale
        @outstanding = 0
        @entry = nil
      end

      # Starts pinging a fresh connection.
      #
      # @return [void] nothing
      def start
        stop
        @outstanding = 0
        @entry = @timer.every(@interval) { tick } if @interval
        nil
      end

      # Stops pinging.
      #
      # @return [void] nothing
      def stop
        @entry&.cancel
        @entry = nil
      end

      # Records that the server answered.
      #
      # @return [void] nothing
      def pong
        @outstanding = 0
        nil
      end

      private

      def tick
        return @on_stale.call if @outstanding >= @max_out

        @outstanding += 1
        @writer.keepalive_ping
      end
    end
  end
end
