# frozen_string_literal: true

require "json"

module Tether
  module Protocol
    # Everything the server has told this connection: its subscriptions, the
    # INFO it announced, the PONGs it has answered with, and the errors it has
    # reported.
    #
    # The parser hands every frame here, on the reader thread, so each handler
    # does as little as it can. A message is looked up and enqueued; nothing
    # waits, and no user code runs.
    class Dispatcher
      # @return [Registry] the connection's subscriptions
      attr_reader :registry

      # @return [ErrorHolder] errors that arrived outside any call
      attr_reader :errors

      # @return [Hash, nil] the INFO the server last announced
      attr_reader :server_info

      # @param connection [Client] asked for its transport when a PING must be answered
      # @param registry [Registry] the subscriptions to route messages to
      # @param errors [ErrorHolder] where reported errors are collected
      def initialize(connection, registry: Registry.new, errors: ErrorHolder.new)
        @connection = connection
        @registry = registry
        @errors = errors
        @pongs = Thread::Queue.new
      end

      # Waits for the server to answer a PING.
      #
      # @param deadline [Deadline] how long to wait
      # @return [true, nil] nil if the deadline passed or the connection closed
      def await_pong(deadline)
        @pongs.pop(timeout: deadline.remaining)
      end

      # Releases anyone waiting on a PONG and closes every subscription.
      #
      # @return [void] nothing
      def close
        @pongs.close unless @pongs.closed?
        @registry.clear
        nil
      end

      # Routes a message to its subscription, on the reader thread.
      #
      # A message whose sid is unknown, because the subscription was cancelled
      # while it was in flight, is dropped rather than raised over.
      #
      # @return [void] nothing
      def on_msg(subject, sid, reply_to, header_bytes, payload)
        subscription = @registry[sid] or return

        subscription.deliver(
          Msg.new(subject: subject, data: payload, reply_to: reply_to,
                  header_bytes: header_bytes, subscription: subscription)
        )
        nil
      end

      # Answered inline rather than queued behind pending writes: a late PONG
      # is what gets a connection dropped as stale.
      #
      # @return [void] nothing
      def on_ping
        @connection.transport&.write(Protocol::PONG)
        nil
      end

      # Completes a {Client#flush}.
      #
      # @return [void] nothing
      def on_pong
        @pongs.push(true) unless @pongs.closed?
        nil
      end

      # Ignored: tether turns off verbose mode, so the server sends these only
      # if a caller asked for them explicitly.
      #
      # @return [void] nothing
      def on_ok = nil

      # Arrives once at connect and again whenever cluster topology changes.
      #
      # @param json [String] the still unparsed INFO payload
      # @return [void] nothing
      def on_info(json)
        @server_info = JSON.parse(json)
        nil
      end

      # An `-ERR` line, which may or may not be followed by the server closing
      # the connection. See {ServerError.fatal?}.
      #
      # @param description [String] the text between the quotes
      # @return [void] nothing
      def on_error(description)
        @errors.record(ServerError.for(description))
        nil
      end

      # The parser gave up on the stream; the connection is no longer usable.
      #
      # @param message [String] what the parser refused to accept
      # @return [void] nothing
      def on_protocol_error(message)
        @errors.record(ProtocolError.new(message))
        nil
      end
    end
  end
end
