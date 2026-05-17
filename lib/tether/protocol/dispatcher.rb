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
        @pongs = []
        @pong_lock = Mutex.new
        @closed = false
      end

      # Sends a PING and waits for the server to answer it, which it only does
      # once it has processed everything sent before that PING.
      #
      # The waiter is queued and the PING written under one lock. Registering
      # after writing would let the answer arrive before anything was waiting
      # for it, and writing outside the lock would let two threads queue in one
      # order and write in the other, so a caller could be woken by a PING that
      # went out before its own work did.
      #
      # @param deadline [Deadline] how long to wait for the answer
      # @return [true] once the server has caught up
      # @raise [TimeoutError] if the server does not answer in time
      # @raise [ConnectionClosedError] if the connection goes away first
      def ping(deadline)
        waiter = queue_ping
        return true if waiter.pop(timeout: deadline.remaining)
        raise ConnectionClosedError, "connection closed while waiting for a PONG" if waiter.closed?

        raise TimeoutError, "the server did not answer a PING within the deadline"
      ensure
        @pong_lock.synchronize { @pongs.delete(waiter) } if waiter
      end

      # Releases anyone waiting on a PONG and closes every subscription.
      #
      # @return [void] nothing
      def close
        @pong_lock.synchronize do
          @closed = true
          @pongs.each(&:close)
          @pongs.clear
        end
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
        @connection.transport&.write(Protocol::PONG, flush: true)
        nil
      end

      # Answers the oldest waiting {#ping}, and only that one. A PONG proves the
      # server reached the PING that earned it, so handing it to any other
      # waiter would report progress that has not happened.
      #
      # @return [void] nothing
      def on_pong
        waiter = @pong_lock.synchronize { @pongs.shift }
        waiter&.push(true)
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

      private

      # Holds one answer, so {#on_pong} never blocks on a waiter that gave up
      # at the same moment.
      def queue_ping
        transport = @connection.transport
        raise ConnectionClosedError, "connection is closed" if transport.nil?

        waiter = Thread::SizedQueue.new(1)
        @pong_lock.synchronize do
          raise ConnectionClosedError, "connection is closed" if @closed

          @pongs << waiter
          transport.write(Protocol::PING, flush: true)
        end
        waiter
      end
    end
  end
end
