# frozen_string_literal: true

require "json"

module Tether
  module Protocol
    # Receives every frame the parser produces, on the reader thread, and does
    # the minimum for each: a message is looked up and handed to its entry,
    # a PING is answered by appending to the write buffer, and everything else
    # is passed to the connection. Nothing here waits and no user code runs.
    class Dispatcher
      # @param connection [Connection::Lifecycle] told about INFO, errors and PONGs
      # @param registry [Registry] the entries to route messages to
      # @param writer [Connection::Writer] where a PONG is appended
      # @param counters [Counters] the connection's traffic counters
      def initialize(connection:, registry:, writer:, counters:)
        @connection = connection
        @registry = registry
        @writer = writer
        @counters = counters
      end

      # @param subject [String] the subject the message was published to
      # @param sid [String] the subscription it matched
      # @param reply_to [String, nil] where a reply should go
      # @param header_bytes [String, nil] the unparsed header block
      # @param payload [String] the message body
      # @return [void] nothing
      def on_msg(subject, sid, reply_to, header_bytes, payload)
        @counters.messages_in += 1
        entry = @registry[sid] or return
        entry.deliver(Msg.new(subject: subject, data: payload, reply_to: reply_to,
                              header_bytes: header_bytes, subscription: entry))
        nil
      end

      # @return [void] nothing
      def on_ping
        @writer.pong
      end

      # @return [void] nothing
      def on_pong
        @connection.pong_received
        @writer.resolve_pong
      end

      # @return [void] nothing
      def on_ok = nil

      # @param json [String] the INFO document
      # @return [void] nothing
      def on_info(json)
        @connection.info_received(JSON.parse(json))
        nil
      rescue JSON::ParserError => e
        on_protocol_error("malformed INFO: #{e.message}")
      end

      # @param description [String] the text of an `-ERR` line
      # @return [void] nothing
      def on_error(description)
        @connection.error_received(ServerError.for(description))
        nil
      end

      # @param message [String] what the parser could not accept
      # @return [void] nothing
      def on_protocol_error(message)
        @connection.error_received(ProtocolError.new(message))
        nil
      end
    end
  end
end
