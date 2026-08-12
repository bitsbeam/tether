# frozen_string_literal: true

module Tether
  module Connection
    # Request/reply over a single wildcard inbox.
    #
    # One resolver on `<prefix>.<nuid>.*` serves every request of a
    # connection. Replies are matched to waiters on the reader thread by the
    # last subject token, so a reply never waits behind user code.
    class Inbox
      # @return [String] the subject prefix this connection's replies arrive on
      attr_reader :prefix

      # @param client [Client] the connection to publish and subscribe through
      def initialize(client)
        @client = client
        @prefix = client.new_inbox
        @offset = @prefix.length + 1
        @pending = {}
        @lock = Mutex.new
        @counter = 0
        @resolver = nil
        @listener = nil
      end

      # Publishes a request and waits for the first reply.
      #
      # @param subject [String] the subject to send the request to
      # @param payload [String, nil] the request body
      # @param deadline [Deadline] when to give up
      # @param headers [Protocol::Headers, Hash, nil] headers to send with the request
      # @return [Protocol::Msg] the first reply
      # @raise [NoRespondersError] if the server reports nobody is subscribed
      # @raise [TimeoutError] if the deadline passes first
      # @raise [DisconnectedError] if the connection is lost while waiting
      def request(subject, payload, deadline:, headers: nil)
        start
        waiter = Thread::Queue.new
        token = register(waiter)
        @client.publish(subject, payload, reply_to: "#{@prefix}.#{token}", headers: headers, flush: true)
        await(waiter, subject, deadline)
      ensure
        @lock.synchronize { @pending.delete(token) } if token
      end

      # @return [Integer] requests currently waiting for a reply
      def pending
        @lock.synchronize { @pending.size }
      end

      # Releases every waiting request with an error.
      #
      # @param error [Exception] what the waiting callers raise
      # @return [void] nothing
      def fail_all(error)
        waiters = @lock.synchronize { @pending.values.tap { @pending.clear } }
        waiters.each { |waiter| waiter.push(error) }
        nil
      end

      private

      def start
        @lock.synchronize do
          return if @resolver

          @resolver = @client.resolver("#{@prefix}.*") { |msg| resolve(msg) }
          @listener = @client.listen { |event, _| interrupted(event) }
        end
      end

      def interrupted(event)
        case event
        when :disconnect then fail_all(DisconnectedError.new("connection lost while waiting for a reply"))
        when :close then fail_all(ConnectionClosedError.new("connection closed while waiting for a reply"))
        end
      end

      def register(waiter)
        @lock.synchronize do
          token = (@counter += 1).to_s(36)
          @pending[token] = waiter
          token
        end
      end

      def await(waiter, subject, deadline)
        reply = waiter.pop(timeout: deadline.remaining)
        raise TimeoutError, "no reply to #{subject} within the deadline" if reply.nil?
        raise reply if reply.is_a?(Exception)
        raise NoRespondersError, "no responders for #{subject}" if reply.no_responders?

        reply
      end

      def resolve(msg)
        token = msg.subject.byteslice(@offset, msg.subject.bytesize)
        @lock.synchronize { @pending.delete(token) }&.push(msg)
      end
    end
  end
end
