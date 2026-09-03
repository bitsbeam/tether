# frozen_string_literal: true

module Tether
  # Request/reply over a single wildcard inbox subscription.
  #
  # A new SUB/UNSUB pair per request would make every request a round trip to
  # the server before the payload even goes out. Instead one subscription on
  # `_INBOX.<nuid>.*` covers the whole connection and replies are demultiplexed
  # in memory by the last subject token.
  class InboxMux
    INBOX_PREFIX = "_INBOX"

    # @return [String] the subject prefix this connection's replies arrive on
    attr_reader :prefix

    # Builds the mux without subscribing; {#start} runs on the first request.
    #
    # @param client [Client]
    # @param prefix [String, nil] defaults to a fresh `_INBOX.<nuid>`
    # @param max_pending [Integer] queue depth for the shared inbox subscription
    def initialize(client, prefix: nil, max_pending: Subscription::DEFAULT_MAX_PENDING)
      @client = client
      @prefix = prefix || "#{INBOX_PREFIX}.#{NUID.next}"
      @max_pending = max_pending
      @pending = {}
      @lock = Mutex.new
      @counter = 0
    end

    # Publishes a request and waits for the first reply.
    #
    # @param subject [String]
    # @param payload [String, nil]
    # @param deadline [Deadline]
    # @param headers [Headers, nil]
    # @return [Msg]
    # @raise [NoRespondersError] if the server reports nobody is subscribed
    # @raise [TimeoutError] if the deadline passes first
    def request(subject, payload = nil, deadline: Deadline.infinite, headers: nil)
      start
      token = next_token
      waiter = Thread::Queue.new
      @lock.synchronize { @pending[token] = waiter }

      @client.publish(subject, payload, reply_to: "#{@prefix}.#{token}", headers: headers)
      await(waiter, subject, deadline)
    ensure
      @lock.synchronize { @pending.delete(token) } if token
    end

    # Subscribes the shared inbox, once.
    #
    # @return [Subscription]
    def start
      @lock.synchronize do
        return @subscription if @subscription

        @subscription = @client.subscribe("#{@prefix}.*", max_pending: @max_pending) { |msg| resolve(msg) }
      end
    end

    # The inbox is subscribed lazily, on the first request, so a connection
    # that never makes one costs nothing.
    #
    # @return [Boolean] whether the shared inbox subscription exists yet
    def started?
      @lock.synchronize { !@subscription.nil? }
    end

    # Should return to zero once requests settle; a number that only grows is
    # a leak.
    #
    # @return [Integer] requests currently waiting for a reply
    def pending
      @lock.synchronize { @pending.size }
    end

    # Releases every waiting request, so callers blocked in {#request} raise
    # rather than waiting out their deadlines.
    #
    # @return [void]
    def close
      waiters = @lock.synchronize { @pending.values.tap { @pending.clear } }
      waiters.each(&:close)
      nil
    end

    private

    def await(waiter, subject, deadline)
      msg = waiter.pop(timeout: deadline.remaining)
      raise ConnectionClosedError, "connection closed while waiting on #{subject}" if msg.nil? && waiter.closed?
      raise TimeoutError, "no reply to #{subject} within the deadline" if msg.nil?
      raise NoRespondersError, "no responders for #{subject}" if msg.no_responders?

      msg
    end

    # Runs on the inbox subscription's worker. A reply whose token has already
    # timed out finds no waiter and is dropped rather than raising.
    def resolve(msg)
      token = msg.subject[(@prefix.length + 1)..]
      waiter = @lock.synchronize { @pending[token] }
      waiter&.push(msg)
    rescue ClosedQueueError
      nil
    end

    def next_token
      @lock.synchronize { (@counter += 1).to_s(36) }
    end
  end
end
