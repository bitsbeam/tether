# frozen_string_literal: true

require "json"

module Tether
  # A connection to a NATS server.
  #
  # One reader thread parses the socket and enqueues to subscriptions; each
  # subscription runs its callback on its own worker. Writes take a lock that
  # spans a whole frame. Nothing here uses `Timeout.timeout` or `Thread#raise`.
  class Client
    DEFAULT_URI = "nats://127.0.0.1:4222"
    DEFAULT_CONNECT_TIMEOUT = 2
    DEFAULT_FLUSH_TIMEOUT = 10
    DEFAULT_REQUEST_TIMEOUT = 2

    # @return [Symbol] :disconnected, :connected, or :closed
    attr_reader :status

    # The socket this connection reads and writes. Exposed because the
    # dispatcher answers PING from the reader thread.
    #
    # @return [Connection::Transport, nil] nil until {#connect} has run
    attr_reader :transport

    # Connects and returns a client, yielding it and closing after if a block is given.
    #
    # Connects, and when a block is given yields the client and closes it after.
    #
    # @param uri [String, nil] the server to reach; defaults to NATS_URL, then nats://127.0.0.1:4222
    # @param options [Hash{Symbol => Object}] CONNECT overrides, plus :connect_timeout and :max_pending
    # @return [Client, Object] the client, or the block's value
    # @raise [ConnectionError] if the server cannot be reached
    def self.connect(uri = nil, **options)
      client = new(uri, **options)
      client.connect
      return client unless block_given?

      begin
        yield client
      ensure
        client.close
      end
    end

    # Builds a client without opening a socket. Call {#connect}, or use
    # {.connect} to do both.
    #
    # @param uri [String, nil] the server to reach; defaults to NATS_URL, then nats://127.0.0.1:4222
    # @param options [Hash{Symbol => Object}] CONNECT fields to override, plus :connect_timeout and :max_pending
    def initialize(uri = nil, **options)
      @uri = uri || ENV.fetch("NATS_URL", DEFAULT_URI)
      @options = options
      @status = :disconnected
      @sid_counter = 0
      @lock = Mutex.new
    end

    # Opens the socket, exchanges INFO and CONNECT, and starts the reader.
    #
    # @param timeout [Numeric, nil] seconds allowed for the socket, the handshake, and the first flush
    # @return [self] this client, connected
    def connect(timeout: @options.fetch(:connect_timeout, DEFAULT_CONNECT_TIMEOUT))
      deadline = Deadline.after(timeout)
      @transport = Connection::Transport.connect(@uri, deadline: deadline)
      read_server_info(deadline)
      @transport.write(Protocol::Encoder.connect(connect_options))
      @status = :connected
      start_reader
      flush(timeout: deadline.remaining)
      self
    end

    # Publishes a message.
    #
    # Fire and forget: the server sends no acknowledgement, so a successful
    # return means the frame reached the socket, not that the server processed
    # it. Follow with {#flush} when you need that guarantee.
    #
    # @param subject [String] the subject to publish to
    # @param payload [String, nil] the message body; nil sends an empty payload
    # @param reply_to [String, nil] a subject a responder should send its answer to
    # @param headers [Protocol::Headers, nil] headers to send alongside the payload
    # @return [void] nothing; the server does not acknowledge a publish
    # @raise [MaxPayloadError] if the frame exceeds what the server advertised
    def publish(subject, payload = nil, reply_to: nil, headers: nil)
      ensure_connected!
      frame = if headers
                Protocol::Encoder.hpub(subject, payload, headers, reply_to)
              else
                Protocol::Encoder.pub(subject, payload, reply_to)
              end
      check_payload_size!(subject, frame)
      @transport.write(frame)
      nil
    end

    # Subscribes, delivering to the block on a worker thread when one is given.
    #
    # @param subject [String] the subject or wildcard pattern to receive on
    # @param queue [String, nil] a queue group to join, so the server gives each
    #   message to only one member
    # @param max_pending [Integer] how many messages may queue before this counts as a slow consumer
    # @return [Connection::Subscription] the subscription, which the caller keeps in order to cancel it
    def subscribe(subject, queue: nil, max_pending: Connection::Subscription::DEFAULT_MAX_PENDING, &callback)
      ensure_connected!
      subscription = Connection::Subscription.new(
        sid: next_sid, subject: subject, queue_group: queue, max_pending: max_pending,
        on_slow_consumer: method(:report_slow_consumer), responder: self, &callback
      )
      dispatcher.registry.add(subscription)
      @transport.write(Protocol::Encoder.sub(subject, subscription.sid, queue: queue))
      subscription
    end

    # Cancels a subscription.
    #
    # With max_msgs the server keeps delivering until that many more messages
    # have arrived, so the subscription stays registered and its worker keeps
    # running until then.
    #
    # @param subscription [Connection::Subscription] the subscription to cancel
    # @param max_msgs [Integer, nil] unsubscribe only after this many more messages
    # @return [void] nothing
    def unsubscribe(subscription, max_msgs = nil)
      @transport.write(Protocol::Encoder.unsub(subscription.sid, max_msgs)) unless @transport.closed?
      return if max_msgs

      dispatcher.registry.remove(subscription.sid)
      subscription.close
      nil
    end

    # Publishes a request and waits for the first reply.
    #
    # Every request on a connection shares one inbox subscription; replies are
    # demultiplexed in memory, so N concurrent requests cost one subscription.
    #
    # @param subject [String] the subject to send the request to
    # @param payload [String, nil] the request body
    # @param timeout [Numeric, nil] seconds to wait for the first reply
    # @param headers [Protocol::Headers, nil] headers to send with the request
    # @return [Protocol::Msg] the first reply to arrive
    # @raise [NoRespondersError] if nobody is subscribed to the subject
    # @raise [TimeoutError] if no reply arrives in time
    def request(subject, payload = nil, timeout: DEFAULT_REQUEST_TIMEOUT, headers: nil)
      ensure_connected!
      inbox.request(subject, payload, deadline: Deadline.after(timeout), headers: headers)
    end

    # Builds a reply subject for a subscribe-then-publish exchange, which is
    # what you want when a single request may draw several replies.
    #
    # @return [String] a fresh inbox subject, unrelated to the shared request inbox
    def build_inbox_subject
      "#{Connection::Inbox::INBOX_PREFIX}.#{Connection::NUID.next}"
    end

    # Round trips a PING, which the server answers only after everything sent
    # before it has been processed.
    #
    # @param timeout [Numeric, nil] seconds to wait for the server's PONG
    # @return [true] always true; it raises rather than returning false
    # @raise [TimeoutError] if the server does not answer in time
    def flush(timeout: DEFAULT_FLUSH_TIMEOUT)
      ensure_connected!
      @transport.write(Protocol::PING)
      raise TimeoutError, "flush timed out after #{timeout}s" if dispatcher.await_pong(Deadline.after(timeout)).nil?

      true
    end

    # Closes the socket, which unblocks the reader's pending read so it can
    # exit on its own. Nothing is killed: a thread stopped mid-parse would
    # leave shared state in whatever shape the interrupt found it.
    #
    # @return [void] nothing
    def close
      return if @status == :closed

      @status = :closed
      @inbox&.close
      @transport&.close
      @reader.join(1) if @reader && @reader != Thread.current
      dispatcher.close
      nil
    end

    # @return [Boolean] whether the handshake completed and the socket is live
    def connected? = @status == :connected

    # @return [Boolean] whether {#close} has run; a closed client cannot be reopened
    def closed? = @status == :closed

    # Registers a handler for errors that arrive outside any call.
    #
    # Errors are never raised into a caller's thread: they are recorded and
    # passed here, on the reader thread.
    #
    # @yieldparam error [Exception] the error that arrived, on the reader thread
    # @return [void] nothing
    def on_error(&handler) = dispatcher.errors.subscribe(&handler)

    # @return [Hash, nil] the INFO the server announced, replaced on every update
    def server_info = dispatcher.server_info

    # @return [Exception, nil] the last error that arrived outside a call
    def last_error = dispatcher.errors.last

    # Counts live subscriptions. Request/reply adds exactly one, however many
    # requests are in flight.
    #
    # @return [Integer] subscriptions currently registered, including the shared request inbox
    def subscription_count = dispatcher.registry.size

    # The ceiling the server advertised in INFO. {#publish} refuses anything
    # larger rather than letting the server close the connection over it.
    #
    # @return [Integer] largest payload the server accepts
    def max_payload = server_info&.fetch("max_payload", nil)

    private

    def inbox
      @lock.synchronize do
        @inbox ||= Connection::Inbox.new(
          self, max_pending: @options.fetch(:max_pending, Connection::Subscription::DEFAULT_MAX_PENDING)
        )
      end
    end

    # The parser's dispatcher, and the registry of live subscriptions. Built on first
    # use, which happens on the connecting thread before the reader starts.
    #
    # @return [Protocol::Dispatcher] the dispatcher for this connection
    def dispatcher
      @dispatcher ||= Protocol::Dispatcher.new(self)
    end

    # @return [Protocol::Parser] the parser fed by the reader thread
    def parser
      @parser ||= Protocol::Parser.new(dispatcher)
    end

    def read_server_info(deadline)
      parser.feed(@transport.read(deadline)) until dispatcher.server_info
      parser.max_payload = max_payload
    end

    def connect_options
      allowed = @options.except(:connect_timeout, :max_pending)
      allowed.merge(name: @options[:name] || "tether")
    end

    def start_reader
      @reader = Thread.new do
        Thread.current.name = "tether:reader"
        read_loop
      end
    end

    def read_loop
      until @transport.closed? || parser.broken?
        data = @transport.read
        break if data.nil?

        parser.feed(data)
      end
    rescue ConnectionClosedError, TimeoutError => e
      dispatcher.errors.record(e) unless @status == :closed
    rescue StandardError => e
      dispatcher.errors.record(e)
    ensure
      dispatcher.close
    end

    def next_sid
      @lock.synchronize { (@sid_counter += 1).to_s }
    end

    def check_payload_size!(subject, frame)
      limit = max_payload
      return unless limit && frame.bytesize > limit

      raise MaxPayloadError, "payload for #{subject} exceeds the server's max_payload of #{limit}"
    end

    def report_slow_consumer(subscription)
      dispatcher.errors.record(SlowConsumerError.new("dropped a message on #{subscription.subject}"))
    end

    def ensure_connected!
      raise ConnectionClosedError, "connection is closed" if @status == :closed
      raise ConnectionError, "not connected" unless @status == :connected
    end
  end
end
