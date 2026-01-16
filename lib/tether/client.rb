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

    # @return [Hash, nil] the INFO the server sent on connect
    attr_reader :server_info

    # @return [Symbol] :disconnected, :connected, or :closed
    attr_reader :status

    # @return [Exception, nil] the last asynchronous error seen
    attr_reader :last_error

    # Connects and returns a client, yielding it and closing after if a block is given.
    #
    # Connects, and when a block is given yields the client and closes it after.
    #
    # @param uri [String, nil]
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
    # @param uri [String, nil]
    # @param options [Hash{Symbol => Object}]
    def initialize(uri = nil, **options)
      @uri = uri || ENV.fetch("NATS_URL", DEFAULT_URI)
      @options = options
      @status = :disconnected
      @sid_counter = 0
      @lock = Mutex.new
      @pongs = Thread::Queue.new
      @error_handlers = []
      @dispatcher = build_dispatcher
      @parser = Protocol::Parser.new(@dispatcher)
    end

    # Opens the socket, exchanges INFO and CONNECT, and starts the reader.
    #
    # @param timeout [Numeric, nil]
    # @return [self]
    def connect(timeout: @options.fetch(:connect_timeout, DEFAULT_CONNECT_TIMEOUT))
      deadline = Deadline.after(timeout)
      @transport = Transport.connect(@uri, deadline: deadline)
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
    # @param subject [String]
    # @param payload [String, nil]
    # @param reply_to [String, nil]
    # @param headers [Headers, nil]
    # @return [void]
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
    # @param subject [String]
    # @param queue [String, nil] queue group
    # @param max_pending [Integer]
    # @return [Subscription]
    def subscribe(subject, queue: nil, max_pending: Subscription::DEFAULT_MAX_PENDING, &callback)
      ensure_connected!
      subscription = Subscription.new(
        sid: next_sid, subject: subject, queue_group: queue, max_pending: max_pending,
        on_slow_consumer: method(:report_slow_consumer), responder: self, &callback
      )
      @dispatcher.add(subscription)
      @transport.write(Protocol::Encoder.sub(subject, subscription.sid, queue: queue))
      subscription
    end

    # Cancels a subscription.
    #
    # With max_msgs the server keeps delivering until that many more messages
    # have arrived, so the subscription stays registered and its worker keeps
    # running until then.
    #
    # @param subscription [Subscription]
    # @param max_msgs [Integer, nil] unsubscribe only after this many more messages
    # @return [void]
    def unsubscribe(subscription, max_msgs = nil)
      @transport.write(Protocol::Encoder.unsub(subscription.sid, max_msgs)) unless @transport.closed?
      return if max_msgs

      @dispatcher.remove(subscription.sid)
      subscription.close
      nil
    end

    # Publishes a request and waits for the first reply.
    #
    # Every request on a connection shares one inbox subscription; replies are
    # demultiplexed in memory, so N concurrent requests cost one subscription.
    #
    # @param subject [String]
    # @param payload [String, nil]
    # @param timeout [Numeric, nil]
    # @param headers [Headers, nil]
    # @return [Msg]
    # @raise [NoRespondersError] if nobody is subscribed to the subject
    # @raise [TimeoutError] if no reply arrives in time
    def request(subject, payload = nil, timeout: DEFAULT_REQUEST_TIMEOUT, headers: nil)
      ensure_connected!
      inbox_mux.request(subject, payload, deadline: Deadline.after(timeout), headers: headers)
    end

    # Builds a reply subject for a subscribe-then-publish exchange, which is
    # what you want when a single request may draw several replies.
    #
    # @return [String] a fresh inbox subject, unrelated to the shared request inbox
    def new_inbox
      "#{InboxMux::INBOX_PREFIX}.#{NUID.next}"
    end

    # Round trips a PING, which the server answers only after everything sent
    # before it has been processed.
    #
    # @param timeout [Numeric, nil]
    # @return [true]
    # @raise [TimeoutError]
    def flush(timeout: DEFAULT_FLUSH_TIMEOUT)
      ensure_connected!
      @transport.write(Protocol::PING)
      raise TimeoutError, "flush timed out after #{timeout}s" if @pongs.pop(timeout: timeout).nil?

      true
    end

    # Closes the socket, which unblocks the reader's pending read so it can
    # exit on its own. Nothing is killed: a thread stopped mid-parse would
    # leave shared state in whatever shape the interrupt found it.
    #
    # @return [void]
    def close
      return if @status == :closed

      @status = :closed
      @inbox_mux&.close
      @transport&.close
      @reader.join(1) if @reader && @reader != Thread.current
      @dispatcher.clear
      @pongs.close
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
    # @yieldparam error [Exception]
    # @return [void]
    def on_error(&handler)
      @error_handlers << handler
      nil
    end

    # Counts live subscriptions. Request/reply adds exactly one, however many
    # requests are in flight.
    #
    # @return [Integer] subscriptions currently registered, including the shared request inbox
    def subscription_count = @dispatcher.size

    # The ceiling the server advertised in INFO. {#publish} refuses anything
    # larger rather than letting the server close the connection over it.
    #
    # @return [Integer] largest payload the server accepts
    def max_payload = @server_info&.fetch("max_payload", nil)

    private

    def inbox_mux
      @lock.synchronize { @inbox_mux ||= InboxMux.new(self, max_pending: @options.fetch(:max_pending, Subscription::DEFAULT_MAX_PENDING)) }
    end

    def build_dispatcher
      Dispatcher.new(
        ping: -> { @transport.write(Protocol::PONG) },
        pong: -> { @pongs.push(true) unless @pongs.closed? },
        info: ->(json) { @server_info = JSON.parse(json) },
        error: ->(description) { record_error(ServerError.for(description)) },
        protocol_error: ->(message) { record_error(ProtocolError.new(message)) }
      )
    end

    def read_server_info(deadline)
      @parser << @transport.read(deadline) until @server_info
      @parser.max_payload = @server_info["max_payload"]
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
      until @transport.closed? || @parser.broken?
        data = @transport.read
        break if data.nil?

        @parser << data
      end
    rescue ConnectionClosedError, TimeoutError => e
      record_error(e) unless @status == :closed
    rescue StandardError => e
      record_error(e)
    ensure
      @pongs.close unless @pongs.closed?
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
      record_error(SlowConsumerError.new("dropped a message on #{subscription.subject}"))
    end

    def record_error(error)
      @last_error = error
      @error_handlers.each { |handler| handler.call(error) }
    rescue StandardError
      nil
    end

    def ensure_connected!
      raise ConnectionClosedError, "connection is closed" if @status == :closed
      raise ConnectionError, "not connected" unless @status == :connected
    end
  end
end
