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

    # @return [Hash, nil] the INFO the server sent on connect
    attr_reader :server_info

    # @return [Symbol] :disconnected, :connected, or :closed
    attr_reader :status

    # @return [Exception, nil] the last asynchronous error seen
    attr_reader :last_error

    # Connects and returns a client, yielding it and closing after if a block is given.
    #
    # @param uri [String, nil]
    # @param options [Hash] CONNECT overrides, plus :connect_timeout and :max_pending
    # @return [Client, Object]
    def self.connect(uri = nil, **)
      client = new(uri, **)
      client.connect
      return client unless block_given?

      begin
        yield client
      ensure
        client.close
      end
    end

    # @param uri [String, nil]
    # @param options [Hash]
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

    # @param subject [String]
    # @param payload [String, nil]
    # @param reply_to [String, nil]
    # @param headers [Headers, nil]
    # @return [void]
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
        sid: next_sid, subject: subject, queue_group: queue,
        max_pending: max_pending, on_slow_consumer: method(:report_slow_consumer), &callback
      )
      @dispatcher.add(subscription)
      @transport.write(Protocol::Encoder.sub(subject, subscription.sid, queue: queue))
      subscription
    end

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
      @transport&.close
      @reader.join(1) if @reader && @reader != Thread.current
      @dispatcher.clear
      @pongs.close
      nil
    end

    # @return [Boolean]
    def connected? = @status == :connected

    # @return [Boolean]
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

    # @return [Integer] largest payload the server accepts
    def max_payload = @server_info&.fetch("max_payload", nil)

    private

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
