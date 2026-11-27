# frozen_string_literal: true

require "json"

module Tether
  # A connection to a NATS server or cluster.
  #
  # One client is safe to use from any number of threads. A reader thread
  # parses the socket and never runs user code; each subscription calls its
  # block on its own worker; and every blocking call is bounded by a
  # {Deadline}.
  class Client
    DEFAULT_URL = "nats://127.0.0.1:4222"
    DEFAULT_REQUEST_TIMEOUT = 2
    DEFAULT_FLUSH_TIMEOUT = 10

    # Publish subjects that passed validation are remembered, up to this many,
    # so a hot subject is a hash lookup instead of a regex scan on every call.
    CHECKED_SUBJECTS_LIMIT = 1024

    # @return [Options] the validated options this client was built with
    attr_reader :options

    # Connects and returns a client. With a block, yields the client and
    # closes it afterwards.
    #
    # @param urls [String, Array<String>, nil] one URL, a comma separated list, or an array;
    #   defaults to NATS_URL, then nats://127.0.0.1:4222
    # @param options [Hash{Symbol => Object}] see {Options::DEFAULTS}
    # @return [Client, Object] the client, or the block's value
    # @raise [ConnectionError, ServerError, TimeoutError] if no server could be reached
    def self.connect(urls = nil, **options)
      client = new(urls, **options).connect
      return client unless block_given?

      begin
        yield client
      ensure
        client.close
      end
    end

    # Builds a client without connecting, so handlers can be registered first.
    #
    # @param urls [String, Array<String>, nil] one URL, a comma separated list, or an array
    # @param options [Hash{Symbol => Object}] see {Options::DEFAULTS}
    # @raise [ArgumentError] if an option is unknown or unusable
    def initialize(urls = nil, **options)
      @options = Options.new(**options)
      @connection = Connection::Lifecycle.new(urls || ENV.fetch("NATS_URL", DEFAULT_URL), @options)
      @lock = Mutex.new
      @inbox = nil
      @inbox_pid = nil
      @checked_subjects = {}
    end

    # Connects to the first reachable server.
    #
    # @return [self] this client
    # @raise [ConnectionError, ServerError, TimeoutError] if no server could be reached
    def connect
      @connection.connect
      self
    end

    # Publishes a message. Fire and forget: the server does not acknowledge it.
    #
    # @param subject [String] the subject to publish to
    # @param payload [String, nil] the body, sent as bytes; nil sends an empty message
    # @param reply_to [String, nil] a subject a responder should answer on
    # @param headers [Protocol::Headers, Hash, nil] headers to send with the message
    # @param flush [Boolean] write now instead of waiting for the flusher
    # @return [self] this client
    # @raise [InvalidSubjectError] if the subject or reply cannot be published to
    # @raise [MaxPayloadError] if headers and payload exceed the server's limit
    # @raise [ReconnectBufferFullError, DisconnectedError] during an outage
    # @raise [ConnectionClosedError] if the connection is closed
    def publish(subject, payload = nil, reply_to: nil, headers: nil, flush: false)
      ensure_started
      check_subject(subject)
      Protocol::Subject.check!(reply_to, "reply subject") if reply_to
      @connection.writer.write(encode(subject, payload, reply_to, headers), flush: flush, messages: 1)
      self
    end

    # Subscribes to a subject. With a block, messages are delivered to it on
    # the client's worker threads, shared by every subscription and capped by
    # the `worker_threads:` option; without one, take them with
    # {Connection::Subscription#next}.
    #
    # @param subject [String] the subject or wildcard pattern
    # @param queue [String, nil] a queue group, so each message goes to one member
    # @param max [Integer, nil] end the subscription after this many messages
    # @param max_pending [Integer, nil] messages that may wait before newer ones are dropped
    # @param max_pending_bytes [Integer, nil] bytes that may wait before newer messages are dropped
    # @param concurrency [Integer] workers that may run the block at once; more than one gives up ordering
    # @param dedicated [Boolean] run the block on threads of its own, for a block that blocks for long
    # @yieldparam msg [Protocol::Msg] each message
    # @return [Connection::Subscription] the subscription
    # @raise [InvalidSubjectError] if the subject or queue is malformed
    # @raise [ConnectionDrainingError, ConnectionClosedError] if the connection is ending
    def subscribe(subject, queue: nil, max: nil, max_pending: nil, max_pending_bytes: nil, concurrency: 1,
                  dedicated: false, &block)
      ensure_accepting
      Protocol::Subject.pattern!(subject)
      Protocol::Subject.check!(queue, "queue group") if queue
      register(Connection::Subscription.new(
                 client: self, sid: @connection.registry.next_sid, subject: subject, queue: queue, max: max,
                 max_pending: max_pending || @options.max_pending,
                 max_pending_bytes: max_pending_bytes || @options.max_pending_bytes, concurrency: concurrency,
                 dedicated: dedicated, &block
               ))
    end

    # Publishes a request and waits for the first reply.
    #
    # @param subject [String] the subject to send the request to
    # @param payload [String, nil] the request body
    # @param headers [Protocol::Headers, Hash, nil] headers to send with the request
    # @param timeout [Numeric, Deadline, nil] how long to wait
    # @return [Protocol::Msg] the first reply
    # @raise [NoRespondersError] if nobody is subscribed to the subject
    # @raise [TimeoutError] if no reply arrives in time
    # @raise [DisconnectedError] if the connection is lost while waiting
    def request(subject, payload = nil, headers: nil, timeout: DEFAULT_REQUEST_TIMEOUT)
      @connection.check_fork
      inbox.request(subject, payload, deadline: Deadline.coerce(timeout), headers: headers)
    end

    # Publishes a request and collects every reply until one of the limits is met.
    #
    # @param subject [String] the subject to send the request to
    # @param payload [String, nil] the request body
    # @param headers [Protocol::Headers, Hash, nil] headers to send with the request
    # @param max [Integer, nil] stop after this many replies
    # @param stall [Numeric, nil] stop when no reply has arrived for this many seconds
    # @param sentinel [#call, nil] stop when it returns true for a reply, which is not included
    # @param timeout [Numeric, Deadline, nil] stop after this long
    # @yieldparam msg [Protocol::Msg] each reply as it arrives, when a block is given
    # @return [Array<Protocol::Msg>, Integer] the replies, or how many were yielded
    # @raise [NoRespondersError] if nobody is subscribed and nothing was received
    def request_many(subject, payload = nil, headers: nil, max: nil, stall: nil, sentinel: nil,
                     timeout: DEFAULT_REQUEST_TIMEOUT, &block)
      @connection.check_fork
      replies = Thread::Queue.new
      reply_to = new_inbox
      resolver = resolver(reply_to) { |msg| replies.push(msg) }
      publish(subject, payload, reply_to: reply_to, headers: headers, flush: true)
      collect(replies, subject, Deadline.coerce(timeout), max: max, stall: stall, sentinel: sentinel, &block)
    ensure
      resolver&.unsubscribe
    end

    # Returns a JetStream context for this connection.
    #
    # @param options [Hash{Symbol => Object}] `domain:`, `api_prefix:`, `timeout:` and the asynchronous
    #   publish settings; see {JetStream#initialize}
    # @return [JetStream] a new context
    def jetstream(**options)
      JetStream.new(self, **options)
    end

    # @return [String] a fresh, unique reply subject
    def new_inbox
      "#{@options.inbox_prefix}.#{Connection::NUID.next}"
    end

    # Round trips a PING, which the server answers only after processing
    # everything sent before it.
    #
    # @param timeout [Numeric, Deadline, nil] how long to wait
    # @return [self] this client
    # @raise [TimeoutError] if the server does not answer in time
    # @raise [DisconnectedError] if there is no live connection
    def flush(timeout: DEFAULT_FLUSH_TIMEOUT)
      @connection.check_fork
      @connection.flush(Deadline.coerce(timeout))
      self
    end

    # @param timeout [Numeric, Deadline, nil] how long to wait
    # @return [Float] seconds a round trip to the server takes
    def rtt(timeout: DEFAULT_FLUSH_TIMEOUT)
      started = Deadline.now
      flush(timeout: timeout)
      Deadline.now - started
    end

    # Unsubscribes everything, lets handlers finish what was received, then
    # closes. Called from a tether thread it starts the drain and returns.
    #
    # @param timeout [Numeric, Deadline, nil] how long to wait; defaults to `drain_timeout`
    # @return [self] this client
    # @raise [TimeoutError] if draining takes too long; the client is closed anyway
    # @raise [DisconnectedError] if there is no live connection; the client is closed
    def drain(timeout: nil)
      deadline = Deadline.coerce(timeout || @options.drain_timeout)
      if own_thread?
        Thread.new { @connection.drain(deadline) }
      else
        @connection.drain(deadline)
      end
      self
    end

    # Closes the connection immediately. Idempotent.
    #
    # @return [self] this client
    def close
      @connection.close
      self
    end

    # Drops the current socket and reconnects.
    #
    # @return [self] this client
    def force_reconnect
      @connection.force_reconnect
      self
    end

    # Replaces a connection inherited across a fork now, instead of on the
    # first call made in the child.
    #
    # @return [self] this client
    def after_fork
      @connection.check_fork
      self
    end

    # @return [Symbol] :disconnected, :connecting, :connected, :reconnecting, :draining or :closed
    def status = @connection.status

    # @return [Boolean] whether there is a live, authenticated connection
    def connected? = status == :connected

    # @return [Boolean] whether the client is trying to re-establish its connection
    def reconnecting? = status == :reconnecting

    # @return [Boolean] whether {#drain} is in progress
    def draining? = status == :draining

    # @return [Boolean] whether the client is closed; a closed client is never reopened
    def closed? = status == :closed

    # @return [Connection::Server, nil] the server connected to, or the last one while reconnecting or once
    #   closed; its `url` carries no credentials
    def server = @connection.server

    # @return [ServerInfo, nil] what the connected server announced
    def server_info = @connection.server_info

    # @return [Addrinfo, nil] this side of the connection, nil while there is none
    def local_address = @connection.transport&.local_address

    # @return [Addrinfo, nil] the server's side of the connection, nil while there is none
    def remote_address = @connection.transport&.remote_address

    # @return [Connection::TLS::State, nil] the TLS version, cipher and server certificate of the connection,
    #   nil when it is not encrypted or there is none
    def tls_state = @connection.transport&.tls_state

    # @return [Integer, nil] the largest headers plus payload the server accepts
    def max_payload = server_info&.max_payload

    # @return [Array<Connection::Server>] the servers this client may connect to
    def servers = @connection.pool.to_a

    # Replaces the server pool.
    #
    # @param urls [String, Array<String>] the new servers
    # @return [void] nothing
    def servers=(urls)
      @connection.pool.replace(urls)
    end

    # @return [Stats] a snapshot of the traffic counters
    def stats = @connection.counters.snapshot

    # @return [Integer] the subscriptions this client holds on the server, including the internal ones
    #   behind requests and JetStream pulls
    def subscription_count = @connection.registry.size

    # @return [Integer] bytes published but not yet written to the socket, including what is held while
    #   reconnecting
    def buffered_bytes = @connection.writer.buffered

    # @return [Exception, nil] the most recent asynchronous error
    def last_error = @connection.events.last_error

    # Asks the server who this connection is authenticated as.
    #
    # @param timeout [Numeric, Deadline, nil] how long to wait
    # @return [Hash{String => Object}] the account, user, permissions and expiry
    def user_info(timeout: DEFAULT_REQUEST_TIMEOUT)
      JSON.parse(request("$SYS.REQ.USER.INFO", timeout: timeout).data).fetch("data", {})
    end

    # The services side of this connection: add a service, or discover the
    # ones running elsewhere.
    #
    # @param prefix [String] the discovery prefix, for services reached across an account import
    # @return [Service::Registry] the entry point for adding and discovering services
    # @raise [InvalidSubjectError] if the prefix is not a literal subject
    def services(prefix: Service::DEFAULT_PREFIX)
      Service::Registry.new(self, prefix: prefix)
    end

    Connection::Events::NAMES.each do |event|
      define_method(:"on_#{event}") { |&handler| @connection.events.on(event, &handler) }
    end

    # @!method on_error
    #   Registers a handler for asynchronous errors.
    #   @yieldparam error [Exception] the error
    #   @return [Connection::Events::Registration] a handle whose `cancel` removes the handler
    # @!method on_connect
    #   @yieldparam server [Connection::Server] the server connected to
    #   @return [Connection::Events::Registration] a handle whose `cancel` removes the handler
    # @!method on_disconnect
    #   @yieldparam error [Exception, nil] why the connection was lost; nil when requested
    #   @return [Connection::Events::Registration] a handle whose `cancel` removes the handler
    # @!method on_reconnect
    #   @yieldparam server [Connection::Server] the server reconnected to
    #   @return [Connection::Events::Registration] a handle whose `cancel` removes the handler
    # @!method on_reconnect_error
    #   @yieldparam error [Exception] why one attempt failed
    #   @return [Connection::Events::Registration] a handle whose `cancel` removes the handler
    # @!method on_close
    #   @return [Connection::Events::Registration] a handle whose `cancel` removes the handler
    # @!method on_lame_duck
    #   @yieldparam server [Connection::Server] the server that is shutting down
    #   @return [Connection::Events::Registration] a handle whose `cancel` removes the handler
    # @!method on_servers_discovered
    #   @yieldparam servers [Array<Connection::Server>] the servers that were added
    #   @return [Connection::Events::Registration] a handle whose `cancel` removes the handler
    # @!method on_status
    #   @yieldparam status [Symbol] the new status
    #   @return [Connection::Events::Registration] a handle whose `cancel` removes the handler

    # Subscribes a block that runs on the reader thread. Internal seam for
    # library components: the block must not block.
    #
    # @api private
    # @param subject [String] the subject or pattern
    # @yieldparam msg [Protocol::Msg] each message, on the reader thread
    # @return [Connection::Resolver] the entry
    def resolver(subject, &block)
      ensure_accepting
      register(Connection::Resolver.new(client: self, sid: @connection.registry.next_sid, subject: subject, &block))
    end

    # Registers a listener called synchronously on connection transitions.
    # Internal seam for library components: the block must not block.
    #
    # @api private
    # @yieldparam event [Symbol] :connect, :disconnect, :reconnect, :reconnect_error or :close
    # @yieldparam detail [Object, nil] the error or server concerned
    # @return [Connection::Events::Registration] a handle whose `cancel` removes the listener
    def listen(&listener)
      @connection.events.listen(&listener)
    end

    # @api private
    # @return [Connection::Timer] the connection's timer
    def timer = @connection.timer

    # @api private
    # @return [Integer] the process the current connection belongs to
    def generation = @connection.pid

    # Tells the server a subscription is over, or will be after a message count.
    #
    # @api private
    # @param entry [Connection::Subscription, Connection::Resolver] the entry
    # @param max [Integer, nil] deliver this many messages in total first
    # @return [void] nothing
    def unsubscribed(entry, max = nil)
      @connection.registry.remove(entry.sid) unless max || (entry.is_a?(Connection::Subscription) && entry.draining?)
      @connection.writer.write(Protocol::Encoder.unsub(entry.sid, max), control: true)
      nil
    rescue ConnectionClosedError
      nil
    end

    # @api private
    # @return [Connection::WorkerPool] the threads that run subscription blocks
    def worker_pool = @connection.worker_pool

    # @api private
    # @param entry [Connection::Subscription, Connection::Resolver] the entry to stop routing to
    # @return [void] nothing
    def forget(entry)
      @connection.registry.remove(entry.sid)
      nil
    end

    # @api private
    # @param subscription [Connection::Subscription] the subscription that dropped a message
    # @return [void] nothing
    def report_slow_consumer(subscription)
      message = "slow consumer on #{subscription.subject}: messages are being dropped"
      @connection.events.error(SlowConsumerError.new(message))
    end

    # @api private
    # @param error [Exception] what a user block raised
    # @param msg [Object, nil] the message it was handling
    # @return [void] nothing
    def report_handler_error(error, msg = nil)
      @connection.events.error(HandlerError.new(error, msg))
    end

    # @api private
    # @param error [Exception] an asynchronous error to report
    # @return [void] nothing
    def report_error(error)
      @connection.events.error(error)
    end

    # @return [String] the status and server without internal state
    def inspect
      "#<Tether::Client status=#{status} server=#{@connection.server&.url.inspect}>"
    end

    private

    def register(entry)
      @connection.registry.add(entry)
      @connection.writer.write(entry.replay_frames, control: true)
      entry
    end

    def ensure_started
      @connection.check_fork
      raise ConnectionError, "not connected: call connect first" if @connection.status == :disconnected
    end

    def ensure_accepting
      ensure_started
      raise ConnectionClosedError, "connection is closed" if status == :closed
      raise ConnectionDrainingError, "connection is draining" if status == :draining
    end

    def own_thread?
      Thread.current.name.to_s.start_with?("tether:")
    end

    def check_subject(subject)
      return if @checked_subjects.key?(subject)

      @options.validate_subjects ? Protocol::Subject.literal!(subject) : Protocol::Subject.check!(subject)
      @checked_subjects.clear if @checked_subjects.size >= CHECKED_SUBJECTS_LIMIT
      @checked_subjects[subject] = true
    end

    def encode(subject, payload, reply_to, headers)
      if headers.nil? || headers.empty?
        return check_size(subject,
                          payload.to_s.bytesize) && Protocol::Encoder.pub(subject, payload, reply_to)
      end

      info = server_info
      raise HeadersNotSupportedError, "the server does not support headers" if info && !info.headers?

      block = (headers.is_a?(Protocol::Headers) ? headers : Protocol::Headers[headers]).to_wire
      check_size(subject, block.bytesize + payload.to_s.bytesize)
      Protocol::Encoder.hpub(subject, payload, block, reply_to)
    end

    def check_size(subject, size)
      limit = max_payload
      return true unless limit && size > limit

      raise MaxPayloadError, "message for #{subject} is #{size} bytes; the server accepts #{limit}"
    end

    def inbox
      @lock.synchronize do
        next @inbox if @inbox && @inbox_pid == @connection.pid

        @inbox_pid = @connection.pid
        @inbox = Connection::Inbox.new(self)
      end
    end

    def collect(replies, subject, deadline, max:, stall:, sentinel:)
      collected = []
      while (msg = replies.pop(timeout: (collected.empty? ? deadline : deadline.with_at_most(stall)).remaining))
        break no_responders(subject, collected) if msg.no_responders?
        break if sentinel&.call(msg)

        block_given? ? yield(msg) : nil
        collected << msg
        break if max && collected.size >= max
      end
      block_given? ? collected.size : collected
    end

    def no_responders(subject, collected)
      raise NoRespondersError, "no responders for #{subject}" if collected.empty?
    end
  end
end
