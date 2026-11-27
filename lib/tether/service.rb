# frozen_string_literal: true

require "json"
require "time"

module Tether
  # A named, versioned set of request handlers that can be discovered and
  # monitored over NATS itself (ADR-32).
  #
  # A service owns one subscription per endpoint and nine control
  # subscriptions that answer `PING`, `INFO` and `STATS`. Several services can
  # share one connection, and closing the connection stops them all.
  class Service
    # Raised when a service or one of its requests is used in a way its state does not allow.
    class Error < Tether::Error; end

    DEFAULT_PREFIX = "$SRV"
    DEFAULT_QUEUE = "q"
    VERBS = %w[PING INFO STATS].freeze
    TYPES = {
      "PING" => "io.nats.micro.v1.ping_response",
      "INFO" => "io.nats.micro.v1.info_response",
      "STATS" => "io.nats.micro.v1.stats_response"
    }.freeze
    ERROR_HEADER = "Nats-Service-Error"
    ERROR_CODE_HEADER = "Nats-Service-Error-Code"
    STARTED_FORMAT = "%Y-%m-%dT%H:%M:%S.%NZ"

    # Builds one of the three forms of a control subject.
    #
    # @param verb [String, Symbol] PING, INFO or STATS, in any case
    # @param name [String, nil] the service name, to address every instance of one service
    # @param id [String, nil] the instance id, to address exactly one instance
    # @param prefix [String] the discovery prefix
    # @return [String] the control subject
    # @raise [ArgumentError] if the verb is unknown or an id is given without a name
    def self.control_subject(verb, name = nil, id = nil, prefix: DEFAULT_PREFIX)
      verb = verb.to_s.upcase
      raise ArgumentError, "unknown service verb: #{verb.inspect}" unless VERBS.include?(verb)
      raise ArgumentError, "a service id needs a name" if id && !name

      [prefix, verb, name, id].compact.join(".")
    end

    # Builds the headers that mark a response as a service error.
    #
    # @param code [Integer] the error code, sent in decimal
    # @param description [String] a description that can be shown to a user
    # @param headers [Protocol::Headers, Hash, nil] further headers to send along
    # @return [Protocol::Headers] headers carrying `Nats-Service-Error` and `Nats-Service-Error-Code`
    # @raise [ArgumentError] if the code is not an integer or the description is empty or spans lines
    def self.error_headers(code, description, headers = nil)
      raise ArgumentError, "service error code must be an Integer, got #{code.inspect}" unless code.is_a?(Integer)

      text = description.to_s
      raise ArgumentError, "service error description is empty or spans lines" if text.empty? || text.match?(/[\r\n]/)

      result = Protocol::Headers.new
      headers&.each { |key, value| Array(value).each { |item| result.add(key.to_s, item.to_s) } }
      result[ERROR_HEADER] = text
      result[ERROR_CODE_HEADER] = code.to_s
      result
    end

    # @return [Client] the connection this service runs on
    attr_reader :client

    # @return [String] the kind of service, shared by all of its instances
    attr_reader :name

    # @return [String] the unique id of this instance
    attr_reader :id

    # @return [String] the semantic version
    attr_reader :version

    # @return [String] the description, empty when none was given
    attr_reader :description

    # @return [Hash{String => String}] the frozen metadata
    attr_reader :metadata

    # @return [String] the discovery prefix the control subjects live under
    attr_reader :prefix

    # Builds a service without subscribing to anything. Use {Registry#add}.
    #
    # @api private
    # @param client [Client] the connection to serve on
    # @param name [String] the kind of service: letters, digits, dash and underscore
    # @param version [String] a semantic version
    # @param description [String, nil] what the service does
    # @param metadata [Hash] string pairs describing the service
    # @param queue [String, false, nil] the default queue group; nil for "q", false for none
    # @param prefix [String] the discovery prefix
    # @param stats_handler [#call, nil] called with each endpoint when stats are built; its result becomes `data`
    # @param on_stop [#call, nil] called once with the stop error, or nil, when the service has stopped
    # @param on_error [#call, nil] called with the error and the endpoint, or nil for a discovery subject, when
    #   the server refuses one of the service's subscriptions
    # @raise [ArgumentError] if the name, version or metadata is unusable
    # @raise [InvalidSubjectError] if the queue group or prefix is malformed
    def initialize(client, name:, version:, description: nil, metadata: {}, queue: nil, prefix: DEFAULT_PREFIX,
                   stats_handler: nil, on_stop: nil, on_error: nil)
      @client = client
      @name = Rules.name!(name, "service name")
      @version = Rules.version!(version)
      @description = description.to_s.dup.freeze
      @metadata = Rules.metadata(metadata)
      @prefix = Protocol::Subject.literal!(prefix).dup.freeze
      @id = Connection::NUID.next.freeze
      @stats_handler = stats_handler
      @root = Group.new(self, prefix: "", queue: Rules.queue(queue, DEFAULT_QUEUE))
      @lock = Mutex.new
      @state = :running
      @endpoints = []
      @controls = []
      @stop_handlers = [on_stop].compact
      @error_handlers = [on_error].compact
      @refusals = nil
      @stop_error = nil
      @started = now
    end

    # Subscribes to the nine control subjects.
    #
    # @api private
    # @return [self] this service
    # @raise [Tether::Error] if a control subscription cannot be made; the service is stopped
    def start
      @refusals = @client.on_error { |error| refused(error) }
      VERBS.each do |verb|
        control_subjects(verb).each do |subject|
          @controls << @client.subscribe(subject) { |msg| answer(verb, msg) }
        end
      end
      @controls.first.on_close { stop }
      self
    rescue StandardError
      @controls.each(&:unsubscribe)
      @refusals&.cancel
      @state = :stopped
      raise
    end

    # Adds an endpoint at the top level of the service.
    #
    # @param name [String] the endpoint name: letters, digits, dash and underscore
    # @param subject [String, nil] the subject to serve; defaults to the name
    # @param queue [String, false, nil] a queue group, nil to use the service's, false for none
    # @param metadata [Hash] string pairs describing the endpoint
    # @param max_pending [Integer, nil] requests that may wait before newer ones are dropped; nil for the client's
    # @param max_pending_bytes [Integer, nil] bytes of requests that may wait; nil for the client's
    # @yieldparam request [Request] each request, on the endpoint's worker thread
    # @return [Endpoint] the endpoint
    # @raise [ArgumentError] if the name or metadata is unusable or no block is given
    # @raise [InvalidSubjectError] if the subject or queue group is malformed or reserved
    # @raise [Error] if the service is stopped
    def endpoint(name, subject: nil, queue: nil, metadata: {}, max_pending: nil, max_pending_bytes: nil, &handler)
      @root.endpoint(name, subject: subject, queue: queue, metadata: metadata, max_pending: max_pending,
                           max_pending_bytes: max_pending_bytes, &handler)
    end

    # Opens a group, whose name prefixes the subject of every endpoint added to it.
    #
    # @param name [String] a subject without `>`, or an empty string for no prefix
    # @param queue [String, false, nil] a queue group, nil to use the service's, false for none
    # @return [Group] the group
    # @raise [InvalidSubjectError] if the name or queue group is malformed
    def group(name, queue: nil)
      @root.group(name, queue: queue)
    end

    # @return [Array<Endpoint>] the endpoints in the order they were added; empty once stopped
    def endpoints
      @lock.synchronize { @endpoints.dup }
    end

    # @return [Info] the service and its endpoints as they are now
    def info
      Info.new(document("INFO"))
    end

    # @return [Stats] the counters of every endpoint as they are now
    def stats
      Stats.new(document("STATS"))
    end

    # Zeroes the counters of every endpoint and restarts the window they cover.
    #
    # @return [self] this service
    def reset_stats
      endpoints = @lock.synchronize do
        @started = now
        @endpoints.dup
      end
      endpoints.each(&:reset)
      self
    end

    # Registers a block to call once the service has stopped. Called at once
    # when it already has.
    #
    # @yieldparam error [Exception, nil] the error given to {#stop}
    # @return [self] this service
    def on_stop(&handler)
      run_now = @lock.synchronize do
        @stop_handlers << handler unless @state == :stopped
        @state == :stopped
      end
      notify(handler, @stop_error) if run_now
      self
    end

    # Registers a block to call when the server refuses one of the service's subscriptions, as it does
    # when the user may not subscribe to an endpoint's subject. The service keeps running; the refusal
    # also counts as an error in the endpoint's stats.
    #
    # @yieldparam error [PermissionsViolationError] the server's error
    # @yieldparam endpoint [Endpoint, nil] the endpoint refused, nil for a discovery subject
    # @return [self] this service
    def on_error(&handler)
      @lock.synchronize { @error_handlers << handler unless @state == :stopped }
      self
    end

    # Drains the endpoints, so requests already received are answered, then
    # the control subscriptions. Idempotent.
    #
    # @param error [Exception, nil] why the service stops, passed to the `on_stop` handlers
    # @param timeout [Numeric, Deadline, nil] how long to wait; defaults to the client's `drain_timeout`
    # @return [self] this service
    # @raise [TimeoutError] if requests are still being handled at the deadline; the service is stopped anyway
    def stop(error = nil, timeout: nil)
      deadline = Deadline.coerce(timeout || @client.options.drain_timeout)
      endpoints = @lock.synchronize do
        return self unless @state == :running

        @state = :stopping
        @endpoints.dup
      end
      failure = drain(endpoints.map(&:subscription), deadline)
      failure = drain(@controls, deadline) || failure
      finish(error)
      raise failure if failure.is_a?(TimeoutError)

      self
    end

    # @return [Boolean] whether the service has stopped and answers nothing anymore
    def stopped?
      @lock.synchronize { @state == :stopped }
    end

    # Subscribes an endpoint and lists it in INFO and STATS.
    #
    # @api private
    # @param endpoint [Endpoint] the endpoint to start
    # @return [Endpoint] the endpoint
    # @raise [InvalidSubjectError] if its subject is under the discovery prefix
    # @raise [Error] if the service is stopped
    def add_endpoint(endpoint)
      if endpoint.subject == @prefix || endpoint.subject.start_with?("#{@prefix}.")
        raise InvalidSubjectError, "endpoint subject #{endpoint.subject.inspect} is under the reserved #{@prefix}"
      end

      @lock.synchronize do
        raise Error, "service #{@name} is stopped" unless @state == :running

        endpoint.start
        @endpoints << endpoint
      end
      endpoint
    end

    # Reads the stats handler's data for one endpoint.
    #
    # @api private
    # @param endpoint [Endpoint] the endpoint the data is for
    # @return [Object, nil] what the stats handler returned, nil without one or when it raised
    def stats_data(endpoint)
      @stats_handler&.call(endpoint)
    rescue StandardError => e
      @client.report_handler_error(e)
      nil
    end

    # @return [String] the identity and state without the handlers
    def inspect
      "#<Tether::Service name=#{@name.inspect} id=#{@id} version=#{@version} state=#{@state}>"
    end

    private

    def now
      Time.now.utc.strftime(STARTED_FORMAT)
    end

    def control_subjects(verb)
      [[], [@name], [@name, @id]].map { |target| Service.control_subject(verb, *target, prefix: @prefix) }
    end

    def answer(verb, msg)
      @client.publish(msg.reply_to, JSON.generate(document(verb))) if msg.reply_to
    end

    def document(verb)
      identity = { "name" => @name, "id" => @id, "version" => @version, "metadata" => @metadata,
                   "type" => TYPES.fetch(verb) }
      case verb
      when "INFO"
        identity.merge("description" => @description, "endpoints" => endpoints.map { |entry| entry.info.to_h })
      when "STATS"
        started, endpoints = @lock.synchronize { [@started, @endpoints.dup] }
        identity.merge("started" => started, "endpoints" => endpoints.map { |entry| entry.stats.to_h })
      else
        identity
      end
    end

    def drain(subscriptions, deadline)
      started = subscriptions.select(&:start_drain)
      return if started.empty?

      failure = attempt { @client.flush(timeout: deadline) }
      started.each { |subscription| failure = attempt { subscription.finish_drain(deadline) } || failure }
      failure
    end

    def attempt
      yield
      nil
    rescue Tether::Error => e
      e
    end

    def refused(error)
      subject = error.subscription_subject if error.is_a?(PermissionsViolationError)
      endpoint, handlers = owner(subject) if subject
      return unless handlers

      endpoint&.failed(error)
      handlers.each { |handler| alert(handler, error, endpoint) }
    end

    def owner(subject)
      @lock.synchronize do
        next unless @state == :running

        endpoint = @endpoints.find { |entry| entry.subject == subject }
        [endpoint, @error_handlers.dup] if endpoint || @controls.any? { |control| control.subject == subject }
      end
    end

    def alert(handler, error, endpoint)
      handler.call(error, endpoint)
    rescue StandardError => e
      @client.report_handler_error(e)
    end

    def finish(error)
      @refusals&.cancel
      handlers = @lock.synchronize do
        @state = :stopped
        @stop_error = error
        @endpoints.clear
        @stop_handlers.dup
      end
      handlers.each { |handler| notify(handler, error) }
    end

    def notify(handler, error)
      handler.call(error)
    rescue StandardError => e
      @client.report_handler_error(e)
    end
  end
end

require "tether/service/rules"
require "tether/service/response"
require "tether/service/info"
require "tether/service/stats"
require "tether/service/request"
require "tether/service/endpoint"
require "tether/service/group"
require "tether/service/registry"
