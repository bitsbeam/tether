# frozen_string_literal: true

module Tether
  class Service
    # The services side of a connection: adds services to it, and discovers
    # the services running anywhere under a discovery prefix.
    class Registry
      # @return [String] the discovery prefix
      attr_reader :prefix

      # @api private
      # @param client [Client] the connection
      # @param prefix [String] the discovery prefix
      # @raise [InvalidSubjectError] if the prefix is not a literal subject
      def initialize(client, prefix: DEFAULT_PREFIX)
        @client = client
        @prefix = Protocol::Subject.literal!(prefix).dup.freeze
      end

      # Starts a service: it answers discovery as soon as this returns.
      #
      # @param name [String] the kind of service: letters, digits, dash and underscore
      # @param version [String] a semantic version
      # @param description [String, nil] what the service does
      # @param metadata [Hash] string pairs describing the service
      # @param queue [String, false, nil] the default queue group; nil for "q", false for none
      # @param stats_handler [#call, nil] called with each endpoint when stats are built; its result becomes `data`
      # @param on_stop [#call, nil] called once with the stop error, or nil, when the service has stopped
      # @param on_error [#call, nil] called with the error and the endpoint, or nil for a discovery subject,
      #   when the server refuses one of the service's subscriptions
      # @return [Service] the running service
      # @raise [ArgumentError] if the name, version or metadata is unusable
      # @raise [InvalidSubjectError] if the queue group is malformed
      # @raise [ConnectionDrainingError, ConnectionClosedError] if the connection is ending
      def add(name:, version:, description: nil, metadata: {}, queue: nil, stats_handler: nil, on_stop: nil,
              on_error: nil)
        Service.new(@client, name: name, version: version, description: description, metadata: metadata,
                             queue: queue, prefix: @prefix, stats_handler: stats_handler, on_stop: on_stop,
                             on_error: on_error).start
      end

      # Asks services to announce themselves. Collects replies until the
      # timeout, unless `max` or `stall` ends it earlier.
      #
      # @param name [String, nil] only instances of this service; nil for every service
      # @param id [String, nil] only this instance, which needs the name too
      # @param max [Integer, nil] stop after this many replies; defaults to 1 when an id is given
      # @param stall [Numeric, nil] stop when no reply has arrived for this many seconds
      # @param timeout [Numeric, Deadline, nil] how long to collect replies
      # @return [Array<Ping>] one entry per instance that answered
      # @raise [ArgumentError] if an id is given without a name
      # @raise [Error] if a reply is not a JSON document
      def ping(name = nil, id: nil, max: nil, stall: nil, timeout: Client::DEFAULT_REQUEST_TIMEOUT)
        discover("PING", Ping, name, id, max: max, stall: stall, deadline: Deadline.coerce(timeout))
      end

      # Asks services for their description and endpoints. Collects replies
      # until the timeout, unless `max` or `stall` ends it earlier.
      #
      # @param name [String, nil] only instances of this service; nil for every service
      # @param id [String, nil] only this instance, which needs the name too
      # @param max [Integer, nil] stop after this many replies; defaults to 1 when an id is given
      # @param stall [Numeric, nil] stop when no reply has arrived for this many seconds
      # @param timeout [Numeric, Deadline, nil] how long to collect replies
      # @return [Array<Info>] one entry per instance that answered
      # @raise [ArgumentError] if an id is given without a name
      # @raise [Error] if a reply is not a JSON document
      def info(name = nil, id: nil, max: nil, stall: nil, timeout: Client::DEFAULT_REQUEST_TIMEOUT)
        discover("INFO", Info, name, id, max: max, stall: stall, deadline: Deadline.coerce(timeout))
      end

      # Asks services for their counters. Collects replies until the timeout,
      # unless `max` or `stall` ends it earlier.
      #
      # @param name [String, nil] only instances of this service; nil for every service
      # @param id [String, nil] only this instance, which needs the name too
      # @param max [Integer, nil] stop after this many replies; defaults to 1 when an id is given
      # @param stall [Numeric, nil] stop when no reply has arrived for this many seconds
      # @param timeout [Numeric, Deadline, nil] how long to collect replies
      # @return [Array<Stats>] one entry per instance that answered
      # @raise [ArgumentError] if an id is given without a name
      # @raise [Error] if a reply is not a JSON document
      def stats(name = nil, id: nil, max: nil, stall: nil, timeout: Client::DEFAULT_REQUEST_TIMEOUT)
        discover("STATS", Stats, name, id, max: max, stall: stall, deadline: Deadline.coerce(timeout))
      end

      # @return [String] the discovery prefix
      def inspect
        "#<Tether::Service::Registry prefix=#{@prefix.inspect}>"
      end

      private

      def discover(verb, type, name, id, max:, stall:, deadline:)
        subject = Service.control_subject(verb, name, id, prefix: @prefix)
        replies = @client.request_many(subject, max: max || (id ? 1 : nil), stall: stall, timeout: deadline)
        replies.map { |reply| type.new(decode(reply, subject)) }
      rescue NoRespondersError
        []
      end

      def decode(reply, subject)
        document = JSON.parse(reply.data)
        return document if document.is_a?(Hash)

        raise Error, "reply to #{subject} is not a JSON object"
      rescue JSON::ParserError => e
        raise Error, "reply to #{subject} is not JSON: #{e.message}"
      end
    end
  end
end
