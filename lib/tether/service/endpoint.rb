# frozen_string_literal: true

module Tether
  class Service
    # One request handler of a service: a subscription whose worker times the
    # handler and keeps the counters that `STATS` reports.
    class Endpoint
      INTERNAL_ERROR = 500

      # @return [String] the endpoint name
      attr_reader :name

      # @return [String] the subject the endpoint is subscribed to
      attr_reader :subject

      # @return [String, nil] the queue group in use, nil when the endpoint has none
      attr_reader :queue_group

      # @return [Hash{String => String}] the frozen metadata
      attr_reader :metadata

      # @api private
      # @return [Connection::Subscription, nil] the subscription, once started
      attr_reader :subscription

      # Builds an endpoint without subscribing. Use {Service#endpoint} or {Group#endpoint}.
      #
      # @api private
      # @param service [Service] the service this endpoint belongs to
      # @param name [String] the endpoint name: letters, digits, dash and underscore
      # @param subject [String] the composed subject to serve
      # @param queue_group [String, nil] the queue group to join, nil for a plain subscription
      # @param metadata [Hash] string pairs describing the endpoint
      # @yieldparam request [Request] each request, on the endpoint's worker thread
      # @raise [ArgumentError] if the name or metadata is unusable or no block is given
      # @raise [InvalidSubjectError] if the subject is malformed
      def initialize(service:, name:, subject:, queue_group: nil, metadata: {}, &handler)
        raise ArgumentError, "endpoint #{name.inspect} needs a block to handle requests" unless handler

        @service = service
        @client = service.client
        @name = Rules.name!(name, "endpoint name")
        @subject = Rules.subject!(subject, "endpoint subject")
        @queue_group = queue_group
        @metadata = Rules.metadata(metadata)
        @handler = handler
        @lock = Mutex.new
        @subscription = nil
        reset
      end

      # Subscribes to the endpoint's subject.
      #
      # @api private
      # @return [self] this endpoint
      # @raise [InvalidSubjectError] if the server could not accept the subject
      # @raise [ConnectionDrainingError, ConnectionClosedError] if the connection is ending
      def start
        @subscription = @client.subscribe(@subject, queue: @queue_group) { |msg| handle(msg) }
        self
      end

      # @return [EndpointInfo] the endpoint as `INFO` lists it
      def info
        EndpointInfo.new(identity.merge("metadata" => @metadata))
      end

      # @return [EndpointStats] a consistent snapshot of the counters
      def stats
        document = @lock.synchronize do
          identity.merge("num_requests" => @requests, "num_errors" => @errors, "last_error" => @last_error,
                         "processing_time" => @processing_time,
                         "average_processing_time" => @requests.zero? ? 0 : @processing_time / @requests)
        end
        data = @service.stats_data(self)
        document["data"] = data unless data.nil?
        EndpointStats.new(document)
      end

      # Zeroes the counters.
      #
      # @api private
      # @return [void] nothing
      def reset
        @lock.synchronize do
          @requests = @errors = @processing_time = 0
          @last_error = ""
        end
        nil
      end

      # Counts a refusal of the endpoint's subscription as an error.
      #
      # @api private
      # @param error [Exception] the server's error
      # @return [void] nothing
      def failed(error)
        @lock.synchronize do
          @errors += 1
          @last_error = error.message
        end
        nil
      end

      # @return [String] the name and where the endpoint listens
      def inspect
        "#<Tether::Service::Endpoint name=#{@name.inspect} subject=#{@subject.inspect} " \
          "queue_group=#{@queue_group.inspect}>"
      end

      private

      def identity
        { "name" => @name, "subject" => @subject, "queue_group" => @queue_group.to_s }
      end

      def handle(msg)
        request = Request.new(msg, @client)
        started = clock
        failure = invoke(request)
        record(clock - started, failure || request.error)
      end

      def invoke(request)
        @handler.call(request)
        nil
      rescue StandardError => e
        @client.report_handler_error(e, request)
        reject(request, e)
      end

      def reject(request, error)
        description = error.message.to_s.gsub(/[\r\n]+/, " ").strip
        description = error.class.name.to_s if description.empty?
        request.respond_error(INTERNAL_ERROR, description) if request.reply_to && !request.responded?
        "#{INTERNAL_ERROR}:#{description}"
      rescue Tether::Error => e
        e.message
      end

      def record(elapsed, error)
        @lock.synchronize do
          @requests += 1
          @processing_time += elapsed
          next unless error

          @errors += 1
          @last_error = error
        end
      end

      def clock
        Process.clock_gettime(Process::CLOCK_MONOTONIC, :nanosecond)
      end
    end
  end
end
