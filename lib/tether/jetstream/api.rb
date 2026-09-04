# frozen_string_literal: true

require "json"

module Tether
  class JetStream
    # Request and response plumbing for the JetStream JSON API.
    class API
      LEVEL_HEADER = "Nats-Required-Api-Level"

      # @return [String] the subject prefix of the API, without a trailing dot
      attr_reader :prefix

      # @return [Client] the connection requests go over
      attr_reader :client

      # @return [Numeric] the default timeout of a request, in seconds
      attr_reader :timeout

      # @param client [Client] the connection
      # @param prefix [String] the subject prefix, for example `$JS.API`
      # @param timeout [Numeric] the default timeout of a request, in seconds
      def initialize(client, prefix:, timeout:)
        @client = client
        @prefix = prefix
        @timeout = timeout
      end

      # Turns what a caller passed for a timeout into a deadline. A deadline comes back as it is, so every
      # step it is given to spends one budget; seconds, or nil for the context's default, start a new clock.
      #
      # @param timeout [Numeric, Deadline, nil] seconds, a deadline, or nil for the context's default
      # @return [Deadline] the deadline to wait against
      def deadline(timeout)
        Deadline.coerce(timeout || @timeout)
      end

      # Sends an API request and decodes the answer.
      #
      # @param subject [String] the subject after the prefix, for example `STREAM.INFO.ORDERS`
      # @param payload [Hash, String, nil] the request body; a hash is sent as JSON
      # @param timeout [Numeric, Deadline, nil] how long to wait; defaults to the context's timeout
      # @param level [Integer] the API level the request needs; above 0 it is sent as a header
      # @return [Hash{String => Object}] the decoded response
      # @raise [APIError] the mapped error when the response carries one
      # @raise [ServiceUnavailableError] if nothing answered on the API subject
      def request(subject, payload = nil, timeout: nil, level: 0)
        body = payload.is_a?(Hash) ? JSON.generate(payload) : payload
        headers = level.positive? ? { LEVEL_HEADER => level.to_s } : nil
        reply = @client.request("#{@prefix}.#{subject}", body, headers: headers, timeout: timeout || @timeout)
        decode(reply.data)
      rescue NoRespondersError
        raise ServiceUnavailableError, "nothing answered #{@prefix}.#{subject}: JetStream is disabled or has no leader"
      end

      # Walks a paged listing.
      #
      # @param subject [String] the subject after the prefix
      # @param key [String] the response member that holds one page of items
      # @param payload [Hash] request fields besides the offset
      # @param timeout [Numeric, Deadline, nil] seconds each page request may take, or a deadline for the whole walk
      # @return [Enumerator::Lazy<Object>] the items, fetched a page at a time as they are consumed
      def pages(subject, key, payload = {}, timeout: nil)
        Enumerator.new do |yielder|
          offset = 0
          loop do
            response = request(subject, payload.merge("offset" => offset), timeout: timeout)
            items = response[key] || []
            items.each { |item| yielder << item }
            offset += items.size
            break if items.empty? || offset >= response["total"].to_i
          end
        end.lazy
      end

      # @param data [String] a JSON response body
      # @return [Hash{String => Object}] the decoded response
      # @raise [APIError] the mapped error when the response carries one
      def decode(data)
        response = JSON.parse(data)
        raise APIError.for(response["error"]) if response["error"]

        response
      rescue JSON::ParserError => e
        raise Error, "the JetStream API returned something that is not JSON: #{e.message}"
      end
    end
  end
end
