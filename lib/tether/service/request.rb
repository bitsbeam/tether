# frozen_string_literal: true

module Tether
  class Service
    # A request delivered to an endpoint: a message that knows how to answer,
    # and that remembers an error answer so the endpoint can count it.
    class Request < Protocol::Msg
      # @api private
      # @return [String, nil] the error this request was answered with, as the stats report it
      attr_reader :error

      # @api private
      # @param msg [Protocol::Msg] the message the endpoint's subscription received
      # @param client [Client] the connection to answer on
      def initialize(msg, client)
        super(subject: msg.subject, data: msg.data, reply_to: msg.reply_to, subscription: msg.subscription)
        @msg = msg
        @client = client
        @responded = false
        @error = nil
      end

      # @return [Protocol::Headers, nil] the request headers, nil when it has none
      # @raise [ProtocolError] if the header block is malformed
      def headers = @msg.headers

      # @return [Integer] bytes of headers and payload together
      def size = @msg.size

      # Answers the request.
      #
      # @param payload [String, nil] the response body
      # @param headers [Protocol::Headers, Hash, nil] headers to send with the response
      # @return [self] this request
      # @raise [Error] if the request carries no reply subject
      # @raise [Tether::Error] if the response cannot be published
      def respond(payload = nil, headers: nil)
        deliver(payload, headers)
        self
      end

      # Answers the request with an object encoded as JSON.
      #
      # @param object [Object] anything `JSON.generate` accepts
      # @param headers [Protocol::Headers, Hash, nil] headers to send with the response
      # @return [self] this request
      # @raise [Error] if the request carries no reply subject
      # @raise [Tether::Error] if the response cannot be published
      def respond_json(object, headers: nil)
        respond(JSON.generate(object), headers: headers)
      end

      # Answers the request with an error, which the endpoint counts in its stats.
      #
      # @param code [Integer] the error code, sent in decimal
      # @param description [String] a description that can be shown to a user
      # @param data [String, nil] an optional response body
      # @param headers [Protocol::Headers, Hash, nil] further headers to send with the response
      # @return [self] this request
      # @raise [ArgumentError] if the code is not an integer or the description is empty or spans lines
      # @raise [Error] if the request carries no reply subject
      # @raise [Tether::Error] if the response cannot be published
      def respond_error(code, description, data = nil, headers: nil)
        deliver(data, Service.error_headers(code, description, headers))
        @error = "#{code}:#{description}"
        self
      end

      # @return [Boolean] whether a response has been published for this request
      def responded?
        @responded
      end

      # @return [String] the subject and payload size
      def inspect
        "#<Tether::Service::Request subject=#{subject.inspect} reply_to=#{reply_to.inspect} bytes=#{data.bytesize}>"
      end

      private

      def deliver(payload, headers)
        raise Error, "request on #{subject} has no reply subject" unless reply_to

        publish(payload, headers)
        @responded = true
      end

      def publish(payload, headers)
        @client.publish(reply_to, payload, headers: headers)
      rescue Tether::Error => e
        @error = e.message
        raise
      end
    end
  end
end
