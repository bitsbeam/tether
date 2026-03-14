# frozen_string_literal: true

module Tether
  # A message delivered to a subscription or returned from a request.
  #
  # Headers are parsed on first use, on whichever thread touches them, so the
  # reader thread never pays for a header block it may not need.
  class Msg
    # @return [String] the subject this was delivered on, not the pattern subscribed to
    attr_reader :subject

    # @return [String] the payload, always binary
    attr_reader :data

    # @return [String, nil] the subject a reply should be published to
    attr_reader :reply_to

    # @return [Subscription, nil] the subscription that received it, absent for a synthesised message
    attr_reader :subscription

    # Built by the dispatcher on the reader thread, so it does no work beyond
    # holding references.
    #
    # @param subject [String] the subject the message was delivered on
    # @param data [String] the payload
    # @param reply_to [String, nil] the subject a reply should be published to
    # @param header_bytes [String, nil] the unparsed header block
    # @param subscription [Subscription, nil] the subscription that received it
    def initialize(subject:, data:, reply_to: nil, header_bytes: nil, subscription: nil)
      @subject = subject
      @data = data
      @reply_to = reply_to
      @header_bytes = header_bytes
      @subscription = subscription
    end

    # Parses the header block on first use, on whichever thread asks for it.
    #
    # @return [Headers, nil] nil when the message arrived without a header block
    # @raise [ProtocolError] if the block is malformed
    def headers
      return @headers if defined?(@headers)

      @headers = @header_bytes && Headers.parse(@header_bytes)
    end

    # JetStream and no-responders both report through this rather than `-ERR`.
    #
    # @return [Integer, nil] the status the server put on the header line
    def status
      headers&.status
    end

    # True on the empty 503 the server sends back when a request reaches a
    # subject with no subscribers.
    #
    # @return [Boolean] whether the server reported that nobody is listening
    def no_responders?
      headers&.no_responders? || false
    end

    # Publishes back to this message's reply subject.
    #
    # @param payload [String, nil] the reply body
    # @param headers [Headers, nil] headers to send with the reply
    # @return [void] nothing
    # @raise [Error] if the message carries no reply subject
    def respond(payload = nil, headers: nil)
      raise Error, "message on #{@subject} has no reply subject" unless @reply_to

      responder = @subscription&.responder
      raise Error, "message on #{@subject} is not attached to a connection" unless responder

      responder.publish(@reply_to, payload, headers: headers)
    end

    # Status only frames, such as a heartbeat or a 503, carry no payload.
    #
    # @return [Boolean] whether this carries no payload
    def empty?
      @data.nil? || @data.empty?
    end

    # @return [String] reports the payload's size rather than its bytes, which
    #   may be binary and large
    def inspect
      "#<Tether::Msg subject=#{@subject.inspect} reply_to=#{@reply_to.inspect} bytes=#{@data.bytesize}>"
    end
  end
end
