# frozen_string_literal: true

module Tether
  # A message delivered to a subscription or returned from a request.
  #
  # Headers are parsed on first use, on whichever thread touches them, so the
  # reader thread never pays for a header block it may not need.
  class Msg
    # @return [String]
    attr_reader :subject

    # @return [String] the payload, always binary
    attr_reader :data

    # @return [String, nil] the subject a reply should be published to
    attr_reader :reply_to

    # @return [Subscription, nil]
    attr_reader :subscription

    # @param subject [String]
    # @param data [String]
    # @param reply_to [String, nil]
    # @param header_bytes [String, nil] the unparsed header block
    # @param subscription [Subscription, nil]
    def initialize(subject:, data:, reply_to: nil, header_bytes: nil, subscription: nil)
      @subject = subject
      @data = data
      @reply_to = reply_to
      @header_bytes = header_bytes
      @subscription = subscription
    end

    # @return [Headers, nil] nil when the message arrived without a header block
    def headers
      return @headers if defined?(@headers)

      @headers = @header_bytes && Headers.parse(@header_bytes)
    end

    # @return [Integer, nil] the status the server put on the header line
    def status
      headers&.status
    end

    # @return [Boolean] whether the server reported that nobody is listening
    def no_responders?
      headers&.no_responders? || false
    end

    # @return [Boolean] whether this carries a payload
    def empty?
      @data.nil? || @data.empty?
    end

    # @return [String]
    def inspect
      "#<Tether::Msg subject=#{@subject.inspect} reply_to=#{@reply_to.inspect} bytes=#{@data.bytesize}>"
    end
  end
end
