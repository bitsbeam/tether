# frozen_string_literal: true

require "json"

module Tether
  module Protocol
    # Builds complete protocol frames as single binary strings.
    #
    # Every method returns one whole frame. The writer takes its lock around the
    # write of that string, never around the pieces of it: frames from different
    # threads must not interleave on the wire.
    module Encoder
      # The server seeds every connection with `verbose:true, pedantic:true,
      # echo:true` and merges CONNECT into that struct, so an absent key keeps
      # the server's default rather than clearing it. Omitting `verbose` alone
      # makes the server acknowledge every PUB and SUB with `+OK`; omitting
      # `headers` makes it rewrite HMSG as MSG with the header bytes cut off,
      # which silently strips all JetStream metadata.
      DEFAULT_CONNECT = {
        verbose: false,
        pedantic: false,
        tls_required: false,
        lang: "ruby",
        version: Tether::VERSION,
        protocol: 1,
        echo: true,
        headers: true,
        no_responders: true
      }.freeze

      class << self
        # Encodes a `PUB` frame.
        #
        # @param subject [String]
        # @param payload [String, nil]
        # @param reply_to [String, nil]
        # @return [String] binary encoded frame
        def pub(subject, payload = nil, reply_to = nil)
          body = binary(payload)
          frame = buffer(subject.bytesize + body.bytesize + 64)
          write_head(frame, "PUB ", subject, reply_to)
          frame << " " << body.bytesize.to_s << CRLF
          frame << body << CRLF
        end

        # Encodes an `HPUB` frame.
        #
        # @param subject [String]
        # @param payload [String, nil]
        # @param headers [Headers, String] headers, or a pre-encoded header block
        # @param reply_to [String, nil]
        # @return [String] binary encoded frame
        def hpub(subject, payload, headers, reply_to = nil)
          block = headers.is_a?(Headers) ? headers.to_wire : binary(headers)
          body = binary(payload)
          frame = buffer(subject.bytesize + block.bytesize + body.bytesize + 64)
          write_head(frame, "HPUB ", subject, reply_to)
          frame << " " << block.bytesize.to_s
          frame << " " << (block.bytesize + body.bytesize).to_s << CRLF
          frame << block << body << CRLF
        end

        # Encodes a `SUB` frame.
        #
        # @param subject [String]
        # @param sid [Integer, String]
        # @param queue [String, nil] queue group
        # @return [String] binary encoded frame
        def sub(subject, sid, queue: nil)
          frame = buffer(subject.bytesize + 32)
          frame << "SUB " << binary(subject)
          frame << " " << binary(queue) if queue
          frame << " " << sid.to_s << CRLF
        end

        # Encodes an `UNSUB` frame.
        #
        # @param sid [Integer, String]
        # @param max_msgs [Integer, nil] unsubscribe after this many messages
        # @return [String] binary encoded frame
        def unsub(sid, max_msgs = nil)
          frame = buffer(32)
          frame << "UNSUB " << sid.to_s
          frame << " " << max_msgs.to_s if max_msgs
          frame << CRLF
        end

        # Encodes a `CONNECT` frame over {DEFAULT_CONNECT}.
        #
        # @param options [Hash] overrides for the defaults
        # @return [String] binary encoded frame
        # @raise [ArgumentError] if no_responders is requested without headers
        def connect(options = {})
          payload = DEFAULT_CONNECT.merge(options)
          if payload[:no_responders] && !payload[:headers]
            raise ArgumentError, "no_responders requires headers; the server closes the connection otherwise"
          end

          json = JSON.generate(payload)
          frame = buffer(json.bytesize + 16)
          frame << "CONNECT " << json << CRLF
        end

        private

        def write_head(frame, verb, subject, reply_to)
          frame << verb << binary(subject)
          frame << " " << binary(reply_to) if reply_to
          frame
        end

        def buffer(capacity)
          String.new(capacity: capacity, encoding: BINARY)
        end

        def binary(value)
          return EMPTY_PAYLOAD if value.nil?
          return value if value.encoding == BINARY || value.ascii_only?

          value.b
        end
      end
    end
  end
end
