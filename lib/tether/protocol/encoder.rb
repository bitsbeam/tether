# frozen_string_literal: true

require "json"
require "tether/version"

module Tether
  module Protocol
    # Builds complete protocol frames as single binary strings.
    #
    # Every method returns one whole frame. The writer takes its lock around the
    # write of that string, never around the pieces of it: frames from different
    # threads must not interleave on the wire.
    #
    # The control line is interpolated and then retagged as binary, which looks
    # roundabout and is deliberate.
    #
    # Interpolation compiles to a single instruction that assembles the string
    # in C, where appending onto a preallocated buffer is one method call per
    # piece: 456ns against 711ns per frame, which on a publish benchmark was the
    # entire remaining difference against nats-pure.
    #
    # `force_encoding` costs nothing, since it only retags bytes on a string
    # this method just built, and it has to happen before the payload is
    # appended. A multi-byte subject makes the interpolated control line UTF-8,
    # and appending binary payload bytes to a UTF-8 string raises.
    #
    # Interpolating the payload into the same string, as nats-pure does, is
    # faster still at 223ns, and cannot express that case at all: a non-ASCII
    # subject with a binary payload raises there. The two appends are what buy
    # binary correctness.
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
        # @param subject [String] the subject to publish to
        # @param payload [String, nil] the payload; nil encodes a zero length body
        # @param reply_to [String, nil] the subject a responder should answer on
        # @return [String] binary encoded frame
        def pub(subject, payload = nil, reply_to = nil)
          body = binary(payload)
          frame = if reply_to
                    "PUB #{subject} #{reply_to} #{body.bytesize}#{CRLF}"
                  else
                    "PUB #{subject} #{body.bytesize}#{CRLF}"
                  end
          frame.force_encoding(BINARY)
          frame << body << CRLF
        end

        # Encodes an `HPUB` frame.
        #
        # @param subject [String] the subject to publish to
        # @param payload [String, nil] the payload; nil encodes a zero length body
        # @param headers [Headers, String] headers, or a pre-encoded header block
        # @param reply_to [String, nil] the subject a responder should answer on
        # @return [String] binary encoded frame
        def hpub(subject, payload, headers, reply_to = nil)
          block = headers.is_a?(Headers) ? headers.to_wire : binary(headers)
          body = binary(payload)
          total = block.bytesize + body.bytesize
          frame = if reply_to
                    "HPUB #{subject} #{reply_to} #{block.bytesize} #{total}#{CRLF}"
                  else
                    "HPUB #{subject} #{block.bytesize} #{total}#{CRLF}"
                  end
          frame.force_encoding(BINARY)
          frame << block << body << CRLF
        end

        # Encodes a `SUB` frame.
        #
        # @param subject [String] the subject or pattern to receive on
        # @param sid [Integer, String] the token the server will echo back on every message
        # @param queue [String, nil] a queue group to join, so the server gives each
        #   message to only one member
        # @return [String] binary encoded frame
        def sub(subject, sid, queue: nil)
          frame = queue ? "SUB #{subject} #{queue} #{sid}#{CRLF}" : "SUB #{subject} #{sid}#{CRLF}"
          frame.force_encoding(BINARY)
        end

        # Encodes an `UNSUB` frame.
        #
        # @param sid [Integer, String] the token given at subscribe time
        # @param max_msgs [Integer, nil] unsubscribe after this many messages
        # @return [String] binary encoded frame
        def unsub(sid, max_msgs = nil)
          frame = max_msgs ? "UNSUB #{sid} #{max_msgs}#{CRLF}" : "UNSUB #{sid}#{CRLF}"
          frame.force_encoding(BINARY)
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

          "CONNECT #{JSON.generate(payload)}#{CRLF}".force_encoding(BINARY)
        end

        private

        def binary(value)
          return EMPTY_PAYLOAD if value.nil?
          return value if value.encoding == BINARY || value.ascii_only?

          value.b
        end
      end
    end
  end
end
