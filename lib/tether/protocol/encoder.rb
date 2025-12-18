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
          frame << "PUB " << subject
          frame << " " << reply_to if reply_to
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
          frame << "HPUB " << subject
          frame << " " << reply_to if reply_to
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
          frame << "SUB " << subject
          frame << " " << queue if queue
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

        # Encodes a `CONNECT` frame.
        #
        # @param options [Hash] the CONNECT payload
        # @return [String] binary encoded frame
        def connect(options)
          json = JSON.generate(options)
          frame = buffer(json.bytesize + 16)
          frame << "CONNECT " << json << CRLF
        end

        private

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
