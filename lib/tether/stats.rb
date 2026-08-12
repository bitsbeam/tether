# frozen_string_literal: true

module Tether
  # A snapshot of a connection's traffic counters.
  Stats = Data.define(:messages_in, :messages_out, :bytes_in, :bytes_out, :reconnects)

  # The live counters behind {Stats}.
  #
  # Each counter has exactly one writer context: inbound counts change only on
  # the reader thread and outbound counts only under the writer's locks, so no
  # lock of its own is needed.
  class Counters
    # @return [Integer] messages received
    attr_accessor :messages_in

    # @return [Integer] messages published
    attr_accessor :messages_out

    # @return [Integer] bytes read from the socket
    attr_accessor :bytes_in

    # @return [Integer] bytes written to the socket
    attr_accessor :bytes_out

    # @return [Integer] successful reconnects
    attr_accessor :reconnects

    def initialize
      @messages_in = @messages_out = @bytes_in = @bytes_out = @reconnects = 0
    end

    # @return [Stats] a frozen copy of the counters as they are now
    def snapshot
      Stats.new(messages_in: @messages_in, messages_out: @messages_out, bytes_in: @bytes_in,
                bytes_out: @bytes_out, reconnects: @reconnects)
    end
  end
end
