# frozen_string_literal: true

module Tether
  # Errors that arrive outside any call: an `-ERR` from the server, a slow
  # consumer, a protocol violation.
  #
  # They are recorded and handed to registered handlers rather than raised.
  # Throwing into an unrelated thread would interrupt it at whatever bytecode
  # it happened to be running, which for this client could be the middle of a
  # frame on the wire or the inside of a lock.
  class ErrorHolder
    def initialize
      @handlers = []
      @last = nil
      @lock = Mutex.new
    end

    # The most recent error, whether or not any handler was registered.
    #
    # @return [Exception, nil] the last error seen
    def last
      @lock.synchronize { @last }
    end

    # Registers a handler, called on whichever thread reported the error.
    #
    # @yieldparam error [Exception] the error that arrived
    # @return [void] nothing
    def subscribe(&handler)
      @lock.synchronize { @handlers << handler }
      nil
    end

    # Records an error and notifies handlers. A handler that raises is ignored:
    # this usually runs on the reader thread, which must survive it.
    #
    # @param error [Exception] the error to record
    # @return [void] nothing
    def record(error)
      handlers = @lock.synchronize do
        @last = error
        @handlers.dup
      end
      handlers.each do |handler|
        handler.call(error)
      rescue StandardError
        nil
      end
      nil
    end
  end
end
