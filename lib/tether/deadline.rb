# frozen_string_literal: true

module Tether
  # An absolute point in time on the monotonic clock, threaded through every
  # blocking call in tether instead of a bare number of seconds.
  #
  # A Deadline is only ever consumed by primitives that interrupt themselves
  # (`Queue#pop(timeout:)`, `IO#timeout=`, `ConditionVariable#wait`). It is never
  # handed to `Timeout.timeout`, which raises at an arbitrary bytecode boundary
  # and can leave a half-written frame on the wire or a mutex locked forever.
  class Deadline
    # A deadline that never expires.
    NONE = nil

    # @return [Float, nil] the monotonic instant this deadline expires at, or nil if it never does
    attr_reader :at

    # Builds a deadline that expires after a duration from now.
    #
    # @param seconds [Numeric, nil] duration in seconds, or nil for a deadline that never expires
    # @return [Deadline]
    def self.after(seconds)
      new(seconds.nil? ? nil : now + seconds)
    end

    # Builds a deadline that never expires.
    #
    # @return [Deadline]
    def self.infinite
      new(nil)
    end

    # Accepts whatever a caller passed for a timeout and returns a Deadline.
    #
    # @param value [Deadline, Numeric, nil]
    # @return [Deadline]
    def self.coerce(value)
      return value if value.is_a?(self)

      after(value)
    end

    # Reads the monotonic clock, which never jumps backwards when the system
    # clock is adjusted.
    #
    # @return [Float] the current monotonic clock reading
    def self.now
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    # Wraps an absolute monotonic instant. Prefer {.after}, which takes a
    # duration.
    #
    # @param at [Float, nil] a monotonic instant, or nil for a deadline that never expires
    def initialize(at)
      @at = at
      freeze
    end

    # An infinite deadline reports nil remaining, which every blocking
    # primitive in Ruby already reads as "wait forever".
    #
    # @return [Boolean] whether this deadline can never expire
    def infinite?
      @at.nil?
    end

    # Seconds left before expiry, clamped at zero.
    #
    # Returns nil for an infinite deadline, which is what every blocking
    # primitive in Ruby already interprets as "wait forever".
    #
    # @return [Float, nil]
    def remaining
      return nil if infinite?

      left = @at - self.class.now
      left.negative? ? 0.0 : left
    end

    # @return [Boolean] whether the instant has passed; always false when infinite
    def expired?
      return false if infinite?

      @at <= self.class.now
    end

    # Raises if this deadline has already expired.
    #
    # @param subject [String, nil] included in the error message for context
    # @raise [TimeoutError]
    # @return [void]
    def check!(subject = nil)
      return unless expired?

      raise TimeoutError, subject ? "deadline expired: #{subject}" : "deadline expired"
    end

    # Narrows this deadline, never widens it.
    #
    # @param seconds [Numeric, nil]
    # @return [Deadline] the earlier of this deadline and now + seconds
    def with_at_most(seconds)
      return self if seconds.nil?

      candidate = self.class.now + seconds
      infinite? || candidate < @at ? self.class.new(candidate) : self
    end

    # Returns the earlier of two deadlines.
    #
    # @param other [Deadline]
    # @return [Deadline]
    def earliest(other)
      return other if infinite?
      return self if other.infinite?

      other.at < @at ? other : self
    end

    # @return [String] shows time remaining rather than the absolute instant,
    #   which is meaningless out of context
    def inspect
      infinite? ? "#<Tether::Deadline infinite>" : format("#<Tether::Deadline remaining=%.3fs>", remaining)
    end
  end
end
