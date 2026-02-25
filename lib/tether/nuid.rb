# frozen_string_literal: true

require "securerandom"

module Tether
  # Generator for NATS unique identifiers: a 12 character random prefix followed
  # by a 10 character base62 sequence, incremented by a random step so that
  # identifiers are neither guessable nor collision prone across processes.
  #
  # Use {NUID.next}. It keeps one generator per thread rather than sharing a
  # locked one, so there is no contention and no shared counter to race over.
  # Two generators collide only if their 12 character random prefixes do, which
  # is the same assumption that makes NUIDs unique across processes.
  #
  # Fibers on a thread share its generator. They never run concurrently and
  # generating an identifier blocks on nothing, so nothing can interleave.
  class NUID
    DIGITS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    BASE = 62
    PREFIX_LENGTH = 12
    SEQUENCE_LENGTH = 10
    TOTAL_LENGTH = PREFIX_LENGTH + SEQUENCE_LENGTH
    MAX_SEQUENCE = BASE**SEQUENCE_LENGTH
    MIN_INCREMENT = 33
    MAX_INCREMENT = 333
    # Reserves room for the sequence so the identifier is built in one string.
    PADDING = ("0" * SEQUENCE_LENGTH).freeze

    # Where each thread keeps its generator.
    STORAGE_KEY = :tether_nuid

    class << self
      # Generates an identifier from this thread's generator, creating one on
      # first use.
      #
      # @return [String] a 22 character identifier
      def next
        thread = Thread.current
        generator = thread.thread_variable_get(STORAGE_KEY)
        generator or thread.thread_variable_set(STORAGE_KEY, generator = new)
        generator.next
      end
    end

    # Seeds a generator with its own random prefix, so two generators in the
    # same process do not collide. Private: use {NUID.next}, which keeps one
    # per thread; a generator shared across threads would race on its counter.
    def initialize
      reseed!
    end

    # Generates the next identifier.
    #
    # @return [String] a 22 character identifier
    def next
      @sequence += @increment
      reseed! if @sequence >= MAX_SEQUENCE

      buffer = @prefix + PADDING
      value = @sequence
      (TOTAL_LENGTH - 1).downto(PREFIX_LENGTH) do |index|
        buffer.setbyte(index, DIGITS.getbyte(value % BASE))
        value /= BASE
      end
      buffer
    end

    private_class_method :new

    private

    def reseed!
      @prefix = SecureRandom.random_bytes(PREFIX_LENGTH).each_byte.with_object(+"") do |byte, out|
        out << DIGITS.getbyte(byte % BASE)
      end
      @sequence = SecureRandom.random_number(MAX_SEQUENCE)
      @increment = MIN_INCREMENT + SecureRandom.random_number(MAX_INCREMENT - MIN_INCREMENT)
    end
  end
end
