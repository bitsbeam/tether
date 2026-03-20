# frozen_string_literal: true

require "securerandom"

module Tether
  module Connection
    # Generator for NATS unique identifiers: a 12 character random prefix followed
    # by a 10 character base62 sequence, incremented by a random step so that
    # identifiers are neither guessable nor collision prone across processes.
    #
    # One generator for the process, behind a lock. Taking the lock costs about
    # as much as the arithmetic it guards, and an unguarded counter would let two
    # threads produce the same identifier, which for an inbox prefix means one
    # connection receiving another's replies.
    module NUID
      # The base62 alphabet, in ascending byte order so that identifiers sort the
      # same way their sequences do.
      DIGITS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

      # Size of the alphabet.
      BASE = 62

      # Characters of randomness at the front, regenerated when the sequence wraps.
      PREFIX_LENGTH = 12

      # Characters of sequence after the prefix.
      SEQUENCE_LENGTH = 10

      # Length of a complete identifier.
      TOTAL_LENGTH = PREFIX_LENGTH + SEQUENCE_LENGTH

      # The sequence wraps here and the prefix is regenerated.
      MAX_SEQUENCE = BASE**SEQUENCE_LENGTH

      # Smallest step between consecutive identifiers.
      MIN_INCREMENT = 33

      # Largest step between consecutive identifiers, exclusive.
      MAX_INCREMENT = 333

      # Reserves room for the sequence so an identifier is built in one string.
      PADDING = ("0" * SEQUENCE_LENGTH).freeze

      # Guards the prefix, sequence, and increment.
      LOCK = Mutex.new

      class << self
        # Generates the next identifier.
        #
        # @return [String] a 22 character identifier
        def next
          LOCK.synchronize do
            reseed! if @pid != Process.pid

            @sequence += @increment
            reseed! if @sequence >= MAX_SEQUENCE
            render
          end
        end

        private

        def render
          buffer = @prefix + PADDING
          value = @sequence
          (TOTAL_LENGTH - 1).downto(PREFIX_LENGTH) do |index|
            buffer.setbyte(index, DIGITS.getbyte(value % BASE))
            value /= BASE
          end
          buffer
        end

        # Also runs after a fork, where the child would otherwise inherit the
        # parent's prefix and sequence and hand out identifiers it already used.
        def reseed!
          @prefix = SecureRandom.random_bytes(PREFIX_LENGTH).each_byte.with_object(+"") do |byte, out|
            out << DIGITS.getbyte(byte % BASE)
          end
          @sequence = SecureRandom.random_number(MAX_SEQUENCE)
          @increment = MIN_INCREMENT + SecureRandom.random_number(MAX_INCREMENT - MIN_INCREMENT)
          @pid = Process.pid
        end
      end

      LOCK.synchronize { send(:reseed!) }
    end
  end
end
