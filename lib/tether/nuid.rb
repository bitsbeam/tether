# frozen_string_literal: true

require "securerandom"

module Tether
  # Generator for NATS unique identifiers: a 12 character random prefix followed
  # by a 10 character base62 sequence, incremented by a random step so that
  # identifiers are neither guessable nor collision prone across processes.
  #
  # Instances are not thread safe. Use {NUID.next} for the process wide,
  # mutex guarded generator.
  class NUID
    DIGITS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    BASE = 62
    PREFIX_LENGTH = 12
    SEQUENCE_LENGTH = 10
    TOTAL_LENGTH = PREFIX_LENGTH + SEQUENCE_LENGTH
    MAX_SEQUENCE = BASE**SEQUENCE_LENGTH
    MIN_INCREMENT = 33
    MAX_INCREMENT = 333
    PADDING = ("0" * SEQUENCE_LENGTH).freeze
    private_constant :PADDING

    @mutex = Mutex.new
    @generator = nil

    class << self
      # Generates an identifier from the process wide generator.
      #
      # @return [String] a 22 character identifier
      def next
        @mutex.synchronize { (@generator ||= new).next }
      end
    end

    # Seeds a generator with its own random prefix, so two instances in the
    # same process never collide.
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
