# frozen_string_literal: true

require "openssl"

module Tether
  class Auth
    # An NKEY user key pair: decodes a seed and signs with Ed25519 through
    # OpenSSL, so no extra gem is needed.
    class NKey
      ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"
      SEED_PREFIX = 18 << 3
      PKCS8_PREFIX = ["302e020100300506032b657004220420"].pack("H*").freeze

      # @return [String] the public key in its text form, starting with the key type letter
      attr_reader :public_key

      # @param seed [String] the text seed, starting with `S`
      # @raise [CredentialsError] if the seed is malformed or fails its checksum
      def initialize(seed)
        raw = self.class.decode(seed.to_s.strip)
        raise CredentialsError, "not an NKEY seed" unless raw.bytesize == 34 && (raw.getbyte(0) & 0xF8) == SEED_PREFIX

        type = ((raw.getbyte(0) & 0x07) << 5) | (raw.getbyte(1) >> 3)
        @key = OpenSSL::PKey.read(PKCS8_PREFIX + raw.byteslice(2, 32))
        @public_key = self.class.encode([type].pack("C") + @key.public_to_der.byteslice(-32, 32))
      rescue OpenSSL::PKey::PKeyError => e
        raise CredentialsError, "could not load the NKEY seed: #{e.message}"
      end

      # @param data [String] the bytes to sign, which is the server's nonce
      # @return [String] the signature, base64url encoded without padding
      def sign(data)
        [@key.sign(nil, data)].pack("m0").tr("+/", "-_").delete("=")
      end

      # @param text [String] base32 text carrying a trailing CRC16
      # @return [String] the bytes before the checksum
      # @raise [CredentialsError] if a character is invalid or the checksum does not match
      def self.decode(text)
        bits = text.each_char.map do |char|
          index = ALPHABET.index(char) or raise CredentialsError, "invalid character in NKEY"
          index.to_s(2).rjust(5, "0")
        end.join
        bytes = [bits[0, bits.size - (bits.size % 8)]].pack("B*")
        body = bytes.byteslice(0, bytes.bytesize - 2)
        raise CredentialsError, "NKEY checksum mismatch" unless bytes.byteslice(-2, 2) == [crc16(body)].pack("v")

        body
      end

      # @param bytes [String] the bytes to encode
      # @return [String] base32 text with a trailing CRC16
      def self.encode(bytes)
        data = bytes + [crc16(bytes)].pack("v")
        bits = data.unpack1("B*")
        bits += "0" * ((5 - (bits.size % 5)) % 5)
        bits.scan(/.{5}/).map { |chunk| ALPHABET[chunk.to_i(2)] }.join
      end

      # @param bytes [String] the bytes to checksum
      # @return [Integer] CRC16 in the XMODEM variant
      def self.crc16(bytes)
        bytes.each_byte.reduce(0) do |crc, byte|
          crc ^= byte << 8
          8.times { crc = crc.anybits?(0x8000) ? ((crc << 1) ^ 0x1021) : (crc << 1) }
          crc & 0xFFFF
        end
      end
    end
  end
end
