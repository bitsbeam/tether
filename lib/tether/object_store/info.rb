# frozen_string_literal: true

require "json"

module Tether
  class ObjectStore
    # The metadata of one object: its directory entry in the bucket.
    class Info
      DIGEST_PREFIX = "SHA-256="
      KEYS = %w[name description headers metadata options bucket nuid size mtime chunks digest deleted].freeze
      OPTION_KEYS = %w[link max_chunk_size].freeze

      # What a link points at: an object of a bucket, or a whole bucket when `name` is nil.
      Link = Data.define(:bucket, :name)

      # @return [String] the object's name
      attr_reader :name

      # @return [String, nil] free text describing the object
      attr_reader :description

      # @return [Hash{String => Array<String>}] the headers stored with the object
      attr_reader :headers

      # @return [Hash{String => String}] the metadata stored with the object
      attr_reader :metadata

      # @return [String] the bucket that holds this entry
      attr_reader :bucket

      # @return [String] the id of the upload, the last token of the subject its chunks are stored under
      attr_reader :nuid

      # @return [Integer] the content's size in bytes
      attr_reader :size

      # @return [Integer] how many chunk messages hold the content
      attr_reader :chunks

      # @return [String, nil] `SHA-256=` followed by the URL-safe base64 of the content's SHA-256
      attr_reader :digest

      # @return [Integer, nil] the chunk size the upload used
      attr_reader :chunk_size

      # @return [Link, nil] what this entry points at when it is a link
      attr_reader :link

      # @return [Time, nil] when the entry was written
      attr_reader :modified_at

      # @return [Hash{String => Object}] the document this entry was decoded from; empty for a new entry
      attr_reader :raw

      # Decodes a stored metadata document.
      #
      # @param data [String] the JSON document
      # @param modified_at [Time, nil] when the message that carried it was stored
      # @return [Info] the entry
      # @raise [Error] if the document is not a JSON object
      def self.parse(data, modified_at: nil)
        raw = JSON.parse(data)
        raise Error, "the object metadata is not a JSON object" unless raw.is_a?(Hash)

        from_h(raw, modified_at: modified_at)
      rescue JSON::ParserError => e
        raise Error, "the object metadata is not valid JSON: #{e.message}"
      end

      # @param raw [Hash{String => Object}] a decoded metadata document
      # @param modified_at [Time, nil] when the message that carried it was stored
      # @return [Info] the entry, holding the hash it came from
      def self.from_h(raw, modified_at: nil)
        options = raw["options"].is_a?(Hash) ? raw["options"] : {}
        link = options["link"]
        new(name: raw["name"].to_s, bucket: raw["bucket"].to_s, nuid: raw["nuid"].to_s, size: raw["size"].to_i,
            chunks: raw["chunks"].to_i, digest: raw["digest"], description: raw["description"],
            headers: raw["headers"], metadata: raw["metadata"], chunk_size: options["max_chunk_size"],
            link: link.is_a?(Hash) ? Link.new(bucket: link["bucket"].to_s, name: link["name"]) : nil,
            deleted: raw["deleted"] == true, modified_at: modified_at, raw: raw)
      end

      # Builds the digest string of a finished hash.
      #
      # @param hash [String] the 32 bytes of a SHA-256
      # @return [String] `SHA-256=` and the URL-safe, padded base64 of the hash
      def self.digest_of(hash)
        DIGEST_PREFIX + [hash].pack("m0").tr("+/", "-_")
      end

      # @param name [String] the object's name
      # @param bucket [String] the bucket that holds the entry
      # @param nuid [String] the id of the upload
      # @param size [Integer] the content's size in bytes
      # @param chunks [Integer] how many chunk messages hold the content
      # @param digest [String, nil] the content's digest
      # @param description [String, nil] free text describing the object
      # @param headers [Protocol::Headers, Hash, nil] headers stored with the object
      # @param metadata [Hash, nil] metadata stored with the object
      # @param chunk_size [Integer, nil] the chunk size the upload used
      # @param link [Link, nil] what the entry points at
      # @param deleted [Boolean] whether the entry is a tombstone
      # @param modified_at [Time, nil] when the entry was written
      # @param raw [Hash{String => Object}, nil] the document the entry was decoded from
      def initialize(name:, bucket:, nuid:, size: 0, chunks: 0, digest: nil, description: nil, headers: nil,
                     metadata: nil, chunk_size: nil, link: nil, deleted: false, modified_at: nil, raw: nil)
        @name = name.dup.freeze
        @bucket = bucket
        @nuid = nuid
        @size = size
        @chunks = chunks
        @digest = digest.to_s.empty? ? nil : digest
        @description = description.to_s.empty? ? nil : description
        @headers = header_values(headers)
        @metadata = (metadata || {}).to_h { |key, value| [key.to_s, value.to_s] }.freeze
        @chunk_size = chunk_size&.positive? ? chunk_size : nil
        @link = link&.with(name: link.name.to_s.empty? ? nil : link.name)
        @deleted = deleted
        @modified_at = modified_at
        @raw = (raw || {}).freeze
        freeze
      end

      # @return [Boolean] whether the entry is the tombstone of a deleted object
      def deleted?
        @deleted
      end

      # @return [Boolean] whether the entry points at another object or bucket instead of holding content
      def link?
        !@link.nil?
      end

      # @return [Boolean] whether the entry points at a whole bucket
      def bucket_link?
        link? && @link.name.nil?
      end

      # @return [String, nil] the raw 32 bytes of the recorded SHA-256, or nil when the digest cannot be decoded
      def digest_bytes
        algorithm, encoded = @digest.to_s.split("=", 2)
        return nil unless algorithm == "SHA-256" && encoded&.match?(/\A[A-Za-z0-9_-]+=*\z/)

        bytes = encoded.tr("-_", "+/").unpack1("m")
        bytes.bytesize == 32 ? bytes : nil
      end

      # @param changes [Hash{Symbol => Object}] fields to replace
      # @return [Info] a modified copy that keeps unknown keys of the stored document
      def with(**changes)
        self.class.new(**fields, **changes)
      end

      # @return [Hash{String => Object}] the stored form: empty optional fields and the modification time
      #   are left out, unknown keys of the document read are kept
      def to_h
        options = stored_options
        document = { "name" => @name, "description" => @description, "headers" => presence(@headers),
                     "metadata" => presence(@metadata), "options" => presence(options), "bucket" => @bucket,
                     "nuid" => @nuid, "size" => @size, "chunks" => @chunks, "digest" => @digest,
                     "deleted" => @deleted || nil }.compact
        document.merge(@raw.except(*KEYS))
      end

      # @return [String] the stored form as JSON
      def to_json(*)
        JSON.generate(to_h)
      end

      # @param other [Object] another object
      # @return [Boolean] whether both entries have the same stored form
      def ==(other)
        other.instance_of?(self.class) && other.to_h == to_h
      end
      alias eql? ==

      # @return [Integer] a hash of the stored form
      def hash
        to_h.hash
      end

      # @return [String] the bucket, name and size without the rest
      def inspect
        state = [("deleted" if deleted?), ("link" if link?)].compact.join(" ")
        "#<Tether::ObjectStore::Info bucket=#{@bucket.inspect} name=#{@name.inspect} size=#{@size}" \
          "#{" #{state}" unless state.empty?}>"
      end

      private

      def fields
        { name: @name, bucket: @bucket, nuid: @nuid, size: @size, chunks: @chunks, digest: @digest,
          description: @description, headers: @headers, metadata: @metadata, chunk_size: @chunk_size,
          link: @link, deleted: @deleted, modified_at: @modified_at, raw: @raw }
      end

      def header_values(headers)
        headers.to_h.to_h { |key, values| [key.to_s, Array(values).map(&:to_s).freeze] }.freeze
      end

      def stored_options
        kept = @raw["options"].is_a?(Hash) ? @raw["options"].except(*OPTION_KEYS) : {}
        link = @link && { "bucket" => @link.bucket, "name" => @link.name }.compact
        { "link" => link, "max_chunk_size" => @chunk_size }.compact.merge(kept)
      end

      def presence(value)
        value.empty? ? nil : value
      end
    end
  end
end
