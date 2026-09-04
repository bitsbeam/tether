# frozen_string_literal: true

require "json"
require "time"

module Tether
  class JetStream
    # A JSON structure of the JetStream API declared once as a table of fields.
    #
    # From the table a subclass gets a keyword initializer that rejects unknown
    # keys, readers, conversion to and from the wire (seconds and `Time` in
    # Ruby, nanoseconds and RFC 3339 on the wire, symbols for enumerations),
    # and `raw`: the hash the server sent, whose unknown keys survive a
    # read-modify-write.
    class Schema
      NANOSECONDS = 1_000_000_000
      ZERO_TIME = "0001-01-01T00:00:00Z"
      DYNAMIC_METADATA = /\A_nats\./

      # One declared field.
      Field = Data.define(:name, :key, :type, :values, :level)

      class << self
        # @return [Hash{Symbol => Field}] the fields of this class and its ancestors
        def fields
          @fields ||= superclass.respond_to?(:fields) ? superclass.fields.dup : {}
        end

        # Declares a field.
        #
        # @param name [Symbol] the Ruby name
        # @param type [Symbol, Class, Array] :string, :integer, :boolean, :float, :duration, :time, :enum,
        #   :object, a Schema subclass, or a one-element array of any of those
        # @param key [String] the JSON key
        # @param values [Hash{Symbol => String}, nil] the wire strings of an enumeration
        # @param level [Integer, Hash{Object => Integer}] the JetStream API level that introduced the field, or
        #   the level each value needs when only some values need one
        # @return [void] nothing
        def field(name, type, key: name.to_s, values: nil, level: 0)
          fields[name] = Field.new(name: name, key: key, type: type, values: values, level: level)
          define_method(name) { @values[name] }
        end

        # @param raw [Hash{String => Object}] a decoded JSON object
        # @return [Schema] the object, holding the hash it came from
        def from_h(raw)
          values = fields.each_value.with_object({}) do |field, memo|
            memo[field.name] = decode(field, field.type, raw[field.key]) if raw.key?(field.key)
          end
          new(raw: raw, **values)
        end

        # @param value [Schema, Hash] an instance, or the keywords to build one from
        # @return [Schema] an instance of this class
        def coerce(value)
          value.is_a?(self) ? value : new(**value)
        end

        # Turns hashes given for nested structures into their classes.
        #
        # @api private
        # @param type [Symbol, Class, Array] the declared type
        # @param value [Object] what the caller passed
        # @return [Object] the value, with nested structures as Schema instances
        def cast(type, value)
          return value if value.nil?
          return value.map { |item| cast(type.first, item) } if type.is_a?(Array) && value.is_a?(Array)
          return type.coerce(value) if type.is_a?(Class) && value.is_a?(Hash)

          value
        end

        # @api private
        # @param field [Field] the declared field
        # @param type [Symbol, Class, Array] the declared type, or the element type inside an array
        # @param value [Object] the wire value
        # @return [Object] the Ruby value
        def decode(field, type, value)
          return nil if value.nil?
          return value.map { |item| decode(field, type.first, item) } if type.is_a?(Array)
          return type.from_h(value) if type.is_a?(Class)

          case type
          when :duration then seconds(value)
          when :time then value == ZERO_TIME ? nil : Time.iso8601(value)
          when :enum then field.values.key(value) || value.to_sym
          else value
          end
        end

        # @api private
        # @param field [Field] the declared field
        # @param type [Symbol, Class, Array] the declared type, or the element type inside an array
        # @param value [Object] the Ruby value
        # @return [Object] the wire value
        # @raise [ArgumentError] if a symbol is not a value of the enumeration
        def encode(field, type, value)
          return value.map { |item| encode(field, type.first, item) } if type.is_a?(Array)
          return type.coerce(value).to_h if type.is_a?(Class)

          case type
          when :duration then (value * NANOSECONDS).round
          when :time then value.getutc.iso8601(9)
          when :enum then field.values.fetch(value) { unknown_enum(field, value) }
          else value
          end
        end

        private

        def seconds(nanoseconds)
          (nanoseconds % NANOSECONDS).zero? ? nanoseconds / NANOSECONDS : nanoseconds.fdiv(NANOSECONDS)
        end

        def unknown_enum(field, value)
          return value.to_s if value.is_a?(String)

          raise ArgumentError,
                "#{field.name}: #{value.inspect} is not one of #{field.values.keys.map(&:inspect).join(", ")}"
        end
      end

      # @return [Hash{String => Object}] the hash the server sent, or an empty hash for a new object
      attr_reader :raw

      # @param raw [Hash{String => Object}, nil] the hash this object was decoded from
      # @param values [Hash{Symbol => Object}] field values by Ruby name
      # @raise [ArgumentError] if a keyword is not a declared field
      def initialize(raw: nil, **values)
        unknown = values.keys - self.class.fields.keys
        unless unknown.empty?
          raise ArgumentError,
                "unknown #{self.class.name.to_s.split("::").last} field: #{unknown.join(", ")}"
        end

        @raw = (raw || {}).freeze
        @values = values.to_h { |name, value| [name, self.class.cast(self.class.fields[name].type, value)] }.freeze
        to_h
        freeze
      end

      # @param name [Symbol] a field name
      # @return [Object, nil] the field's value
      def [](name)
        @values[name.to_sym]
      end

      # @param changes [Hash{Symbol => Object}] fields to replace; nil removes a field
      # @return [Schema] a modified copy that keeps unknown keys
      def with(**changes)
        self.class.new(raw: @raw, **@values.merge(changes))
      end

      # @return [Hash{String => Object}] the wire form: only fields that are set, plus unknown keys from `raw`
      def to_h
        known = self.class.fields.each_value.to_h { |field| [field.key, field] }
        wire = @raw.reject { |key, _| known.key?(key) }
        @values.each do |name, value|
          field = self.class.fields[name]
          next if value.nil?

          wire[field.key] = unlisted?(field, value) ? value.to_s : self.class.encode(field, field.type, value)
        end
        strip_dynamic_metadata(wire)
      end

      # @return [String] the wire form as JSON
      def to_json(*)
        JSON.generate(to_h)
      end

      # @return [Integer] the highest API level among the fields that are set
      def api_level
        @values.map { |name, value| level_of(self.class.fields[name], value) }.max || 0
      end

      # @param other [Object] another object
      # @return [Boolean] whether both are the same class with the same wire form
      def ==(other)
        other.instance_of?(self.class) && other.to_h == to_h
      end
      alias eql? ==

      # @return [Integer] a hash of the wire form
      def hash
        to_h.hash
      end

      # @return [String] the class and the fields that are set
      def inspect
        set = @values.compact.map { |name, value| "#{name}=#{value.inspect}" }.join(" ")
        "#<#{self.class.name} #{set}>"
      end

      private

      def unlisted?(field, value)
        field.type == :enum && !field.values.key?(value) && @raw[field.key] == value.to_s
      end

      def level_of(field, value)
        return 0 if [nil, false, 0].include?(value) || (value.respond_to?(:empty?) && value.empty?)

        field.level.is_a?(Hash) ? field.level.fetch(value, 0) : field.level
      end

      def strip_dynamic_metadata(wire)
        metadata = wire["metadata"]
        return wire unless metadata.is_a?(Hash) && metadata.keys.any? { |key| key.to_s.match?(DYNAMIC_METADATA) }

        kept = metadata.reject { |key, _| key.to_s.match?(DYNAMIC_METADATA) }
        kept.empty? ? wire.except("metadata") : wire.merge("metadata" => kept)
      end
    end
  end
end
