# frozen_string_literal: true

module Tether
  class Service
    # The validation rules of ADR-32, shared by services, groups and endpoints.
    #
    # @api private
    module Rules
      NAME = /\A[A-Za-z0-9\-_]+\z/
      SUBJECT = /\A[^ >]*>?\z/
      SEMVER = /\A(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)
                (?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?
                (?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?\z/x

      class << self
        # @param value [String] a service or endpoint name
        # @param kind [String] what the value is, for the error message
        # @return [String] a frozen copy of the name
        # @raise [ArgumentError] if it holds anything but letters, digits, dash and underscore
        def name!(value, kind)
          return value.dup.freeze if value.is_a?(String) && value.match?(NAME)

          raise ArgumentError, "invalid #{kind}: #{value.inspect}; use letters, digits, - and _"
        end

        # @param value [String] a service version
        # @return [String] a frozen copy of the version
        # @raise [ArgumentError] if it is not a semantic version
        def version!(value)
          return value.dup.freeze if value.is_a?(String) && value.match?(SEMVER)

          raise ArgumentError, "invalid service version: #{value.inspect}; use a semantic version such as 1.2.3"
        end

        # @param value [String] a composed endpoint subject or a queue group
        # @param kind [String] what the value is, for the error message
        # @return [String] a frozen copy of the value
        # @raise [InvalidSubjectError] if it is empty, holds whitespace, or holds `>` anywhere but last
        def subject!(value, kind)
          text = Protocol::Subject.check!(value, kind)
          raise InvalidSubjectError, "invalid #{kind}: #{value.inspect}" unless text.match?(SUBJECT)

          text.dup.freeze
        end

        # @param value [String] a group name
        # @return [String] a frozen copy of the name
        # @raise [InvalidSubjectError] if it holds `>`, whitespace or an empty token
        def group!(value)
          text = value.to_s
          return "" if text.empty?
          raise InvalidSubjectError, "invalid group name: #{value.inspect}; > is not allowed" if text.include?(">")

          Protocol::Subject.pattern!(text).dup.freeze
        end

        # Resolves the queue group of one level against the level above it.
        #
        # @param own [String, false, nil] this level's setting: a name, false for none, nil to inherit
        # @param inherited [String, false] what the parent resolved to
        # @return [String, false] the queue group in effect, false for none
        # @raise [InvalidSubjectError] if the name is malformed
        def queue(own, inherited)
          return false if own == false
          return inherited if own.nil?

          subject!(own, "queue group")
        end

        # @param value [Hash, nil] metadata as given by the caller
        # @return [Hash{String => String}] a frozen copy with string keys and values
        # @raise [ArgumentError] if it is not a hash
        def metadata(value)
          raise ArgumentError, "metadata must be a Hash, got #{value.inspect}" unless value.nil? || value.is_a?(Hash)

          (value || {}).to_h { |key, item| [key.to_s.dup.freeze, item.to_s.dup.freeze] }.freeze
        end
      end
    end
  end
end
