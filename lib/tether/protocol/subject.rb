# frozen_string_literal: true

module Tether
  module Protocol
    # Subject validation and matching, as the server defines them.
    module Subject
      UNSAFE = /[\s\0]/
      WILDCARD_TOKEN = /(?:\A|\.)[*>](?:\.|\z)/
      NOT_LITERAL = /[\s\0]|\A\.|\.\z|\.\.|(?:\A|\.)[*>](?:\.|\z)/

      class << self
        # Checks that a value cannot corrupt the protocol stream.
        #
        # @param value [String] a subject, reply subject or queue group
        # @param kind [String] what the value is, for the error message
        # @return [String] the value
        # @raise [InvalidSubjectError] if it is empty or contains whitespace or a control character
        def check!(value, kind = "subject")
          text = value.to_s
          return text unless text.empty? || text.match?(UNSAFE)

          raise InvalidSubjectError, "invalid #{kind}: #{value.inspect}"
        end

        # Checks that a subject can be published to: safe, with no empty token
        # and no wildcard token.
        #
        # @param value [String] a publish subject
        # @return [String] the subject
        # @raise [InvalidSubjectError] if it is not a literal subject
        def literal!(value)
          return value if value.is_a?(String) && !value.empty? && !value.match?(NOT_LITERAL)

          check!(value)
          raise InvalidSubjectError, "not a literal subject: #{value.inspect}"
        end

        # Checks that a subject can be subscribed to: safe, with no empty token.
        #
        # @param value [String] a subscription subject, which may hold wildcards
        # @return [String] the subject
        # @raise [InvalidSubjectError] if it has an empty token
        def pattern!(value)
          text = check!(value)
          raise InvalidSubjectError, "subject has an empty token: #{value.inspect}" if empty_token?(text)

          text
        end

        # @param pattern [String] a subject that may hold `*` and `>` tokens
        # @param subject [String] a literal subject
        # @return [Boolean] whether a subscription on the pattern receives the subject
        def match?(pattern, subject)
          wanted = pattern.split(".", -1)
          given = subject.split(".", -1)
          wanted.each_with_index do |token, index|
            return index < given.size if token == ">"
            return false unless token == "*" ? index < given.size : token == given[index]
          end
          wanted.size == given.size
        end

        private

        def empty_token?(text)
          text.start_with?(".") || text.end_with?(".") || text.include?("..")
        end
      end
    end
  end
end
