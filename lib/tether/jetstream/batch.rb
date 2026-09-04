# frozen_string_literal: true

module Tether
  class JetStream
    # The bounded result of one fetch.
    class Batch
      include Enumerable

      # @return [Exception, nil] why the pull ended early after delivering something, or nil
      attr_reader :error

      # @param messages [Array<Message>] what the pull delivered, in order
      # @param error [Exception, nil] why the pull ended early
      def initialize(messages, error = nil)
        @messages = messages.freeze
        @error = error
        freeze
      end

      # @yieldparam message [Message] each message, in the order delivered
      # @return [self, Enumerator] an enumerator when no block is given
      def each(&block)
        return enum_for(:each) { size } unless block

        @messages.each(&block)
        self
      end

      # @return [Integer] how many messages were delivered
      def size
        @messages.size
      end
      alias length size

      # @return [Boolean] whether nothing was delivered
      def empty?
        @messages.empty?
      end

      # @return [Array<Message>] the messages, in the order delivered
      def to_a
        @messages.dup
      end

      # @return [String] the size and the error without the messages
      def inspect
        "#<Tether::JetStream::Batch size=#{size} error=#{@error.inspect}>"
      end
    end
  end
end
