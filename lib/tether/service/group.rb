# frozen_string_literal: true

module Tether
  class Service
    # A subject prefix and queue group shared by the endpoints added to it.
    # Groups nest.
    class Group
      # @return [String] the subject prefix, empty when the group adds none
      attr_reader :prefix

      # @api private
      # @param service [Service] the service the group belongs to
      # @param prefix [String] the composed subject prefix
      # @param queue [String, false] the queue group in effect, false for none
      def initialize(service, prefix:, queue:)
        @service = service
        @prefix = prefix
        @queue = queue
      end

      # @return [String, nil] the queue group endpoints inherit, nil when they get none
      def queue_group
        @queue || nil
      end

      # Adds an endpoint whose subject is prefixed by this group.
      #
      # @param name [String] the endpoint name: letters, digits, dash and underscore
      # @param subject [String, nil] the subject under the prefix; defaults to the name
      # @param queue [String, false, nil] a queue group, nil to use the group's, false for none
      # @param metadata [Hash] string pairs describing the endpoint
      # @yieldparam request [Request] each request, on the endpoint's worker thread
      # @return [Endpoint] the endpoint
      # @raise [ArgumentError] if the name or metadata is unusable or no block is given
      # @raise [InvalidSubjectError] if the subject or queue group is malformed or reserved
      # @raise [Error] if the service is stopped
      def endpoint(name, subject: nil, queue: nil, metadata: {}, &handler)
        @service.add_endpoint(
          Endpoint.new(service: @service, name: name, subject: join(subject || name),
                       queue_group: Rules.queue(queue, @queue) || nil, metadata: metadata, &handler)
        )
      end

      # Opens a nested group.
      #
      # @param name [String] a subject without `>`, or an empty string for no further prefix
      # @param queue [String, false, nil] a queue group, nil to use this group's, false for none
      # @return [Group] the nested group
      # @raise [InvalidSubjectError] if the name or queue group is malformed
      def group(name, queue: nil)
        Group.new(@service, prefix: join(Rules.group!(name)), queue: Rules.queue(queue, @queue))
      end

      # @return [String] the prefix and queue group
      def inspect
        "#<Tether::Service::Group prefix=#{@prefix.inspect} queue_group=#{queue_group.inspect}>"
      end

      private

      def join(part)
        [@prefix, part.to_s].reject(&:empty?).join(".")
      end
    end
  end
end
