# frozen_string_literal: true

module Tether
  class JetStream
    # Consumer management on the context, taking the stream name first.
    module Consumers
      MAX_NAME_BYTES = 255
      ACTIONS = { create: "create", update: "update", create_or_update: "" }.freeze

      # Creates a consumer. Without `name:` or `durable:` it is ephemeral and gets a generated name.
      #
      # @param stream [String] the stream's name
      # @param config [ConsumerConfig, Hash, nil] the configuration, or give its fields as keywords
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @param fields [Hash{Symbol => Object}] {ConsumerConfig} fields
      # @return [Consumer] a handle to the consumer
      # @raise [ConsumerExistsError] if a consumer with this name exists with another configuration
      # @raise [StreamNotFoundError] if the stream does not exist
      def create_consumer(stream, config = nil, timeout: nil, **fields)
        write_consumer(stream, :create, config, fields, timeout)
      end

      # Replaces the configuration of an existing consumer.
      #
      # @param stream [String] the stream's name
      # @param config [ConsumerConfig, Hash, nil] the whole configuration, or give its fields as keywords
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @param fields [Hash{Symbol => Object}] {ConsumerConfig} fields
      # @return [Consumer] a handle to the consumer
      # @raise [ConsumerNotFoundError] if the consumer does not exist
      def update_consumer(stream, config = nil, timeout: nil, **fields)
        write_consumer(stream, :update, config, fields, timeout)
      end

      # Creates a consumer, or replaces its configuration when it exists.
      #
      # @param stream [String] the stream's name
      # @param config [ConsumerConfig, Hash, nil] the configuration, or give its fields as keywords
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @param fields [Hash{Symbol => Object}] {ConsumerConfig} fields
      # @return [Consumer] a handle to the consumer
      def create_or_update_consumer(stream, config = nil, timeout: nil, **fields)
        write_consumer(stream, :create_or_update, config, fields, timeout)
      end

      # Looks a consumer up. Never creates one.
      #
      # @param stream [String] the stream's name
      # @param name [String] the consumer's name
      # @param lookup [Boolean] false returns a handle bound by name alone, without asking the server: its
      #   {Consumer#cached_info} holds only the names until {Consumer#info} is called, so pulls are not
      #   checked against its priority groups and its messages take acknowledgments
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @return [Consumer] a handle to the consumer
      # @raise [ConsumerNotFoundError] if the consumer does not exist and was looked up
      # @raise [StreamNotFoundError] if the stream does not exist and was looked up
      def consumer(stream, name, lookup: true, timeout: nil)
        stream = check_name(stream)
        name = check_consumer_name(name)
        unless lookup
          return Consumer.new(self, ConsumerInfo.new(stream: stream, name: name, config: { name: name }),
                              described: false)
        end

        Consumer.new(self, ConsumerInfo.from_h(@api.request("CONSUMER.INFO.#{stream}.#{name}", timeout: timeout)))
      end

      # @param stream [String] the stream's name
      # @param name [String] the consumer's name
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @return [Boolean] whether the consumer exists
      def consumer?(stream, name, timeout: nil)
        consumer(stream, name, timeout: timeout)
        true
      rescue ConsumerNotFoundError, StreamNotFoundError
        false
      end

      # @param stream [String] the stream's name
      # @param timeout [Numeric, nil] how long each page request may take
      # @return [Enumerator::Lazy<Consumer>] every consumer of the stream, fetched a page at a time
      def consumers(stream, timeout: nil)
        @api.pages("CONSUMER.LIST.#{check_name(stream)}", "consumers", timeout: timeout).map do |info|
          Consumer.new(self, ConsumerInfo.from_h(info))
        end
      end

      # @param stream [String] the stream's name
      # @param timeout [Numeric, nil] how long each page request may take
      # @return [Enumerator::Lazy<String>] every consumer name of the stream, fetched a page at a time
      def consumer_names(stream, timeout: nil)
        @api.pages("CONSUMER.NAMES.#{check_name(stream)}", "consumers", timeout: timeout)
      end

      # Deletes a consumer.
      #
      # @param stream [String] the stream's name
      # @param name [String] the consumer's name
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @return [Boolean] whether the server confirmed the deletion
      # @raise [ConsumerNotFoundError] if the consumer does not exist
      def delete_consumer(stream, name, timeout: nil)
        subject = "CONSUMER.DELETE.#{check_name(stream)}.#{check_consumer_name(name)}"
        @api.request(subject, timeout: timeout)["success"] == true
      end

      # Describes an ordered consumer: strictly ordered, gap-free delivery
      # without acknowledgments. Nothing is created on the server until it is read from.
      #
      # @param stream [String] the stream's name
      # @param options [Hash{Symbol => Object}] see {Ordered#initialize}
      # @return [Ordered] the ordered consumer
      def ordered_consumer(stream, **options)
        Ordered.new(self, check_name(stream), **options)
      end

      private

      def write_consumer(stream, action, config, fields, timeout)
        config = consumer_config(config, fields)
        subject = "CONSUMER.CREATE.#{check_name(stream)}.#{config.name}"
        subject = "#{subject}.#{config.filter_subject}" if filter_in_subject?(config)
        request = { "stream_name" => stream.to_s, "config" => config.to_h, "action" => ACTIONS.fetch(action) }
        response = @api.request(subject, request, timeout: timeout, level: config.api_level)
        Consumer.new(self, ConsumerInfo.from_h(response))
      end

      def consumer_config(config, fields)
        config = config ? ConsumerConfig.coerce(config).with(**fields) : ConsumerConfig.new(**fields)
        if config.name && config.durable && config.name != config.durable
          raise ArgumentError, "name: and durable: must be equal when both are given"
        end

        config.with(name: check_consumer_name(config.name || config.durable || Connection::NUID.next),
                    deliver_policy: config.deliver_policy || start_policy(config),
                    ack_policy: config.ack_policy || :explicit,
                    replay_policy: config.replay_policy || :instant)
      end

      def start_policy(config)
        return :by_start_sequence if config.start_sequence
        return :by_start_time if config.start_time

        :all
      end

      def filter_in_subject?(config)
        filter = config.filter_subject.to_s
        !filter.empty? && config.filter_subjects.to_a.empty? && !filter.match?(Protocol::Subject::NOT_LITERAL)
      end

      def check_consumer_name(name)
        text = check_name(name)
        return text if text.bytesize <= MAX_NAME_BYTES && !text.include?("\0")

        raise ArgumentError, "invalid consumer name: at most #{MAX_NAME_BYTES} bytes and no NUL"
      end
    end

    # Consumer management on a stream handle; see {Consumers} for each call.
    module StreamConsumers
      # @param config [ConsumerConfig, Hash, nil] the configuration, or give its fields as keywords
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @param fields [Hash{Symbol => Object}] {ConsumerConfig} fields
      # @return [Consumer] a handle to the new consumer
      # @raise [ConsumerExistsError] if a consumer with this name exists with another configuration
      def create_consumer(config = nil, timeout: nil, **fields)
        @jetstream.create_consumer(@name, config, timeout: timeout, **fields)
      end

      # @param config [ConsumerConfig, Hash, nil] the whole configuration, or give its fields as keywords
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @param fields [Hash{Symbol => Object}] {ConsumerConfig} fields
      # @return [Consumer] a handle to the consumer
      # @raise [ConsumerNotFoundError] if the consumer does not exist
      def update_consumer(config = nil, timeout: nil, **fields)
        @jetstream.update_consumer(@name, config, timeout: timeout, **fields)
      end

      # @param config [ConsumerConfig, Hash, nil] the configuration, or give its fields as keywords
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @param fields [Hash{Symbol => Object}] {ConsumerConfig} fields
      # @return [Consumer] a handle to the consumer
      def create_or_update_consumer(config = nil, timeout: nil, **fields)
        @jetstream.create_or_update_consumer(@name, config, timeout: timeout, **fields)
      end

      # @param name [String] the consumer's name
      # @param lookup [Boolean] false returns a handle bound by name alone; see {Consumers#consumer}
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @return [Consumer] a handle to the consumer
      # @raise [ConsumerNotFoundError] if the consumer does not exist and was looked up
      def consumer(name, lookup: true, timeout: nil)
        @jetstream.consumer(@name, name, lookup: lookup, timeout: timeout)
      end

      # @param name [String] the consumer's name
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @return [Boolean] whether the consumer exists
      def consumer?(name, timeout: nil)
        @jetstream.consumer?(@name, name, timeout: timeout)
      end

      # @param timeout [Numeric, nil] how long each page request may take
      # @return [Enumerator::Lazy<Consumer>] every consumer of the stream
      def consumers(timeout: nil)
        @jetstream.consumers(@name, timeout: timeout)
      end

      # @param timeout [Numeric, nil] how long each page request may take
      # @return [Enumerator::Lazy<String>] every consumer name of the stream
      def consumer_names(timeout: nil)
        @jetstream.consumer_names(@name, timeout: timeout)
      end

      # @param name [String] the consumer's name
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @return [Boolean] whether the server confirmed the deletion
      # @raise [ConsumerNotFoundError] if the consumer does not exist
      def delete_consumer(name, timeout: nil)
        @jetstream.delete_consumer(@name, name, timeout: timeout)
      end

      # @param options [Hash{Symbol => Object}] see {Ordered#initialize}
      # @return [Ordered] an ordered consumer over this stream
      def ordered_consumer(**options)
        @jetstream.ordered_consumer(@name, **options)
      end
    end
  end
end

Tether::JetStream.include(Tether::JetStream::Consumers)
Tether::JetStream::Stream.include(Tether::JetStream::StreamConsumers)
