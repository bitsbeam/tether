# frozen_string_literal: true

# Steps the JetStream consumer specs share: a memory stream with a unique name
# that is deleted after the example, and helpers to fill and read it.
module ConsumerHelper
  def self.included(base)
    base.include ClientHelper
    base.let(:nc) { connect }
    base.let(:js) { nc.jetstream }
    base.let(:name) { "C#{Tether::Connection::NUID.next}" }
    base.let(:stream) { js.create_stream(name: name, subjects: ["#{name}.>"], storage: :memory) }
    base.let(:errors) { Thread::Queue.new }
    base.let(:received) { Thread::Queue.new }
    base.after { delete_consumer_stream }
  end

  def store(count, subject: "#{name}.a", from: 1)
    stream
    count.times { |index| nc.publish(subject, "m#{from + index}") }
    nc.flush
    eventually { stream.info.state.last_sequence >= from + count - 1 }
  end

  def durable(**config)
    stream.create_consumer(durable: "worker", **config)
  end

  def push_consumer
    config = { "name" => "push", "deliver_subject" => "deliver.#{name}", "ack_policy" => "none" }
    js.api.request("CONSUMER.CREATE.#{stream.name}.push",
                   { "stream_name" => name, "config" => config, "action" => "create" })
    stream.consumer("push")
  end

  def data_of(messages)
    messages.map { |message| message&.data }
  end

  def expected(count, from: 1)
    Array.new(count) { |index| "m#{from + index}" }
  end

  def sequences(count, from: 1)
    Array.new(count) { |index| from + index }
  end

  def require_priority_groups
    level = nc.server_info.api_level.to_i
    skip "needs JetStream API level 1; the server implements #{level}" if level < 1
  end

  private

  def delete_consumer_stream
    Tether::Client.connect(SharedNatsServer.instance.uri, reconnect: false) do |client|
      error_from { client.jetstream.delete_stream(name) }
    end
  end
end
