# frozen_string_literal: true

# Steps the Key/Value specs share: memory buckets with unique names that are
# deleted after the example, raw access to the stream behind a bucket, and
# watchers that collect what they deliver.
module KeyValueHelper
  def self.included(base)
    base.include ClientHelper
    base.let(:nc) { connect }
    base.let(:js) { nc.jetstream }
    base.let(:bucket) { "B#{Tether::Connection::NUID.next}" }
    base.let(:kv) { make_bucket }
    base.let(:entries) { Thread::Queue.new }
    base.after { delete_buckets }
  end

  def made_buckets
    @made_buckets ||= []
  end

  def make_bucket(name = bucket, context: js, **options)
    made_buckets << name
    context.create_key_value(bucket: name, storage: :memory, **options)
  end

  def stream_config(name = bucket)
    js.stream("KV_#{name}").config.raw
  end

  def raw_put(key, value = nil, headers: nil, name: bucket)
    js.publish("$KV.#{name}.#{key}", value, headers: headers).sequence
  end

  def stored(key, name: bucket)
    message = js.stream("KV_#{name}").get(last_for: "$KV.#{name}.#{key}", direct: false)
    { subject: message.subject, data: message.data, headers: message.headers.to_h.transform_values(&:first) }
  end

  def collecting(*keys, **options)
    kv.watch(*keys, **options) { |entry| entries.push(entry) }
  end

  def taken(count, timeout: 5)
    Array.new(count) { entries.pop(timeout: timeout) }
  end

  def summary(list)
    list.map { |entry| entry && [entry.key, entry.value, entry.operation] }
  end

  def server_consumers(name = bucket)
    js.consumer_names("KV_#{name}").to_a
  end

  def require_limit_markers
    level = js.account_info.api.to_h["level"].to_i
    skip "needs JetStream API level 1; the server implements #{level}" if level < 1
  end

  private

  def delete_buckets
    return if made_buckets.empty?

    Tether::Client.connect(SharedNatsServer.instance.uri, reconnect: false) do |client|
      made_buckets.each { |name| error_from { client.jetstream.delete_key_value(name) } }
    end
  end
end
