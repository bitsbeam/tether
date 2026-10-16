# frozen_string_literal: true

require "digest"
require "json"

# Steps the Object Store specs share: memory buckets with unique names that
# are deleted after the example, raw access to the stream behind a bucket, and
# watchers that collect what they deliver.
module ObjectStoreHelper
  def self.included(base)
    base.include ClientHelper
    base.let(:nc) { connect }
    base.let(:js) { nc.jetstream }
    base.let(:bucket) { "O#{Tether::Connection::NUID.next}" }
    base.let(:store) { make_store }
    base.let(:entries) { Thread::Queue.new }
    base.after { delete_stores }
  end

  def made_stores
    @made_stores ||= []
  end

  def make_store(name = bucket, context: js, **options)
    made_stores << name
    context.create_object_store(bucket: name, storage: :memory, **options)
  end

  def backing_stream(name = bucket)
    js.stream("OBJ_#{name}")
  end

  def stored_messages(name = bucket)
    backing_stream(name).info.state.messages
  end

  def stored_subjects(name = bucket)
    backing_stream(name).info(subjects: ">").state.subjects || {}
  end

  def chunk_subject(nuid, name: bucket)
    "$O.#{name}.C.#{nuid}"
  end

  def meta_subject(object, name: bucket)
    "$O.#{name}.M.#{[object].pack("m0").tr("+/", "-_")}"
  end

  def chunks_of(info)
    stored_subjects(info.bucket).fetch(chunk_subject(info.nuid, name: info.bucket), 0)
  end

  def stored_meta(object, name: bucket)
    message = backing_stream(name).get(last_for: meta_subject(object, name: name), direct: false)
    { subject: message.subject, document: JSON.parse(message.data),
      headers: message.headers.to_h.transform_values(&:first) }
  end

  def raw_meta(object, document, name: bucket)
    js.publish(meta_subject(object, name: name), JSON.generate(document), headers: { "Nats-Rollup" => "sub" })
  end

  def raw_object(object, content, nuid: Tether::Connection::NUID.next, chunk_size: 4, **extra)
    pieces = content.b.scan(/.{1,#{chunk_size}}/mn)
    pieces.each { |piece| js.publish(chunk_subject(nuid), piece) }
    raw_meta(object, { "name" => object, "options" => { "max_chunk_size" => chunk_size }, "bucket" => bucket,
                       "nuid" => nuid, "size" => content.bytesize, "mtime" => "0001-01-01T00:00:00Z",
                       "chunks" => pieces.size, "digest" => digest_of(content) }.merge(extra))
  end

  def digest_of(content)
    "SHA-256=#{[Digest::SHA256.digest(content)].pack("m0").tr("+/", "-_")}"
  end

  def binary(size)
    Random.new(size).bytes(size)
  end

  def collecting(**options)
    store.watch(**options) { |info| entries.push(info) }
  end

  def taken(count, timeout: 5)
    Array.new(count) { entries.pop(timeout: timeout) }
  end

  def names_of(list)
    list.map { |info| info && [info.name, info.deleted?] }
  end

  def server_consumers(name = bucket)
    js.consumer_names("OBJ_#{name}").to_a
  end

  private

  def delete_stores
    return if made_stores.empty?

    Tether::Client.connect(SharedNatsServer.instance.uri, reconnect: false) do |client|
      made_stores.each { |name| error_from { client.jetstream.delete_stream("OBJ_#{name}") } }
    end
  end
end
