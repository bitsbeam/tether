# frozen_string_literal: true

# Steps the JetStream publishing specs share: streams with unique names that
# are deleted after the example, and guards for what the server implements.
module PublishingHelper
  def self.included(base)
    base.after { delete_ingest_streams }
  end

  # Creates a memory stream that captures `<name>.>` and returns its name.
  def ingest_stream(jetstream, **config)
    name = "PUB#{Tether::Connection::NUID.next}"
    jetstream.create_stream(name: name, subjects: ["#{name}.>"], storage: :memory, **config)
    ingest_streams << name
    name
  end

  def require_api_level(jetstream, level)
    actual = jetstream.account_info.api.to_h["level"].to_i
    skip "needs JetStream API level #{level}; the server implements #{actual}" if actual < level
  end

  def stored_messages(jetstream, stream)
    jetstream.stream(stream).info.state.messages
  end

  def stored_subjects(jetstream, stream)
    jetstream.stream(stream).info(subjects: ">").state.subjects || {}
  end

  def last_stored(jetstream, stream, subject)
    message = jetstream.api.request("STREAM.MSG.GET.#{stream}", { "last_by_subj" => subject })["message"]
    headers = message["hdrs"] ? decode_headers(message["hdrs"]) : {}
    { data: message["data"].to_s.unpack1("m"), headers: headers }
  end

  private

  def ingest_streams
    @ingest_streams ||= []
  end

  def decode_headers(encoded)
    Tether::Protocol::Headers.parse(encoded.unpack1("m")).to_h.transform_values(&:first)
  end

  def delete_ingest_streams
    return if ingest_streams.empty?

    Tether::Client.connect(SharedNatsServer.instance.uri, reconnect: false) do |client|
      ingest_streams.each { |name| error_from { client.jetstream.delete_stream(name) } }
    end
  end
end
