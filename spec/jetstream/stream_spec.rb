# frozen_string_literal: true

RSpec.describe Tether::JetStream::Stream do
  include ClientHelper

  let(:nc) { connect }
  let(:js) { nc.jetstream }
  let(:name) { "S#{Tether::Connection::NUID.next}" }
  let(:direct) { true }
  let(:stream) { js.create_stream(name: name, subjects: ["#{name}.>"], storage: :memory, allow_direct: direct) }

  after { error_from { js.delete_stream(name) } }

  def store(count, subject: nil, headers: nil)
    stream
    Array.new(count) { |index| js.publish(subject || "#{name}.#{index}", "m#{index + 1}", headers: headers).sequence }
  end

  def flood(count)
    stream
    count.times { |index| nc.publish("#{name}.#{index}") }
    nc.flush
    eventually { stream.info.state.messages == count }
  end

  def direct_gets
    allow(nc).to receive(:publish).and_call_original
    yield
    expect(nc).to have_received(:publish).with("$JS.API.DIRECT.GET.#{name}", any_args)
  end

  def sent_to(subject, &action)
    sent = nil
    allow(js.api).to receive(:request).and_wrap_original do |original, *arguments, **options|
      sent = arguments[1] if arguments[0] == subject
      original.call(*arguments, **options)
    end
    error_from(&action)
    sent
  end

  describe "#name" do
    it "is the name of the stream" do
      expect(stream.name).to eq(name)
    end
  end

  describe "#jetstream" do
    it "is the context the stream was made with" do
      expect(stream.jetstream).to be(js)
    end
  end

  describe "#cached_info" do
    it "is the info the server returned when the handle was made" do
      expect(stream.cached_info.config.name).to eq(name)
    end

    it "stays as it was until info is read again" do
      store(2)

      expect(stream.cached_info.state.messages).to eq(0)
    end
  end

  describe "#info" do
    it "reads the current state from the server" do
      store(3)

      expect(stream.info.state).to have_attributes(messages: 3, first_sequence: 1, last_sequence: 3, subject_count: 3)
    end

    it "replaces the cached info" do
      store(3)
      stream.info

      expect(stream.cached_info.state.messages).to eq(3)
    end

    it "reads when the stream was created and when the info was taken" do
      info = stream.info

      expect(info.created).to be_a(Time)
      expect(info.time).to be >= info.created
    end

    it "leaves out the sequences of deleted messages unless asked" do
      store(3)
      stream.delete_message(2)

      expect(stream.info.state).to have_attributes(deleted_count: 1, deleted: nil)
    end

    it "lists the sequences of deleted messages when asked" do
      store(3)
      stream.delete_message(2)

      expect(stream.info(deleted_details: true).state.deleted).to eq([2])
    end

    it "counts the messages of each subject matching the filter" do
      store(2, subject: "#{name}.a")
      store(1, subject: "#{name}.b.c")

      expect(stream.info(subjects: "#{name}.>").state.subjects).to eq("#{name}.a" => 2, "#{name}.b.c" => 1)
    end

    it "counts only the subjects the filter matches" do
      store(2, subject: "#{name}.a")
      store(1, subject: "#{name}.b.c")

      expect(stream.info(subjects: "#{name}.*").state.subjects).to eq("#{name}.a" => 2)
    end

    it "has no subject counts when the filter matches nothing" do
      store(1)

      expect(stream.info(subjects: "#{name}.none.>").state.subjects).to eq({})
    end

    it "merges every page of subject counts when there are more subjects than one page holds" do
      flood(100_010)

      subjects = stream.info(subjects: ">").state.subjects

      expect(subjects.size).to eq(100_010)
      expect(subjects).to include("#{name}.0" => 1, "#{name}.100009" => 1)
    end

    it "keeps the subject counts out of the cached info" do
      store(2)
      stream.info(subjects: ">")

      expect(stream.cached_info.state).to have_attributes(messages: 2, subjects: nil)
    end

    it "raises StreamNotFoundError once the stream is gone" do
      stream.delete

      expect { stream.info }.to raise_error(Tether::JetStream::StreamNotFoundError)
    end

    it "raises TimeoutError when the deadline it is given has passed" do
      stream

      expect { stream.info(subjects: ">", timeout: Tether::Deadline.after(0)) }.to raise_error(Tether::TimeoutError)
    end
  end

  describe "#config" do
    it "is the configuration from the cached info" do
      expect(stream.config).to have_attributes(name: name, subjects: ["#{name}.>"], storage: :memory,
                                               allow_direct: true, retention: :limits)
    end
  end

  describe "#state" do
    it "is the state from the cached info" do
      store(2)
      stream.info

      expect(stream.state).to have_attributes(messages: 2, last_sequence: 2)
    end
  end

  describe "#update" do
    it "changes the given fields on the server" do
      stream.update(max_age: 3600, description: "orders")

      expect(js.stream(name).config).to have_attributes(max_age: 3600, description: "orders")
    end

    it "keeps the fields it was not given" do
      stream.update(max_messages: 10)

      expect(js.stream(name).config).to have_attributes(subjects: ["#{name}.>"], allow_direct: true, storage: :memory)
    end

    it "starts from the configuration on the server, not from the cached one" do
      js.update_stream(stream.config.with(description: "set elsewhere"))
      stream.update(max_messages: 10)

      expect(js.stream(name).config).to have_attributes(description: "set elsewhere", max_messages: 10)
    end

    it "returns the stream with fresh cached info" do
      expect(stream.update(max_messages: 10)).to be(stream)
      expect(stream.config.max_messages).to eq(10)
    end

    it "raises the server's error for a change the server refuses" do
      expect { stream.update(storage: :file) }.to raise_error(Tether::JetStream::APIError, /storage type/)
    end

    it "raises ArgumentError for a field that does not exist" do
      expect { stream.update(max_agee: 1) }.to raise_error(ArgumentError, /max_agee/)
    end

    it "raises StreamNotFoundError once the stream is gone" do
      stream.delete

      expect { stream.update(max_messages: 10) }.to raise_error(Tether::JetStream::StreamNotFoundError)
    end

    it "raises TimeoutError when the deadline it is given has passed" do
      stream

      expect { stream.update(max_messages: 10, timeout: Tether::Deadline.after(0)) }
        .to raise_error(Tether::TimeoutError)
    end
  end

  describe "#purge" do
    it "removes every message and returns how many" do
      store(5)

      expect(stream.purge).to eq(5)
      expect(stream.info.state).to have_attributes(messages: 0, last_sequence: 5)
    end

    it "removes only the messages on subjects matching the filter" do
      store(2, subject: "#{name}.a")
      store(3, subject: "#{name}.b")

      expect(stream.purge(subject: "#{name}.a")).to eq(2)
      expect(stream.info(subjects: ">").state.subjects).to eq("#{name}.b" => 3)
    end

    it "removes the messages below a sequence" do
      store(5)

      expect(stream.purge(sequence: 4)).to eq(3)
      expect(stream.info.state.first_sequence).to eq(4)
    end

    it "keeps the newest messages" do
      store(5)

      expect(stream.purge(keep: 2)).to eq(3)
      expect(stream.info.state).to have_attributes(messages: 2, first_sequence: 4)
    end

    it "keeps the newest messages of the subjects matching the filter" do
      store(3, subject: "#{name}.a")
      store(2, subject: "#{name}.b")

      expect(stream.purge(subject: "#{name}.a", keep: 1)).to eq(2)
      expect(stream.info(subjects: ">").state.subjects).to eq("#{name}.a" => 1, "#{name}.b" => 2)
    end

    it "returns 0 when there is nothing to remove" do
      expect(stream.purge).to eq(0)
    end

    it "raises ArgumentError when both a sequence and a number to keep are given" do
      expect { stream.purge(sequence: 3, keep: 1) }.to raise_error(ArgumentError, /cannot be combined/)
    end

    it "raises the server's error when the stream denies purging" do
      js.create_stream(name: name, storage: :memory, deny_purge: true)

      expect { js.stream(name).purge }.to raise_error(Tether::JetStream::APIError, /purge not permitted/)
    end
  end

  describe "#get" do
    def expect_stored(message, subject:, sequence:, data:)
      expect(message).to be_a(Tether::JetStream::StoredMessage).and be_frozen
      expect(message).to have_attributes(subject: subject, sequence: sequence, data: data)
      expect(message.time).to be_within(60).of(Time.now)
    end

    def reads_by_sequence
      store(3)

      expect_stored(stream.get(2), subject: "#{name}.1", sequence: 2, data: "m2")
    end

    def reads_last_for_subject
      store(3, subject: "#{name}.a")
      store(1, subject: "#{name}.b")

      expect_stored(stream.get(last_for: "#{name}.a"), subject: "#{name}.a", sequence: 3, data: "m3")
    end

    def reads_next_for_filter
      store(2, subject: "#{name}.a")
      store(2, subject: "#{name}.b")

      expect_stored(stream.get(next_for: "#{name}.b"), subject: "#{name}.b", sequence: 3, data: "m1")
    end

    def reads_next_from_sequence
      store(4, subject: "#{name}.a")

      expect_stored(stream.get(next_for: "#{name}.*", from: 3), subject: "#{name}.a", sequence: 3, data: "m3")
    end

    def reads_headers
      store(1, headers: { "X-Kind" => "order", "Nats-Msg-Id" => "one" })

      headers = stream.get(1).headers

      expect(headers["X-Kind"]).to eq("order")
      expect(headers["Nats-Msg-Id"]).to eq("one")
    end

    def reads_empty_payload
      stream
      js.publish("#{name}.empty")

      expect(stream.get(1).data).to eq("")
    end

    def reads_binary_payload
      stream
      payload = Random.bytes(2048)
      js.publish("#{name}.bin", payload)

      expect(stream.get(1).data).to eq(payload)
    end

    def misses(**selector)
      store(1)

      expect { selector.empty? ? stream.get(99) : stream.get(**selector) }
        .to raise_error(Tether::JetStream::MessageNotFoundError) { |error| expect(error.code).to eq(404) }
    end

    context "when the stream allows direct access" do
      it "reads a message by its sequence" do
        direct_gets { reads_by_sequence }
      end

      it "reads the newest message on a subject" do
        reads_last_for_subject
      end

      it "asks for the newest message on a subject with the subject in the request subject" do
        allow(nc).to receive(:request).and_call_original
        reads_last_for_subject

        expect(nc).to have_received(:request).with("$JS.API.DIRECT.GET.#{name}.#{name}.a", nil, any_args)
      end

      it "reads the first message on a subject matching a filter" do
        reads_next_for_filter
      end

      it "reads the first message matching a filter from a sequence on" do
        reads_next_from_sequence
      end

      it "reads the headers the message was published with" do
        reads_headers
      end

      it "adds the headers that describe the message" do
        store(1)

        expect(stream.get(1).headers.to_h).to include("Nats-Stream" => [name], "Nats-Sequence" => ["1"])
      end

      it "reads a message with an empty payload" do
        reads_empty_payload
      end

      it "reads a binary payload unchanged" do
        reads_binary_payload
      end

      it "raises MessageNotFoundError for a sequence the stream does not hold" do
        misses
      end

      it "raises MessageNotFoundError for a subject without messages" do
        misses(last_for: "#{name}.none")
      end

      it "raises MessageNotFoundError when nothing matches a filter" do
        misses(next_for: "#{name}.none.>")
      end

      it "uses the JSON API when told not to read directly" do
        store(1)

        expect(stream.get(1, direct: false).headers.to_h).to eq({})
      end

      it "raises ServiceUnavailableError once the stream is gone" do
        stream.delete

        expect { stream.get(1) }.to raise_error(Tether::JetStream::ServiceUnavailableError, /direct/)
      end
    end

    context "when the stream does not allow direct access" do
      let(:direct) { false }

      it "reads a message by its sequence" do
        reads_by_sequence
      end

      it "reads the newest message on a subject" do
        reads_last_for_subject
      end

      it "reads the first message on a subject matching a filter" do
        reads_next_for_filter
      end

      it "reads the first message matching a filter from a sequence on" do
        reads_next_from_sequence
      end

      it "reads the headers the message was published with" do
        reads_headers
      end

      it "has empty headers for a message published without any" do
        store(1)

        expect(stream.get(1).headers).to be_empty
      end

      it "reads a message with an empty payload" do
        reads_empty_payload
      end

      it "reads a binary payload unchanged" do
        reads_binary_payload
      end

      it "raises MessageNotFoundError for a sequence the stream does not hold" do
        misses
      end

      it "raises MessageNotFoundError for a subject without messages" do
        misses(last_for: "#{name}.none")
      end

      it "raises MessageNotFoundError when nothing matches a filter" do
        misses(next_for: "#{name}.none.>")
      end

      it "raises ServiceUnavailableError when told to read directly" do
        store(1)

        expect { stream.get(1, direct: true) }.to raise_error(Tether::JetStream::ServiceUnavailableError)
      end

      it "raises StreamNotFoundError once the stream is gone" do
        stream.delete

        expect { stream.get(1) }.to raise_error(Tether::JetStream::StreamNotFoundError)
      end
    end

    it "raises ArgumentError when nothing says which message to read" do
      expect { stream.get }.to raise_error(ArgumentError, /exactly one/)
    end

    it "raises ArgumentError when a sequence and a subject are both given" do
      expect { stream.get(1, last_for: "#{name}.a") }.to raise_error(ArgumentError, /exactly one/)
    end

    it "raises ArgumentError when a starting sequence is given without a filter" do
      expect { stream.get(1, from: 2) }.to raise_error(ArgumentError, /next_for/)
    end

    it "raises TimeoutError when the deadline it is given has passed" do
      store(1)

      expect { stream.get(1, timeout: Tether::Deadline.after(0)) }.to raise_error(Tether::TimeoutError)
    end
  end

  describe "#each_message" do
    def pages(count)
      allow(nc).to receive(:publish).and_call_original
      yield
      expect(nc).to have_received(:publish).with("$JS.API.DIRECT.GET.#{name}", any_args).exactly(count).times
    end

    context "when a block is given" do
      it "yields every message in order" do
        store(5)
        seen = []
        stream.each_message { |message| seen << [message.sequence, message.subject, message.data] }

        expect(seen).to eq(Array.new(5) { |index| [index + 1, "#{name}.#{index}", "m#{index + 1}"] })
      end

      it "returns the stream" do
        store(1)

        expect(stream.each_message { nil }).to be(stream)
      end

      it "stops asking for pages when the block breaks" do
        store(10)

        pages(1) { stream.each_message(batch: 3) { |message| break if message.sequence == 2 } }
      end
    end

    context "when no block is given" do
      it "returns an enumerator of the messages" do
        store(3)

        expect(stream.each_message).to be_an(Enumerator)
        expect(stream.each_message.map(&:sequence)).to eq([1, 2, 3])
      end

      it "reads only the pages that are consumed" do
        store(10)

        pages(2) { expect(stream.each_message(batch: 3).first(4).map(&:sequence)).to eq([1, 2, 3, 4]) }
      end
    end

    it "pages through a stream longer than one batch" do
      store(10)

      pages(4) { expect(stream.each_message(batch: 3).map(&:sequence)).to eq((1..10).to_a) }
    end

    it "ends after the page that leaves nothing pending" do
      store(6)

      pages(2) { expect(stream.each_message(batch: 3).map(&:sequence)).to eq((1..6).to_a) }
    end

    it "starts at the given sequence" do
      store(6)

      expect(stream.each_message(from: 4, batch: 2).map(&:sequence)).to eq([4, 5, 6])
    end

    it "yields only the messages on subjects matching the filter, across pages" do
      stream
      9.times { |index| js.publish("#{name}.#{index.even? ? "even" : "odd"}", index.to_s) }

      expect(stream.each_message(subject: "#{name}.even", batch: 2).map(&:sequence)).to eq([1, 3, 5, 7, 9])
    end

    it "starts at the first message stored at or after a time" do
      store(5)

      expect(stream.each_message(start_time: stream.get(3).time, batch: 2).map(&:sequence)).to eq([3, 4, 5])
    end

    it "skips deleted messages" do
      store(6)
      stream.delete_message(2)
      stream.delete_message(3)

      expect(stream.each_message(batch: 2).map(&:sequence)).to eq([1, 4, 5, 6])
    end

    it "continues after a page the server ended early for its size" do
      stream
      6.times { |index| js.publish("#{name}.big", index.to_s * 1000) }

      pages(6) { expect(stream.each_message(max_bytes: 500).map(&:sequence)).to eq((1..6).to_a) }
    end

    it "yields nothing for an empty stream" do
      expect(stream.each_message.to_a).to eq([])
    end

    it "yields nothing when it starts past the last message" do
      store(2)

      expect(stream.each_message(from: 10).to_a).to eq([])
    end

    it "yields messages with their headers and time" do
      store(1, headers: { "X-Kind" => "order" })
      message = stream.each_message.first

      expect(message.headers["X-Kind"]).to eq("order")
      expect(message.time).to be_within(60).of(Time.now)
    end

    it "raises ArgumentError when a start time is combined with a start sequence" do
      expect { stream.each_message(from: 2, start_time: Time.now).to_a }.to raise_error(ArgumentError, /start_time/)
    end

    it "raises ServiceUnavailableError when the stream does not allow direct access" do
      js.create_stream(name: name, storage: :memory)

      expect { js.stream(name).each_message.to_a }.to raise_error(Tether::JetStream::ServiceUnavailableError)
    end

    it "raises TimeoutError when a deadline for the whole iteration has passed" do
      store(2)

      expect { stream.each_message(timeout: Tether::Deadline.after(0)).to_a }.to raise_error(Tether::TimeoutError)
    end
  end

  describe "#last_messages" do
    def store_keys
      stream
      %w[eu.a eu.b us.a eu.a us.b eu.b].each_with_index { |key, index| js.publish("#{name}.#{key}", "v#{index + 1}") }
    end

    def last(*arguments, **options)
      stream.last_messages(*arguments, **options).map do |message|
        [message.subject.delete_prefix("#{name}."), message.data]
      end
    end

    it "returns the last message of every subject matching a filter" do
      store_keys

      expect(last(["#{name}.eu.*"])).to eq([%w[eu.a v4], %w[eu.b v6]])
    end

    it "returns the last message of every subject matching any of several filters" do
      store_keys

      expect(last(["#{name}.eu.a", "#{name}.us.>"])).to eq([%w[us.a v3], %w[eu.a v4], %w[us.b v5]])
    end

    it "takes a single filter as a string" do
      store_keys

      expect(last("#{name}.us.a")).to eq([%w[us.a v3]])
    end

    it "returns stored messages with their sequence and time" do
      store_keys
      message = stream.last_messages("#{name}.us.b").first

      expect(message).to be_a(Tether::JetStream::StoredMessage).and have_attributes(sequence: 5)
      expect(message.time).to be_within(60).of(Time.now)
    end

    it "considers only the messages at or below a sequence" do
      store_keys

      expect(last("#{name}.>", up_to_sequence: 3)).to eq([%w[eu.a v1], %w[eu.b v2], %w[us.a v3]])
    end

    it "considers only the messages stored before a time" do
      store_keys

      expect(last("#{name}.>", up_to_time: stream.get(3).time)).to eq([%w[eu.a v1], %w[eu.b v2]])
    end

    it "returns nothing when no subject matches" do
      store_keys

      expect(last("#{name}.asia.>")).to eq([])
    end

    it "returns nothing for an empty stream" do
      expect(last("#{name}.>")).to eq([])
    end

    it "raises TooManyResultsError when the filters match more subjects than the server returns" do
      flood(1030)

      expect { stream.last_messages("#{name}.>") }.to raise_error(Tether::JetStream::TooManyResultsError)
    end

    it "raises ArgumentError when both a sequence and a time bound are given" do
      expect { stream.last_messages(">", up_to_sequence: 1, up_to_time: Time.now) }
        .to raise_error(ArgumentError, /cannot be combined/)
    end

    it "raises ServiceUnavailableError when the stream does not allow direct access" do
      js.create_stream(name: name, storage: :memory)

      expect { js.stream(name).last_messages(">") }.to raise_error(Tether::JetStream::ServiceUnavailableError)
    end

    it "raises TimeoutError when the deadline it is given has passed" do
      store_keys

      expect { stream.last_messages(">", timeout: Tether::Deadline.after(0)) }.to raise_error(Tether::TimeoutError)
    end
  end

  describe "#delete_message" do
    it "removes the message" do
      store(3)

      expect(stream.delete_message(2)).to be(true)
      expect { stream.get(2) }.to raise_error(Tether::JetStream::MessageNotFoundError)
      expect(stream.info.state).to have_attributes(messages: 2, deleted_count: 1)
    end

    it "asks the server to erase the message by default" do
      store(1)

      expect(sent_to("STREAM.MSG.DELETE.#{name}") { stream.delete_message(1) }).to eq("seq" => 1)
    end

    it "removes the message without erasing it when told so" do
      store(2)

      expect(sent_to("STREAM.MSG.DELETE.#{name}") { stream.delete_message(1, erase: false) })
        .to eq("seq" => 1, "no_erase" => true)
      expect(stream.info.state.messages).to eq(1)
    end

    it "raises MessageNotFoundError for a sequence the stream does not hold" do
      store(1)

      expect { stream.delete_message(7) }
        .to raise_error(Tether::JetStream::MessageNotFoundError, /sequence 7 not found/)
    end

    it "raises the server's error when the stream denies deleting" do
      js.create_stream(name: name, storage: :memory, deny_delete: true)
      js.publish(name, "kept")

      expect { js.stream(name).delete_message(1) }.to raise_error(Tether::JetStream::APIError, /delete not permitted/)
    end
  end

  describe "#step_down" do
    it "raises the server's error on a server that is not clustered" do
      expect { stream.step_down }
        .to raise_error(Tether::JetStream::APIError) { |error| expect(error.err_code).to eq(10_010) }
    end

    it "sends no body when no placement is given" do
      expect(sent_to("STREAM.LEADER.STEPDOWN.#{name}") { stream.step_down }).to be_nil
    end

    it "sends the placement of the next leader" do
      placement = { preferred: "n2", tags: ["ssd"] }

      expect(sent_to("STREAM.LEADER.STEPDOWN.#{name}") { stream.step_down(placement: placement) })
        .to eq("placement" => { "preferred" => "n2", "tags" => ["ssd"] })
    end

    it "raises ArgumentError for a placement field that does not exist" do
      expect { stream.step_down(placement: { server: "n2" }) }.to raise_error(ArgumentError, /Placement field: server/)
    end
  end

  describe "#remove_peer" do
    it "raises the server's error on a server that is not clustered" do
      expect { stream.remove_peer("n2") }
        .to raise_error(Tether::JetStream::APIError) { |error| expect(error.err_code).to eq(10_010) }
    end

    it "names the peer in the request" do
      expect(sent_to("STREAM.PEER.REMOVE.#{name}") { stream.remove_peer("n2") }).to eq("peer" => "n2")
    end
  end

  describe "#snapshot" do
    let(:file_stream) { js.create_stream(name: name, subjects: ["#{name}.>"], allow_direct: true) }

    def fill(count, size)
      file_stream
      Array.new(count) { Random.bytes(size).tap { |payload| js.publish("#{name}.data", payload) } }
    end

    def add_consumer(durable)
      request = { "stream_name" => name, "config" => { "durable_name" => durable, "name" => durable } }
      js.api.request("CONSUMER.CREATE.#{name}.#{durable}", request)
    end

    def download(**options)
      chunks = []
      config, state = file_stream.snapshot(**options) { |chunk| chunks << chunk }
      [chunks, config, state]
    end

    it "returns the configuration and the state of the stream" do
      fill(3, 10)
      _, config, state = download

      expect(config).to be_a(Tether::JetStream::StreamConfig).and have_attributes(name: name, allow_direct: true)
      expect(state).to be_a(Tether::JetStream::StreamState).and have_attributes(messages: 3, last_sequence: 3)
    end

    it "yields a snapshot small enough for a single chunk" do
      fill(1, 10)
      chunks, = download

      expect(chunks.size).to eq(1)
      expect(chunks.first).not_to be_empty
    end

    it "yields a large snapshot in chunks of the size asked for" do
      fill(20, 4096)
      chunks, = download(chunk_size: 1024)

      expect(chunks.size).to be > 80
      expect(chunks[0..-2].map(&:bytesize).uniq).to eq([1024])
    end

    it "yields bytes that restore the stream with the same messages" do
      payloads = fill(20, 4096)
      chunks, config, state = download(chunk_size: 2048)
      file_stream.delete

      restored = js.restore_stream(config, state) { |writer| chunks.each { |chunk| writer.write(chunk) } }

      expect(restored.info.state).to have_attributes(messages: 20, first_sequence: 1, last_sequence: 20)
      expect(restored.each_message.map(&:data)).to eq(payloads)
    end

    it "keeps the consumers of the stream in the snapshot" do
      fill(1, 10)
      add_consumer("worker")
      chunks, config, state = download
      file_stream.delete

      js.restore_stream(config, state) { |writer| writer.write(chunks.join) }

      expect(js.api.pages("CONSUMER.NAMES.#{name}", "consumers").to_a).to eq(["worker"])
    end

    it "leaves the consumers out when told so" do
      fill(1, 10)
      add_consumer("worker")
      chunks, config, state = download(no_consumers: true)
      file_stream.delete

      js.restore_stream(config, state) { |writer| writer.write(chunks.join) }

      expect(js.api.pages("CONSUMER.NAMES.#{name}", "consumers").to_a).to eq([])
    end

    it "stops listening for chunks when it is done" do
      fill(1, 10)
      listening = nil
      allow(nc).to(receive(:subscribe).and_wrap_original { |original, *given| listening = original.call(*given) })
      download

      expect(listening).to be_closed
    end

    it "raises ArgumentError without a block" do
      expect { stream.snapshot }.to raise_error(ArgumentError, /block/)
    end

    it "raises StreamNotFoundError once the stream is gone" do
      stream.delete

      expect { stream.snapshot { nil } }.to raise_error(Tether::JetStream::StreamNotFoundError)
    end

    it "raises TimeoutError when a deadline for the whole snapshot has passed" do
      fill(1, 10)

      expect { download(timeout: Tether::Deadline.after(0)) }.to raise_error(Tether::TimeoutError)
    end
  end

  describe "#delete" do
    it "deletes the stream" do
      expect(stream.delete).to be(true)
      expect(js.stream?(name)).to be(false)
    end

    it "raises StreamNotFoundError when the stream is already gone" do
      stream.delete

      expect { stream.delete }.to raise_error(Tether::JetStream::StreamNotFoundError)
    end
  end

  describe "#inspect" do
    it "shows the name without the cached info" do
      expect(stream.inspect).to eq(%(#<Tether::JetStream::Stream name="#{name}">))
    end
  end

  describe "#create_consumer" do
    it "creates a consumer on this stream" do
      consumer = stream.create_consumer(durable: "billing", filter_subjects: ["#{name}.eu.>"], max_deliver: 5)

      expect(consumer).to have_attributes(class: Tether::JetStream::Consumer, name: "billing", stream: name)
      expect(consumer.config).to have_attributes(filter_subjects: ["#{name}.eu.>"], max_deliver: 5)
    end

    it "creates an ephemeral consumer with a generated name when none is given" do
      expect(stream.create_consumer.config).to have_attributes(durable: nil, name: /\A[A-Za-z0-9]{22}\z/)
    end

    it "raises ConsumerExistsError when it exists with another configuration" do
      stream.create_consumer(durable: "billing", max_deliver: 5)

      expect { stream.create_consumer(durable: "billing", max_deliver: 6) }
        .to raise_error(Tether::JetStream::ConsumerExistsError)
    end
  end

  describe "#update_consumer" do
    it "replaces the configuration of an existing consumer" do
      stream.create_consumer(durable: "billing", max_deliver: 5)

      expect(stream.update_consumer(durable: "billing", max_deliver: 7).config.max_deliver).to eq(7)
    end

    it "raises ConsumerNotFoundError when the consumer does not exist" do
      expect { stream.update_consumer(durable: "billing") }.to raise_error(Tether::JetStream::ConsumerNotFoundError)
    end
  end

  describe "#create_or_update_consumer" do
    it "creates the consumer, then replaces its configuration" do
      stream.create_or_update_consumer(durable: "billing", max_deliver: 5)

      expect(stream.create_or_update_consumer(durable: "billing", max_deliver: 7).config.max_deliver).to eq(7)
    end
  end

  describe "#consumer" do
    it "looks an existing consumer up" do
      stream.create_consumer(durable: "billing")

      expect(stream.consumer("billing").name).to eq("billing")
    end

    it "raises ConsumerNotFoundError when the consumer does not exist" do
      expect { stream.consumer("billing") }.to raise_error(Tether::JetStream::ConsumerNotFoundError)
    end
  end

  describe "#consumer?" do
    it "is true for a consumer that exists" do
      stream.create_consumer(durable: "billing")

      expect(stream.consumer?("billing")).to be(true)
    end

    it "is false for a consumer that does not exist" do
      expect(stream.consumer?("billing")).to be(false)
    end
  end

  describe "#consumers" do
    it "returns a lazy enumerator of handles to the stream's consumers" do
      %w[a b].each { |consumer| stream.create_consumer(durable: consumer) }

      expect(stream.consumers.map(&:name).to_a).to eq(%w[a b])
    end
  end

  describe "#consumer_names" do
    it "returns a lazy enumerator of the stream's consumer names" do
      %w[a b].each { |consumer| stream.create_consumer(durable: consumer) }

      expect(stream.consumer_names.to_a).to eq(%w[a b])
    end
  end

  describe "#delete_consumer" do
    it "removes the consumer" do
      stream.create_consumer(durable: "billing")

      expect(stream.delete_consumer("billing")).to be(true)
      expect(stream.consumer?("billing")).to be(false)
    end

    it "raises ConsumerNotFoundError when the consumer does not exist" do
      expect { stream.delete_consumer("billing") }.to raise_error(Tether::JetStream::ConsumerNotFoundError)
    end
  end

  describe "#ordered_consumer" do
    it "returns an ordered consumer over this stream" do
      expect(stream.ordered_consumer(filter_subjects: ["#{name}.>"]))
        .to have_attributes(class: Tether::JetStream::Ordered, stream: name)
    end
  end
end
