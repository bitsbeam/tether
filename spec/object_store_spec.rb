# frozen_string_literal: true

require "stringio"
require "tempfile"

RSpec.describe Tether::ObjectStore do
  include ObjectStoreHelper

  def round_trip(content, **options)
    store.put("object", content, **options)
    store.get("object").read
  end

  def failing_source(pieces)
    queue = pieces.dup
    Object.new.tap do |source|
      source.define_singleton_method(:read) do |_length|
        piece = queue.shift
        raise IOError, "the source broke" if piece == :fail

        piece
      end
    end
  end

  describe "#name" do
    it "returns the bucket's name" do
      expect(store.name).to eq(bucket)
    end
  end

  describe "#jetstream" do
    it "returns the context the bucket was made through" do
      expect(store.jetstream).to be(js)
    end
  end

  describe "#stream" do
    it "returns the stream behind the bucket" do
      expect(store.stream.name).to eq("OBJ_#{bucket}")
    end
  end

  describe "#put" do
    it "stores an empty object" do
      expect(round_trip("")).to eq("")
    end

    it "stores an object without data as an empty object" do
      expect(store.put("object")).to have_attributes(size: 0, chunks: 0)
    end

    it "stores a small object" do
      expect(round_trip("hello world")).to eq("hello world")
    end

    it "stores an object of exactly one chunk in one chunk message" do
      info = store.put("object", binary(128 * 1024))

      expect(info).to have_attributes(size: 128 * 1024, chunks: 1)
    end

    it "returns the content of an object of exactly one chunk" do
      content = binary(128 * 1024)

      expect(round_trip(content)).to eq(content)
    end

    it "splits an object one byte over the chunk size into two chunks" do
      expect(store.put("object", binary((128 * 1024) + 1)).chunks).to eq(2)
    end

    it "stores a multi-chunk object" do
      content = binary(300_000)

      expect(round_trip(content)).to eq(content)
    end

    it "stores a binary object of several megabytes" do
      content = binary(5 * 1024 * 1024)

      expect(Digest::SHA256.hexdigest(round_trip(content))).to eq(Digest::SHA256.hexdigest(content))
    end

    it "cuts the content into chunks of the given size" do
      info = store.put("object", "hello world", chunk_size: 4)

      expect(info).to have_attributes(chunks: 3, chunk_size: 4, size: 11)
    end

    it "stores one message per chunk plus the metadata" do
      store.put("object", "hello world", chunk_size: 4)

      expect(stored_messages).to eq(4)
    end

    it "reads a StringIO in pieces of the chunk size" do
      source = StringIO.new(binary(1000))
      lengths = []
      allow(source).to receive(:read).and_wrap_original do |original, length|
        lengths << length
        original.call(length)
      end

      store.put("object", source, chunk_size: 300)

      expect(lengths.uniq).to eq([300])
    end

    it "stores the content of a StringIO" do
      content = binary(70_000)

      expect(round_trip(StringIO.new(content), chunk_size: 9_000)).to eq(content)
    end

    it "stores the content of a File" do
      Tempfile.create("tether-object") do |file|
        file.binmode
        file.write(binary(200_000))
        file.rewind

        expect(store.put("object", file).size).to eq(200_000)
      end
    end

    it "records the SHA-256 of the content as the digest" do
      content = binary(200_000)

      expect(store.put("object", content).digest).to eq(digest_of(content))
    end

    it "returns the current time as the modification time" do
      before = Time.now

      expect(store.put("object", "x").modified_at).to be_between(before, Time.now)
    end

    it "stores the description, headers and metadata" do
      store.put("object", "x", description: "a thing", headers: { "Content-Type" => "text/plain" },
                               metadata: { "owner" => "ops" })

      expect(store.info("object")).to have_attributes(description: "a thing", metadata: { "owner" => "ops" },
                                                      headers: { "Content-Type" => ["text/plain"] })
    end

    it "publishes the chunks without headers on the chunk subject of a fresh id" do
      info = store.put("object", "hello world", chunk_size: 4)

      chunks = backing_stream.each_message(subject: "$O.#{bucket}.C.>").to_a

      expect(chunks.map { |chunk| [chunk.subject, chunk.data] })
        .to eq(["hell", "o wo", "rld"].map { |piece| [chunk_subject(info.nuid), piece] })
    end

    it "publishes the metadata as a per-subject rollup on the encoded name" do
      store.put("a.txt", "hello world", chunk_size: 4)

      expect(stored_meta("a.txt")).to include(subject: "$O.#{bucket}.M.YS50eHQ=", headers: { "Nats-Rollup" => "sub" })
    end

    it "encodes the name with the URL-safe alphabet and padding" do
      store.put("x??>", "x")

      expect(stored_subjects.keys).to include("$O.#{bucket}.M.eD8_Pg==")
    end

    it "writes the metadata document other clients read" do
      info = store.put("greeting.txt", "hello world", chunk_size: 4)

      expect(stored_meta("greeting.txt")[:document])
        .to eq("name" => "greeting.txt", "options" => { "max_chunk_size" => 4 }, "bucket" => bucket,
               "nuid" => info.nuid, "size" => 11, "chunks" => 3,
               "digest" => "SHA-256=uU0nuZNNPgilLlLX2n2r-sSE7-N6U4DukIj3rOLvzek=")
    end

    it "uses a 22 character id for the upload" do
      expect(store.put("object", "x").nuid).to match(/\A[0-9A-Za-z]{22}\z/)
    end

    it "replaces the content of an existing object" do
      store.put("object", "old")

      expect(round_trip("new content")).to eq("new content")
    end

    it "removes the chunks of the content it replaces" do
      old = store.put("object", "hello world", chunk_size: 4)
      store.put("object", "hi", chunk_size: 4)

      expect(chunks_of(old)).to eq(0)
    end

    it "leaves one metadata message and the new chunks after an overwrite" do
      store.put("object", "hello world", chunk_size: 4)
      store.put("object", "hello", chunk_size: 4)

      expect(stored_messages).to eq(3)
    end

    it "stores an object under a name that was deleted" do
      store.put("object", "old")
      store.delete("object")

      expect(round_trip("again")).to eq("again")
    end

    it "leaves no chunks behind when reading the source fails" do
      source = failing_source(["aaaa", "bbbb", :fail])

      expect { store.put("object", source, chunk_size: 4) }.to raise_error(IOError, /broke/)
      expect(stored_messages).to eq(0)
    end

    it "keeps the previous content when an upload fails" do
      store.put("object", "survivor")

      expect { store.put("object", failing_source(["aaaa", :fail]), chunk_size: 4) }.to raise_error(IOError)
      expect(store.get("object").read).to eq("survivor")
    end

    it "leaves no chunks behind when the bucket is full" do
      small = make_store("#{bucket}-small", max_bytes: 20_000)

      expect { small.put("object", binary(100_000), chunk_size: 4_000) }
        .to raise_error(Tether::JetStream::APIError, /maximum bytes exceeded/i)
      expect(stored_messages("#{bucket}-small")).to eq(0)
    end

    it "leaves no chunks behind when a chunk is larger than the server accepts" do
      store
      too_large = nc.max_payload + 1

      expect { store.put("object", binary(too_large * 2), chunk_size: too_large) }
        .to raise_error(Tether::MaxPayloadError)
      expect(stored_messages).to eq(0)
    end

    it "stores nothing when the deadline has passed" do
      store
      expired = Tether::Deadline.after(0)

      expect { store.put("object", "hello world", chunk_size: 4, timeout: expired) }
        .to raise_error(Tether::TimeoutError)
      expect(stored_messages).to eq(0)
    end

    it "raises InvalidObjectNameError for an empty name" do
      expect { store.put("", "x") }.to raise_error(Tether::ObjectStore::InvalidObjectNameError)
    end

    it "raises InvalidObjectNameError for a name that is not text" do
      expect { store.put(nil, "x") }.to raise_error(Tether::ObjectStore::InvalidObjectNameError)
    end

    it "accepts a name with spaces, dots and wildcards" do
      store.put("dir/a b.*.>", "x")

      expect(store.get("dir/a b.*.>").read).to eq("x")
    end

    it "raises ArgumentError for data that cannot be read" do
      expect { store.put("object", 42) }.to raise_error(ArgumentError, /String or something to read/)
    end

    it "raises ArgumentError for a chunk size that is not positive" do
      expect { store.put("object", "x", chunk_size: 0) }.to raise_error(ArgumentError, /chunk_size/)
    end

    it "raises BucketSealedError on a sealed bucket" do
      store.seal

      expect { store.put("object", "x") }.to raise_error(Tether::ObjectStore::BucketSealedError)
    end

    it "raises BucketNotFoundError when the bucket was deleted" do
      store
      js.delete_object_store(bucket)

      expect { store.put("object", "x") }.to raise_error(Tether::ObjectStore::BucketNotFoundError)
    end
  end

  describe "#put_file" do
    it "stores the bytes of a file" do
      content = binary(300_000)
      Tempfile.create("tether-object") do |file|
        File.binwrite(file.path, content)
        store.put_file("object", file.path)
      end

      expect(store.get("object").read).to eq(content)
    end

    it "passes its options on to the upload" do
      Tempfile.create("tether-object") do |file|
        File.binwrite(file.path, "hello world")

        expect(store.put_file("object", file.path, chunk_size: 4, description: "a file"))
          .to have_attributes(chunks: 3, description: "a file")
      end
    end
  end

  describe "#get" do
    context "when no block is given" do
      it "returns a reader over the content" do
        store.put("object", "hello world")

        expect(store.get("object")).to be_a(Tether::ObjectStore::Reader)
      end

      it "creates no consumer until the reader is read" do
        store.put("object", "hello world")
        store.get("object")

        expect(server_consumers).to be_empty
      end

      it "writes the content to an IO given as into:" do
        content = binary(300_000)
        store.put("object", content)
        out = StringIO.new("".b)

        store.get("object", into: out)

        expect(out.string).to eq(content)
      end

      it "returns the metadata when the content went into an IO" do
        store.put("object", "hello world")

        expect(store.get("object", into: StringIO.new)).to have_attributes(name: "object", size: 11)
      end

      it "removes its consumer once the content went into an IO" do
        store.put("object", "hello world", chunk_size: 4)
        store.get("object", into: StringIO.new)

        eventually { server_consumers.empty? }
      end
    end

    context "when a block is given" do
      it "yields the content a chunk at a time" do
        store.put("object", "hello world", chunk_size: 4)
        chunks = []

        store.get("object") { |chunk| chunks << chunk }

        expect(chunks).to eq(["hell", "o wo", "rld"])
      end

      it "returns the metadata" do
        store.put("object", "hello world")

        expect(store.get("object") { |chunk| chunk }).to have_attributes(name: "object", size: 11)
      end

      it "yields nothing for an empty object" do
        store.put("object", "")
        chunks = []

        store.get("object") { |chunk| chunks << chunk }

        expect(chunks).to be_empty
      end

      it "raises ArgumentError when into: is given as well" do
        expect { store.get("object", into: StringIO.new) { |chunk| chunk } }.to raise_error(ArgumentError, /not both/)
      end

      it "raises DigestMismatchError when an extra chunk was stored for the object" do
        info = store.put("object", "hello world", chunk_size: 4)
        js.publish(chunk_subject(info.nuid), "!")

        expect { store.get("object") { |chunk| chunk } }.to raise_error(Tether::ObjectStore::DigestMismatchError)
      end
    end

    it "reads an object another client built with raw publishes" do
      store
      raw_object("greeting.txt", "hello world")

      expect(store.get("greeting.txt").read).to eq("hello world")
    end

    it "reads an object whose metadata carries unknown keys" do
      store
      raw_object("object", "hello world", "future" => { "a" => 1 }, "metadata" => nil)

      expect(store.get("object").read).to eq("hello world")
    end

    it "reads the content a link to an object in the same bucket points at" do
      target = store.put("target", "linked content")
      store.add_link("alias", target)

      expect(store.get("alias").read).to eq("linked content")
    end

    it "returns the metadata of the target when read through a link" do
      store.add_link("alias", store.put("target", "linked content"))

      expect(store.get("alias") { |chunk| chunk }.name).to eq("target")
    end

    it "reads the content a link to an object in another bucket points at" do
      other = make_store("#{bucket}-other")
      store.add_link("alias", other.put("target", "from elsewhere"))

      expect(store.get("alias").read).to eq("from elsewhere")
    end

    it "raises ObjectNotFoundError through a link whose target was deleted" do
      store.add_link("alias", store.put("target", "x"))
      store.delete("target")

      expect { store.get("alias") }.to raise_error(Tether::ObjectStore::ObjectNotFoundError)
    end

    it "raises BucketNotFoundError through a link into a bucket that is gone" do
      other = make_store("#{bucket}-other")
      store.add_link("alias", other.put("target", "x"))
      js.delete_object_store("#{bucket}-other")

      expect { store.get("alias") }.to raise_error(Tether::ObjectStore::BucketNotFoundError)
    end

    it "raises LinkError for a link to a bucket" do
      store.add_bucket_link("elsewhere", make_store("#{bucket}-other"))

      expect { store.get("elsewhere") }.to raise_error(Tether::ObjectStore::LinkError, /link to the bucket/)
    end

    it "raises LinkError when a link leads to another link" do
      store.add_link("first", store.put("target", "x"))
      raw_meta("target", { "name" => "target", "bucket" => bucket, "nuid" => "n", "size" => 0, "chunks" => 0,
                           "options" => { "link" => { "bucket" => bucket, "name" => "first" } } })

      expect { store.get("first") }.to raise_error(Tether::ObjectStore::LinkError, /another link/)
    end

    it "raises ObjectNotFoundError for a name that was never stored" do
      expect { store.get("missing") }.to raise_error(Tether::ObjectStore::ObjectNotFoundError, /missing/)
    end

    it "raises ObjectNotFoundError for a deleted object" do
      store.put("object", "x")
      store.delete("object")

      expect { store.get("object") }.to raise_error(Tether::ObjectStore::ObjectNotFoundError)
    end

    it "returns an empty reader for a deleted object when asked to show deleted" do
      store.put("object", "x")
      store.delete("object")

      expect(store.get("object", show_deleted: true).read).to eq("")
    end

    it "raises InvalidObjectNameError for an empty name" do
      expect { store.get("") }.to raise_error(Tether::ObjectStore::InvalidObjectNameError)
    end

    it "raises BucketNotFoundError when the bucket was deleted" do
      store
      js.delete_object_store(bucket)

      expect { store.get("object") }.to raise_error(Tether::ObjectStore::BucketNotFoundError)
    end
  end

  describe "#get_file" do
    it "writes the content to the path" do
      content = binary(300_000)
      store.put("object", content)

      Dir.mktmpdir do |dir|
        store.get_file("object", File.join(dir, "out.bin"))

        expect(File.binread(File.join(dir, "out.bin"))).to eq(content)
      end
    end

    it "returns the metadata" do
      store.put("object", "hello world")

      Dir.mktmpdir do |dir|
        expect(store.get_file("object", File.join(dir, "out.bin"))).to have_attributes(name: "object", size: 11)
      end
    end

    it "leaves no file behind when the object is corrupt" do
      info = store.put("object", "hello world", chunk_size: 4)
      js.publish(chunk_subject(info.nuid), "!")

      Dir.mktmpdir do |dir|
        expect { store.get_file("object", File.join(dir, "out.bin")) }
          .to raise_error(Tether::ObjectStore::DigestMismatchError)
        expect(Dir.children(dir)).to be_empty
      end
    end

    it "raises ObjectNotFoundError and writes nothing for a missing object" do
      Dir.mktmpdir do |dir|
        expect { store.get_file("missing", File.join(dir, "out.bin")) }
          .to raise_error(Tether::ObjectStore::ObjectNotFoundError)
        expect(Dir.children(dir)).to be_empty
      end
    end
  end

  describe "#info" do
    it "returns the metadata of an object" do
      store.put("object", "hello world", chunk_size: 4)

      expect(store.info("object")).to have_attributes(name: "object", bucket: bucket, size: 11, chunks: 3)
    end

    it "returns a digest that matches an independent SHA-256 of the content" do
      content = binary(300_000)
      store.put("object", content)

      encoded = store.info("object").digest.delete_prefix("SHA-256=")

      expect(encoded.tr("-_", "+/").unpack1("m")).to eq(Digest::SHA256.digest(content))
    end

    it "takes the modification time from the stored message, not from the document" do
      store
      raw_object("object", "hello world")

      expect(store.info("object").modified_at).to be_within(60).of(backing_stream.info.state.last_time)
    end

    it "returns nil for a name that was never stored" do
      expect(store.info("missing")).to be_nil
    end

    it "returns nil for a deleted object" do
      store.put("object", "x")
      store.delete("object")

      expect(store.info("object")).to be_nil
    end

    it "returns the tombstone of a deleted object when asked to show deleted" do
      store.put("object", "x")
      store.delete("object")

      expect(store.info("object", show_deleted: true)).to have_attributes(deleted?: true, size: 0, chunks: 0)
    end

    it "raises InvalidObjectNameError for an empty name" do
      expect { store.info("") }.to raise_error(Tether::ObjectStore::InvalidObjectNameError)
    end

    it "raises BucketNotFoundError when the bucket was deleted" do
      store
      js.delete_object_store(bucket)

      expect { store.info("object") }.to raise_error(Tether::ObjectStore::BucketNotFoundError)
    end

    it "raises Error for metadata that is not JSON" do
      store
      js.publish(meta_subject("object"), "not json")

      expect { store.info("object") }.to raise_error(Tether::ObjectStore::Error, /not valid JSON/)
    end
  end

  describe "#exist?" do
    it "returns true for a stored object" do
      store.put("object", "x")

      expect(store.exist?("object")).to be(true)
    end

    it "returns false for a name that was never stored" do
      expect(store.exist?("missing")).to be(false)
    end

    it "returns false for a deleted object" do
      store.put("object", "x")
      store.delete("object")

      expect(store.exist?("object")).to be(false)
    end
  end

  describe "#delete" do
    it "replaces the metadata with a tombstone" do
      info = store.put("object", "hello world", chunk_size: 4)
      store.delete("object")

      expect(stored_meta("object")[:document])
        .to eq("name" => "object", "options" => { "max_chunk_size" => 4 }, "bucket" => bucket,
               "nuid" => info.nuid, "size" => 0, "chunks" => 0, "deleted" => true)
    end

    it "publishes the tombstone as a per-subject rollup" do
      store.put("object", "x")
      store.delete("object")

      expect(stored_meta("object")[:headers]).to eq("Nats-Rollup" => "sub")
    end

    it "removes the object's chunks" do
      store.put("object", "hello world", chunk_size: 4)
      store.delete("object")

      expect(stored_messages).to eq(1)
    end

    it "leaves other objects alone" do
      store.put("keep", "kept")
      store.put("object", "x")
      store.delete("object")

      expect(store.get("keep").read).to eq("kept")
    end

    it "returns the bucket" do
      store.put("object", "x")

      expect(store.delete("object")).to be(store)
    end

    it "deletes an object that is already deleted again" do
      store.put("object", "x")
      store.delete("object")

      expect(store.delete("object")).to be(store)
    end

    it "raises ObjectNotFoundError for a name that was never stored" do
      expect { store.delete("missing") }.to raise_error(Tether::ObjectStore::ObjectNotFoundError)
    end

    it "raises BucketSealedError on a sealed bucket" do
      store.put("object", "x")
      store.seal

      expect { store.delete("object") }.to raise_error(Tether::ObjectStore::BucketSealedError)
    end
  end

  describe "#update_meta" do
    it "changes the description, headers and metadata" do
      store.put("object", "x", description: "old")

      store.update_meta("object", description: "new", headers: { "K" => "v" }, metadata: { "m" => "1" })

      expect(store.info("object")).to have_attributes(description: "new", headers: { "K" => ["v"] },
                                                      metadata: { "m" => "1" })
    end

    it "keeps the fields that are not given" do
      store.put("object", "x", description: "kept", metadata: { "m" => "1" })

      store.update_meta("object", headers: { "K" => "v" })

      expect(store.info("object")).to have_attributes(description: "kept", metadata: { "m" => "1" })
    end

    it "clears a field given as an empty value" do
      store.put("object", "x", description: "old", metadata: { "m" => "1" })

      store.update_meta("object", description: "", metadata: {})

      expect(store.info("object")).to have_attributes(description: nil, metadata: {})
    end

    it "leaves the content and its identity untouched" do
      before = store.put("object", "hello world", chunk_size: 4)

      after = store.update_meta("object", description: "new")

      expect(after).to have_attributes(nuid: before.nuid, size: 11, chunks: 3, digest: before.digest, chunk_size: 4)
    end

    it "renames an object" do
      store.put("old", "content")

      store.update_meta("old", name: "new")

      expect(store.get("new").read).to eq("content")
    end

    it "removes the old name on a rename" do
      store.put("old", "content")
      store.update_meta("old", name: "new")

      expect(store.info("old", show_deleted: true)).to be_nil
    end

    it "keeps the chunks where they are on a rename" do
      before = store.put("old", "hello world", chunk_size: 4)
      store.update_meta("old", name: "new")

      expect(chunks_of(before)).to eq(3)
    end

    it "renames onto the name of a deleted object" do
      store.put("gone", "x")
      store.delete("gone")
      store.put("old", "content")

      store.update_meta("old", name: "gone")

      expect(store.get("gone").read).to eq("content")
    end

    it "updates the metadata of a link" do
      store.add_link("alias", store.put("target", "x"))

      store.update_meta("alias", description: "a pointer")

      expect(store.info("alias")).to have_attributes(description: "a pointer", link?: true)
    end

    it "raises ObjectExistsError when the new name belongs to a live object" do
      store.put("old", "x")
      store.put("taken", "y")

      expect { store.update_meta("old", name: "taken") }.to raise_error(Tether::ObjectStore::ObjectExistsError)
    end

    it "raises ObjectNotFoundError for a deleted object" do
      store.put("object", "x")
      store.delete("object")

      expect { store.update_meta("object", description: "new") }
        .to raise_error(Tether::ObjectStore::ObjectNotFoundError)
    end

    it "raises ObjectNotFoundError for a name that was never stored" do
      expect { store.update_meta("missing", description: "new") }
        .to raise_error(Tether::ObjectStore::ObjectNotFoundError)
    end

    it "raises InvalidObjectNameError for an empty new name" do
      store.put("object", "x")

      expect { store.update_meta("object", name: "") }.to raise_error(Tether::ObjectStore::InvalidObjectNameError)
    end
  end

  describe "#add_link" do
    it "stores a link entry without content" do
      target = store.put("target", "x")

      expect(store.add_link("alias", target)).to have_attributes(name: "alias", link?: true, size: 0, chunks: 0)
    end

    it "writes the link document other clients read" do
      link = store.add_link("latest", store.put("greeting.txt", "x"))

      expect(stored_meta("latest")[:document])
        .to eq("name" => "latest", "options" => { "link" => { "bucket" => bucket, "name" => "greeting.txt" } },
               "bucket" => bucket, "nuid" => link.nuid, "size" => 0, "chunks" => 0)
    end

    it "gives the link an id of its own" do
      target = store.put("target", "x")

      expect(store.add_link("alias", target).nuid).not_to eq(target.nuid)
    end

    it "replaces an existing link" do
      store.add_link("alias", store.put("first", "one"))
      store.add_link("alias", store.put("second", "two"))

      expect(store.get("alias").read).to eq("two")
    end

    it "takes the name of a deleted object" do
      store.put("alias", "x")
      store.delete("alias")

      expect(store.add_link("alias", store.put("target", "y"))).to have_attributes(link?: true)
    end

    it "raises ObjectExistsError when the name belongs to a live object" do
      store.put("alias", "x")

      expect { store.add_link("alias", store.put("target", "y")) }
        .to raise_error(Tether::ObjectStore::ObjectExistsError)
    end

    it "raises LinkError for a deleted target" do
      store.put("target", "x")
      store.delete("target")

      expect { store.add_link("alias", store.info("target", show_deleted: true)) }
        .to raise_error(Tether::ObjectStore::LinkError, /deleted/)
    end

    it "raises LinkError for a target that is a link" do
      link = store.add_link("first", store.put("target", "x"))

      expect { store.add_link("second", link) }.to raise_error(Tether::ObjectStore::LinkError, /is a link/)
    end

    it "raises ArgumentError for a target that is not the metadata of an object" do
      expect { store.add_link("alias", "target") }.to raise_error(ArgumentError, /Info/)
    end

    it "raises InvalidObjectNameError for an empty name" do
      target = store.put("target", "x")

      expect { store.add_link("", target) }.to raise_error(Tether::ObjectStore::InvalidObjectNameError)
    end
  end

  describe "#add_bucket_link" do
    it "stores a link to another bucket" do
      link = store.add_bucket_link("elsewhere", make_store("#{bucket}-other"))

      expect(link).to have_attributes(link?: true, bucket_link?: true,
                                      link: have_attributes(bucket: "#{bucket}-other", name: nil))
    end

    it "writes the bucket link document other clients read" do
      link = store.add_bucket_link("archive", "OLD-DOCS")

      expect(stored_meta("archive")[:document])
        .to eq("name" => "archive", "options" => { "link" => { "bucket" => "OLD-DOCS" } }, "bucket" => bucket,
               "nuid" => link.nuid, "size" => 0, "chunks" => 0)
    end

    it "raises ObjectExistsError when the name belongs to a live object" do
      store.put("elsewhere", "x")

      expect { store.add_bucket_link("elsewhere", "other") }.to raise_error(Tether::ObjectStore::ObjectExistsError)
    end

    it "raises InvalidBucketNameError for a bucket name that breaks the rules" do
      expect { store.add_bucket_link("elsewhere", "no.dots") }
        .to raise_error(Tether::ObjectStore::InvalidBucketNameError)
    end
  end

  describe "#list" do
    it "returns the live objects" do
      store.put("a", "1")
      store.put("b", "2")

      expect(store.list.map(&:name).to_a).to contain_exactly("a", "b")
    end

    it "leaves deleted objects out" do
      store.put("a", "1")
      store.put("b", "2")
      store.delete("a")

      expect(store.list.map(&:name).to_a).to eq(["b"])
    end

    it "includes the tombstones when asked for deleted objects" do
      store.put("a", "1")
      store.put("b", "2")
      store.delete("a")

      expect(names_of(store.list(deleted: true))).to contain_exactly(["a", true], ["b", false])
    end

    it "returns nothing for an empty bucket" do
      expect(store.list.to_a).to be_empty
    end

    it "returns nothing for a bucket that holds only deleted objects" do
      store.put("a", "1")
      store.delete("a")

      expect(store.list.to_a).to be_empty
    end

    it "returns one entry for an object that was overwritten" do
      store.put("a", "1")
      store.put("a", "2")

      expect(store.list.map(&:size).to_a).to eq([1])
    end

    it "lists many objects" do
      120.times { |index| store.put("object-#{index}") }

      expect(store.list.count).to eq(120)
    end

    it "sets the modification time of each entry from its stored message" do
      store.put("a", "1")

      expect(store.list.first.modified_at).to be_within(60).of(backing_stream.info.state.last_time)
    end

    it "removes its consumer when the enumeration completes" do
      store.put("a", "1")
      store.list.to_a

      eventually { server_consumers.empty? }
    end

    it "removes its consumer when the enumeration is abandoned" do
      3.times { |index| store.put("object-#{index}") }
      store.list.first

      eventually { server_consumers.empty? }
    end

    it "raises BucketNotFoundError when the bucket was deleted" do
      store
      js.delete_object_store(bucket)

      expect { store.list.to_a }.to raise_error(Tether::ObjectStore::BucketNotFoundError)
    end
  end

  describe "#watch" do
    it "delivers the current entries, then becomes ready, then delivers updates" do
      store.put("a", "1")
      store.put("b", "2")
      watcher = store.watch(on_ready: -> { entries.push(:ready) }) { |info| entries.push(info.name) }
      watcher.wait_until_ready(timeout: 5)
      store.put("c", "3")

      expect(taken(4)).to eq(["a", "b", :ready, "c"])
    end

    it "delivers deletions as tombstones" do
      store.put("a", "1")
      collecting.wait_until_ready(timeout: 5)
      store.delete("a")

      expect(names_of(taken(2))).to eq([["a", false], ["a", true]])
    end

    it "leaves tombstones out with ignore_deletes" do
      store.put("a", "1")
      store.put("b", "2")
      store.delete("a")
      collecting(ignore_deletes: true).wait_until_ready(timeout: 5)
      store.delete("b")
      store.put("c", "3")

      expect(names_of(taken(2))).to eq([["b", false], ["c", false]])
    end

    it "delivers only later changes with updates_only" do
      store.put("a", "1")
      collecting(updates_only: true)
      store.put("b", "2")

      expect(names_of(taken(1))).to eq([["b", false]])
    end

    it "starts from the oldest stored entry with include_history" do
      store.put("a", "1")
      store.put("b", "2")
      collecting(include_history: true).wait_until_ready(timeout: 5)

      expect(names_of(taken(2))).to eq([["a", false], ["b", false]])
    end

    it "delivers nothing more after it was stopped" do
      watcher = collecting
      watcher.wait_until_ready(timeout: 5)
      watcher.stop
      store.put("a", "1")
      store.put("b", "2")

      expect(entries.pop(timeout: 0.2)).to be_nil
    end

    it "returns a watcher to iterate when no block is given" do
      store.put("a", "1")

      expect(store.watch.next(timeout: 5).name).to eq("a")
    end

    it "raises ArgumentError when include_history is combined with updates_only" do
      expect { store.watch(include_history: true, updates_only: true) }
        .to raise_error(ArgumentError, /cannot be combined/)
    end

    it "raises BucketNotFoundError when the bucket was deleted" do
      store
      js.delete_object_store(bucket)

      expect { store.watch }.to raise_error(Tether::ObjectStore::BucketNotFoundError)
    end
  end

  describe "#seal" do
    it "marks the stream behind the bucket as sealed" do
      store.seal

      expect(backing_stream.info.config.sealed).to be(true)
    end

    it "returns the bucket" do
      expect(store.seal).to be(store)
    end

    it "keeps the stored objects readable" do
      store.put("object", "hello world")
      store.seal

      expect(store.get("object").read).to eq("hello world")
    end

    it "raises BucketNotFoundError when the bucket was deleted" do
      store
      js.delete_object_store(bucket)

      expect { store.seal }.to raise_error(Tether::ObjectStore::BucketNotFoundError)
    end
  end

  describe "#status" do
    it "returns the status of the bucket" do
      expect(store.status).to be_a(Tether::ObjectStore::Status).and have_attributes(bucket: bucket)
    end

    it "reads fresh numbers from the server on every call" do
      before = store.status.size
      store.put("object", binary(10_000))

      expect(store.status.size).to be > before + 10_000
    end

    it "raises BucketNotFoundError when the bucket was deleted" do
      store
      js.delete_object_store(bucket)

      expect { store.status }.to raise_error(Tether::ObjectStore::BucketNotFoundError)
    end
  end

  describe "#inspect" do
    it "shows the bucket's name" do
      expect(store.inspect).to eq(%(#<Tether::ObjectStore bucket="#{bucket}">))
    end
  end
end
