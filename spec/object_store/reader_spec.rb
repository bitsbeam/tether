# frozen_string_literal: true

require "stringio"

RSpec.describe Tether::ObjectStore::Reader do
  include ObjectStoreHelper

  let(:proxied) { connect(proxy.uri, reconnect: ClientHelper::FAST_RECONNECT) }

  def reader_for(content, **options)
    store.put("object", content, **options)
    store.get("object")
  end

  def tampered
    info = store.put("object", "hello world", chunk_size: 4)
    yield info
    store.get("object")
  end

  def with_extra_chunk
    tampered { |info| js.publish(chunk_subject(info.nuid), "!") }
  end

  def with_missing_chunks
    tampered { |info| backing_stream.purge(subject: chunk_subject(info.nuid), keep: 1) }
  end

  def with_wrong_digest
    tampered { |info| raw_meta("object", info.to_h.merge("digest" => digest_of("something else"))) }
  end

  def with_malformed_digest
    tampered { |info| raw_meta("object", info.to_h.merge("digest" => "MD5=abc")) }
  end

  def with_swapped_chunk
    tampered do |info|
      backing_stream.purge(subject: chunk_subject(info.nuid), keep: 2)
      js.publish(chunk_subject(info.nuid), "XYZ!")
    end
  end

  def through_proxy(content, chunk_size:)
    store.put("object", content, chunk_size: chunk_size)
    proxied.jetstream.object_store(bucket).get("object", timeout: 10)
  end

  describe "#info" do
    it "returns the metadata of the object being read" do
      expect(reader_for("hello world").info).to have_attributes(name: "object", size: 11)
    end
  end

  describe "#read" do
    context "when no length is given" do
      it "returns the whole content" do
        content = binary(400_000)

        expect(reader_for(content).read).to eq(content)
      end

      it "returns binary data" do
        expect(reader_for("héllo").read.encoding).to eq(Encoding::BINARY)
      end

      it "returns an empty string for an empty object" do
        expect(reader_for("").read).to eq("")
      end

      it "returns an empty string once everything was read" do
        reader = reader_for("hello world")
        reader.read

        expect(reader.read).to eq("")
      end

      it "returns what is left after a partial read" do
        reader = reader_for("hello world", chunk_size: 4)
        reader.read(6)

        expect(reader.read).to eq("world")
      end

      it "raises DigestMismatchError when an extra chunk was stored" do
        expect { with_extra_chunk.read }.to raise_error(Tether::ObjectStore::DigestMismatchError, /12 bytes/)
      end

      it "raises DigestMismatchError when chunks are missing" do
        expect { with_missing_chunks.read }.to raise_error(Tether::ObjectStore::DigestMismatchError, /3 bytes/)
      end

      it "raises DigestMismatchError when every chunk is missing" do
        reader = tampered { |info| backing_stream.purge(subject: chunk_subject(info.nuid)) }

        expect { reader.read }.to raise_error(Tether::ObjectStore::DigestMismatchError, /0 bytes/)
      end

      it "raises DigestMismatchError when a chunk of the right size has other bytes" do
        expect { with_swapped_chunk.read }
          .to raise_error(Tether::ObjectStore::DigestMismatchError, /does not match its digest/)
      end

      it "raises DigestMismatchError when the recorded digest was rewritten" do
        expect { with_wrong_digest.read }
          .to raise_error(Tether::ObjectStore::DigestMismatchError, /does not match its digest/)
      end

      it "raises DigestMismatchError when the recorded digest cannot be decoded" do
        expect { with_malformed_digest.read }
          .to raise_error(Tether::ObjectStore::DigestMismatchError, /invalid format/)
      end

      it "raises the same error again after a corrupt read" do
        reader = with_extra_chunk
        error_from { reader.read }

        expect { reader.read }.to raise_error(Tether::ObjectStore::DigestMismatchError)
      end

      it "removes its consumer when the end is reached" do
        reader_for("hello world", chunk_size: 4).read

        eventually { server_consumers.empty? }
      end

      it "returns the content intact when the connection drops mid-download" do
        content = binary(2_000_000)
        reader = through_proxy(content, chunk_size: 1024)
        head = reader.read(50_000)
        proxy.drop

        expect([Digest::SHA256.hexdigest(head + reader.read), proxied.stats.reconnects])
          .to eq([Digest::SHA256.hexdigest(content), 1])
      end

      it "raises TimeoutError when the chunks do not arrive in time" do
        store.put("object", binary(2_000_000), chunk_size: 1024)
        client = connect(proxy.uri, reconnect: { wait: 30, jitter: 0, max_attempts: -1 })
        reader = client.jetstream.object_store(bucket).get("object", timeout: 0.2)
        reader.read(10)
        proxy.stop

        expect { reader.read }.to raise_error(Tether::TimeoutError)
      end
    end

    context "when a length is given" do
      it "returns that many bytes across chunk boundaries" do
        reader = reader_for("hello world", chunk_size: 4)

        expect([reader.read(6), reader.read(3), reader.read(2)]).to eq(["hello ", "wor", "ld"])
      end

      it "returns what is left when fewer bytes remain" do
        reader = reader_for("hello world", chunk_size: 4)
        reader.read(8)

        expect(reader.read(100)).to eq("rld")
      end

      it "returns nil at the end" do
        reader = reader_for("hello world")
        reader.read(11)

        expect(reader.read(1)).to be_nil
      end

      it "returns nil for an empty object" do
        expect(reader_for("").read(10)).to be_nil
      end

      it "returns an empty string for a length of zero" do
        expect(reader_for("hello world").read(0)).to eq("")
      end

      it "fills the buffer it is given" do
        buffer = +"old"
        reader_for("hello world").read(5, buffer)

        expect(buffer).to eq("hello")
      end

      it "copies through IO.copy_stream" do
        content = binary(300_000)
        out = StringIO.new("".b)

        IO.copy_stream(reader_for(content), out)

        expect(out.string).to eq(content)
      end

      it "raises ArgumentError for a negative length" do
        expect { reader_for("hello world").read(-1) }.to raise_error(ArgumentError, /negative/)
      end

      it "raises DigestMismatchError on the read that reaches the end of a corrupt object" do
        reader = with_extra_chunk
        reader.read(4)

        expect { reader.read(100) }.to raise_error(Tether::ObjectStore::DigestMismatchError)
      end
    end
  end

  describe "#each" do
    context "when a block is given" do
      it "yields the chunks as they were stored" do
        content = +""

        reader_for("hello world", chunk_size: 4).each { |chunk| content << chunk << "|" }

        expect(content).to eq("hell|o wo|rld|")
      end

      it "returns the reader" do
        reader = reader_for("hello world")

        expect(reader.each { |chunk| chunk }).to be(reader)
      end

      it "yields a large object without a gap or a repeat when the connection drops mid-download" do
        content = binary(1_500_000)
        reader = through_proxy(content, chunk_size: 1024)
        digest = Digest::SHA256.new
        count = 0

        reader.each do |chunk|
          digest.update(chunk)
          proxy.drop if (count += 1) == 100
        end

        expect([digest.hexdigest, proxied.stats.reconnects]).to eq([Digest::SHA256.hexdigest(content), 1])
      end

      it "raises DigestMismatchError after the last chunk of a corrupt object" do
        expect { with_wrong_digest.each { |chunk| chunk } }.to raise_error(Tether::ObjectStore::DigestMismatchError)
      end
    end

    context "when no block is given" do
      it "returns an enumerator over the chunks" do
        expect(reader_for("hello world", chunk_size: 4).each.to_a).to eq(["hell", "o wo", "rld"])
      end
    end
  end

  describe "#eof?" do
    it "returns false before the content was read" do
      expect(reader_for("hello world").eof?).to be(false)
    end

    it "returns false while bytes are left" do
      reader = reader_for("hello world", chunk_size: 4)
      reader.read(6)

      expect(reader.eof?).to be(false)
    end

    it "returns true once everything was read" do
      reader = reader_for("hello world")
      reader.read

      expect(reader.eof?).to be(true)
    end
  end

  describe "#close" do
    it "removes the consumer of a download that was not finished" do
      reader = reader_for(binary(50_000), chunk_size: 1024)
      reader.read(10)
      reader.close

      eventually { server_consumers.empty? }
    end

    it "makes further reads raise IOError" do
      reader = reader_for("hello world", chunk_size: 4)
      reader.read(2)
      reader.close

      expect { reader.read }.to raise_error(IOError, /closed/)
    end

    it "returns nil when called twice" do
      reader = reader_for("hello world")
      reader.close

      expect(reader.close).to be_nil
    end
  end

  describe "#closed?" do
    it "returns false for a new reader" do
      expect(reader_for("hello world").closed?).to be(false)
    end

    it "returns true after the reader was closed" do
      reader = reader_for("hello world")
      reader.close

      expect(reader.closed?).to be(true)
    end

    it "returns false for a reader that reached the end" do
      reader = reader_for("hello world")
      reader.read
      reader.close

      expect(reader.closed?).to be(false)
    end
  end

  describe "#inspect" do
    it "shows the object and how far the reader is" do
      reader = reader_for("hello world", chunk_size: 4)
      reader.read(4)

      expect(reader.inspect)
        .to eq(%(#<Tether::ObjectStore::Reader bucket="#{bucket}" name="object" read=4/11>))
    end
  end
end
