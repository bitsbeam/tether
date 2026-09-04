# frozen_string_literal: true

RSpec.describe Tether::JetStream::API do
  include ClientHelper

  let(:nc) { connect }
  let(:api) { described_class.new(nc, prefix: "$JS.API", timeout: 5) }
  let(:name) { "API#{Tether::Connection::NUID.next}" }

  def error_of(subject, payload = nil, **options)
    error_from { api.request(subject, payload, **options) }
  end

  describe "#deadline" do
    it "returns a deadline it is given, so several steps spend one budget" do
      deadline = Tether::Deadline.after(1)

      expect(api.deadline(deadline)).to be(deadline)
    end

    it "starts a clock for a number of seconds" do
      expect(api.deadline(0.5).remaining).to be_between(0.4, 0.5)
    end

    it "falls back to the default timeout for nil" do
      expect(api.deadline(nil).remaining).to be_between(4.9, 5)
    end
  end

  describe "#request" do
    after { error_from { api.request("STREAM.DELETE.#{name}") } }

    it "returns the decoded response" do
      expect(api.request("INFO")).to include("type" => "io.nats.jetstream.api.v1.account_info_response",
                                             "limits" => a_kind_of(Hash))
    end

    it "sends a hash as a JSON body" do
      response = api.request("STREAM.CREATE.#{name}", { "name" => name, "storage" => "memory" })

      expect(response.dig("config", "storage")).to eq("memory")
    end

    it "sends a string body as it is" do
      response = api.request("STREAM.CREATE.#{name}", %({"name":"#{name}","storage":"memory"}))

      expect(response.dig("config", "name")).to eq(name)
    end

    it "raises the error class mapped to the server's error code" do
      expect { api.request("STREAM.INFO.#{name}") }.to raise_error(Tether::JetStream::StreamNotFoundError)
    end

    it "carries the code, error code and description of the server's error" do
      expect(error_of("STREAM.INFO.#{name}"))
        .to have_attributes(code: 404, err_code: 10_059, description: "stream not found",
                            message: "stream not found (code 404, err_code 10059)")
    end

    it "raises a plain APIError for an error code without a class of its own" do
      api.request("STREAM.CREATE.#{name}", { "name" => name, "storage" => "memory" })
      error = error_of("STREAM.LEADER.STEPDOWN.#{name}")

      expect(error).to be_an_instance_of(Tether::JetStream::APIError)
      expect(error).to have_attributes(code: 503, err_code: 10_010)
    end

    it "raises BadRequestError for a request the server calls bad" do
      api.request("STREAM.CREATE.#{name}", { "name" => name, "storage" => "memory" })

      expect { api.request("STREAM.PURGE.#{name}", { "seq" => 2, "keep" => 1 }) }
        .to raise_error(Tether::JetStream::BadRequestError)
    end

    it "raises RequiredApiLevelError when the server is older than the level the request needs" do
      expect(error_of("INFO", level: 9999))
        .to be_a(Tether::JetStream::RequiredApiLevelError).and have_attributes(code: 412, err_code: 10_185)
    end

    it "is answered when the server meets the level the request needs" do
      expect(api.request("INFO", level: 1)).to include("api")
    end

    it "raises TimeoutError when the deadline it is given has passed" do
      expect { api.request("INFO", timeout: Tether::Deadline.after(0)) }.to raise_error(Tether::TimeoutError)
    end

    it "raises ServiceUnavailableError when JetStream is not enabled on the server" do
      NatsServer.run do |plain|
        offline = described_class.new(connect(plain.uri), prefix: "$JS.API", timeout: 5)

        expect { offline.request("INFO") }
          .to raise_error(Tether::JetStream::ServiceUnavailableError, /nothing answered \$JS\.API\.INFO/)
      end
    end

    it "raises ServiceUnavailableError when nothing serves the prefix" do
      elsewhere = described_class.new(nc, prefix: "$JS.nowhere.API", timeout: 5)

      expect(error_from { elsewhere.request("INFO") })
        .to be_a(Tether::JetStream::ServiceUnavailableError).and be_retryable
    end
  end

  describe "#pages" do
    before(:all) do
      @listed = NatsServer.new(config: "jetstream {}").start
      Tether.connect(@listed.uri) do |client|
        @names = Array.new(1030) { |index| format("P%04d", index) }
        @names.each { |stream| client.request("$JS.API.STREAM.CREATE.#{stream}", %({"name":"#{stream}"})) }
      end
    end

    after(:all) { @listed.stop }

    let(:listing) { described_class.new(connect(@listed.uri), prefix: "$JS.API", timeout: 5) }

    it "walks every page of a listing longer than one page" do
      expect(listing.pages("STREAM.NAMES", "streams").to_a).to eq(@names)
    end

    it "asks for a page only when the items before it were consumed" do
      allow(listing).to receive(:request).and_call_original

      expect(listing.pages("STREAM.NAMES", "streams").first(3)).to eq(%w[P0000 P0001 P0002])
      expect(listing).to have_received(:request).once
    end

    it "asks for the next page at the offset of the items it has received" do
      allow(listing).to receive(:request).and_call_original
      listing.pages("STREAM.NAMES", "streams").to_a

      expect(listing).to have_received(:request).with("STREAM.NAMES", { "offset" => 0 }, timeout: nil).ordered
      expect(listing).to have_received(:request).with("STREAM.NAMES", { "offset" => 1024 }, timeout: nil).ordered
    end

    it "sends the request fields with every page" do
      expect(listing.pages("STREAM.NAMES", "streams", { "subject" => "P0007" }).to_a).to eq(["P0007"])
    end

    it "yields nothing when nothing matches" do
      expect(listing.pages("STREAM.NAMES", "streams", { "subject" => "none.x" }).to_a).to eq([])
    end

    it "raises TimeoutError once a deadline for the whole walk has passed" do
      expect { listing.pages("STREAM.NAMES", "streams", timeout: Tether::Deadline.after(0)).to_a }
        .to raise_error(Tether::TimeoutError)
    end
  end

  describe "#decode" do
    it "returns the decoded object" do
      expect(api.decode(%({"stream":"ORDERS","seq":3}))).to eq("stream" => "ORDERS", "seq" => 3)
    end

    it "raises the mapped error when the object carries one" do
      body = %({"error":{"code":400,"err_code":10071,"description":"wrong last sequence: 5"},"stream":"S","seq":0})

      expect { api.decode(body) }
        .to raise_error(Tether::JetStream::WrongLastSequenceError, "wrong last sequence: 5 (code 400, err_code 10071)")
    end

    it "raises a JetStream error for a body that is not JSON" do
      expect { api.decode("+OK") }.to raise_error(Tether::JetStream::Error, /not JSON/)
    end
  end
end
