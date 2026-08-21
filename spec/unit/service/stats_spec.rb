# frozen_string_literal: true

RSpec.describe Tether::Service::Stats do
  def reference
    {
      "name" => "EchoService", "id" => "hG7RmJkP2sTqLw9XcVb4Nd", "version" => "1.0.0", "metadata" => nil,
      "type" => "io.nats.micro.v1.stats_response", "started" => "2026-11-22T10:15:00.123456789Z",
      "endpoints" => [{ "name" => "echo", "subject" => "svc.echo", "queue_group" => "q", "num_requests" => 2,
                        "num_errors" => 1, "last_error" => "400:empty request", "processing_time" => 184_000,
                        "average_processing_time" => 92_000 }]
    }
  end

  describe "#started" do
    it "reads the nanosecond timestamp another client sends" do
      started = described_class.new(reference).started

      expect([started.utc?, started.iso8601(9)]).to eq([true, "2026-11-22T10:15:00.123456789Z"])
    end

    it "returns nil for a timestamp that cannot be read" do
      expect(described_class.new(reference.merge("started" => "yesterday")).started).to be_nil
    end
  end

  describe "#endpoints" do
    it "converts the wire's nanoseconds to seconds" do
      entry = described_class.new(reference).endpoints.first

      expect([entry.processing_time, entry.average_processing_time]).to eq([0.000184, 0.000092])
    end

    it "reads the counters another client sends" do
      entry = described_class.new(reference).endpoints.first

      expect([entry.requests, entry.errors, entry.last_error, entry.data]).to eq([2, 1, "400:empty request", nil])
    end
  end

  describe "#metadata" do
    it "returns an empty hash when another client sent null" do
      expect(described_class.new(reference).metadata).to eq({})
    end
  end
end
