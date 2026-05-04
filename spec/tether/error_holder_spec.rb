# frozen_string_literal: true

RSpec.describe Tether::ErrorHolder do
  subject(:errors) { described_class.new }

  let(:failure) { Tether::TimeoutError.new("too slow") }

  describe "#record" do
    it "keeps the most recent error whether or not anyone is listening" do
      errors.record(failure)

      expect(errors.last).to be(failure)
    end

    it "hands the error to every registered handler" do
      seen = []
      errors.subscribe { |error| seen << [:first, error] }
      errors.subscribe { |error| seen << [:second, error] }
      errors.record(failure)

      expect(seen).to eq([[:first, failure], [:second, failure]])
    end

    it "survives a handler that raises, since the reader thread reports through here" do
      reached = []
      errors.subscribe { raise "handler is broken" }
      errors.subscribe { |error| reached << error }

      expect { errors.record(failure) }.not_to raise_error
      expect(reached).to eq([failure])
    end
  end

  describe "#last" do
    it "is nil before anything goes wrong" do
      expect(errors.last).to be_nil
    end
  end

  it "records from many threads without losing a handler" do
    seen = Thread::Queue.new
    errors.subscribe { |error| seen << error }
    8.times.map { |t| Thread.new { errors.record(Tether::Error.new("e#{t}")) } }.each(&:join)

    expect(seen.size).to eq(8)
  end
end
