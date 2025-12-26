# frozen_string_literal: true

RSpec.describe Tether do
  it "has a version number" do
    expect(Tether::VERSION).to match(/\A\d+\.\d+\.\d+/)
  end

  it "never reaches for Timeout.timeout or Thread#raise, which fire at an arbitrary bytecode boundary" do
    banned = /Timeout\.timeout|Thread\.(?:current|main)\.raise|Thread#raise|\.kill\b/
    offenders = Dir[File.expand_path("../lib/**/*.rb", __dir__)].filter_map do |path|
      code = File.readlines(path).reject { _1.lstrip.start_with?("#") }.join
      path if code.match?(banned)
    end

    expect(offenders).to be_empty
  end
end
