# frozen_string_literal: true

RSpec.describe Tether do
  def lib_files_matching(pattern)
    Dir[File.expand_path("../../lib/**/*.rb", __dir__)].select do |path|
      File.readlines(path).reject { |line| line.lstrip.start_with?("#") }.join.match?(pattern)
    end
  end

  describe ".connect" do
    it "relies on no Timeout.timeout anywhere in lib, which fires at an arbitrary bytecode boundary" do
      expect(lib_files_matching(/Timeout\.timeout|require ["']timeout["']/)).to be_empty
    end

    it "relies on no Thread#raise anywhere in lib" do
      expect(lib_files_matching(/Thread\.(?:current|main)\.raise|thread\.raise|Thread#raise/)).to be_empty
    end

    it "relies on no Thread#kill anywhere in lib" do
      expect(lib_files_matching(/\.kill\b|\.terminate\b|Thread\.exit/)).to be_empty
    end
  end
end
