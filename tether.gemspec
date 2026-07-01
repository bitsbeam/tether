# frozen_string_literal: true

require_relative "lib/tether/version"

Gem::Specification.new do |spec|
  spec.name = "tether"
  spec.version = Tether::VERSION
  spec.authors = ["Dmitry Vorotilin"]
  spec.email = ["d.vorotilin@gmail.com"]

  spec.summary = "A NATS client for Ruby"
  spec.description = "A NATS client for Ruby, built around explicit deadlines and per-subscription " \
                     "dispatch so one connection is safe to use from many threads at once."
  spec.homepage = "https://github.com/bitsbeam/tether"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.2.0"
  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = spec.homepage
  spec.metadata["changelog_uri"] = "#{spec.homepage}/blob/main/CHANGELOG.md"
  spec.metadata["rubygems_mfa_required"] = "true"

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  gemspec = File.basename(__FILE__)
  spec.files = IO.popen(%w[git ls-files -z], chdir: __dir__, err: IO::NULL) do |ls|
    ls.readlines("\x0", chomp: true).reject do |f|
      (f == gemspec) ||
        f.start_with?(*%w[bin/ Gemfile .gitignore .rspec spec/ .github/ .rubocop.yml docs/]) ||
        f.end_with?("-notes.md", "-plan.md")
    end
  end
  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  # tether takes no runtime dependencies: Ruby 3.2 covers every blocking
  # primitive it needs, and a transport level gem should not impose more.
end
