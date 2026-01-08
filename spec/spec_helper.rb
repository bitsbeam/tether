# frozen_string_literal: true

require "tether"

Dir[File.join(__dir__, "support", "**", "*.rb")].each { |file| require file }

module SharedNatsServer
  def self.instance
    @instance ||= NatsServer.new.start
  end

  def self.stop
    @instance&.stop
    @instance = nil
  end
end

RSpec.configure do |config|
  config.after(:suite) { SharedNatsServer.stop }

  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = ".rspec_status"

  # Disable RSpec exposing methods globally on `Module` and `main`
  config.disable_monkey_patching!

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end
end
