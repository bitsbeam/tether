# frozen_string_literal: true

module Eventually
  POLL_INTERVAL = 0.005

  # Polls the block until it returns something truthy and returns that.
  #
  # @param timeout [Numeric] seconds to keep polling before failing the example
  def eventually(timeout: 2)
    deadline = Tether::Deadline.after(timeout)
    pause = Thread::Queue.new
    loop do
      result = yield
      return result if result

      if deadline.expired?
        raise RSpec::Expectations::ExpectationNotMetError, "the condition was not met within #{timeout}s"
      end

      pause.pop(timeout: POLL_INTERVAL)
    end
  end

  def tether_threads
    Thread.list.select { |thread| thread.alive? && thread.name.to_s.start_with?("tether:") }
  end
end
