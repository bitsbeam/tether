# frozen_string_literal: true

# Stands in for a socket under the writer: records what is written and fails
# on demand.
class FakeTransport
  def initialize
    @lock = Mutex.new
    @written = String.new(encoding: Encoding::BINARY)
    @failure = nil
  end

  def fail_with(error)
    @lock.synchronize { @failure = error }
  end

  def write(data, _deadline = nil)
    @lock.synchronize do
      raise @failure if @failure

      @written << data
    end
    data.bytesize
  end

  def written
    @lock.synchronize { @written.dup }
  end
end
