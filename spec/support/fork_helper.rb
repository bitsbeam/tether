# frozen_string_literal: true

module ForkHelper
  # Runs the block in a forked child and returns its value, or the class
  # name and message of what it raised.
  #
  # @param timeout [Numeric] seconds the child may take
  def in_child(timeout: 10, &block)
    reader, writer = IO.pipe
    pid = fork do
      reader.close
      writer.write(child_result(&block))
      writer.close
      exit!(0)
    end
    writer.close
    Process.kill("KILL", pid) unless reader.wait_readable(timeout)
    data = reader.read
    Process.wait(pid)
    raise "the forked child did not finish within #{timeout}s" if data.empty?

    Marshal.load(data) # rubocop:disable Security/MarshalLoad
  ensure
    reader&.close
  end

  private

  def child_result
    Marshal.dump(yield)
  rescue Exception => e # rubocop:disable Lint/RescueException
    Marshal.dump([e.class.name, e.message])
  end
end
