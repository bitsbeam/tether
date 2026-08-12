# frozen_string_literal: true

RSpec.describe Tether::Connection::Writer do
  subject(:writer) { build_writer }

  let(:counters) { Tether::Counters.new }
  let(:failures) { Thread::Queue.new }
  let(:transport) { FakeTransport.new }

  after { writer.close }

  def build_writer(reconnect_buffer: 1024)
    described_class.new(write_timeout: 1, reconnect_buffer: reconnect_buffer, counters: counters,
                        on_failure: ->(transport, error) { failures.push([transport, error]) })
  end

  def attach(target = transport, frames = "")
    writer.attach(target) { frames }
  end

  describe "#attach" do
    it "writes the block's frames ahead of what was buffered" do
      writer.write("PUB a 0\r\n\r\n")
      attach(transport, "SUB a 1\r\n")
      writer.kick

      expect(eventually { transport.written == "SUB a 1\r\nPUB a 0\r\n\r\n" }).to be(true)
    end

    it "does nothing once the writer is closed" do
      writer.close
      attach

      expect(writer.buffered).to eq(0)
    end
  end

  describe "#detach" do
    it "releases callers waiting for a PONG with the error" do
      attach
      waiter = Thread::Queue.new
      writer.ping(waiter)
      writer.detach(Tether::DisconnectedError.new("connection lost"))

      expect(waiter.pop(timeout: 2)).to be_a(Tether::DisconnectedError)
    end

    it "keeps later frames for the next connection" do
      attach
      writer.detach(Tether::DisconnectedError.new("connection lost"))
      writer.write("PUB a 0\r\n\r\n")

      expect(writer.buffered).to eq(11)
    end
  end

  describe "#write" do
    it "writes before returning when asked to flush" do
      attach
      writer.write("PUB a 0\r\n\r\n", flush: true)

      expect(transport.written).to eq("PUB a 0\r\n\r\n")
    end

    it "leaves an unflushed frame to the flusher" do
      attach
      writer.write("PUB a 0\r\n\r\n")

      expect(eventually { transport.written == "PUB a 0\r\n\r\n" }).to be(true)
    end

    it "writes frames from many threads whole and in each thread's order" do
      attach
      8.times.map { |thread| Thread.new { 200.times { |index| writer.write("#{thread}:#{index}\n") } } }.each(&:join)
      eventually { transport.written.count("\n") == 1600 }
      orders = transport.written.lines.group_by { |line| line.split(":").first }.values
      orders = orders.map { |lines| lines.map { |line| line.split(":").last.to_i } }

      expect(orders).to all(eq((0...200).to_a))
    end

    it "counts published messages and bytes written" do
      attach
      writer.write("PUB a 0\r\n\r\n", flush: true, messages: 1)

      expect(counters.snapshot).to have_attributes(messages_out: 1, bytes_out: 11)
    end

    it "buffers a publish made during an outage" do
      writer.write("PUB a 0\r\n\r\n")

      expect(writer.buffered).to eq(11)
    end

    it "drops a control frame made during an outage, since reconnect replays the registry" do
      writer.write("SUB a 1\r\n", control: true)

      expect(writer.buffered).to eq(0)
    end

    it "raises ReconnectBufferFullError once an outage has filled the buffer" do
      writer.write("x" * 1024)

      expect { writer.write("y") }.to raise_error(Tether::ReconnectBufferFullError, /1024 bytes is full/)
    end

    it "raises DisconnectedError during an outage when buffering is disabled" do
      unbuffered = build_writer(reconnect_buffer: 0)

      expect { unbuffered.write("PUB a 0\r\n\r\n") }.to raise_error(Tether::DisconnectedError)
    end

    it "raises ConnectionClosedError once closed" do
      writer.close

      expect { writer.write("PUB a 0\r\n\r\n") }.to raise_error(Tether::ConnectionClosedError)
    end

    it "reports a failed write with the transport it failed on" do
      attach
      transport.fail_with(Tether::ConnectionClosedError.new("broken pipe"))
      writer.write("PUB a 0\r\n\r\n", flush: true)

      expect(failures.pop(timeout: 2)).to match([transport, an_instance_of(Tether::ConnectionClosedError)])
    end

    it "sends each buffered frame once after a failed write and a reconnect" do
      attach
      transport.fail_with(Tether::ConnectionClosedError.new("broken pipe"))
      writer.write("lost\n", flush: true)
      writer.detach(Tether::DisconnectedError.new("connection lost"))
      writer.write("during outage\n")
      replacement = FakeTransport.new
      attach(replacement)
      writer.write("first after\n", flush: true)
      writer.write("second after\n", flush: true)

      expect(replacement.written).to eq("during outage\nfirst after\nsecond after\n")
    end
  end

  describe "#ping" do
    it "raises DisconnectedError without a live connection" do
      expect { writer.ping(Thread::Queue.new) }.to raise_error(Tether::DisconnectedError)
    end

    it "raises ConnectionClosedError once closed" do
      writer.close

      expect { writer.ping(Thread::Queue.new) }.to raise_error(Tether::ConnectionClosedError)
    end

    it "writes a PING" do
      attach
      writer.ping(Thread::Queue.new)

      expect(eventually { transport.written == "PING\r\n" }).to be(true)
    end
  end

  describe "#resolve_pong" do
    it "releases the oldest waiter first, since a PONG only proves progress up to its own PING" do
      attach
      first = Thread::Queue.new
      second = Thread::Queue.new
      writer.ping(first)
      writer.ping(second)
      writer.resolve_pong

      expect(first.pop(timeout: 2)).to be(true)
      expect(second).to be_empty
    end

    it "ignores a PONG nobody asked for" do
      expect { writer.resolve_pong }.not_to raise_error
    end
  end

  describe "#keepalive_ping" do
    it "takes its own place in line, so its PONG does not release a later flush" do
      attach
      waiter = Thread::Queue.new
      writer.keepalive_ping
      writer.ping(waiter)
      writer.resolve_pong

      expect(waiter).to be_empty
    end

    it "sends nothing during an outage" do
      writer.keepalive_ping

      expect(writer.buffered).to eq(0)
    end
  end

  describe "#pong" do
    it "appends a PONG for the flusher to write" do
      attach
      writer.pong

      expect(eventually { transport.written == "PONG\r\n" }).to be(true)
    end
  end

  describe "#kick" do
    it "tolerates being called after close" do
      writer.close

      expect { writer.kick }.not_to raise_error
    end
  end

  describe "#buffered" do
    it "returns zero once everything has been written" do
      attach
      writer.write("PUB a 0\r\n\r\n", flush: true)

      expect(writer.buffered).to eq(0)
    end
  end

  describe "#close" do
    it "releases callers waiting for a PONG with ConnectionClosedError" do
      attach
      waiter = Thread::Queue.new
      writer.ping(waiter)
      writer.close

      expect(waiter.pop(timeout: 2)).to be_a(Tether::ConnectionClosedError)
    end

    it "writes what is buffered when asked to flush" do
      attach
      writer.write("PUB a 0\r\n\r\n")
      writer.close(flush: true)

      expect(transport.written).to eq("PUB a 0\r\n\r\n")
    end

    it "ends the flusher thread" do
      before = tether_threads
      attach
      writer.close

      expect(tether_threads - before).to be_empty
    end
  end
end
