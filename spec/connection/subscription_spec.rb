# frozen_string_literal: true

RSpec.describe Tether::Connection::Subscription do
  include ClientHelper

  let(:nc) { connect }
  let(:other) { connect }

  def publish(count, subject = topic)
    count.times { |index| other.publish(subject, index.to_s) }
    other.flush
    nc.flush
  end

  def blocked_subscription(**options)
    entered = Thread::Queue.new
    subscription = nc.subscribe(topic, **options) do |msg|
      entered.push(msg)
      gate.pop
    end
    nc.flush
    [subscription, entered]
  end

  describe "#next" do
    it "returns the messages in the order they arrived" do
      subscription = nc.subscribe(topic)
      publish(3)

      expect(Array.new(3) { subscription.next(timeout: 2).data }).to eq(%w[0 1 2])
    end

    it "waits for a message that has not arrived yet" do
      subscription = nc.subscribe(topic)
      nc.flush
      waiting = Thread.new { subscription.next(timeout: 5).data }
      eventually { waiting.status == "sleep" }
      other.publish(topic, "late")

      expect(waiting.value).to eq("late")
    end

    it "raises TimeoutError when nothing arrives in time" do
      subscription = nc.subscribe(topic)

      expect { subscription.next(timeout: 0.05) }.to raise_error(Tether::TimeoutError, /no message on #{topic}/)
    end

    it "accepts a Deadline as the timeout" do
      subscription = nc.subscribe(topic)

      expect { subscription.next(timeout: Tether::Deadline.after(0.05)) }.to raise_error(Tether::TimeoutError)
    end

    it "raises SubscriptionClosedError after unsubscribe" do
      subscription = nc.subscribe(topic).unsubscribe

      expect { subscription.next(timeout: 1) }.to raise_error(Tether::SubscriptionClosedError, /#{topic}/)
    end

    it "returns what is left of an auto-unsubscribed subscription, then raises SubscriptionClosedError" do
      subscription = nc.subscribe(topic, max: 2)
      publish(4)

      expect(Array.new(2) { subscription.next(timeout: 2).data }).to eq(%w[0 1])
      expect { subscription.next(timeout: 1) }.to raise_error(Tether::SubscriptionClosedError)
    end

    it "raises the PermissionsViolationError the server answered the SUB with" do
      limited = connect(SharedNatsServer.users.uri, user: "limited", password: "wonderland")
      subscription = limited.subscribe("forbidden.subject")
      eventually { limited.flush.last_error }

      expect { subscription.next(timeout: 0.05) }.to raise_error(Tether::PermissionsViolationError)
    end

    it "serves several threads taking from one subscription without losing or repeating a message" do
      subscription = nc.subscribe(topic)
      publish(100)
      taken = 4.times.map { Thread.new { Array.new(25) { subscription.next(timeout: 5).data } } }.flat_map(&:value)

      expect(taken.map(&:to_i).sort).to eq((0...100).to_a)
    end
  end

  describe "#each" do
    context "when a block is given" do
      it "yields each message until nothing has arrived for idle_timeout" do
        subscription = nc.subscribe(topic)
        publish(3)
        yielded = []
        result = subscription.each(idle_timeout: 0.1) { |msg| yielded << msg.data }

        expect(yielded).to eq(%w[0 1 2])
        expect(result).to be(subscription)
      end

      it "returns when the subscription ends" do
        subscription = nc.subscribe(topic, max: 2)
        publish(2)
        yielded = +""
        subscription.each { |msg| yielded << msg.data }

        expect(yielded).to eq("01")
      end

      it "returns when another thread unsubscribes" do
        subscription = nc.subscribe(topic)
        iterating = Thread.new { subscription.each { nil } }
        eventually { iterating.status == "sleep" }
        subscription.unsubscribe

        expect(iterating.join(2)).to be(iterating)
      end
    end

    context "when no block is given" do
      it "returns an enumerator over the messages" do
        subscription = nc.subscribe(topic)
        publish(3)

        expect(subscription.each.first(2).map(&:data)).to eq(%w[0 1])
      end

      it "passes idle_timeout on to the enumerator" do
        subscription = nc.subscribe(topic)
        publish(2)

        expect(subscription.each(idle_timeout: 0.1).map(&:data)).to eq(%w[0 1])
      end
    end
  end

  describe "#unsubscribe" do
    it "stops delivery" do
      received = Thread::Queue.new
      subscription = nc.subscribe(topic) { |msg| received.push(msg) }
      nc.flush
      subscription.unsubscribe
      publish(1)

      expect(received.pop(timeout: 0.05)).to be_nil
    end

    it "tells the server, so no more messages are sent" do
      subscription = nc.subscribe(topic)
      before = nc.flush.stats.messages_in
      subscription.unsubscribe
      publish(3)

      expect(nc.stats.messages_in).to eq(before)
    end

    it "returns the subscription, closed" do
      subscription = nc.subscribe(topic)

      expect(subscription.unsubscribe).to be(subscription).and be_closed
    end

    it "discards messages still queued" do
      subscription = nc.subscribe(topic)
      publish(3)
      subscription.unsubscribe

      expect { subscription.next(timeout: 0.05) }.to raise_error(Tether::SubscriptionClosedError)
    end

    it "tolerates being called twice" do
      subscription = nc.subscribe(topic).unsubscribe

      expect { subscription.unsubscribe }.not_to raise_error
    end

    it "ends the subscription after the given total with after:" do
      subscription = nc.subscribe(topic)
      nc.flush
      subscription.unsubscribe(after: 3)
      publish(5)

      expect(Array.new(3) { subscription.next(timeout: 2).data }).to eq(%w[0 1 2])
      expect(subscription).to be_closed
    end

    it "counts messages already received toward after:" do
      subscription = nc.subscribe(topic)
      publish(2)
      subscription.unsubscribe(after: 3)
      publish(5)

      expect(subscription.each.map(&:data)).to eq(%w[0 1 0])
    end

    it "ends the subscription at once when after: has already been reached" do
      subscription = nc.subscribe(topic)
      publish(3)

      expect(subscription.unsubscribe(after: 2)).to be_closed
    end

    it "keeps an unsubscribed subscription from being restored by a reconnect" do
      client = connect(proxy.uri, reconnect: ClientHelper::FAST_RECONNECT)
      subscription = client.subscribe(topic)
      proxy.stop
      eventually { client.reconnecting? }
      subscription.unsubscribe
      proxy.start
      eventually { client.connected? }
      before = client.flush.stats.messages_in
      other.publish(topic, "x").flush

      expect(client.flush.stats.messages_in).to eq(before)
    end

    it "stops delivery when called from the subscription's own block" do
      received = Thread::Queue.new
      nc.subscribe(topic) do |msg|
        received.push(msg.data)
        msg.subscription.unsubscribe
      end
      nc.flush
      publish(3)

      expect(received.pop(timeout: 2)).to eq("0")
      expect(received.pop(timeout: 0.05)).to be_nil
    end
  end

  describe "#drain" do
    it "lets the block handle everything already received, then closes" do
      handled = Thread::Queue.new
      subscription = nc.subscribe(topic) do |msg|
        gate.pop(timeout: 0.005)
        handled.push(msg.data)
      end
      nc.flush
      publish(20)
      subscription.drain

      expect(handled.size).to eq(20)
      expect(subscription).to be_closed
    end

    it "returns the subscription and leaves the client connected" do
      subscription = nc.subscribe(topic) { nil }

      expect(subscription.drain).to be(subscription)
      expect(nc).to be_connected
    end

    it "stops delivery of messages published after it started" do
      received = Thread::Queue.new
      nc.subscribe(topic) { |msg| received.push(msg) }.drain
      publish(1)

      expect(received.pop(timeout: 0.05)).to be_nil
    end

    it "keeps the queued messages of a subscription without a block available to next" do
      subscription = nc.subscribe(topic)
      publish(2)
      subscription.drain

      expect(subscription.each.map(&:data)).to eq(%w[0 1])
    end

    it "raises TimeoutError when the block does not finish in time, and closes anyway" do
      subscription, entered = blocked_subscription
      publish(2)
      entered.pop(timeout: 2)

      expect { subscription.drain(timeout: 0.05) }.to raise_error(Tether::TimeoutError, /did not drain/)
      expect(subscription).to be_closed
    end

    it "returns at once from the subscription's own block and ends after the queue is empty" do
      handled = Thread::Queue.new
      subscription = nc.subscribe(topic) do |msg|
        msg.subscription.drain if msg.data == "0"
        handled.push(msg.data)
      end
      nc.flush
      publish(3)

      expect(Array.new(3) { handled.pop(timeout: 2) }).to eq(%w[0 1 2])
      expect(eventually { subscription.closed? }).to be(true)
    end

    it "does nothing on a closed subscription" do
      subscription = nc.subscribe(topic).unsubscribe

      expect(subscription.drain).to be(subscription)
    end
  end

  describe "#on_close" do
    it "runs the block once the subscription has ended" do
      closed = Thread::Queue.new
      subscription = nc.subscribe(topic) { nil }
      subscription.on_close { closed.push(true) }
      subscription.unsubscribe

      expect(closed.pop(timeout: 2)).to be(true)
      expect(closed.pop(timeout: 0.05)).to be_nil
    end

    it "runs the block after the last message was handled" do
      order = Thread::Queue.new
      subscription = nc.subscribe(topic, max: 2) { |msg| order.push(msg.data) }
      subscription.on_close { order.push(:closed) }
      nc.flush
      publish(2)

      expect(Array.new(3) { order.pop(timeout: 2) }).to eq(["0", "1", :closed])
    end

    it "runs the block when the client closes" do
      closed = Thread::Queue.new
      nc.subscribe(topic) { nil }.on_close { closed.push(true) }
      nc.close

      expect(closed.pop(timeout: 2)).to be(true)
    end

    it "runs the block at once when the subscription has already ended" do
      closed = Thread::Queue.new
      nc.subscribe(topic).unsubscribe.on_close { closed.push(Thread.current) }

      expect(closed.pop(timeout: 2)).to be(Thread.current)
    end

    it "returns the subscription" do
      subscription = nc.subscribe(topic)

      expect(subscription.on_close { nil }).to be(subscription)
    end

    it "reports a HandlerError when the block raises" do
      subscription = nc.subscribe(topic)
      subscription.on_close { raise "close hook bug" }
      subscription.unsubscribe

      expect(nc.last_error).to be_a(Tether::HandlerError).and have_attributes(message: /close hook bug/)
    end
  end

  describe "#pending" do
    it "counts the messages waiting to be taken" do
      subscription = nc.subscribe(topic)
      publish(3)
      subscription.next(timeout: 2)

      expect(subscription.pending).to eq(2)
    end

    it "excludes the message a block is handling" do
      subscription, entered = blocked_subscription
      publish(3)
      entered.pop(timeout: 2)

      expect(subscription.pending).to eq(2)
    end
  end

  describe "#pending_bytes" do
    it "counts the bytes of headers and payload waiting to be taken" do
      subscription = nc.subscribe(topic)
      nc.publish(topic, "12345").publish(topic, "678", headers: { "A" => "b" }).flush

      expect(subscription.pending_bytes).to eq(8 + "NATS/1.0\r\nA: b\r\n\r\n".bytesize)
    end

    it "returns zero once everything was taken" do
      subscription = nc.subscribe(topic)
      publish(1)
      subscription.next(timeout: 2)

      expect(subscription.pending_bytes).to eq(0)
    end
  end

  describe "#delivered" do
    it "counts the messages returned by next" do
      subscription = nc.subscribe(topic)
      publish(3)
      2.times { subscription.next(timeout: 2) }

      expect(subscription.delivered).to eq(2)
    end

    it "counts the messages handed to the block" do
      received = Thread::Queue.new
      subscription = nc.subscribe(topic) { |msg| received.push(msg) }
      nc.flush
      publish(3)
      3.times { received.pop(timeout: 2) }

      expect(subscription.delivered).to eq(3)
    end
  end

  describe "#dropped" do
    it "returns zero while the subscription keeps up" do
      subscription = nc.subscribe(topic)
      publish(3)

      expect(subscription.dropped).to eq(0)
    end

    it "counts the messages that arrived while the queue was full" do
      subscription = nc.subscribe(topic, max_pending: 2)
      publish(5)

      expect(subscription.dropped).to eq(3)
      expect(subscription.pending).to eq(2)
    end

    it "keeps the oldest messages and drops the newest" do
      subscription = nc.subscribe(topic, max_pending: 2)
      publish(5)

      expect(subscription.each(idle_timeout: 0.05).map(&:data)).to eq(%w[0 1])
    end

    it "accepts messages again once the queue has room" do
      subscription = nc.subscribe(topic, max_pending: 2)
      publish(5)
      2.times { subscription.next(timeout: 2) }
      publish(1)

      expect(subscription.next(timeout: 2).data).to eq("0")
      expect(subscription.dropped).to eq(3)
    end

    it "reports a new SlowConsumerError for a new episode after the queue recovered" do
      errors = record(nc, :error)
      subscription = nc.subscribe(topic, max_pending: 2)
      publish(3)
      2.times { subscription.next(timeout: 2) }
      publish(3)

      expect(take(errors, 2).map(&:last)).to all(be_a(Tether::SlowConsumerError))
    end
  end

  describe "#closed?" do
    it "returns false for a live subscription" do
      expect(nc.subscribe(topic)).not_to be_closed
    end

    it "returns true after unsubscribe" do
      expect(nc.subscribe(topic).unsubscribe).to be_closed
    end

    it "returns true once the client is closed" do
      subscription = nc.subscribe(topic)
      nc.close

      expect(subscription).to be_closed
    end

    it "returns true once max messages have been received" do
      subscription = nc.subscribe(topic, max: 1)
      publish(1)

      expect(subscription).to be_closed
    end
  end

  describe "#draining?" do
    it "returns true while a drain waits for the block" do
      subscription, entered = blocked_subscription
      publish(1)
      entered.pop(timeout: 2)
      draining = Thread.new { subscription.drain }

      expect(eventually { subscription.draining? }).to be(true)
      gate.close
      draining.join
    end

    it "returns false for a live subscription" do
      expect(nc.subscribe(topic)).not_to be_draining
    end
  end

  describe "#sid" do
    it "returns a different token for each subscription of a connection" do
      expect(nc.subscribe(topic).sid).not_to eq(nc.subscribe(topic).sid)
    end
  end

  describe "#inspect" do
    it "shows the subject and state without the queued messages" do
      subscription = nc.subscribe(topic)

      expect(subscription.inspect)
        .to eq(%(#<Tether::Subscription subject="#{topic}" sid=#{subscription.sid} state=active>))
    end
  end
end
