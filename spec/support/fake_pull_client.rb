# frozen_string_literal: true

require "json"

# Stands in for a client under a pull loop: records the pull requests it
# publishes and lets a spec play the reader thread, the timer and the
# connection.
class FakePullClient
  Entry = Struct.new(:subject, :block, :closed) do
    def unsubscribe = self.closed = true
    def cancel = self.closed = true
  end

  Published = Struct.new(:subject, :payload, :reply_to, :headers) do
    def request = JSON.parse(payload)
  end

  attr_reader :published, :errors, :handler_errors
  attr_accessor :connected, :response

  def initialize
    @published = []
    @errors = []
    @handler_errors = []
    @resolvers = []
    @listeners = []
    @timers = []
    @connected = true
    @inboxes = 0
  end

  def options = Tether::Options.new
  def generation = Process.pid
  def connected? = @connected
  def timer = self
  def new_inbox = "_INBOX.fake.#{@inboxes += 1}"

  def publish(subject, payload = nil, reply_to: nil, headers: nil, flush: false) # rubocop:disable Lint/UnusedMethodArgument
    @published << Published.new(subject, payload, reply_to, headers)
    @on_pull&.call if subject.include?(".CONSUMER.MSG.NEXT.")
    self
  end

  def request(subject, _payload = nil, headers: nil, timeout: nil) # rubocop:disable Lint/UnusedMethodArgument
    raise Tether::TimeoutError, "no reply to #{subject}" unless @response

    Tether::Protocol::Msg.new(subject: "_INBOX.reply", data: JSON.generate(@response))
  end

  # Runs the block right after each pull request is published, as the server answering it would.
  def on_pull(&block)
    @on_pull = block
  end

  def resolver(subject, &block)
    Entry.new(subject, block, false).tap { |entry| @resolvers << entry }
  end

  def listen(&block)
    Entry.new(nil, block, false).tap { |entry| @listeners << entry }
  end

  def every(_seconds, &block)
    Entry.new(nil, block, false).tap { |entry| @timers << entry }
  end

  def report_error(error) = @errors << error
  def report_handler_error(error, _msg = nil) = @handler_errors << error

  def pulls
    @published.select { |frame| frame.subject.include?(".CONSUMER.MSG.NEXT.") }.map(&:request)
  end

  def inbox
    @resolvers.reject(&:closed).last&.subject
  end

  def subscribed?
    @resolvers.any? { |entry| !entry.closed }
  end

  def message(payload = "data", sequence: 1, headers: nil, consumer: "worker", pending: 0)
    reply = "$JS.ACK._.hash.ORDERS.#{consumer}.1.#{sequence}.#{sequence}.1763800000000000000.#{pending}"
    block = headers && "NATS/1.0\r\n#{headers.map { |key, value| "#{key}: #{value}\r\n" }.join}\r\n"
    deliver(Tether::Protocol::Msg.new(subject: "ORDERS.new", data: payload, reply_to: reply, header_bytes: block))
  end

  def status(line, headers = {})
    block = "NATS/1.0 #{line}\r\n#{headers.map { |key, value| "#{key}: #{value}\r\n" }.join}\r\n"
    deliver(Tether::Protocol::Msg.new(subject: inbox, data: "", header_bytes: block))
  end

  def transition(event)
    @listeners.reject(&:closed).each { |entry| entry.block.call(event, nil) }
  end

  def tick
    @timers.reject(&:closed).each { |entry| entry.block.call }
  end

  private

  def deliver(msg)
    @resolvers.reject(&:closed).last.block.call(msg)
  end
end
