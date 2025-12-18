# frozen_string_literal: true

class RecordingSink
  attr_reader :events

  def initialize
    @events = []
  end

  def on_msg(subject, sid, reply_to, headers, payload)
    @events << [:msg, subject, sid, reply_to, headers, payload]
  end

  def on_ping
    @events << [:ping]
  end

  def on_pong
    @events << [:pong]
  end

  def on_ok
    @events << [:ok]
  end

  def on_info(json)
    @events << [:info, json]
  end

  def on_error(description)
    @events << [:error, description]
  end

  def on_protocol_error(message)
    @events << [:protocol_error, message]
  end
end
