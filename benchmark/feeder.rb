# frozen_string_literal: true

require "socket"
require "uri"

# Raw-socket publisher: ruby feeder.rb URL COUNT SIZE SUBJECT...
# Prints "ready", waits for a line on stdin, then writes COUNT messages round-robin over the subjects.
url, count, size, *subjects = ARGV
uri = URI(url)
socket = TCPSocket.new(uri.host, uri.port)
socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
socket.gets
socket.write(%(CONNECT {"verbose":false,"pedantic":false}\r\nPING\r\n))
socket.gets
payload = "x" * size.to_i
frames = subjects.map { |subject| "PUB #{subject} #{payload.bytesize}\r\n#{payload}\r\n" }
batch = Array.new(1000) { |i| frames[i % frames.size] }.join
rounds, rest = count.to_i.divmod(1000)
puts "ready"
$stdout.flush
$stdin.gets
rounds.times { socket.write(batch) }
socket.write(Array.new(rest) { |i| frames[i % frames.size] }.join) if rest.positive?
socket.write("PING\r\n")
socket.gets
socket.close
