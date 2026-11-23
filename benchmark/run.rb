# frozen_string_literal: true

require "json"
require "socket"
require "open3"

# ruby benchmark/run.rb [ROUNDS] [SCENARIO_FILTER]
#
# Starts nats:alpine in Docker (or uses NATS_URL), runs every scenario in a fresh process for each library,
# alternating which goes first, and writes the median of the rounds to benchmark/results.json.
ROOT = File.expand_path("..", __dir__)
rounds = (ARGV[0] || 3).to_i
filter = ARGV[1] && Regexp.new(ARGV[1])
libs = %w[tether nats-pure]

def free_port = TCPServer.open("127.0.0.1", 0) { |s| s.addr[1] }

url = ENV.fetch("NATS_URL", nil)
container = nil
unless url
  port = free_port
  container = `docker run -d -p 127.0.0.1:#{port}:4222 nats:alpine`.strip
  url = "nats://127.0.0.1:#{port}"
  begin
    TCPSocket.new("127.0.0.1", port).close
  rescue SystemCallError
    sleep 0.1
    retry
  end
  sleep 1
end

scenarios = `ruby #{ROOT}/benchmark/worker.rb list`.split
scenarios.select! { |s| s.match?(filter) } if filter
results = Hash.new { |h, k| h[k] = Hash.new { |h2, k2| h2[k2] = [] } }

begin
  scenarios.each do |scenario|
    rounds.times do |round|
      (round.odd? ? libs.reverse : libs).each do |lib|
        out, status = Open3.capture2e("ruby", "-I#{ROOT}/lib", "#{ROOT}/benchmark/worker.rb", lib, scenario, url)
        line = out.lines.reverse.find { |l| l.start_with?("{") }
        unless status.success? && line
          warn "#{lib} #{scenario}: FAILED\n#{out}"
          next
        end
        results[scenario][lib] << JSON.parse(line)
        warn "#{scenario} #{lib} round #{round + 1}: #{line.strip}"
      end
    end
  end
ensure
  system("docker", "rm", "-f", container, out: File::NULL) if container
end

median = lambda do |runs, key|
  values = runs.map { |r| r[key] }.compact.sort
  values.empty? ? nil : values[values.size / 2]
end
summary = results.to_h do |scenario, by_lib|
  [scenario, by_lib.to_h do |lib, runs|
    keys = runs.first.keys - %w[lib scenario]
    [lib, keys.to_h { |k| [k, median.call(runs, k)] }.merge("runs" => runs.size)]
  end]
end
File.write("#{ROOT}/benchmark/results.json", JSON.pretty_generate(summary))
puts JSON.pretty_generate(summary)
