# frozen_string_literal: true

module Tether
  module Connection
    # The threads that run subscription blocks.
    #
    # Threads start on demand, up to a limit, and end after sitting idle. A
    # subscription is scheduled as one job, not one job per message: a worker
    # takes it, handles a batch of its messages and puts it back at the end of
    # the line when more are waiting, so one busy subscription cannot keep the
    # others waiting for long.
    class WorkerPool
      # Seconds a worker waits for a job before it ends.
      IDLE_TIMEOUT = 30

      # @param max_threads [Integer] the most workers that may run at once
      # @param name [String] the thread name; it starts with "tether:" so {Client#drain} knows its own threads
      # @param idle_timeout [Numeric] seconds a worker waits for a job before it ends
      def initialize(max_threads:, name: "tether:worker", idle_timeout: IDLE_TIMEOUT)
        @max_threads = max_threads
        @name = name
        @idle_timeout = idle_timeout
        @jobs = Thread::Queue.new
        @lock = Mutex.new
        @threads = 0
        @idle = 0
      end

      # Queues a job and starts a worker when none is free. Never blocks, so
      # the reader thread may call it.
      #
      # @param job [#run] called on a worker; returns true to be queued again
      # @return [Boolean] false when the pool is closed
      def schedule(job)
        @jobs.push(job)
        @lock.synchronize { start_worker if @idle.zero? && @threads < @max_threads }
        true
      rescue ClosedQueueError
        false
      end

      # @return [Integer] workers currently running
      def threads = @lock.synchronize { @threads }

      # Stops taking jobs. Jobs already queued still run, then the workers end.
      #
      # @return [void] nothing
      def close
        @jobs.close
        nil
      end

      private

      def start_worker
        Thread.new { work }
        @threads += 1
      rescue ThreadError
        nil
      end

      def work
        Thread.current.name = @name
        while (job = take)
          requeue(job) if job.run
        end
      end

      def take
        loop do
          @lock.synchronize { @idle += 1 }
          job = @jobs.pop(timeout: @idle_timeout)
          retired = @lock.synchronize do
            @idle -= 1
            next false if job || !@jobs.empty?

            @threads -= 1
            true
          end
          return job if job
          return if retired
        end
      end

      def requeue(job)
        @jobs.push(job)
      rescue ClosedQueueError
        loop { break unless job.run }
      end
    end
  end
end
