# frozen_string_literal: true

module Tether
  class JetStream
    # A handle to a stream on the server, carrying the last info read for it.
    class Stream
      # @return [String] the stream's name
      attr_reader :name

      # @return [JetStream] the context this handle belongs to
      attr_reader :jetstream

      # @return [StreamInfo] the info last read from the server
      attr_reader :cached_info

      # @param jetstream [JetStream] the context
      # @param info [StreamInfo] what the server returned for the stream
      def initialize(jetstream, info)
        @jetstream = jetstream
        @api = jetstream.api
        @cached_info = info
        @name = info.config.name
      end

      # Reads the stream's info from the server.
      #
      # @param deleted_details [Boolean] include the sequences of deleted messages
      # @param subjects [String, nil] include per-subject message counts for subjects matching this filter
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @return [StreamInfo] fresh info, also kept as {#cached_info} without the per-subject counts
      def info(deleted_details: false, subjects: nil, timeout: nil)
        deadline = @api.deadline(timeout)
        request = {}
        request["deleted_details"] = true if deleted_details
        request["subjects_filter"] = subjects if subjects
        response = @api.request("STREAM.INFO.#{@name}", request.empty? ? nil : request, timeout: deadline)
        return @cached_info = StreamInfo.from_h(response) unless subjects

        response = merge_subject_pages(response, request, deadline)
        @cached_info = StreamInfo.from_h(response.merge("state" => response["state"].except("subjects")))
        StreamInfo.from_h(response)
      end

      # @return [StreamConfig] the configuration from the cached info
      def config
        @cached_info.config
      end

      # @return [StreamState] the state from the cached info
      def state
        @cached_info.state
      end

      # Changes the stream's configuration.
      #
      # @param timeout [Numeric, Deadline, nil] how long reading the configuration and writing it back may take
      # @param changes [Hash{Symbol => Object}] fields to replace in the current configuration
      # @return [self] this stream, with fresh cached info
      # @raise [StreamNotFoundError] if the stream is gone
      def update(timeout: nil, **changes)
        deadline = @api.deadline(timeout)
        @cached_info = @jetstream.update_stream(info(timeout: deadline).config.with(**changes),
                                                timeout: deadline).cached_info
        self
      end

      # Removes messages.
      #
      # @param subject [String, nil] only messages on subjects matching this filter
      # @param sequence [Integer, nil] only messages below this sequence
      # @param keep [Integer, nil] keep this many of the newest messages
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @return [Integer] how many messages were removed
      # @raise [ArgumentError] if both sequence: and keep: are given
      def purge(subject: nil, sequence: nil, keep: nil, timeout: nil)
        raise ArgumentError, "sequence: and keep: cannot be combined" if sequence && keep

        request = { "filter" => subject, "seq" => sequence, "keep" => keep }.compact
        @api.request("STREAM.PURGE.#{@name}", request.empty? ? nil : request, timeout: timeout).fetch("purged", 0)
      end

      # Reads one message by its sequence, or the last or next one on a subject.
      #
      # @param sequence [Integer, nil] the message's sequence
      # @param last_for [String, nil] the newest message on this subject
      # @param next_for [String, nil] the first message on a subject matching this filter
      # @param from [Integer, nil] with next_for:, the sequence to search from
      # @param direct [Boolean, nil] force Direct Get, which any replica answers and may be stale, or the JSON
      #   API, which the leader answers; nil picks Direct Get when the cached configuration allows it
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @return [StoredMessage] the message
      # @raise [ArgumentError] unless exactly one of a sequence, last_for: and next_for: is given
      # @raise [MessageNotFoundError] if no message matches
      def get(sequence = nil, last_for: nil, next_for: nil, from: nil, direct: nil, timeout: nil)
        request = { "seq" => sequence, "last_by_subj" => last_for, "next_by_subj" => next_for }.compact
        raise ArgumentError, "give exactly one of a sequence, last_for: or next_for:" unless request.size == 1
        raise ArgumentError, "from: is only used with next_for:" if from && !next_for

        request["seq"] = from if from
        deadline = @api.deadline(timeout)
        return direct_get.get(request, deadline) if direct.nil? ? config.allow_direct : direct

        StoredMessage.from_api(@api.request("STREAM.MSG.GET.#{@name}", request, timeout: deadline).fetch("message"))
      end

      # Reads the stream's messages in order with batched Direct Gets, a page at a time. The stream must
      # allow direct access.
      #
      # @param from [Integer] the sequence to start at
      # @param subject [String, nil] only messages on subjects matching this filter
      # @param start_time [Time, nil] start at the first message stored at or after this time, instead of from:
      # @param batch [Integer] messages to ask for in one page
      # @param max_bytes [Integer, nil] bytes after which the server ends a page early
      # @param timeout [Numeric, Deadline, nil] seconds each page may take, or a deadline for the whole iteration
      # @yieldparam message [StoredMessage] each message, on the calling thread
      # @return [self, Enumerator<StoredMessage>] this stream, or an enumerator when no block is given
      # @raise [ArgumentError] if start_time: is combined with from:
      # @raise [ServiceUnavailableError] if the stream does not allow direct access
      def each_message(from: 1, subject: nil, start_time: nil, batch: 100, max_bytes: nil, timeout: nil, &block)
        raise ArgumentError, "start_time: and from: cannot be combined" if start_time && from != 1

        unless block
          return enum_for(:each_message, from: from, subject: subject, start_time: start_time, batch: batch,
                                         max_bytes: max_bytes, timeout: timeout)
        end

        request = { "batch" => batch, "next_by_subj" => subject, "max_bytes" => max_bytes }.compact
        request.merge!(start_time ? { "start_time" => start_time.getutc.iso8601(9) } : { "seq" => from })
        direct_get.each(request, timeout, &block)
        self
      end

      # Reads the last message of every subject matching any of the filters. The stream must allow direct
      # access.
      #
      # @param subjects [Array<String>, String] subjects or wildcard filters
      # @param up_to_sequence [Integer, nil] only consider messages at or below this sequence
      # @param up_to_time [Time, nil] only consider messages stored before this time
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @return [Array<StoredMessage>] the messages, ordered by sequence; empty when nothing matches
      # @raise [ArgumentError] if both up_to_sequence: and up_to_time: are given
      # @raise [TooManyResultsError] if the filters match more subjects than the server returns
      # @raise [ServiceUnavailableError] if the stream does not allow direct access
      def last_messages(subjects, up_to_sequence: nil, up_to_time: nil, timeout: nil)
        raise ArgumentError, "up_to_sequence: and up_to_time: cannot be combined" if up_to_sequence && up_to_time

        request = { "multi_last" => Array(subjects), "up_to_seq" => up_to_sequence,
                    "up_to_time" => up_to_time&.getutc&.iso8601(9) }.compact
        messages = []
        direct_get.each(request, @api.deadline(timeout)) { |message| messages << message }
        messages
      end

      # Counts the messages on subjects matching a filter without reading them: the server answers a Direct Get of
      # one message with how many more match. The stream must allow direct access.
      #
      # @param subject [String, nil] only messages on subjects matching this filter; nil counts every message
      # @param from [Integer] count only the messages at or after this sequence
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @return [Integer] how many messages match
      # @raise [ServiceUnavailableError] if the stream does not allow direct access
      def count(subject: nil, from: 1, timeout: nil)
        direct_get.count({ "seq" => from, "next_by_subj" => subject || ">" }, @api.deadline(timeout))
      end

      # Removes one message.
      #
      # @param sequence [Integer] the message's sequence
      # @param erase [Boolean] overwrite the message's bytes on disk; false only marks it deleted, which is faster
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @return [Boolean] true once the server confirmed the removal
      # @raise [MessageNotFoundError] if the stream holds no message with this sequence
      def delete_message(sequence, erase: true, timeout: nil)
        request = erase ? { "seq" => sequence } : { "seq" => sequence, "no_erase" => true }
        confirm(@api.request("STREAM.MSG.DELETE.#{@name}", request, timeout: timeout), "delete message #{sequence} of")
      end

      # Asks the stream's leader to give up leadership. Needs a clustered server.
      #
      # @param placement [Placement, Hash, nil] where the next leader should be: `cluster:`, `tags:` or `preferred:`
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @return [Boolean] true once the server confirmed
      # @raise [APIError] if the server is not clustered or no other peer can lead
      def step_down(placement: nil, timeout: nil)
        request = placement && { "placement" => Placement.coerce(placement).to_h }
        confirm(@api.request("STREAM.LEADER.STEPDOWN.#{@name}", request, timeout: timeout), "step down the leader of")
      end

      # Removes one replica from the stream's peers. Needs a clustered server.
      #
      # @param name [String] the server's name or peer id
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @return [Boolean] true once the server confirmed
      # @raise [APIError] if the server is not clustered or the peer is not a member
      def remove_peer(name, timeout: nil)
        confirm(@api.request("STREAM.PEER.REMOVE.#{@name}", { "peer" => name.to_s }, timeout: timeout),
                "remove peer #{name} from")
      end

      # Downloads a backup of the stream, yielding it in chunks as the server sends them.
      #
      # @param no_consumers [Boolean] leave the state of the stream's consumers out
      # @param chunk_size [Integer, nil] bytes per chunk; the server defaults to 128 KiB
      # @param timeout [Numeric, Deadline, nil] seconds to wait for each chunk, or a deadline for the whole snapshot
      # @yieldparam chunk [String] the next bytes of the snapshot
      # @return [Array(StreamConfig, StreamState)] the configuration and state {JetStream#restore_stream} needs
      # @raise [ArgumentError] if no block is given
      # @raise [Error] if the server ends the snapshot with a failure
      def snapshot(no_consumers: false, chunk_size: nil, timeout: nil, &block)
        raise ArgumentError, "snapshot needs a block to give the chunks to" unless block

        options = { "no_consumers" => no_consumers || nil, "chunk_size" => chunk_size }.compact
        Snapshot.new(@jetstream, @name).run(options, timeout, &block)
      end

      # Deletes the stream and everything in it.
      #
      # @param timeout [Numeric, Deadline, nil] how long to wait
      # @return [Boolean] whether the server confirmed the deletion
      def delete(timeout: nil)
        @jetstream.delete_stream(@name, timeout: timeout)
      end

      # @return [String] the name without the cached info
      def inspect
        "#<Tether::JetStream::Stream name=#{@name.inspect}>"
      end

      private

      def direct_get
        DirectGet.new(@jetstream, @name)
      end

      def confirm(response, action)
        return true if response["success"] == true

        raise Error, "the server did not #{action} #{@name}"
      end

      def merge_subject_pages(response, request, deadline)
        subjects = response.dig("state", "subjects") || {}
        while subjects.size < response["total"].to_i
          page = @api.request("STREAM.INFO.#{@name}", request.merge("offset" => subjects.size), timeout: deadline)
          more = page.dig("state", "subjects") || {}
          break if more.empty?

          subjects = subjects.merge(more)
        end
        response.merge("state" => response["state"].merge("subjects" => subjects))
      end
    end
  end
end
