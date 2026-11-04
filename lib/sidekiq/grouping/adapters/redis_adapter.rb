# frozen_string_literal: true

require_relative "base_adapter"
require_relative "../redis_dispatcher"
require_relative "../redis_scripts"

module Sidekiq
  module Grouping
    module Adapters
      class RedisAdapter < BaseAdapter
        include RedisDispatcher

        def push_messages(name, messages, remember_unique: false)
          keys = [
            ns("batches"),
            name,
            ns(name),
            unique_messages_key(name),
            remember_unique.to_s
          ]
          argv = [messages]
          with_script(:merge_array) do |sha|
            redis_call(:evalsha, sha, keys, argv)
          end
        end

        def pluck(name, limit)
          keys = [ns(name), unique_messages_key(name)]
          args = [limit]
          with_script(:pluck) { |sha| redis_call(:evalsha, sha, keys, args) }
        end

        def reliable_pluck(name, limit)
          keys = [
            ns(name),
            unique_messages_key(name),
            pending_jobs(name),
            Time.now.to_i,
            this_job_name(name)
          ]
          argv = [limit]
          with_script(:reliable_pluck) do |sha|
            redis_call(:evalsha, sha, keys, argv)
          end
        end

        def requeue_expired(conn, unique, expired, name)
          keys = [
            expired,
            ns(name),
            pending_jobs(name),
            unique_messages_key(name)
          ]
          with_script(requeue_script_key(unique)) do |sha|
            redis_connection_call(conn, :evalsha, sha, keys, [])
          end
        end
      end
    end
  end
end
