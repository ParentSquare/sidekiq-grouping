# frozen_string_literal: true

require_relative "./base_adapter"
require_relative "../redis_dispatcher"
require_relative "../redis_scripts"

module Sidekiq
  module Grouping
    module Adapters
      class RedisClientAdapter < BaseAdapter
        include RedisDispatcher

        def push_messages(name, messages, remember_unique: false)
          redis_call(
            :evalsha,
            RedisScripts::HASHES[:merge_array],
            5,
            ns("batches"),
            name,
            ns(name),
            unique_messages_key(name),
            remember_unique.to_s,
            messages
          )
        end

        def pluck(name, limit)
          redis_call(
            :evalsha,
            RedisScripts::HASHES[:pluck],
            2,
            ns(name),
            unique_messages_key(name),
            limit
          )
        end

        def reliable_pluck(name, limit)
          redis_call(
            :evalsha,
            RedisScripts::HASHES[:reliable_pluck],
            5,
            ns(name),
            unique_messages_key(name),
            pending_jobs(name),
            Time.now.to_i,
            this_job_name(name),
            limit
          )
        end

        def requeue_expired(conn, unique, expired, name)
          redis_connection_call(
            conn,
            :evalsha,
            requeue_script(unique),
            4,
            expired,
            ns(name),
            pending_jobs(name),
            unique_messages_key(name)
          )
        end
      end
    end
  end
end
