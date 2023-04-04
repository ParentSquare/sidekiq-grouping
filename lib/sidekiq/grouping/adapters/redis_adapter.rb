# frozen_string_literal: true

require_relative "./base_adapter"
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
          redis_call(:evalsha, RedisScripts::HASHES[:merge_array], keys, argv)
        end

        def pluck(name, limit)
          keys = [ns(name), unique_messages_key(name)]
          args = [limit]
          redis_call(:evalsha, RedisScripts::HASHES[:pluck], keys, args)
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
          redis_call(
            :evalsha,
            RedisScripts::HASHES[:reliable_pluck],
            keys,
            argv
          )
        end

        def requeue_expired(conn, unique, expired, name)
          keys = [
            expired,
            ns(name),
            pending_jobs(name),
            unique_messages_key(name)
          ]
          redis_connection_call(
            conn, :evalsha, requeue_script(unique), keys, []
          )
        end
      end
    end
  end
end
