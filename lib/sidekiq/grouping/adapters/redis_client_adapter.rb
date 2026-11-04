# frozen_string_literal: true

require_relative "base_adapter"
require_relative "../redis_dispatcher"
require_relative "../redis_scripts"

module Sidekiq
  module Grouping
    module Adapters
      class RedisClientAdapter < BaseAdapter
        include RedisDispatcher

        def push_messages(name, messages, remember_unique: false)
          with_script(:merge_array) do |sha|
            redis_call(
              :evalsha,
              sha,
              5,
              ns("batches"),
              name,
              ns(name),
              unique_messages_key(name),
              remember_unique.to_s,
              messages
            )
          end
        end

        def pluck(name, limit)
          with_script(:pluck) do |sha|
            redis_call(
              :evalsha,
              sha,
              2,
              ns(name),
              unique_messages_key(name),
              limit
            )
          end
        end

        def reliable_pluck(name, limit)
          with_script(:reliable_pluck) do |sha|
            redis_call(
              :evalsha,
              sha,
              5,
              ns(name),
              unique_messages_key(name),
              pending_jobs(name),
              Time.now.to_i,
              this_job_name(name),
              limit
            )
          end
        end

        def requeue_expired(conn, unique, expired, name)
          with_script(requeue_script_key(unique)) do |sha|
            redis_connection_call(
              conn,
              :evalsha,
              sha,
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
end
