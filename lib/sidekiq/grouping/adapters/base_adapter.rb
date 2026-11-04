# frozen_string_literal: true

require_relative "../redis_dispatcher"
require_relative "../redis_scripts"

module Sidekiq
  module Grouping
    module Adapters
      class BaseAdapter
        private

        # The server drops its script cache on restart, failover and
        # SCRIPT FLUSH, but RedisScripts keeps SHAs for the life of the
        # process, so reload the script once on NOSCRIPT.
        def with_script(key)
          yield RedisScripts.script_hash(key)
        rescue StandardError => e
          raise unless e.message.start_with?("NOSCRIPT")

          yield RedisScripts.reload_script(key)
        end

        def requeue_script_key(unique)
          unique ? :unique_requeue : :requeue
        end

        def unique_messages_key(name)
          ns("#{name}:unique_messages")
        end

        def pending_jobs(name)
          ns("#{name}:pending_jobs")
        end

        def this_job_name(name)
          ns("#{name}:#{SecureRandom.hex}")
        end

        def ns(key = nil)
          "batching:#{key}"
        end
      end
    end
  end
end
