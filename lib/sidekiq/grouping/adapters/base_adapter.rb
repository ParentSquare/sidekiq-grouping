# frozen_string_literal: true

require_relative "../redis_dispatcher"
require_relative "../redis_scripts"

module Sidekiq
  module Grouping
    module Adapters
      class BaseAdapter
        private

        def requeue_script(unique)
          if unique
            RedisScripts.script_hash(:unique_requeue)
          else
            RedisScripts.script_hash(:requeue)
          end
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
