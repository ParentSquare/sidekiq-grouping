# frozen_string_literal: true

module Sidekiq
  module Grouping
    class RedisScripts
      class << self
        include RedisDispatcher
      end

      PLUCK_SCRIPT = <<-SCRIPT
        local pluck_values = redis.call('lpop', KEYS[1], ARGV[1]) or {}
        if #pluck_values > 0 then
          redis.call('srem', KEYS[2], unpack(pluck_values))
        end
        return pluck_values
      SCRIPT

      RELIABLE_PLUCK_SCRIPT = <<-LUA
        local queue = KEYS[1]
        local unique_messages = KEYS[2]
        local pending_jobs = KEYS[3]
        local current_time = KEYS[4]
        local this_job = KEYS[5]
        local limit = tonumber(ARGV[1])

        redis.call('zadd', pending_jobs, current_time, this_job)
        local values = {}
        for i = 1, math.min(limit, redis.call('llen', queue)) do
          table.insert(values, redis.call('lmove', queue, this_job, 'left', 'right'))
        end
        if #values > 0 then
          redis.call('srem', unique_messages, unpack(values))
        end

        return {this_job, values}
      LUA

      REQUEUE_SCRIPT = <<-LUA
        local expired_queue = KEYS[1]
        local queue = KEYS[2]
        local pending_jobs = KEYS[3]

        local to_requeue = redis.call('llen', expired_queue)
        for i = 1, to_requeue do
          redis.call('lmove', expired_queue, queue, 'left', 'right')
        end
        redis.call('zrem', pending_jobs, expired_queue)
      LUA

      UNIQUE_REQUEUE_SCRIPT = <<-LUA
        local expired_queue = KEYS[1]
        local queue = KEYS[2]
        local pending_jobs = KEYS[3]
        local unique_messages = KEYS[4]

        local to_requeue = redis.call('lrange', expired_queue, 0, -1)
        for i = 1, #to_requeue do
          local message = to_requeue[i]
          if redis.call('sismember', unique_messages, message) == 0 then
            redis.call('lmove', expired_queue, queue, 'left', 'right')
          else
            redis.call('lpop', expired_queue)
          end
        end
        redis.call('zrem', pending_jobs, expired_queue)
      LUA

      MERGE_ARRAY_SCRIPT = <<-LUA
        local batches = KEYS[1]
        local name = KEYS[2]
        local namespaced_name = KEYS[3]
        local unique_messages_key = KEYS[4]
        local remember_unique = KEYS[5]
        local messages = ARGV

        if remember_unique == 'true' then
          local existing_messages = redis.call('smismember', unique_messages_key, unpack(messages))
          local result = {}

          for index, value in ipairs(messages) do
            if existing_messages[index] == 0 then
              result[#result + 1] = value
            end
          end

          messages = result
        end

        redis.call('sadd', batches, name)
        redis.call('rpush', namespaced_name, unpack(messages))
        if remember_unique == 'true' then
          redis.call('sadd', unique_messages_key, unpack(messages))
        end
      LUA

      SCRIPTS = {
        pluck: PLUCK_SCRIPT,
        reliable_pluck: RELIABLE_PLUCK_SCRIPT,
        requeue: REQUEUE_SCRIPT,
        unique_requeue: UNIQUE_REQUEUE_SCRIPT,
        merge_array: MERGE_ARRAY_SCRIPT
      }.freeze

      HASHES = {
        pluck: redis_call(:script, "LOAD", SCRIPTS[:pluck]),
        reliable_pluck: redis_call(:script, "LOAD", SCRIPTS[:reliable_pluck]),
        requeue: redis_call(:script, "LOAD", SCRIPTS[:requeue]),
        unique_requeue: redis_call(:script, "LOAD", SCRIPTS[:unique_requeue]),
        merge_array: redis_call(:script, "LOAD", SCRIPTS[:merge_array])
      }.freeze
    end
  end
end
