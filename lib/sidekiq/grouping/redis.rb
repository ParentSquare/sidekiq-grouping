# frozen_string_literal: true

require_relative "./redis_dispatcher"
require_relative "./redis_scripts"

module Sidekiq
  module Grouping
    class Redis # rubocop:disable Metrics/ClassLength
      include RedisDispatcher

      def initialize
        @script_hashes = {
          pluck: nil,
          reliable_pluck: nil,
          requeue: nil,
          unique_requeue: nil,
          merge_array: nil
        }

        RedisScripts::SCRIPTS.each_pair do |key, value|
          @script_hashes[key] = redis { |conn| conn.script(:load, value) }
        end
      end

      def push_msg(name, msg, remember_unique: false)
        redis do |conn|
          conn.multi do |pipeline|
            sadd = pipeline.respond_to?(:sadd?) ? :sadd? : :sadd
            redis_connection_call(pipeline, sadd, ns("batches"), name)
            redis_connection_call(pipeline, :rpush, ns(name), msg)

            if remember_unique
              redis_connection_call(
                pipeline, sadd, unique_messages_key(name), msg
              )
            end
          end
        end
      end

      def push_messages(name, messages, remember_unique: false)
        if new_redis_client?
          push_messages_new(name, remember_unique, messages)
        else
          push_messages_legacy(name, remember_unique, messages)
        end
      end

      def enqueued?(name, msg)
        member = redis_call(:sismember, unique_messages_key(name), msg)
        return member if member.is_a?(TrueClass) || member.is_a?(FalseClass)

        member != 0
      end

      def batch_size(name)
        redis_call(:llen, ns(name))
      end

      def batches
        redis_call(:smembers, ns("batches"))
      end

      def pluck(name, limit)
        if new_redis_client?
          redis_call(
            :eval,
            RedisScripts::PLUCK_SCRIPT,
            2,
            ns(name),
            unique_messages_key(name),
            limit
          )
        else
          keys = [ns(name), unique_messages_key(name)]
          args = [limit]
          redis_call(:eval, RedisScripts::PLUCK_SCRIPT, keys, args)
        end
      end

      def reliable_pluck(name, limit)
        if new_redis_client?
          reliable_pluck_new(name, limit)
        else
          reliable_pluck_legacy(name, limit)
        end
      end

      def get_last_execution_time(name)
        redis_call(:get, ns("last_execution_time:#{name}"))
      end

      def set_last_execution_time(name, time)
        redis_call(
          :set, ns("last_execution_time:#{name}"), time.to_json
        )
      end

      def lock(name)
        redis_call(
          :set,
          ns("lock:#{name}"),
          "true",
          nx: true,
          ex: Sidekiq::Grouping::Config.lock_ttl
        )
      end

      def delete(name)
        redis do |conn|
          redis_connection_call(conn, :del, ns("last_execution_time:#{name}"))
          redis_connection_call(conn, :del, ns(name))
          redis_connection_call(conn, :srem, ns("batches"), name)
        end
      end

      def remove_from_pending(name, batch_name)
        redis do |conn|
          conn.multi do |pipeline|
            redis_connection_call(pipeline, :del, batch_name)
            redis_connection_call(
              pipeline, :zrem, pending_jobs(name), batch_name
            )
          end
        end
      end

      def requeue_expired(name, unique: false, ttl: 3600)
        redis do |conn|
          redis_connection_call(
            conn, :zrangebyscore, pending_jobs(name), "0", Time.now.to_i - ttl
          ).each do |expired|
            if new_redis_client?
              requeue_expired_new(conn, unique, expired, name)
            else
              requeue_expired_legacy(conn, unique, expired, name)
            end
          end
        end
      end

      private

      def push_messages_new(name, remember_unique, messages)
        redis_call(
          :evalsha,
          @script_hashes[:merge_array],
          5,
          ns("batches"),
          name,
          ns(name),
          unique_messages_key(name),
          remember_unique.to_s,
          messages
        )
      end

      def push_messages_legacy(name, remember_unique, messages)
        keys = [
          ns("batches"),
          name,
          ns(name),
          unique_messages_key(name),
          remember_unique.to_s
        ]
        argv = [messages]
        redis_call(:evalsha, @script_hashes[:merge_array], keys, argv)
      end

      def reliable_pluck_new(name, limit)
        redis_call(
          :evalsha,
          @script_hashes[:reliable_pluck],
          5,
          ns(name),
          unique_messages_key(name),
          pending_jobs(name),
          Time.now.to_i,
          this_job_name(name),
          limit
        )
      end

      def reliable_pluck_legacy(name, limit)
        keys = [
          ns(name),
          unique_messages_key(name),
          pending_jobs(name),
          Time.now.to_i,
          this_job_name(name)
        ]
        argv = [limit]
        redis_call(:evalsha, @script_hashes[:reliable_pluck], keys, argv)
      end

      def requeue_expired_new(conn, unique, expired, name)
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

      def requeue_expired_legacy(conn, unique, expired, name)
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

      def requeue_script(unique)
        unique ? @script_hashes[:unique_requeue] : @script_hashes[:requeue]
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
