# frozen_string_literal: true

require_relative "./redis_dispatcher"
require_relative "./redis_scripts"
require_relative "./adapters/redis_adapter"
require_relative "./adapters/redis_client_adapter"

module Sidekiq
  module Grouping
    class Redis
      include RedisDispatcher

      def initialize
        @adapter = if new_redis_client?
                     Adapters::RedisClientAdapter.new
                   else
                     Adapters::RedisAdapter.new
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
        @adapter.push_messages(name, messages, remember_unique: remember_unique)
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
        @adapter.pluck(name, limit)
      end

      def reliable_pluck(name, limit)
        @adapter.reliable_pluck(name, limit)
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
            @adapter.requeue_expired(conn, unique, expired, name)
          end
        end
      end

      private

      def unique_messages_key(name)
        ns("#{name}:unique_messages")
      end

      def pending_jobs(name)
        ns("#{name}:pending_jobs")
      end

      def ns(key = nil)
        "batching:#{key}"
      end
    end
  end
end
