# frozen_string_literal: true

require "spec_helper"

describe Sidekiq::Grouping::Redis do # rubocop:disable RSpec/SpecFilePathFormat
  include Sidekiq::Grouping::RedisDispatcher

  subject(:redis_service) { described_class.new }

  let(:queue_name)    { "my_queue" }
  let(:key)           { "batching:#{queue_name}" }
  let(:unique_key)    { "batching:#{queue_name}:unique_messages" }
  let(:pending_jobs)  { "batching:#{queue_name}:pending_jobs" }

  describe "#push_msg" do
    it "adds message to queue", :aggregate_failures do
      redis_service.push_msg(queue_name, "My message")
      expect(redis_call(:llen, key)).to eq 1
      expect(redis_call(:lrange, key, 0, 1)).to eq ["My message"]
      expect(redis_call(:smembers, unique_key)).to eq []
    end

    it "remembers unique message if specified" do
      redis_service.push_msg(queue_name, "My message", remember_unique: true)
      expect(redis_call(:smembers, unique_key)).to eq ["My message"]
    end
  end

  describe "#pluck" do
    it "removes messages from queue" do
      redis_service.push_msg(queue_name, "Message 1")
      redis_service.push_msg(queue_name, "Message 2")
      redis_service.pluck(queue_name, 2)
      expect(redis_call(:llen, key)).to eq 0
    end

    it "forgets unique messages", :aggregate_failures do
      redis_service.push_msg(queue_name, "Message 1", remember_unique: true)
      redis_service.push_msg(queue_name, "Message 2", remember_unique: true)
      expect(redis_call(:scard, unique_key)).to eq 2
      redis_service.pluck(queue_name, 2)
      expect(redis_call(:smembers, unique_key)).to eq []
    end
  end

  describe "#reliable_pluck" do
    it "removes messages from queue" do
      redis_service.push_msg(queue_name, "Message 1")
      redis_service.push_msg(queue_name, "Message 2")
      redis_service.reliable_pluck(queue_name, 1000)
      expect(redis_call(:llen, key)).to eq 0
    end

    it "forgets unique messages", :aggregate_failures do
      redis_service.push_msg(queue_name, "Message 1", remember_unique: true)
      redis_service.push_msg(queue_name, "Message 2", remember_unique: true)
      expect(redis_call(:scard, unique_key)).to eq 2
      redis_service.reliable_pluck(queue_name, 2)
      expect(redis_call(:smembers, unique_key)).to eq []
    end

    it "tracks the pending jobs", :aggregate_failures do
      redis_service.push_msg(queue_name, "Message 1", remember_unique: true)
      redis_service.push_msg(queue_name, "Message 2", remember_unique: true)
      redis_service.reliable_pluck(queue_name, 2)
      expect(redis_call(:zcount, pending_jobs, 0, Time.now.utc.to_i)).to eq 1
      pending_queue_name = redis_call(:zscan, pending_jobs, 0)[1][0]
      if pending_queue_name.is_a?(Array)
        pending_queue_name = pending_queue_name.first
      end
      expect(redis_call(:llen, pending_queue_name)).to eq 2
    end

    it "keeps extra items in the queue", :aggregate_failures do
      redis_service.push_msg(queue_name, "Message 1", remember_unique: true)
      redis_service.push_msg(queue_name, "Message 2", remember_unique: true)
      redis_service.reliable_pluck(queue_name, 1)
      expect(redis_call(:zcount, pending_jobs, 0, Time.now.utc.to_i)).to eq 1
      pending_queue_name = redis_call(:zscan, pending_jobs, 0)[1][0]
      if pending_queue_name.is_a?(Array)
        pending_queue_name = pending_queue_name.first
      end
      expect(redis_call(:llen, pending_queue_name)).to eq 1
      expect(redis_call(:llen, key)).to eq 1
    end
  end

  describe "#remove_from_pending" do
    it "removes pending jobs by name", :aggregate_failures do
      redis_service.push_msg(queue_name, "Message 1", remember_unique: true)
      redis_service.push_msg(queue_name, "Message 2", remember_unique: true)
      pending_queue_name, = redis_service.reliable_pluck(queue_name, 2)
      expect(redis_call(:lrange, pending_queue_name, 0, -1)).to eq(
        ["Message 1", "Message 2"]
      )
      redis_service.remove_from_pending(queue_name, pending_queue_name)
      expect(redis_call(:zcount, pending_jobs, 0, Time.now.utc.to_i)).to eq 0
      expect(redis_call(:lrange, pending_queue_name, 0, -1)).to eq([])
      expect(redis_call(:keys, "*")).not_to include(pending_queue_name)
    end
  end

  describe "#requeue_expired" do
    it "requeues expired jobs", :aggregate_failures do
      redis_service.push_msg(queue_name, "Message 1")
      redis_service.push_msg(queue_name, "Message 2")
      pending_queue_name, = redis_service.reliable_pluck(queue_name, 2)
      expect(
        redis_service.requeue_expired(queue_name, unique: false, ttl: 500).size
      ).to eq 0
      redis_call(:zincrby, pending_jobs, -1000, pending_queue_name)
      redis_service.push_msg(queue_name, "Message 2", remember_unique: false)
      expect(
        redis_service.requeue_expired(queue_name, unique: false, ttl: 500).size
      ).to eq 1
      expect(redis_call(:llen, key)).to eq 3
      expect(redis_call(:lrange, key, 0, -1)).to contain_exactly(
        "Message 1", "Message 2", "Message 2"
      )
    end

    it "removes pending job once enqueued", :aggregate_failures do
      redis_service.push_msg(queue_name, "Message 1", remember_unique: true)
      redis_service.push_msg(queue_name, "Message 2", remember_unique: true)
      pending_queue_name, = redis_service.reliable_pluck(queue_name, 2)
      expect(
        redis_service.requeue_expired(queue_name, unique: false, ttl: 500).size
      ).to eq 0
      redis_call(:zincrby, pending_jobs, -1000, pending_queue_name)
      expect(
        redis_service.requeue_expired(queue_name, unique: false, ttl: 500).size
      ).to eq 1
      expect(redis_call(:zcount, pending_jobs, 0, Time.now.utc.to_i)).to eq 0
    end

    context "with batch_unique == true", :aggregate_failures do
      it "requeues expired jobs that are not already present" do
        redis_service.push_msg(queue_name, "Message 1", remember_unique: true)
        redis_service.push_msg(queue_name, "Message 2", remember_unique: true)
        redis_service.push_msg(queue_name, "Message 3", remember_unique: true)
        pending_queue_name, = redis_service.reliable_pluck(
          queue_name,
          3
        )
        expect(
          redis_service.requeue_expired(queue_name, unique: true, ttl: 500).size
        ).to eq 0
        redis_call(:zincrby, pending_jobs, -1000, pending_queue_name)
        redis_service.push_msg(queue_name, "Message 2", remember_unique: true)
        expect(
          redis_service.requeue_expired(queue_name, unique: true, ttl: 500).size
        ).to eq 1
        expect(redis_call(:llen, key)).to eq 3
        expect(redis_call(:lrange, key, 0, -1)).to contain_exactly(
          "Message 1", "Message 2", "Message 3"
        )
      end

      it "removes pending job once enqueued", :aggregate_failures do
        redis_service.push_msg(queue_name, "Message 1", remember_unique: true)
        redis_service.push_msg(queue_name, "Message 2", remember_unique: true)
        pending_queue_name, = redis_service.reliable_pluck(
          queue_name,
          2
        )
        expect(
          redis_service.requeue_expired(queue_name, unique: true, ttl: 500).size
        ).to eq 0
        redis_call(:zincrby, pending_jobs, -1000, pending_queue_name)
        redis_service.push_msg(queue_name, "Message 1", remember_unique: true)
        expect(
          redis_service.requeue_expired(queue_name, unique: true, ttl: 500).size
        ).to eq 1
        expect(
          redis_call(:zcount, pending_jobs, 0, Time.now.utc.to_i)
        ).to eq 0
      end
    end
  end

  describe "#push_messages" do
    it "adds messages to queue", :aggregate_failures do
      redis_service.push_messages(
        queue_name,
        ["My message", "My other message", "My last message"]
      )
      expect(redis_call(:llen, key)).to eq 3
      expect(redis_call(:lrange, key, 0, 3)).to eq(
        ["My message", "My other message", "My last message"]
      )
      expect(redis_call(:smembers, unique_key)).to eq []
    end

    it "remembers unique messages if specified", :aggregate_failures do
      redis_service.push_messages(
        queue_name,
        ["My message", "My other message", "My last message"],
        remember_unique: true
      )
      expect(redis_call(:lrange, key, 0, 3)).to eq(
        ["My message", "My other message", "My last message"]
      )
      expect(redis_call(:smembers, unique_key)).to contain_exactly(
        "My message", "My other message", "My last message"
      )
    end

    it "adds new messages in order", :aggregate_failures do
      redis_service.push_messages(
        queue_name,
        ["My message"],
        remember_unique: true
      )
      expect(redis_call(:smembers, unique_key)).to contain_exactly(
        "My message"
      )
      redis_service.push_messages(
        queue_name,
        ["My other message", "My message", "My last message"],
        remember_unique: true
      )
      expect(redis_call(:lrange, key, 0, 3)).to eq(
        ["My message", "My other message", "My last message"]
      )
      expect(redis_call(:smembers, unique_key)).to contain_exactly(
        "My message", "My other message", "My last message"
      )
    end
  end

  describe "after the server loses its script cache" do
    let(:scripts) { Sidekiq::Grouping::RedisScripts }

    after do
      scripts::SCRIPTS.each_value do |source|
        redis_call(:script, "load", source)
      end
    end

    def flush_script_cache
      redis_call(:script, "flush")
    end

    def script_loaded?(name)
      result = redis_call(:script, "exists", scripts.script_hash(name))
      [true, 1].include?(Array(result).first)
    end

    it "push_messages reloads the script and adds the messages",
       :aggregate_failures do
      redis_service.push_messages(queue_name, ["Message 1"])
      flush_script_cache
      redis_service.push_messages(queue_name, ["Message 2"])
      expect(redis_call(:llen, key)).to eq 2
      expect(script_loaded?(:merge_array)).to be true
    end

    it "pluck reloads the script and returns the messages",
       :aggregate_failures do
      redis_service.push_msg(queue_name, "Message 1")
      redis_service.pluck(queue_name, 1)
      redis_service.push_msg(queue_name, "Message 2")
      flush_script_cache
      expect(redis_service.pluck(queue_name, 1)).to eq ["Message 2"]
      expect(script_loaded?(:pluck)).to be true
    end

    it "reliable_pluck reloads the script and plucks the messages",
       :aggregate_failures do
      redis_service.push_msg(queue_name, "Message 1")
      redis_service.reliable_pluck(queue_name, 1)
      redis_service.push_msg(queue_name, "Message 2")
      flush_script_cache
      redis_service.reliable_pluck(queue_name, 1)
      expect(redis_call(:llen, key)).to eq 0
      expect(script_loaded?(:reliable_pluck)).to be true
    end

    [false, true].each do |unique|
      context "with unique: #{unique}" do
        it "requeue_expired reloads the script and requeues expired jobs",
           :aggregate_failures do
          script = unique ? :unique_requeue : :requeue
          redis_service.push_msg(
            queue_name, "Message 1", remember_unique: unique
          )
          pending_queue_name, = redis_service.reliable_pluck(queue_name, 1)
          redis_call(:zincrby, pending_jobs, -1000, pending_queue_name)
          scripts.script_hash(script)
          flush_script_cache
          expect(
            redis_service.requeue_expired(queue_name, unique: unique, ttl: 500)
              .size
          ).to eq 1
          expect(redis_call(:llen, key)).to eq 1
          expect(script_loaded?(script)).to be true
        end
      end
    end

    it "raises NOSCRIPT when the script is still missing after one reload",
       :aggregate_failures do
      redis_service.push_msg(queue_name, "Message 1")
      redis_service.pluck(queue_name, 1)
      flush_script_cache
      allow(scripts).to receive(:reload_script).and_return("0" * 40)
      expect { redis_service.pluck(queue_name, 1) }.to raise_error(/NOSCRIPT/)
      expect(scripts).to have_received(:reload_script).once
    end

    it "raises errors other than NOSCRIPT without reloading",
       :aggregate_failures do
      redis_call(:set, key, "not a list")
      allow(scripts).to receive(:reload_script)
      expect { redis_service.pluck(queue_name, 1) }.to raise_error(/WRONGTYPE/)
      expect(scripts).not_to have_received(:reload_script)
    end
  end
end
