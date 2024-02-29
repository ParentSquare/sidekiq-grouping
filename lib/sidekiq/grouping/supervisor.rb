# frozen_string_literal: true

module Sidekiq
  module Grouping
    class Supervisor
      def requeue_expired
        Sidekiq::Grouping::Batch.all.each do |batch|
          next unless batch.is_a?(Sidekiq::Grouping::ReliableBatch)

          batch.requeue_expired
        end
      end
    end
  end
end
