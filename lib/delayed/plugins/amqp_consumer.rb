module Delayed
  module Plugins
    class AmqpConsumer < Plugin
      callbacks do |lifecycle|
        lifecycle.before(:execute) do |worker|
          Delayed::Backend::ActiveRecord::Job.become_amqp_consumer!
        end

        lifecycle.after(:error) do |worker, job|
          job.amqp_enqueue if job.attempts < worker.max_attempts(job)
        end

        lifecycle.after(:perform) do |worker, job|
          job.amqp_acknowledge
        end
      end
    end
  end
end

Delayed::Worker.plugins << Delayed::Plugins::AmqpConsumer
