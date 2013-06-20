require 'active_record/version'
module Delayed
  module Backend
    module ActiveRecord
      # A job object that is persisted to the database.
      # Contains the work object as a YAML field.
      class Job < ::ActiveRecord::Base
        include Delayed::Backend::Base

        if ::ActiveRecord::VERSION::MAJOR < 4 || defined?(::ActiveRecord::MassAssignmentSecurity)
          attr_accessible :priority, :run_at, :queue, :payload_object,
                          :failed_at, :locked_at, :locked_by, :handler
        end

        scope :by_priority, lambda { order('priority ASC, run_at ASC') }

        before_save :set_default_run_at
        after_commit :amqp_enqueue, :on => :create

        cattr_accessor :amqp_queue_manager
        attr_accessor :amqp_work_item

        def self.amqp_connect(*args)
          session = if args.last.is_a?(Bunny::Session)
            args.last
          else
            Bunny.new(*args).tap { |s| s.start }
          end
          @@amqp_manager = AmqpQueueManager.new(session, "delayed_job")
        end

        def self.become_amqp_consumer!
          @@amqp_manager.become_consumer!
        end

        def amqp_enqueue
          @@amqp_manager.enqueue({ :job_id => self.id }, { :priority => :normal, :run_at => self.run_at })
        end

        def amqp_acknowledge
          @@amqp_manager.ack(self.amqp_work_item)
        end

        def self.set_delayed_job_table_name
          delayed_job_table_name = "#{::ActiveRecord::Base.table_name_prefix}delayed_jobs"
          self.table_name = delayed_job_table_name
        end

        self.set_delayed_job_table_name

        def self.ready_to_run(worker_name, max_run_time)
          where('(run_at <= ? AND (locked_at IS NULL OR locked_at < ?) OR locked_by = ?) AND failed_at IS NULL', db_time_now, db_time_now - max_run_time, worker_name)
        end

        def self.before_fork
          ::ActiveRecord::Base.clear_all_connections!
        end

        def self.after_fork
          ::ActiveRecord::Base.establish_connection
          # TODO: Bunny?
        end

        # When a worker is exiting, make sure we don't have any locked jobs.
        def self.clear_locks!(worker_name)
          where(:locked_by => worker_name).update_all(:locked_by => nil, :locked_at => nil)
        end

        def self.reserve(worker, max_run_time = Worker.max_run_time)
          work_item = @@amqp_manager.pop
          job_id = work_item.payload['job_id']

          transaction do
            job = self.ready_to_run(worker.name, max_run_time).where(:id => job_id).lock.first

            unless job
              @@amqp_manager.ack(work_item)
              return
            end

            job.amqp_work_item = work_item
            job.update_attributes!({ :locked_at => db_time_now, :locked_by => worker.name }, { :without_protection => true })
            return job
          end
        end

        # Get the current time (GMT or local depending on DB)
        # Note: This does not ping the DB to get the time, so all your clients
        # must have syncronized clocks.
        def self.db_time_now
          if Time.zone
            Time.zone.now
          elsif ::ActiveRecord::Base.default_timezone == :utc
            Time.now.utc
          else
            Time.now
          end
        end

        def reload(*args)
          reset
          super
        end
      end
    end
  end
end
