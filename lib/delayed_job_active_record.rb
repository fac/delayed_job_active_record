require 'active_record'
require 'amqp_queue_manager'
require 'bunny'
require 'delayed_job'
require 'delayed/backend/active_record'
require 'delayed/plugins/amqp_consumer'

Delayed::Worker.backend = :active_record
