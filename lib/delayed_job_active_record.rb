require 'active_record'
require 'bunny'
require 'delayed_job'
require 'delayed/backend/active_record'

Delayed::Worker.backend = :active_record
