require 'active_record'
require 'delayed_job'
require 'delayed/backend/active_record'

require 'tom_queue'

Delayed::Worker.backend = :active_record
