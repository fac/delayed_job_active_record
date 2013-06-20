# This is a bit of a library-in-a-library, for the time being
# 
# This module manages the interaction with AMQP, handling publishing of
# work messages, scheduling of work off the AMQP queue, etc.
#
##
#
# You probably want to start with TomQueue::QueueManager
#
module TomQueue

  require 'tom_queue/queue_manager'
  require 'tom_queue/work'
  
  # Public: Sets the bunny instance to use for new QueueManager objects
  def bunny=(new_bunny)
    @@bunny = new_bunny
  end
  # Public: Returns the current bunny instance
  #
  # Returns whatever was passed to TomQueue.bunny = 
  def bunny
    @@bunny
  end
  module_function :bunny, :bunny=

end