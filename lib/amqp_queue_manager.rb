class AmqpQueueManager
  class Work
    attr_reader :payload, :delivery_tag, :priority
    attr_reader :run_at

    def initialize(meta, headers, json)
      @delivery_tag = meta.delivery_tag
      @data = JSON.load(json)
      @payload = @data['payload']
      @run_at = Time.at(@data['run_at']) rescue Time.now
    end
  end

  # Public: Create the queue manager
  #
  # name - name that is used as a prefix for queues and exchanges used
  #
  def initialize(bunny, name)
    @bunny = bunny
    @name = name

    @channel = @bunny.create_channel
    @channel.prefetch(1)

    # Ensure the queues are configured
    @exchange_bulk = @channel.fanout("#{name}.exchange-bulk")
    @queue_bulk = @channel.queue("#{name}.balance-bulk").bind(@exchange_bulk)

    @exchange_normal = @channel.fanout("#{name}.exchange-normal")
    @queue_normal = @channel.queue("#{name}.balance-normal").bind(@exchange_normal)

    @exchange_high = @channel.fanout("#{name}.exchange-high")
    @queue_high = @channel.queue("#{name}.balance-high").bind(@exchange_high)

    # Create a special channel, exchange and queue for deferred work
    @deferred_channel = @bunny.create_channel
    @deferred_exchange = @deferred_channel.fanout("#{name}.exchange-deferred")
    @deferred_queue = @deferred_channel.queue("#{name}.queue-deferred").bind(@deferred_exchange)
    @deferred_work = []
    @deferred_mutex = Mutex.new
  end


  # Public: call this once before entering a consumer loop to setup the consumer
  #
  def become_consumer!
    @mutex = Mutex.new
    @condvar = ConditionVariable.new

    # Subscribe to all queues so we don't get stuck if we're only receiving high priority
    # messages!
    @queue_bulk.subscribe(:ack => true, &method(:consumer_handle_work))
    @queue_normal.subscribe(:ack => true, &method(:consumer_handle_work))
    @queue_high.subscribe(:ack => true, &method(:consumer_handle_work))

    @deferred_queue.subscribe(:ack => true, &method(:deferred_consumer))
  end

  # Internal: Handle new deferred work arriving
  def deferred_consumer(delivery_info, headers, payload)
    puts "Deferred work arrived!"
    data = JSON.load(payload)

    run_at = data['run_at']
    data['delivery_tag'] = p delivery_info.delivery_tag

    @deferred_mutex.synchronize do
      @deferred_work << data
      @deferred_work.sort { |a, b| a['run_at'] <=> b['run_at'] }

      while(!@deferred_work.empty? && (interval = (Time.at(@deferred_work.first['run_at']) - Time.now)) < 0)
        deliver_deferred_work(@deferred_work.pop)
      end

      puts "Sleep for #{interval}"
    end
  end

  def deliver_deferred_work(payload)
    
    p payload['delivery_tag']
    @deferred_channel.acknowledge(payload['delivery_tag'], false)
    enqueue(payload['payload'], :priority => payload['priority'], :run_at => Time.at(payload['run_at']))
  end

  # Internal: Handle a subscription notification
  #
  # NOTE: this is called on a bunny thread-pool thread. use a cond-var to set
  # the next_work variable and un-freeze the main thread if necessary
  def consumer_handle_work(delivery_info, headers, payload)
    @mutex.synchronize do
      @next_work = [delivery_info, headers, payload]
      @condvar.signal
    end
  end

  # Public: Pop a message from the queue, blocking the calling thread
  #   until one is available
  #
  #
  # Returns a QueueManager::Work object
  def pop
    # If @next_work is high priority, return it immediately !!
    @mutex.synchronize {
      if @next_work &&  @next_work.first.exchange == @exchange_high.name
        return Work.new(*@next_work).tap { |a| @next_work = nil }
      end
    }

    # any waiting high-priority work in the remote queue?
    work = @queue_high.pop(:ack => true)
    return Work.new(*work) if work.first

    # If @next_work is normal priority, return it immediately !!
    @mutex.synchronize {
      if @next_work &&  @next_work.first.exchange == @exchange_normal.name
        return Work.new(*@next_work).tap { |a| @next_work = nil }
      end

    }
    
    # any waiting normal-priority work in the remote queue?
    work = @queue_normal.pop(:ack => true)
    return Work.new(*work) if work.first
    
    # If not, block on any queue setting @next_work (probably the next bulk message)
    @mutex.synchronize do
      @condvar.wait(@mutex) unless @next_work
      Work.new(*@next_work).tap { |a| @next_work = nil }
    end
  end

  # Public: Acknowledge completion of work to the queue broker
  #
  # work - pass the QueueManager::Work object to ack
  #
  # This must be called when a job has completed, otherwise un'ackd
  # messages will back up in the broker.
  def ack(work)
    @channel.acknowledge(work.delivery_tag)
  end

  # Public: Publish a message to this queue
  #
  # work    - the work "payload", must be JSON serialisable
  # options - publish options, use keys:
  #   :priority = use :bulk, :normal or :high
  #   :run_at   = run this job at a particular future time (defaults to immediate)
  #
  def enqueue(work, opts = {})
    priority = opts.fetch(:priority, :normal)
    run_at = opts.fetch(:run_at, Time.now-0.1)

    ex = if run_at > Time.now
      @deferred_exchange
    else
      if priority == :high
        @exchange_high
      elsif priority == :bulk
        @exchange_bulk
      else
        @exchange_normal
      end
    end

    ex.publish(p JSON.dump({
      :run_at   => run_at.to_f,
      :priority => priority,
      :payload  => work
    }))
  end
end
