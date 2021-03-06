module TomQueue

  # Internal: This class wraps the pool of work items that are waiting for their run_at 
  # time to be reached.
  # 
  # It also incorporates the logic and coordination required to stall a thread until the 
  # work is ready to run.
  #
  class DeferredWorkSet

    # Internal: A wrapper object to store the run at and the opaque
    # work object inside the @work array.
    class Element < Struct.new(:run_at, :work)
      def <=> (other)
        run_at <=> other.run_at
      end
      def sleep_interval
        run_at - Time.now
      end
    end

    def initialize
      @mutex = Mutex.new
      @condvar = ConditionVariable.new
      @work = Set.new
    end

    # Public: Returns the integer number of elements in the set
    #
    # Returns integer
    def size
      @work.size
    end

    # Public: Block the calling thread until some work is ready to run
    # or the timeout expires.
    #
    # This is intended to be called from a single worker thread, for the
    # time being, if you try and block on this method concurrently in 
    # two threads, it will raise an exception!
    #
    # timeout - (Fixnum, seconds) how long to wait before timing out
    #
    # Returns previously scheduled work, or
    #         nil if the thread was interrupted or the timeout expired
    def pop(timeout)
      timeout_end = Time.now + timeout
      returned_work = nil

      @interrupt = false

      @mutex.synchronize do
        raise RuntimeError, 'DeferredWorkSet: another thread is already blocked on a pop' unless @blocked_thread.nil? 

        begin
          @blocked_thread = Thread.current
          begin
            end_time = [earliest_element.try(:run_at), timeout_end].compact.min
            @condvar.wait(@mutex, end_time - Time.now) if end_time > Time.now
          end while Time.now < end_time and @interrupt == false

          element = earliest_element
          if element && element.run_at < Time.now
            @work.delete(element)
            returned_work = element.work
          end

        ensure
          @blocked_thread = nil
        end
      end

      returned_work
    end
    
    # Public: Interrupt anything sleeping on this set
    #
    # This is "thread-safe" and is designed to be called from threads
    # to interrupt the work loop thread blocked on a pop.
    #
    def interrupt
      @mutex.synchronize do
        @interrupt = true
        @condvar.signal
      end
    end

    # Public: Add some work to the set
    #
    # This is "threa-safe" in that it can be (and is intended to
    # be) called from threads other than the one calling pop without
    # any additional synchronization.
    #
    # run_at - (Time) when the work is to be run
    # work   - the DeferredWork object
    #
    def schedule(run_at, work)
      @mutex.synchronize do
        @work << Element.new(run_at, work)
        @condvar.signal
      end
    end

    # Public: Returns the temporally "soonest" element in the set
    # i.e. the work that is next to be run
    #
    # Returns a DeferredWork instance or
    #         nil if there is no work in the set
    def earliest
      earliest_element.try(:work)
    end

    # Internal: The earliest element (i.e. wrapper object)
    def earliest_element
      @work.sort.first
    end
  end

end