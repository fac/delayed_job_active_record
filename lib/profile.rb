class SampleSet
  
  def initialize
    reset!
  end

  attr_reader :last_reset, :values
  def reset!
    @last_reset = Time.now
    @values = []
  end

  def <<(value)
    @values << value
  end

  def count
    @values.count
  end
  def rate
    count / (Time.now - @last_reset)
  end
  def average
    @values.sum / @values.count
  end
end

class Profile

  def initialize(name = '')
    @name = name
    @durations = SampleSet.new
  end


  # Public: Record the duration and rate of an event
  def event(name)
    start_time = Time.now
    yield
    end_time = Time.now

    @durations << (end_time - start_time)
  end

  # Public: Spits out a profile report every <x> interval
  #
  def report!
    if Time.now - @durations.last_reset > 5.0
      $stderr.write "#{@name}: %.02f jobs/sec  (%.02fms average insertion)\n" % [@durations.rate, @durations.average * 1000]

      @durations.reset!
    end
  end

end