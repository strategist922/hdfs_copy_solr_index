class SimpleLogger
  def initialize(logfile)
    @file = File.new(logfile, 'w')
  end

  def log(msg)
    @file.write(msg)
    @file.flush()
  end
end