module Kernel
  @@path = 'unknown'
  @@count = 20
  @@sleep_time = 0.1

  def path
    @@path
  end

  def path=(value)
    @@path = value
  end

  def get_key(cmd)
    "key '#{"%02d" % /-(\d+)/.match(cmd)[1].to_s.to_i}': 62700332 documents"
  end

  def count
    @@count
  end

  def count=(value)
    @@count = value
  end

  def sleep_time
    @@sleep_time
  end

  def sleep_time=(value)
    @@sleep_time = value
  end

  def `(cmd)
    sleep(sleep_time)
    random = Random.new(10)
    itr = (1..count)
    if cmd.start_with? 'hadoop fs -du'
      return itr.map { |n|
        r = random.rand(10..50)
        "#{r*1024*1024*1024} #{Kernel::path}/part-r-#{n}"
      }.join "\n"
    elsif cmd.start_with? 'hadoop fs -cat'
      return Kernel::get_key(cmd)
    elsif cmd.start_with? 'ls /'
      return itr.map { |n| "#{n}" }.join("\n")
    elsif cmd.start_with? 'du -k'
      return "20110104/t2342"
    end
  end
end
