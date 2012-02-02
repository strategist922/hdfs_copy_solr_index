require "rspec"
require "pp"
require_relative '../lib/solr_index_manager'

describe SolrIndexManager do
  before(:each) do
    Readline.stub(:readline).and_return 'y'
    Kernel.stub(:path).and_return('unknown')
    Kernel.stub(:get_key).and_return(lambda { |cmd| "key '#{"%02d" % /-(\d+)/.match(cmd)[1].to_s.to_i}': 62700332 documents" })

    module Kernel
      def `(cmd)
        random = Random.new(10)
        itr = (1..20)
        if cmd.start_with? 'hadoop fs -du'
          return itr.map { |n|
            r = random.rand(10..50)
            "#{r*1024*1024*1024} solrindex/#{Kernel::path}/part-r-#{n}"
          }.join "\n"
        elsif cmd.start_with? 'hadoop fs -cat'
          return Kernel::get_key.call(cmd)
        elsif cmd.start_with? 'ls /'
          return itr.map { |n| "#{n}" }.join("\n")
        elsif cmd.start_with? 'du -k'
          return "20110104/t2342"
        end
      end
    end
  end

  it "should simulate job" do
    args =
        {
            hadoop_src: 'solrindex/test_20110730',
            copy_dst: '/copy_to/test_20110730',
            max_merge_size: '100Gb',
            config_src_folder: 'src_conf',
            core_prefix: 'news_',
            core_admin: 'http://localhost:8983/solr/admin/cores',
            dst_distribution:
                ['/data/a/solr/news/20110815/#{key}',
                 '/data/b/solr/news/20110815/#{key}',
                 '/data/c/solr/news/20110815/#{key}',
                 '/data/d/solr/news/20110815/#{key}',
                 '/data/e/solr/news/20110815/#{key}',
                 '/data/f/solr/news/20110815/#{key}']
        }
    @manager = SolrIndexManager.new(args)
    @manager.go()
  end

  after do
    File.delete('test.yaml') if File.exist?'test.yaml'
  end


  it "should have correct commands when no key" do
    Kernel.stub(:path).and_return('test')
    Kernel.stub(:get_key).and_return(lambda { |cmd| "key '': 62700332 documents" })

    args = {
        hadoop_src: 'solrindex/test',
        copy_dst: '/copy_to/test',
        max_merge_size: '100Gb',
        dst_distribution:
            ['/data/a/solr/test']
    }

    @manager = SolrIndexManager.new(args)
    commands = @manager.get_commands
    commands.size.should == 8
    first = commands.first
    first.folders.should == ["/copy_to/test/part-r-1", "/copy_to/test/part-r-2", "/copy_to/test/part-r-3", "/copy_to/test/part-r-4"]
    first.merge_to.should == "/data/a/solr/test/data/index"
    first.result_folder_name.should == "part-r-1-part-r-4"
    first.hadoop_commands.map { |src, dst, size, key, prog_info| src }.should == ["solrindex/test/part-r-1", "solrindex/test/part-r-2", "solrindex/test/part-r-3", "solrindex/test/part-r-4"]
    first.hadoop_commands.map { |src, dst, size, key, prog_info| dst }.should == ["/copy_to/test/part-r-1", "/copy_to/test/part-r-2", "/copy_to/test/part-r-3", "/copy_to/test/part-r-4"]
    first.hadoop_commands.map { |src, dst, size, key, prog_info| key }.should == ["part-r-1", "part-r-2", "part-r-3", "part-r-4"]
  end

  it "should read config from yaml" do
    Kernel.stub(:path).and_return('test')

    args = {
        name: 'no_2012',
        hadoop_src: 'solrindex/#{name}',
        copy_dst: '/copy_to/#{name}',
        max_merge_size: '100Gb',
        dst_distribution:
            ['/data/a/solr/#{name}/#{key}']
    }
    file_name = "test.yaml"
    File.open(file_name, 'w:UTF-8') { |out| YAML::dump(args, out) }
    @manager = SolrIndexManager.new(file_name)

    @manager.opts[:name].should == 'no_2012'
    @manager.opts[:dst_distribution].should == ['/data/a/solr/#{name}/#{key}']
  end

  it "should use name template correct" do
    Kernel.stub(:path).and_return('no_2012')

    args = {
        name: 'no_2012',
        hadoop_src: 'solrindex/#{name}',
        copy_dst: '/copy_to/#{name}',
        max_merge_size: '100Gb',
        dst_distribution:
            ['/data/a/solr/#{name}/#{key}']
    }

    @manager = SolrIndexManager.new(args)
    commands = @manager.get_commands
    first = commands.first
    first.folders.should == ["/copy_to/no_2012/01", "/copy_to/no_2012/02", "/copy_to/no_2012/03", "/copy_to/no_2012/04"]
    first.merge_to.should == "/data/a/solr/no_2012/01-04/data/index"
    first.hadoop_commands.map { |src, dst, size, key, prog_info| src }.should == ["solrindex/no_2012/part-r-1", "solrindex/no_2012/part-r-2", "solrindex/no_2012/part-r-3", "solrindex/no_2012/part-r-4"]
    first.hadoop_commands.map { |src, dst, size, key, prog_info| dst }.should == ["/copy_to/no_2012/01", "/copy_to/no_2012/02", "/copy_to/no_2012/03", "/copy_to/no_2012/04"]
  end

  it 'should parse content from hadoop fs -ls' do
    Kernel.stub(:path).and_return('test_20110730')
    args = {
        hadoop_src: 'solrindex/test_20110730',
        copy_dst: '/copy_to/test_20110730',
        max_merge_size: '100Gb',
        dst_distribution:
            ['/data/a/solr/test_20110730/']
    }

    @manager = SolrIndexManager.new(args)
    list = @manager.get_files_with_info_from_hdfs('solrindex/test_20110730')
    random = Random.new(10)
    (1..20).each do |n|
      r = random.rand(10..50)
      list[n-1].to_s.should ==
          [
              "solrindex/test_20110730/part-r-#{n}",
              r*1024,
              "key '#{"%02d" % n.to_i}': 62700332 documents",
              "part-r-#{n}"
          ].to_s
    end
  end
end