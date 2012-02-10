$LOAD_PATH << '.'
require 'lib/solr_index_manager'

args =
    {
        :simulate => true,
        :verify => false,
        :name => 'news20110820_all',
        :core_prefix => 'news_',
        :hadoop_src => 'solrindex/#{name}',
        :copy_dst => '/data/f/copy_to/#{name}',
        #            job_id: 'job_201107280750_0094',
        :max_merge_size => '150Gb',
        :dst_distribution =>
            ['/data/a/solr/#{name}/#{key}',
             '/data/b/solr/#{name}/#{key}',
             '/data/c/solr/#{name}/#{key}',
             '/data/d/solr/#{name}/#{key}',
             '/data/e/solr/#{name}/#{key}',
            ],
        :solr_version => "3.5.0",
        :solr_lib_path => "/usr/local/solr/apache-solr-3.5.0/example/webapps/WEB-INF/lib/",
        :sleep_time => 0.1
    }
manager = SolrIndexManager.new(ARGV[0] || args)
filename =  "#{manager.index_name}.running"
begin
  File.open(filename, 'w') { |f| f.write('running') }
  manager.go()
rescue Exception => ex
  File.delete(filename)
  manager.log.log '---- error -----' + "\n"
  manager.log.log ex.to_s + "\n"
  manager.log.log ex.backtrace.join("\n")
  raise ex
end
