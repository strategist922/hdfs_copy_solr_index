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
            ]
    }


#require 'yaml'
#File.open("go.yaml", 'w:UTF-8') { |out| YAML::dump(args, out) }

manager = SolrIndexManager.new(args)
manager.go()
