register '/usr/local/wonderdog/target/*.jar';
register '/usr/local/elasticsearch/lib/*.jar';

define ElasticSearch com.infochimps.elasticsearch.pig.ElasticSearchStorage();

sent_counts = LOAD '/tmp/sent_counts.txt' AS (from:chararray, to:chararray, total:int);
STORE sent_counts INTO 'es://sent_counts/sent_counts?json=false&size=1000' USING ElasticSearch('/usr/local/elasticsearch/config/elasticsearch.yml', '/usr/local/elasticsearch/plugins');
--STORE sent_counts INTO 'es://sent_counts/sent_counts?json=false&size=1000' USING ElasticSearch('/foobar/elasticsearch.yml', '/usr/local/elasticsearch/plugins');