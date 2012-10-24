REGISTER /usr/local/mongo-hadoop/mongo-2.9.2.jar
REGISTER /usr/local/mongo-hadoop/core/target/mongo-hadoop-core-1.0.0.jar
REGISTER /usr/local/mongo-hadoop/pig/target/mongo-hadoop-pig-1.0.0.jar
REGISTER /usr/local/mongo-hadoop/target/mongo-hadoop-1.0.0.jar

/* I must be set, or we can see duplicate values in MongoDB! */
set mapred.map.tasks.speculative.execution false

sent_counts = LOAD '/tmp/sent_counts.txt' AS (from:chararray, to:chararray, total:int);
STORE sent_counts INTO 'mongodb://localhost/agile_data.sent_counts' USING com.mongodb.hadoop.pig.MongoStorage;