REGISTER /usr/local/pig-0.10.0/contrib/piggybank/java/piggybank.jar
REGISTER /usr/local/pig-0.10.0/build/ivy/lib/Pig/avro-1.5.3.jar
REGISTER /usr/local/pig-0.10.0/build/ivy/lib/Pig/json-simple-1.1.jar

/* This gives us a shortcut to call our Avro storage function */
DEFINE AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();
rmf '/tmp/sent_counts.txt'

-- Load our emails using Pig's AvroStorage User Defined Function (UDF)
-- The below command from the website is *wrong*
-- messages = LOAD '/tmp/my_emails.avro' USING AvroStorage();
-- Use this instead so that the subsequent commands have a clue as to what you are talking about
messages = LOAD '/tmp/myemails.avro' USING AvroStorage() AS (thread_id:int, date:chararray, from:chararray, to:chararray, subject:chararray);


-- Filter out missing from/to addresses to limit our processed data to valid records
messages = FILTER messages BY from IS NOT NULL AND to IS NOT NULL;

-- Project out all unique combinations of from/to in this message, then lowercase the emails
-- Note: Bug here if dupes with different case in one email.  Do in a foreach/generate.
smaller = FOREACH messages GENERATE FLATTEN(from), FLATTEN(to) AS to;
pairs = FOREACH smaller GENERATE LOWER(from) AS from, LOWER(to) AS to;

-- Now group the data by unique pairs of addresses, take a count, and store as text in /tmp
froms = GROUP pairs BY (from, to);
sent_counts = FOREACH froms GENERATE FLATTEN(group) AS (from, to), COUNT(pairs) AS total;
sent_counts = ORDER sent_counts BY total;
STORE sent_counts INTO '/tmp/sent_counts.txt';