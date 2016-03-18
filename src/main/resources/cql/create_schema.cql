// Create Keyspace
CREATE KEYSPACE IF NOT EXISTS test
WITH replication = {'class':'NetworkTopologyStrategy', 'DC1': 2};

// Create table for text data
CREATE TABLE IF NOT EXISTS test.text_data (
   id    text PRIMARY KEY,
   data  text
) WITH compression = { 'sstable_compression' : 'LZ4Compressor' };

CREATE TABLE IF NOT EXISTS test.details (
   reg_num  text PRIMARY KEY,
   title    text,
   building text
);
