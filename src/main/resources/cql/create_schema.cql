// Create Keyspace
DROP KEYSPACE test;

CREATE KEYSPACE IF NOT EXISTS test
WITH replication = {'class':'NetworkTopologyStrategy', 'Analytics': 1};

// Create table for text data
CREATE TABLE IF NOT EXISTS test.xml_data (
   doc_id text,
   interval timestamp,
   created timestamp,
   status text,
   data text,
   PRIMARY KEY((interval), status, doc_id)
) WITH compression = { 'sstable_compression' : 'LZ4Compressor' };

CREATE TABLE IF NOT EXISTS test.xml_details (
   doc_id text,
   detail_id text,
   name text,
   description text,
   PRIMARY KEY((doc_id, detail_id))
);
