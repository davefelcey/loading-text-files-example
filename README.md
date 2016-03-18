# Loading an XML file into Cassandra

This example shows how an XML file can be loaded into a Cassandra table 

Steps to test;

Create Cassandra schema. In the project dir run the following command

  ```
  cqlsh -f 'src/resources/cql/create_schema.cql'
  ```

Build and run the project tests

  ```
  mvn test
  ```

View the inserted XML file and extracted file details. Using cqlsh run the following CQL commands
  
To output the extracted values in the details table enter

  ```sql
  select * from test.details
  ```
To see the XML file contents enter
  
  ```sql 
  select data from test.text_data
  ```
    
Look at the XML file size on the filesystem, it should be 345,116 bytes. Run the "nodetool flush" command 
to persist all memtable records to disk. Then run "nodetool cfstats test.text_data" to output details about 
the compression ratio for the table text_data
   
The 101 records persisted to disk are compressed because LZ4 compression has been specified
for the text_date table. The compression ratio should be about 0.11, in other words they are only taking
up have 11% of their origional size.
   
**Note:** compression ratios will vary depending on the file contents


