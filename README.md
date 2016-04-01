# Loading an XML file into Cassandra

This example shows how an XML file can be loaded into a Cassandra table 

These are the following steps to run the example. It is assumed that 
authentication has been turned on for Cassandra in these examples and the 
username/password is cassandra/cassandra (default). To do this just change the 
$DSE_HOME/resources/cassandra/conf/cassandra.yaml file and set the following 
parameters;

  ```
  authorizer: CassandraAuthorizer
  authenticator: PasswordAuthenticator
  ```
  
Startup at DSE;

  ```
  dse -u cassandra -p cassandra cassandra -k -f
  ```

Create Cassandra schema. In the project dir run the following command

  ```
  cqlsh -u cassandra -p cassandra -f 'src/main/resources/cql/create_schema.cql'
  ```

To start the web server, in another terminal run 

  ```
  mvn jetty:run
  ```

Build and run the project tests

  ```
  mvn test
  ```

To add an XML document from the command line run the command

  ```
  curl -v -H "Content-Type: application/xml" -H "DOC-ID: 003" -d @sample.xml http://localhost:8080/loading-xml-example/rest/addXML
  ```

View the inserted XML file and extracted file details. Using cqlsh run the following CQL commands
  
To output the extracted values in the details table enter

  ```sql
  select * from test.xml_details
  ```
To see the XML file contents enter
  
  ```sql 
  select data from test.xml_data
  ```
    
Look at the XML file size on the filesystem, it should be 345,116 bytes. Run the "nodetool flush" command 
to persist all memtable records to disk. Then run "nodetool cfstats test.text_data" to output details about 
the compression ratio for the table text_data
   
The 101 records persisted to disk are compressed because LZ4 compression has been specified
for the text_date table. The compression ratio should be about 0.11, in other words they are only taking
up 11% of their origional size.
   
**Note:** compression ratios will vary depending on the file contents

There is a simple performance testing script;

  ```
  pert-test.sh
  ```
  
This issues a number of HTTP requests using curl

To run the Spark job first build the Jar;

 ```
  mvn package
  ```
  
Then execute the following command to run the Spark job;

  ```
  dse -u cassandra -p cassandra spark-submit --packages com.databricks:spark-xml_2.10:0.3.2 --class com.datastax.example.DSESparkExample --master spark://127.0.0.1:7077 target/loading-xml-app-spark-job.jar spark://127.0.0.1:7077 127.0.0.1
  ```