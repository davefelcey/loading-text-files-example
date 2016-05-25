# Loading an XML file into Cassandra

This example shows how an XML file can be loaded into a Cassandra table 

These are the following steps to run the example. 
  
Startup at DSE;

  ```
  dse cassandra -k -f
  ```

Create Cassandra schema. In the project dir run the following command

  ```
  cqlsh -f 'src/main/resources/cql/create_schema.cql'
  ```

Build and run the project tests to create some sample data

  ```
  mvn test
  ```

View the inserted XML file. Using cqlsh run the following CQL commands
  
To output the extracted values in the details table enter

  ```sql
  select * from test.xml_data
  ```
         
To run the Spark job first build the Jar;

 ```
  mvn package
  ```
  
Then execute the following command to run the Spark job;

  ```
dse spark-submit --class com.datastax.example.DSESparkExample --master spark://127.0.0.1:7077 target/loading-xml-app-spark-job.jar spark://127.0.0.1:7077 127.0.0.1
  ```
