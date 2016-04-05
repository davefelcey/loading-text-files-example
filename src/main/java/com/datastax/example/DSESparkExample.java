package com.datastax.example;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.*;
import java.util.HashMap;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

/**
 * Created by davidfelcey on 29/03/2016.
 */
public class DSESparkExample {
    public static void CassandraTest(SparkContext sc, String docId) {
        CassandraSQLContext sqlContext = new CassandraSQLContext(sc);
        DataFrame df = sqlContext.sql("SELECT data FROM test.xml_data where doc_id = '" + docId + "'");
        df.show();
    }

    public static void XMLTest(SparkContext sc, String fileName) {
        String filePath = "file://" + System.getProperty("user.dir") + File.separator + fileName;
        SQLContext sqlContext = new SQLContext(sc);
        HashMap<String, String> options = new HashMap<String, String>();
        options.put("rowTag", "course");
        options.put("path", filePath);
        options.put("failFast", "true");
        options.put("treatEmptyValuesAsNulls", "true");

        final DataFrame df = sqlContext.load("com.databricks.spark.xml", options);
        System.out.println("XML DataFrame - no schema");
        df.show(10);

        int result = df.select("reg_num").collect().length;
        System.out.println("Result: " + result);
    }

    public static void LoadDetails(SparkContext sc, String docId) {
        String filePath = "file://" + System.getProperty("user.dir") + File.separator + docId;
        SQLContext xmlSqlContext = new SQLContext(sc);
        StructType customSchema = new StructType(new StructField[]{
                new StructField("doc_id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("detail_id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("description", DataTypes.StringType, true, Metadata.empty()),
        });

        // Load XML from file
        final DataFrame xmlDF = xmlSqlContext.read()
                .format("com.databricks.spark.xml")
                .option("rowTag", "course")
                .load(filePath);

        System.out.println("XML DataFrame");
        xmlDF.show(10);

        // Select only required elements
        final DataFrame df = xmlDF.select("reg_num", "place", "title");

        // Map selected elements onto a new DataFrame with an additional column
        final DataFrame xmlDFWithDocId;
        xmlDFWithDocId = xmlSqlContext.createDataFrame(df.javaRDD().map(row -> {
            return RowFactory.create(docId, row.get(0), row.get(1), row.get(2));
        }), customSchema);

        System.out.println("Updated XML DataFrame");
        xmlDFWithDocId.show(10);

        // Load transformed DataFrame into details table
        xmlDFWithDocId.write().mode(SaveMode.Append).format("org.apache.spark.sql.cassandra")
                .option("keyspace", "test")
                .option("table", "xml_details")
                .save();
    }

    private static void WaitUntilAvailable(String fileToRead) {
        System.out.println("Waiting for new files in: " + fileToRead);

        try {
            FileChannel channel = new RandomAccessFile(fileToRead, "rw").getChannel();

            // Use the file channel to create a lock on the file.
            // This method blocks until it can retrieve the lock.
            FileLock lock = channel.lock();
            lock.release();
            channel.close();
        } catch (Exception e) {
            System.err.println(e);
        }
    }

    public static void ProcessXMLFiles(SparkContext sc, String inputDirectory, String outputDirectory) throws IOException {
        // Create watcher service to monitor directory
        WatchService watcher = FileSystems.getDefault().newWatchService();
        Path inDir = FileSystems.getDefault().getPath(inputDirectory);
        Path outDir = FileSystems.getDefault().getPath(outputDirectory);

        inDir.register(watcher, ENTRY_CREATE);

        for (; ; ) {
            // wait for key to be signaled
            System.out.println("Waiting for events");
            WatchKey key;
            try {
                key = watcher.take();
            } catch (InterruptedException x) {
                return;
            }

            for (WatchEvent<?> event : key.pollEvents()) {
                WatchEvent.Kind<?> kind = event.kind();

                System.out.println("Got event");

                // This key is registered only
                // for ENTRY_CREATE events,
                // but an OVERFLOW event can
                // occur regardless if events
                // are lost or discarded.
                if (kind == OVERFLOW) {
                    continue;
                }

                // The filename is the
                // context of the event.
                WatchEvent<Path> ev = (WatchEvent<Path>) event;
                Path fileName = ev.context();

                System.out.println(kind.name() + ": " + fileName);

                // Resolve the filename against the directory.
                Path filePath = inDir.resolve(fileName);

                // Process file
                System.out.format("Processing file %s%n", filePath);

                WaitUntilAvailable(filePath.toString());

                String fileURI = "file://" + filePath.toString();
                SQLContext sqlContext = new SQLContext(sc);
                HashMap<String, String> options = new HashMap<String, String>();
                options.put("rowTag", "course");
                options.put("path", fileURI);
                options.put("failFast", "true");
                options.put("treatEmptyValuesAsNulls", "true");

                final DataFrame df = sqlContext.load("com.databricks.spark.xml", options);
                System.out.println("XML DataFrame - no schema");
                df.show(10);

                int result = df.select("reg_num").collect().length;
                System.out.println("Result: " + result);

                // Move file to output directory
                Path newFile = Paths.get(outDir.toString(), fileName.toString());
                Files.move(filePath, newFile, REPLACE_EXISTING);
            }

            // Reset the key -- this step is critical if you want to
            // receive further watch events.  If the key is no longer valid,
            // the directory is inaccessible so exit the loop.
            boolean valid = key.reset();

            if (!valid) {
                break;
            }
        }
    }

    public static void main(String[] args) {
        String docId = "sample.xml";
        SparkConf conf = new SparkConf(true)
                .setAppName("My application")
                .set("spark.driver.allowMultipleContexts", "true")
                .set("spark.cassandra.auth.username", "cassandra")
                .set("spark.cassandra.auth.password", "cassandra")
                .set("spark.cassandra.connection.host", "127.0.0.1");
        SparkContext sc = new SparkContext(conf);

        // CassandraTest(sc, docId);
        // XMLTest(sc, docId);
        // LoadDetails(sc, docId);
        try {
            ProcessXMLFiles(sc, System.getProperty("user.dir"), System.getProperty("java.io.tmpdir"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
