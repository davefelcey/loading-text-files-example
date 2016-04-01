package com.datastax.example;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
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
import java.util.HashMap;

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
        // Get XML
        String filePath = "file://" + System.getProperty("user.dir") + File.separator + docId;
        SQLContext xmlSqlContext = new SQLContext(sc);
        StructType customSchema = new StructType(new StructField[] {
                new StructField("doc_id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("detail_id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("description", DataTypes.StringType, true, Metadata.empty()),
        });

        final DataFrame xmlDF = xmlSqlContext.read()
                .format("com.databricks.spark.xml")
                .option("rowTag", "course")
                .load(filePath);

        System.out.println("XML DataFrame");
        xmlDF.show(10);

        final DataFrame df = xmlDF.select("reg_num","place","title");

        final DataFrame xmlDFWithDocId;
        xmlDFWithDocId = xmlSqlContext.createDataFrame(df.javaRDD().map(row -> {
            return RowFactory.create(docId, row.get(0), row.get(1), row.get(2));
        }), customSchema);

        System.out.println("Updated XML DataFrame");
        xmlDFWithDocId.show(10);

        // Load into details table
        xmlDFWithDocId.write().mode(SaveMode.Append).format("org.apache.spark.sql.cassandra")
                .option("keyspace", "test")
                .option("table", "xml_details")
                .save();
    }

    public static void main(String [] args) {
        String docId = "sample.xml";
        SparkConf conf = new SparkConf(true)
                .setAppName("My application")
                .set("spark.driver.allowMultipleContexts", "true")
                .set("spark.cassandra.auth.username","cassandra")
                .set("spark.cassandra.auth.password","cassandra")
                .set("spark.cassandra.connection.host", "127.0.0.1");
        SparkContext sc = new SparkContext(conf);

        CassandraTest(sc, docId);
        XMLTest(sc, docId);
        LoadDetails(sc, docId);
    }
}
