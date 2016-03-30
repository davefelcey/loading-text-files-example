package com.datastax.example;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
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
    public static void CassandraTest(SparkContext sc) {
        CassandraSQLContext sqlContext = new CassandraSQLContext(sc);
        DataFrame df = sqlContext.sql("SELECT data FROM test.xml_data");
        df.show();
    }

    public static void XMLTest(SparkContext sc) {
        String filePath = "file://" + System.getProperty("user.dir") + File.separator + "sample.xml";
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

    public static void LoadDetails(SparkContext sc) {
        // Get XML
        final String DOC_ID = "999";
        String filePath = "file://" + System.getProperty("user.dir") + File.separator + "sample.xml";
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
            return RowFactory.create(DOC_ID, row.get(0), row.get(1), row.get(2));
        }), customSchema);

        System.out.println("Updated XML DataFrame");
        xmlDFWithDocId.show(10);

        // Load into details table
        xmlDFWithDocId.write().format("org.apache.spark.sql.cassandra")
                .option("keyspace", "test")
                .option("table", "xml_details")
                .save();
    }

    public static void main(String [] args) {
        SparkConf conf = new SparkConf(true)
                .setAppName("My application")
                .set("spark.driver.allowMultipleContexts", "true")
                .set("spark.cassandra.auth.username","cassandra")
                .set("spark.cassandra.auth.password","cassandra")
                .set("spark.cassandra.connection.host", "127.0.0.1");
        SparkContext sc = new SparkContext(conf);

        CassandraTest(sc);
        XMLTest(sc);
        LoadDetails(sc);
    }
}
