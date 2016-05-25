package com.datastax.example;

import com.datastax.spark.connector.japi.CassandraRow;
import com.ximpleware.AutoPilot;
import com.ximpleware.VTDGen;
import com.ximpleware.VTDNav;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

/**
 * Created by davidfelcey on 29/03/2016.
 */
public class DSESparkExample {
    public static final String DOC_ID = "sample.xml";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf(true)
                .setAppName("My application")
                .set("spark.cassandra.connection.host", "127.0.0.1");
        SparkContext sc = new SparkContext(conf);

        try {
            ProcessXML(sc, DOC_ID);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void ProcessXML(SparkContext sc, String id) {
        // Get details from XML documents in current interval and status is "loaded"
        JavaRDD<Detail> detailsRDD =
                javaFunctions(sc).cassandraTable("test", "xml_data")
                .select("data").where("interval=?", DSEXMLUtils.GetInterval(), "status=?", "loaded")
                .flatMap(new FlatMapFunction<CassandraRow, Detail>() {
                    @Override
                    public List<Detail> call(CassandraRow cassandraRow) throws Exception {
                        String xml = cassandraRow.getString(0);
                        List<Detail> details = new ArrayList<Detail>();

                        VTDGen vg = new VTDGen();
                        vg.setDoc(xml.getBytes(StandardCharsets.UTF_8));
                        vg.parse(true);
                        VTDNav vn = vg.getNav();
                        AutoPilot ap = new AutoPilot();
                        ap.bind(vn);
                        ap.selectXPath("/root/course/reg_num");
                        int result = -1;

                        // For each entity
                        while ((result = ap.evalXPath()) != -1) {
                            System.out.println("Element name: " + vn.toString(result));
                            int t = vn.getText(); // get the index of the text (char data or CDATA)
                            if (t != -1) {
                                String v = vn.toNormalizedString(t);
                                System.out.println("Value: " + v);
                                details.add(new Detail(id, v, null, null));
                            }
                        }

                        return details;
                    }
                });


        // Update details table with new entries
        javaFunctions(detailsRDD).writerBuilder("test", "xml_details", mapToRow(Detail.class)).saveToCassandra();

        // Change status on XML documents table to "processed"
        // TODO
    }
}