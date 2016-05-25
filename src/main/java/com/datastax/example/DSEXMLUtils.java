package com.datastax.example;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Paths.get;

/**
 * Created by davidfelcey on 14/03/2016.
 */
public class DSEXMLUtils {
    private static final Logger logger = LoggerFactory.getLogger(DSEXMLUtils.class);
    private final Session session;
    private final Cluster cluster;

    public DSEXMLUtils() {
        cluster = Cluster.builder()
                .addContactPoint("127.0.0.1")
                .withCredentials("cassandra", "cassandra")
                .build();
        session = cluster.connect("test");
    }

    public void execCQL(String cql, Object... arguments) {
        session.execute(cql, arguments);
    }

    public static  Date GetInterval() {
        long day = 24 * 60 * 60 * 1000;
        return new Date((System.currentTimeMillis() / day) * day);
    }

    // Cassandra functions

    public void clearXML() {
        session.execute("TRUNCATE TABLE test.xml_data");
    }

    public void storeXML(String xmlData, String id) {
        session.execute("INSERT INTO test.xml_data ( doc_id, created, interval, status, data ) values ( ?, ?, ?, ?, ? )", id, new Date(), GetInterval(), "loaded", xmlData);
    }

    public String getXML(String id) throws UnsupportedEncodingException {
        ResultSet rows = session.execute("SELECT data FROM test.xml_data WHERE interval = ? and status = ? and doc_id = ?", GetInterval(), "loaded", id);
        String result = null;
        Row row = rows.one();

        if (row != null) {
            result = row.getString("data");
        }

        return result;
    }

    public void shutDown() {
        session.close();
        cluster.close();
    }

    public void write(String location, String data) throws IOException {
        Files.write(get(location), data.getBytes(UTF_8));
    }

    // File handling functions

    public String read(String location) throws IOException {
        return new String(Files.readAllBytes(get(location)), UTF_8);
    }
}
