package com.datastax.example;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Paths.get;

/**
 * Created by davidfelcey on 14/03/2016.
 */
public class DSEXMLLoader {
    private static final Logger logger = LoggerFactory.getLogger(DSEXMLLoader.class);
    private final Session session;
    private final Cluster cluster;

    public DSEXMLLoader() {
        cluster = Cluster.builder()
                .addContactPoint("127.0.0.1")
                .withCredentials("cassandra", "cassandra")
                .build();
        session = cluster.connect("test");
    }

    public void execCQL(String cql, Object... arguments) {
        session.execute(cql, arguments);
    }

    // Cassandra functions

    public void clearXML() {
        session.execute("TRUNCATE TABLE test.xml_data");
    }

    public void storeXML(String xmlData, String id) {
        session.execute("INSERT INTO test.xml_data ( doc_id, data ) values ( ?, ? )", id, xmlData);
    }

    public String getXML(String id) throws UnsupportedEncodingException {
        ResultSet rows = session.execute("SELECT data FROM test.xml_data WHERE doc_id = ?", id);
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

    private Properties loadProperties(String fileName) {
        Properties prop = new Properties();
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
        try {
            prop.load(in);
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return prop;
    }

    private String getXPath(Node node) {
        Node parent = node.getParentNode();
        if (parent == null) {
            return "";
        }
        return getXPath(parent) + "/" + node.getNodeName();
    }

    // XML functions

    public List<Tuple> parseXML(String input) throws IOException, XPathExpressionException, XPathFactoryConfigurationException, ParserConfigurationException, SAXException {
        DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        InputSource inputSource = new InputSource(new StringReader(input));
        Document document = builder.parse(inputSource);

        // Get mappings

        Properties mappings = loadProperties("table-map.properties");
        StringBuffer exprList = new StringBuffer("");

        for (final String name : mappings.stringPropertyNames()) {
            exprList.append((exprList.length() == 0 ? "" : " | ") + name);
        }

        XPath xpath = XPathFactory.newInstance().newXPath();
        XPathExpression expression = xpath.compile(exprList.toString());
        NodeList nodes = (NodeList) expression.evaluate(document, XPathConstants.NODESET);
        List<Tuple> values = new ArrayList<Tuple>();

        // Add matched entries to list

        for (int i = 0; i < nodes.getLength(); i++) {
            String textValue = nodes.item(i).getTextContent();
            Node node = nodes.item(i);
            String path = getXPath(node);
            // logger.debug("Path: " + path + " value: " + textValue);
            values.add(new Tuple(mappings.getProperty(path), textValue));
        }

        return values;
    }

    public static class Tuple {
        public String name;
        public String value;

        public Tuple(String name, String value) {
            this.name = name;
            this.value = value;
        }
    }
}
