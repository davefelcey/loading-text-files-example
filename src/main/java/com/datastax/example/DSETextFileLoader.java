package com.datastax.example;

import com.datastax.driver.core.*;
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
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by davidfelcey on 14/03/2016.
 */
public class DSETextFileLoader {
    private static final Logger logger = LoggerFactory.getLogger(DSETextFileLoader.class);
    private final Session session;
    private final Cluster cluster;

    public static class Tuple {
        public String name;
        public String value;

        public Tuple(String name, String value) {
            this.name = name;
            this.value = value;
        }
    };

    public DSETextFileLoader() {
        cluster = Cluster.builder()
                .addContactPoint("127.0.0.1")
                .build();
        session = cluster.connect("test");
    }

    // Cassandra functions

    public void storeDetails(String cql, Object... arguments) {
        session.execute(cql, arguments);
    }

    public void storeText(ByteBuffer textData, String id) {
        session.execute("INSERT INTO test.text_data ( id, data) values ( ?, ? )", id, textData);
    }

    public ByteBuffer getText(String id) throws UnsupportedEncodingException {
        ResultSet rows = session.execute("SELECT data FROM test.text_data WHERE id = ?", id);
        List<ByteBuffer> buffers = new ArrayList<ByteBuffer>();
        for (Row row : rows) {
            buffers.add(ByteBuffer.wrap(row.getString("data").getBytes("UTF-8")));
        }
        if (buffers.size() == 1) {
            return buffers.get(0);
        } else if (buffers.size() > 1) {
            throw new RuntimeException("More than one matching text data for id '" + id + "' found");
        }
        throw new RuntimeException("None matching text data for id '" + id + "' found");
    }

    public void shutDown() {
        session.close();
        cluster.close();
    }

    // File handling functions

    public void write(String location, ByteBuffer blob) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(blob.limit());
        int n = 0;
        buf.clear();
        buf.put(blob);
        buf.flip();

        RandomAccessFile file = new RandomAccessFile(location, "rw");
        FileChannel channel = file.getChannel();
        try {
            while (buf.hasRemaining()) {
                n += channel.write(buf);
            }
        } finally {
            channel.force(true);
            channel.close();
        }

        logger.info(String.format("Written %d bytes", n));
    }

    public ByteBuffer read(String location) throws IOException {
        RandomAccessFile file = new RandomAccessFile(location, "r");
        FileChannel channel = file.getChannel();
        ByteBuffer buf = ByteBuffer.allocate((int) channel.size());

        try {
            while (channel.read(buf) > 0) {
                buf.flip();
                buf.clear();
            }
        } finally {
            channel.force(true);
            channel.close();
        }
        return buf;
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

    // XML functions

    private String getXPath(Node node) {
        Node parent = node.getParentNode();
        if (parent == null) {
            return "";
        }
        return getXPath(parent) + "/" + node.getNodeName();
    }

    public List<Tuple> parseXML(ByteBuffer input) throws IOException, XPathExpressionException, XPathFactoryConfigurationException, ParserConfigurationException, SAXException {
        DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        InputSource inputSource = new InputSource(new StringReader(new String(input.array(), "UTF-8")));
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

    // Main

    public static void main(String[] args) throws ClassNotFoundException, IOException {
        DSETextFileLoader fileLoader = new DSETextFileLoader();
        final String KEY = "001";

        try {
            String userHome = System.getProperty("user.dir");
            String textFile = userHome + File.separator + "sample.xml";

            ByteBuffer input = fileLoader.read(textFile);
            fileLoader.storeText(input, KEY);
            ByteBuffer output = fileLoader.getText(KEY);
            fileLoader.write(textFile + ".new", output);

            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            fileLoader.shutDown();
        }
    }

}
