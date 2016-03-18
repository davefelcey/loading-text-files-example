package com.datastax.example.test;

import com.datastax.example.DSETextFileLoader;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactoryConfigurationException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by davidfelcey on 14/03/2016.
 */
public class DSETextFileLoaderTest {
    private String textFile;
    private DSETextFileLoader fileLoader = null;

    @org.junit.Before
    public void setUp() throws Exception {
        String userHome = System.getProperty("user.dir");
        textFile = userHome + File.separator + "sample.xml";

        fileLoader = new DSETextFileLoader();
    }

    @org.junit.After
    public void tearDown() throws Exception {
        if (fileLoader != null) {
            fileLoader.shutDown();
        }
    }

    @org.junit.Test
    public void testLoadFileAndDetails() throws IOException, XPathExpressionException, XPathFactoryConfigurationException, ParserConfigurationException, SAXException {
        ByteBuffer input = fileLoader.read(textFile);
        fileLoader.storeText(input, Integer.toString(999));
        List<DSETextFileLoader.Tuple> values = fileLoader.parseXML(input);
        String cql = "INSERT INTO test.details ( reg_num, title, building ) values ( ?, ?, ? )";
        String regNum = null;
        String title = "";
        String building = "";

        for(int i = 0; i < values.size(); i++) {
            DSETextFileLoader.Tuple t = values.get(i);
            if(t.name.equals("reg_num")) {
                if (regNum == null) {
                    regNum = t.value;
                } else {
                    fileLoader.storeDetails(cql, regNum, title, building);
                    regNum = null;
                }
            } else if(t.name.equals("title")) {
                title = t.value;
            } else if(t.name.equals("building")) {
                building = t.value;
            } else {
                throw new RuntimeException("Name not expected: " + t.name);
            }
        }

    }

    @org.junit.Test
    public void testMultipleFiles() throws IOException {
        ByteBuffer input = fileLoader.read(textFile);

        for (int i = 0; i < 100; i++) {
            fileLoader.storeText(input, Integer.toString(i));
        }
    }
}