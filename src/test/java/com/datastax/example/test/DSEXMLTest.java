package com.datastax.example.test;

import com.datastax.example.DSEXMLUtils;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactoryConfigurationException;
import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by davidfelcey on 14/03/2016.
 */
public class DSEXMLTest {
    public static final String DOC_ID = "sample.xml";
    private DSEXMLUtils fileLoader = null;

    private String getTestFile() {
        String userHome = System.getProperty("user.dir");
        String textFile = userHome + File.separator + "sample.xml";
        return textFile;
    }

    @org.junit.Before
    public void setUp() throws Exception {
        fileLoader = new DSEXMLUtils();

        // Initialise
        fileLoader.clearXML();
        fileLoader.execCQL("TRUNCATE TABLE test.xml_details");
    }

    @org.junit.After
    public void tearDown() throws Exception {
        if (fileLoader != null) {
            fileLoader.shutDown();
        }
    }

    @org.junit.Test
    public void loadXMLFileTest() throws ClassNotFoundException, IOException {
        try {
            String input = fileLoader.read(getTestFile());
            fileLoader.storeXML(input, DOC_ID);
            String output = fileLoader.getXML(DOC_ID);

            assertEquals(output, input);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            fileLoader.shutDown();
        }
    }
}