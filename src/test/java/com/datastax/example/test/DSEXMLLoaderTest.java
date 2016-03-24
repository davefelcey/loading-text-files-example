package com.datastax.example.test;

import com.datastax.example.DSEXMLLoader;
import com.datastax.example.DSEXMLLoaderService;
import com.datastax.example.DSEXMLLoaderWS;
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
import java.nio.file.Files;

import static org.junit.Assert.assertSame;

/**
 * Created by davidfelcey on 14/03/2016.
 */
public class DSEXMLLoaderTest {
    private static final String DOC_ID = "001";
    private DSEXMLLoader fileLoader = null;

    private String getTestFile() {
        String userHome = System.getProperty("user.dir");
        String textFile = userHome + File.separator + "sample.xml";
        return textFile;
    }

    @org.junit.Before
    public void setUp() throws Exception {
        fileLoader = new DSEXMLLoader();

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
    public void loadXMLFileAndDetailsTest() throws IOException, XPathExpressionException, XPathFactoryConfigurationException, ParserConfigurationException, SAXException {
        String input = fileLoader.read(getTestFile());
        DSEXMLLoaderService service = new DSEXMLLoaderService();
        service.loadXMLAndDetails(input, DOC_ID);
        String output = fileLoader.getXML(DOC_ID);

        // assertSame(output, input);
    }

    @org.junit.Test
    public void loadXMLFileTest() throws ClassNotFoundException, IOException {
        DSEXMLLoader fileLoader = new DSEXMLLoader();

        try {
            String input = fileLoader.read(getTestFile());
            fileLoader.storeXML(input, DOC_ID);
            String output = fileLoader.getXML(DOC_ID);

            // assertSame(output, input);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            fileLoader.shutDown();
        }
    }

    @org.junit.Test
    public void loadXMLViaRESTTest() throws IOException {
        String input = fileLoader.read(getTestFile());
        final String BASE_URI = "http://localhost:8080/loading-xml-example/rest/";
        ClientConfig config = new com.sun.jersey.api.client.config.DefaultClientConfig();
        String output = null;
        Client client = Client.create(config);
        WebResource webResource = client.resource(BASE_URI).path("addXML");

        // Post
        webResource.header(DSEXMLLoaderWS.HEADER_ID, DOC_ID).type(javax.ws.rs.core.MediaType.APPLICATION_XML).post(String.class, input);

        // Get
        ClientResponse response = webResource.accept(javax.ws.rs.core.MediaType.APPLICATION_XML).get(ClientResponse.class);

        if (response.getStatus() != ClientResponse.Status.ACCEPTED.getStatusCode()) {
            throw new RuntimeException("Failed : HTTP error code : "
                    + response.getStatus());
        } else {
            output = response.getEntity(String.class);
        }

        fileLoader.write(System.getProperty("user.dir") + File.separator + "input.xml", input);
        fileLoader.write(System.getProperty("user.dir") + File.separator + "output.xml", output);

        // assertSame(output, input);

        // Clean up
        client.destroy();
    }
}