package com.datastax.example;

import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactoryConfigurationException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * Created by davidfelcey on 23/03/2016.
 */
public class DSEXMLLoaderService {
    private DSEXMLLoader xmlLoader = null;

    public DSEXMLLoaderService() {
        xmlLoader = new DSEXMLLoader();
    }

    public void shutdown() {
        xmlLoader.shutDown();
    }

    public void loadXMLAndDetails(String xmlData, String key) throws IOException, XPathExpressionException, XPathFactoryConfigurationException, ParserConfigurationException, SAXException {
        // Load XML
        xmlLoader.storeXML(xmlData, key);

        // Load details
        List<DSEXMLLoader.Tuple> values = xmlLoader.parseXML(xmlData);
        String cql = "INSERT INTO test.xml_details ( doc_id, detail_id, name, description ) values ( ?, ?, ?, ? )";
        String id = null;
        String name = "";
        String description = "";

        for (DSEXMLLoader.Tuple t : values) {
            switch (t.name) {
                case "detail_id":
                    if (id == null) {
                        id = t.value;
                    } else {
                        xmlLoader.execCQL(cql, key, id, name, description);
                        id = null;
                    }
                    break;
                case "name":
                    name = t.value;
                    break;
                case "description":
                    description = t.value;
                    break;
                default:
                    throw new RuntimeException("Name not expected: " + t.name);
            }
        }
    }

    public String getXML(String key) throws UnsupportedEncodingException {
        return xmlLoader.getXML(key);
    }
}
