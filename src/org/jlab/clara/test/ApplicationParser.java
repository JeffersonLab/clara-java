package org.jlab.clara.test;

import org.jlab.clara.util.CUtility;
import org.jlab.clara.util.XMLContainer;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.List;

/**
 * XML parser test
 *
 * @author gurjyan
 * @version 1.x
 * @since 4/9/15
 */
public class ApplicationParser {
    public static void main(String[] args) {

        try {
            Document doc = CUtility.getXMLDocument(args[0]);

            String[] serviceTags = {"dpe", "container", "engine", "pool"};
            List<XMLContainer> services = CUtility.parseXML(doc, "service", serviceTags);

            for (XMLContainer s : services) {
                System.out.println(s);
            }

            String[] applicationTags = {"composition", "data"};
            List<XMLContainer> application = CUtility.parseXML(doc, "application", applicationTags);

            for (XMLContainer a : application) {
                System.out.println(a);
            }


        } catch (ParserConfigurationException | IOException | SAXException e) {
            e.printStackTrace();
        }


    }
}
