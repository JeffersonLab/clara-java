package org.jlab.clara.util.xml;

import org.jlab.clara.util.ClaraUtil;
import org.jlab.clara.util.xml.XMLContainer;
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
            Document doc = ClaraUtil.getXMLDocument(args[0]);

            String[] serviceTags = {"dpe", "container", "engine", "pool"};
            List<XMLContainer> services = ClaraUtil.parseXML(doc, "service", serviceTags);

            for (XMLContainer s : services) {
                System.out.println(s);
            }

            String[] applicationTags = {"composition", "data"};
            List<XMLContainer> application = ClaraUtil.parseXML(doc, "application", applicationTags);

            for (XMLContainer a : application) {
                System.out.println(a);
            }


        } catch (ParserConfigurationException | IOException | SAXException e) {
            e.printStackTrace();
        }


    }
}
