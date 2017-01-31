/*
 *   Copyright (c) 2016.  Jefferson Lab (JLab). All rights reserved. Permission
 *   to use, copy, modify, and distribute  this software and its documentation for
 *   educational, research, and not-for-profit purposes, without fee and without a
 *   signed licensing agreement.
 *
 *   IN NO EVENT SHALL JLAB BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL
 *   INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING
 *   OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF JLAB HAS
 *   BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 *   JLAB SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 *   THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 *   PURPOSE. THE CLARA SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY,
 *   PROVIDED HEREUNDER IS PROVIDED "AS IS". JLAB HAS NO OBLIGATION TO PROVIDE
 *   MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 *   This software was developed under the United States Government license.
 *   For more information contact author at gurjyan@jlab.org
 *   Department of Experimental Nuclear Physics, Jefferson Lab.
 */

package org.jlab.clara.util.xml;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * XML parser test.
 *
 * @author gurjyan
 * @version 4.x
 * @since 4/9/15
 */
public final class ApplicationParser {

    private ApplicationParser() { }

    public static void main(String[] args) {
        try {
            Document doc = getXMLDocument(args[0]);

            String[] serviceTags = {"dpe", "container", "engine", "pool"};
            List<XMLContainer> services = parseXML(doc, "service", serviceTags);

            for (XMLContainer s : services) {
                System.out.println(s);
            }

            String[] appTags = {"composition", "data"};
            List<XMLContainer> app = parseXML(doc, "application", appTags);

            for (XMLContainer a : app) {
                System.out.println(a);
            }
        } catch (ParserConfigurationException | IOException | SAXException e) {
            e.printStackTrace();
        }
    }

    public static Document getXMLDocument(String fileName)
            throws ParserConfigurationException, IOException, SAXException {
        File fXmlFile = new File(fileName);
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        Document doc = dBuilder.parse(fXmlFile);
        doc.getDocumentElement().normalize();
        return doc;
    }

    /**
     * Parser for the XML having a structure:
     * <pre>
     * {@literal
     * <containerTag>
     *   <tag>value</tag>
     *   .....
     *   <tag>value</tag>
     * </containerTag>
     * ....
     * <containerTag>
     *   <tag>value</tag>
     *   .....
     *   <tag>value</tag>
     * </containerTag>
     * }
     * </pre>.
     *
     * @param doc          XML document object
     * @param containerTag first container tag
     * @param tags         tag names
     * @return list of list of tag value pairs
     */
    public static List<XMLContainer> parseXML(Document doc,
                                              String containerTag,
                                              String[] tags) {
        List<XMLContainer> result = new ArrayList<>();
        NodeList nList = doc.getElementsByTagName(containerTag);
        for (int temp = 0; temp < nList.getLength(); temp++) {
            Node nNode = nList.item(temp);

            if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                XMLContainer container = new XMLContainer();
                Element eElement = (Element) nNode;
                for (String tag : tags) {
                    NodeList tElements = eElement.getElementsByTagName(tag);
                    if (tElements.getLength() > 0) {
                        String value = eElement.getElementsByTagName(tag).item(0).getTextContent();
                        XMLTagValue tv = new XMLTagValue(tag, value);
                        container.addTagValue(tv);
                    }
                }
                result.add(container);
            }
        }
        return result;
    }
}
