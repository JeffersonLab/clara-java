/*
 * Copyright (C) 2015. Jefferson Lab, CLARA framework (JLAB). All Rights Reserved.
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for educational, research, and not-for-profit purposes,
 * without fee and without a signed licensing agreement.
 *
 * Author Vardan Gyurjyan
 * Department of Experimental Nuclear Physics, Jefferson Lab.
 *
 * IN NO EVENT SHALL JLAB BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL,
 * INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF
 * THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF JLAB HAS BEEN ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * JLAB SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE. THE CLARA SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY, PROVIDED
 * HEREUNDER IS PROVIDED "AS IS". JLAB HAS NO OBLIGATION TO PROVIDE MAINTENANCE,
 * SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 */

package org.jlab.clara.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.net.SocketException;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.jlab.clara.base.CException;
import org.jlab.clara.base.ClaraUtil;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * Utility class containing useful methods.
 *
 * @author gurjyan
 * @version 1.x
 * @since 1/31/15
 */
public class CUtility {

    public static Boolean isHostLocal(String hostName)
            throws SocketException {
        for(String s:xMsgUtil.getLocalHostIps()){
            if(s.equals(hostName)) return true;
        }
        return false;
    }

    /**
     * Checks to see if the service is locally deployed
     * @param serviceName service canonical name (dpe-ip:container:engine)
     *
     * @return true/false
     * @throws CException
     */
    public static Boolean isRemoteService(String serviceName)
            throws CException {

        try {
            xMsgTopic topic = xMsgTopic.wrap(serviceName);
            for (String s : xMsgUtil.getLocalHostIps()) {
                if (s.equals(topic.domain())) {
                    return false;
                }
            }
        } catch (SocketException e) {
            throw new CException(e.getMessage());
        }

        return true;
    }


    /**
     * <p>
     *    Parses the specified composition and replaces the engine names with
     *    a proper service canonical names, using specified dpe and container
     *    names.
     *    Note: This method assumes that all engines specified in the
     *    composition are deployed on a same dpe and the same container
     *
     * </p>
     * @param dpe dpe canonical name
     *            (IP address of the host where DPE is deployed)
     * @param container the name of the container
     * @param composition Clara composition string (non canonical)
     * @return canonical name of the application composition
     */
    public static String engineToCanonical(String dpe,
                                           String container,
                                           String composition) throws CException {

        // the container canonical name
        String contCanonName = ClaraUtil.formContainerName(dpe, container);

        // find branching compositions in supplied composition string
        StringTokenizer st = new StringTokenizer(composition, ";");

        // List of sub compositions, i.e. branched compositions
        List<String> sub_comps = new ArrayList<>();
        while(st.hasMoreTokens()){
            sub_comps.add(st.nextToken());
        }

        // final canonical composition string
        StringBuilder can_comp = new StringBuilder();

        // Go over the sub compositions
        for (String sb:sub_comps){

            // Find participating services engine names in the composition
            st = new StringTokenizer(sb, "+");

            // List of engine names within the sub composition
            List<String> se_list = new ArrayList<>();
            while(st.hasMoreTokens()){
                se_list.add(st.nextToken());
            }

            // Canonical sub composition string
            StringBuilder sub_can_comp = new StringBuilder();

            // Go over engine names with the sub composition
            for(String se:se_list){

                // Check to see if we have multiple
                // service outputs as an input to a
                // service, i.e. logical OR (a+c,b)
                if(se.contains(",")){
                    st = new StringTokenizer(se, ",");
                    StringBuilder or_can = new StringBuilder();
                    while(st.hasMoreTokens()) {
                        String can = ClaraUtil.formServiceName(contCanonName, st.nextToken());
                        or_can.append(can).append(",");
                    }
                    // remove the last character and add
                    // to the sub canonical composition
                    sub_can_comp.append(or_can.substring(0, or_can.capacity()-1)).append("+");

                    // logical AND case. (a,b+&c)
                } else if (se.startsWith("&")){
                    String can = ClaraUtil.formServiceName(contCanonName, remove_first(se));
                    sub_can_comp.append("&").append(can);

                    // single engine case (a+b)
                } else {
                    String can = ClaraUtil.formServiceName(contCanonName, se);
                    sub_can_comp.append(can);
                }
            }
            can_comp.append(sub_can_comp.toString()).append(";");
        }
        return remove_last(can_comp.toString());
    }

    public static String remove_first(String s) {
        if (s == null || s.length() == 0) {
            return s;
        }
        return s.substring(1, s.length());
    }

    public static String remove_last(String s) {
        if (s == null || s.length() == 0) {
            return s;
        }
        return s.substring(0, s.length()-1);
    }

    /**
     * Gets the current time and returns string representation of it.
     * @return string representing the current time.
     */
    public static String getCurrentTimeInH(){
        Format formatter = new SimpleDateFormat("HH:mm:ss MM/dd");
        return formatter.format(new Date());
    }

    /**
     * Gets the current time and returns string representation of it.
     * @return string representing the current time.
     */
    public static String getCurrentTime(){
        Format formatter = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        return formatter.format(new Date());
    }

    /**
     * Gets the current time and returns string representation of it.
     * @return string representing the current time.
     */
    public static String getCurrentTime(String format){
        Format formatter = new SimpleDateFormat(format);
        return formatter.format(new Date());
    }

    /**
     * Current time in milli-seconds.
     * @return current time in ms.
     */
    public static long getCurrentTimeInMs(){
        return new GregorianCalendar().getTimeInMillis();
    }

    public static double getCpuUsage()
            throws MalformedObjectNameException, ReflectionException, InstanceNotFoundException {

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName name = ObjectName.getInstance("java.lang:type=OperatingSystem");
        AttributeList list = mbs.getAttributes(name, new String[]{"ProcessCpuLoad"});

        if (list.isEmpty()) {
            return Double.NaN;
        }

        Attribute att = (Attribute) list.get(0);
        Double value = (Double) att.getValue();

        if (value == -1.0) {
            return Double.NaN;
        }

        return ((int) (value * 1000) / 10.0);        // returns a percentage value with 1 decimal point precision
    }


    public static long getMemoryUsage() {
        return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    }


    /**
     * Returns the stack trace of a exception as a string
     * @param e an exception
     * @return a string with the stack trace of the exception
     */
    public static String reportException(Throwable e){
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }

    public static byte[] serialize(Object o) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        byte[] res = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(o);
            res = bos.toByteArray();
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException ex) {
                // ignore close exception
            }
            try {
                bos.close();
            } catch (IOException ex) {
                // ignore close exception
            }
        }
        return res;
    }

    public static Object deSerialize(byte[] ba) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(ba);
        ObjectInput in = null;
        Object o = null;
        try {
            in = new ObjectInputStream(bis);
            o = in.readObject();
        } finally {
            try {
                bis.close();
            } catch (IOException ex) {
                // ignore close exception
            }
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                // ignore close exception
            }
        }
        return o;
    }

    public static Document getXMLDocument(String fileName) throws ParserConfigurationException,
            IOException,
            SAXException {

        Document doc;

        File fXmlFile = new File(fileName);
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();

        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        doc = dBuilder.parse(fXmlFile);

        doc.getDocumentElement().normalize();

        return doc;

    }

    /**
     * parser for the XML having a structure:
     * <p/>
     * <containerTag2>
     * <tag>value</tag>
     * .....
     * <tag>value</tag>
     * </containerTag2>
     * ....
     * <containerTag2>
     * <tag>value</tag>
     * .....
     * <tag>value</tag>
     * </containerTag2>
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
                    String value = eElement.getElementsByTagName(tag).item(0).getTextContent();
                    XMLTagValue tv = new XMLTagValue(tag, value);
                    container.addTagValue(tv);
                }
                result.add(container);
            }
        }
        return result;
    }

    public static void sleep(int s) {
        try {
            Thread.sleep(s);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static String getJSetElementAt(Set<String> set, int index){
        int ind = -1;
        for(String s:set){
            ind++;
            if(index==ind)return s;
        }
        return null;
    }

    public static String removeFirst(String input, String firstCharacter){
        input = input.startsWith(firstCharacter) ? input.substring(1) : input;
        return input;
    }

    public static void testRegexMatch(Matcher m){
        System.out.println("=============regex============ \n");
        while(m.find()) {
            System.out.println(m.group());
        }
        m.reset();
        System.out.println(m.matches());
        System.out.println("=============regex============ \n");
    }



}
