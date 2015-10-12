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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.SocketException;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.util.CConstants;
import org.jlab.clara.util.xml.XMLContainer;
import org.jlab.clara.util.xml.XMLTagValue;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import sun.jvm.hotspot.ui.tree.CStringTreeNodeAdapter;

/**
 * Extra helper methods for Clara orchestrator and services.
 */
@ParametersAreNonnullByDefault
public final class ClaraUtil {

    private ClaraUtil() { }

    /**
     * Regex to validate a full canonical name.
     * Groups are used to separate each component.
     * <p>
     * A canonical name should have any of the following structures:
     * <pre>
     * {@literal <host>%<port>_<language>}
     * {@literal <host>%<port>_<language>:<container>}
     * {@literal <host>%<port>_<language>:<container>:<engine>}
     * </pre>
     */
    public static final Pattern CANONICAL_NAME_PATTERN =
            Pattern.compile("^([^:_ ]+(%\\d*)+_(java|python|cpp))(:(\\w+)(:(\\w+))?)?$");


    /**
     * Checks if the given name is a proper Clara canonical name.
     * <p>
     * A canonical name should have any of the following structures:
     * <pre>
     * {@literal <host>%<port>_<language>}
     * {@literal <host>%<port>_<language>:<container>}
     * {@literal <host>%<port>_<language>:<container>:<engine>}
     * </pre>
     *
     * @param name the name to be checked
     */
    public static boolean isCanonicalName(String name) {
        Matcher matcher = CANONICAL_NAME_PATTERN.matcher(name);
        return matcher.matches();
    }

    public static String getDpeName(String canonicalName) throws ClaraException {
        if(!isCanonicalName(canonicalName)) {
            throw new ClaraException("Clara-Error: not a canonical name");
        }
        xMsgTopic topic = xMsgTopic.wrap(canonicalName);
        return topic.domain();
    }

    public static String getContainerName(String canonicalName) throws ClaraException {
        if(!isCanonicalName(canonicalName)) {
            throw new ClaraException("Clara-Error: not a canonical name");
        }
        xMsgTopic topic = xMsgTopic.wrap(canonicalName);
        return topic.subject();
    }

    public static String getEngineName(String canonicalName) throws ClaraException {
        if(!isCanonicalName(canonicalName)) {
            throw new ClaraException("Clara-Error: not a canonical name");
        }
        xMsgTopic topic = xMsgTopic.wrap(canonicalName);
        return topic.type();
    }

    public static String getDpeHost(String canonicalName) throws ClaraException {
      if(!isCanonicalName(canonicalName)) {
          throw new ClaraException("Clara-Error: not a canonical name");
      }
        String dpeName = getDpeName(canonicalName);
        StringTokenizer st = new StringTokenizer(dpeName,CConstants.PRXHOSTPORT_SEP);
        if(st.countTokens()!=2){
            throw new ClaraException("Clara-Error: malformed name of a DPE");
        }
        return st.nextToken();
    }

    public static int getDpePort(String canonicalName) throws ClaraException {
      if(!isCanonicalName(canonicalName)) {
          throw new ClaraException("Clara-Error: not a canonical name");
      }
        String dpeName = getDpeName(canonicalName);
        StringTokenizer st = new StringTokenizer(dpeName,CConstants.PRXHOSTPORT_SEP);
        if(st.countTokens()!=2){
            throw new ClaraException("Clara-Error: malformed name of a DPE");
        }
        st.nextToken();
        String dpl = st.nextToken();
        String p = dpl.substring(0, dpl.indexOf(CConstants.LANG_SEP));
        return Integer.parseInt(p);
    }

    public static String getDpeLang(String canonicalName) throws ClaraException {
      if(!isCanonicalName(canonicalName)) {
          throw new ClaraException("Clara-Error: not a canonical name");
      }
        String dpeName = getDpeName(canonicalName);
         return dpeName.substring(dpeName.indexOf(CConstants.LANG_SEP));
    }

    /**
     * Helps creating a set of engine data types.
     *
     * @param dataTypes all the data types
     * @return a set with the data types
     */
    public static Set<EngineDataType> buildDataTypes(EngineDataType... dataTypes) {
        Set<EngineDataType> set = new HashSet<EngineDataType>();
        for (EngineDataType dt : dataTypes) {
            set.add(dt);
        }
        return set;
    }


    /**
     * Returns the stack trace of a exception as a string.
     * @param e an exception
     * @return a string with the stack trace of the exception
     */
    public static String reportException(Throwable e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }

    /**
     *     Converts exception stack trace to a string
     * <p>
     * @param e exception
     * @return String of the stack trace
     */
    public static String stack2str(Exception e) {
        try {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            return sw.toString();
        }
        catch(Exception e2) {
            return "bad stack";
        }
    }


    public static Boolean isHostLocal(String hostName)
            throws SocketException {
        for(String s: xMsgUtil.getLocalHostIps()){
            if(s.equals(hostName)) return true;
        }
        return false;
    }

    /**
     * Checks to see if the service is locally deployed
     * @param serviceName service canonical name (dpe-ip:container:engine)
     *
     * @return true/false
     * @throws org.jlab.clara.base.error.ClaraException
     */
    public static Boolean isRemoteService(String serviceName)
            throws ClaraException {

        try {
            xMsgTopic topic = xMsgTopic.wrap(serviceName);
            for (String s : xMsgUtil.getLocalHostIps()) {
                if (s.equals(topic.domain())) {
                    return false;
                }
            }
        } catch (SocketException e) {
            throw new ClaraException(e.getMessage());
        }

        return true;
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
        return s.substring(0, s.length() - 1);
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

    public static  xMsgTopic buildTopic(Object... args) {
        StringBuilder topic  = new StringBuilder();
        topic.append(args[0]);
        for (int i = 1; i < args.length; i++) {
            topic.append(CConstants.TOPIC_SEP);
            topic.append(args[i]);
        }
        return xMsgTopic.wrap(topic.toString());
    }


    public static String buildData(Object... args) {
        StringBuilder topic  = new StringBuilder();
        topic.append(args[0]);
        for (int i = 1; i < args.length; i++) {
            topic.append(CConstants.DATA_SEP);
            topic.append(args[i]);
        }
        return topic.toString();
    }


}
