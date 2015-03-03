package org.jlab.clara.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.StringTokenizer;

import org.jlab.clara.base.CException;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.excp.xMsgException;

import com.google.protobuf.ByteString;

/**
 * Utility class containing useful methods.
 *
 * @author gurjyan
 * @version 1.x
 * @since 1/31/15
 */
public class CUtility {

    /**
     * Constructs and returns CLARA specified canonical name.
     * For e.g. service name convention, i.e. host:container:engine
     *
     * @param host DPE host IP address
     * @param container Clara service container name
     * @param engine_name Clara service engine name
     * @return canonical name of the Clara service
     */
    public static String form_service_name(String host,
                                           String container,
                                           String engine_name)
            throws CException {

        try {
            return xMsgUtil.host_to_ip(host) +
                    ":" +
                    container +
                    ":" +
                    engine_name;
        } catch (xMsgException | SocketException e) {
            throw new CException(e.getMessage());
        }
    }

    /**
     * Constructs and returns CLARA specified canonical name.
     * For e.g. service name convention, i.e. host:container:engine
     *
     * @param container Clara service container name
     * @param engine_name Clara service engine name
     * @return canonical name of the Clara service
     */
    public static String form_service_name(String container,
                                           String engine_name)
            throws CException {

        try {
            return xMsgUtil.host_to_ip("localhost") +
                    ":" +
                    container +
                    ":" +
                    engine_name;
        } catch (xMsgException  | SocketException e) {
            throw new CException(e.getMessage());
        }
    }

    /**
     * Constructs and returns CLARA specified canonical name.
     * For e.g. service name convention, i.e. host:container:engine
     *
     * @param host DPE host IP address
     * @param container Clara service container name
     * @return canonical name of the Clara service
     */
    public static String form_container_name(String host,
                                           String container)
            throws CException {

        try {
            return xMsgUtil.host_to_ip(host) +
                    ":" +
                    container;
        } catch (xMsgException  | SocketException e) {
            throw new CException(e.getMessage());
        }
    }

    /**
     * Constructs and returns CLARA specified canonical name.
     * For e.g. service name convention, i.e. host:container:engine
     *
     * @param container Clara service container name
     * @return canonical name of the Clara service
     */
    public static String form_container_name(String container)
            throws CException {

        try {
            return xMsgUtil.host_to_ip("localhost") +
                    ":" +
                    container;
        } catch (xMsgException  | SocketException e) {
            throw new CException(e.getMessage());
        }
    }

    /**
     * Checks to see if the service is locally deployed
     * @param s_name service canonical name (dpe-ip:container:engine)
     *
     * @return true/false
     * @throws CException
     */
    public static Boolean isRemoteService(String s_name)
            throws CException {

        try{
        String s_host = xMsgUtil.getTopicDomain(s_name);
        for(String s:xMsgUtil.getLocalHostIps()){
            if(s.equals(s_host)) return true;
        }
        } catch (xMsgException | SocketException e) {
            throw new CException(e.getMessage());
        }

        return false;
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
                        String can = CUtility.form_service_name(dpe, container,st.nextToken());
                        or_can.append(can).append(",");
                    }
                    // remove the last character and add
                    // to the sub canonical composition
                   sub_can_comp.append(or_can.substring(0, or_can.capacity()-1)).append("+");

                // logical AND case. (a,b+&c)
                } else if (se.startsWith("&")){
                    String can = CUtility.form_service_name(dpe, container,remove_first(se));
                    sub_can_comp.append("&").append(can);

                // single engine case (a+b)
                } else {
                    String can = CUtility.form_service_name(dpe, container,se);
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

    public static String getDpeName(String serviceCanonicalName)
            throws CException {
        try {
            return xMsgUtil.getTopicDomain(serviceCanonicalName);
        } catch (xMsgException e) {
            throw new CException(e.getMessage());
        }
    }

    public static String getContainerCanonicalName(String serviceCanonicalName)
            throws CException {
        try {
            return xMsgUtil.getTopicDomain(serviceCanonicalName) +
                    ":" + xMsgUtil.getTopicSubject(serviceCanonicalName);
        } catch (xMsgException e) {
            throw new CException("wrong dpe and/or container");
        }
    }

    public static String getContainerName(String serviceCanonicalName)
            throws CException {
        try {
            return xMsgUtil.getTopicSubject(serviceCanonicalName);
        } catch (xMsgException e) {
            throw new CException(e.getMessage());
        }
    }

    public static String getEngineName(String serviceCanonicalName)
            throws CException {
        try {
            return xMsgUtil.getTopicType(serviceCanonicalName);
        } catch (xMsgException e) {
            throw new CException(e.getMessage());
        }
    }

    public static Boolean isCanonical(String name){
        return name.contains(":");
    }


    /**
     * Converts object into a byte array
     * @param object to be converted. Probably it must be serializable.
     * @return ByteString or null in case of error
     */
    public static ByteString O2B(Object object) {
        if (object instanceof byte[]) {
            return ByteString.copyFrom((byte[]) object);
        } else {
            try (ByteString.Output bs = ByteString.newOutput();
                 ObjectOutputStream out = new ObjectOutputStream(bs)) {
                out.writeObject(object);
                out.flush();
                return bs.toByteString();
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }
    }


    /**
     * Converts byte array into an Object, that can be cast into pre-known class object.
     * @param bytes the byte array
     * @return Object or null in case of error
     */
    public static Object B2O(ByteString bytes) {
        byte[] bb = bytes.toByteArray();
        try (ByteArrayInputStream bs = new ByteArrayInputStream(bb);
             ObjectInputStream in = new ObjectInputStream(bs)) {
            return in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
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


    /**
     * Returns IP address of the host
     * @param hostname the host
     * @return textual representation of the IP address
     */
    public static String getIPAddress(String hostname) {
        String host = null;
        try {
            // resolve the host name for IP address
            if(hostname.equalsIgnoreCase("localhost")){
                InetAddress address = InetAddress.getLocalHost();
                // host will always be in the form of IP address
                host =address.getHostAddress();
            } else {
                // find the IP address based on the host name
                InetAddress address = InetAddress.getByName(hostname);
                host = address.getHostAddress();
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return host;
    }
}
