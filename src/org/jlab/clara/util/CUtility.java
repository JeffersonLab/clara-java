package org.jlab.clara.util;

import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.excp.xMsgException;

import java.net.SocketException;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.*;

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
            throws xMsgException {

        return xMsgUtil.host_to_ip(host) +
                ":" +
                container +
                ":" +
                engine_name;
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
            throws xMsgException {

        return xMsgUtil.host_to_ip("localhost") +
                ":" +
                container +
                ":" +
                engine_name;
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
            throws xMsgException {

        return xMsgUtil.host_to_ip(host) +
                ":" +
                container;
    }

    /**
     * Constructs and returns CLARA specified canonical name.
     * For e.g. service name convention, i.e. host:container:engine
     *
     * @param container Clara service container name
     * @return canonical name of the Clara service
     */
    public static String form_container_name(String container)
            throws xMsgException {

        return xMsgUtil.host_to_ip("localhost") +
                ":" +
                container;
    }

    /**
     * Checks to see if the service is locally deployed
     * @param s_name service canonical name (dpe-ip:container:engine)
     *
     * @return true/false
     * @throws xMsgException
     * @throws SocketException
     */
    public static Boolean isRemoteService(String s_name)
            throws xMsgException, SocketException {

        String s_host = xMsgUtil.getTopicDomain(s_name);
        for(String s:xMsgUtil.getLocalHostIps()){
            if(s.equals(s_host)) return true;
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
                                           String composition) throws xMsgException {

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
}
