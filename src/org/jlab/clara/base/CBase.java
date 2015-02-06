package org.jlab.clara.base;

import org.jlab.coda.xmsg.core.xMsg;
import org.jlab.coda.xmsg.core.xMsgCallBack;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistrationData;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgAddress;
import org.jlab.coda.xmsg.net.xMsgConnection;

import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * <p>
 *   Clara base class
 * </p>
 *
 * @author gurjyan
 * @version 1.x
 * @since 1/31/15
 */
public class CBase extends xMsg {

    private String name =
            xMsgConstants.UNDEFINED.getStringValue();
    private xMsgConnection node_connection = null;


    /**
     * Constructor
     * @param feHost the host name of the Clara FE
     * @throws xMsgException
     */
    public CBase(String feHost) throws xMsgException {
        super(feHost);

        // Create a socket connections to the xMsg node.
        // This is a local DPE, and uses default port number.
        xMsgAddress address = new xMsgAddress("localhost");
        this.node_connection = connect(address);
    }

    /**
     * Constructor
     * @param dpeHost the host name of the Clara DPE of interest
     * @param feHost the host name of the Clara FE
     * @throws xMsgException
     */
    public CBase(String dpeHost, String feHost) throws xMsgException {
        super(feHost);

        // Create a socket connections to the xMsg node.
        // This is a local DPE, and uses default port number.
        xMsgAddress address = new xMsgAddress(dpeHost);
        this.node_connection = connect(address);
    }

    /**
     * Constructor
     * @param feHost the host name of the Clara FE
     * @param pool_size thread pool size for servicing subscription callbacks
     * @throws xMsgException
     */
    public CBase(String feHost,
                 int pool_size) throws xMsgException {
        super(feHost, pool_size);

        // Create a socket connections to the xMsg node.
        // This is a local DPE, and uses default port number.
        xMsgAddress address = new xMsgAddress("localhost");
        this.node_connection = connect(address);
    }

    /**
     * Constructor
     * @throws xMsgException
     */
    public CBase() throws xMsgException {
        super("localhost");

        // Create a socket connections to the xMsg node.
        // This is a local DPE, and uses default port number.
        xMsgAddress address = new xMsgAddress("localhost");
        this.node_connection = connect(address);
    }

    /**
     * Constructor
     * @param pool_size thread pool size for servicing subscription callbacks
     * @throws xMsgException
     */
    public CBase(int pool_size) throws xMsgException {
        super("localhost", pool_size);

        // Create a socket connections to the xMsg node.
        // This is a local DPE, and uses default port number.
        xMsgAddress address = new xMsgAddress("localhost");
        this.node_connection = connect(address);
    }


    /**
     * Sets the name of this component
     * @param name the name of this component
     */
    public void setName(String name){
        this.name = name;
    }

    /**
     * <p>
     *     Returns given name of this component
     * </p>
     * @return name of the component
     */
    public String getName(){
        return name;
    }

    /**
     * <p>
     *    Parses composition field of the transient data
     *    and returns the list of services output linked
     *    to this service, i.e. that are getting output
     *    data of this service.
     *    Attention: service name CAN NOT appear twice
     *               in the composition.
     * </p>
     *
     * @param service_name the name of the service
     *                     for which we find input links
     * @param composition the string of the composition
     * @param link_direction 0 = input-inked, >0 = output-linked
     * @return the list containing names of linked services
     */
    public List<String> parse_linked(String service_name,
                                     String composition,
                                     int link_direction) throws CException {

        // List of output service names
        List<String> out_list = new ArrayList<>();

        // List that contains composition elements
        List<String> elm_list = new ArrayList<>();

        StringTokenizer st = new StringTokenizer(composition,"+");
        while(st.hasMoreTokens()){
            elm_list.add(st.nextToken());
        }

        // List that contains service names
        List<String> elm2_list = new ArrayList<>();
        for (String s:elm_list){
            // remove  '&' from the service name
            // (e.g. 129.57.81.247:cont1:&Engine3 to 129.57.81.247:cont1:Engine3
            if(s.equals("&")){
                s = s.replace("&","");
            }
            elm2_list.add(s);
        }

        // See if the string contains this service name, and record the index,
        // and analyze index+1 element.
        // Note: multiple services can send to a single service, like: s1,s2+s3.
        // (this is the reason we use in:contains)
        int index = -1;
        for(String s:elm2_list){
            if(s.contains(service_name)){
                index = elm2_list.indexOf(s);
            }
        }
        if(index == -1) {
            throw new CException("Composition parsing exception. " +
                    "Service name can not be found in the composition.");
        } else {
            if(link_direction==0 && index>0) {
                // index of the next component in the composition
                index -= 1;
            } else {
                index += 1;
            }
            if(elm2_list.size() > index){
                String element = elm2_list.get(index);
                // the case to fan out the output of this service
                if(element.contains(",")){
                    StringTokenizer st1 = new StringTokenizer(element,",");
                    while(st1.hasMoreTokens()){
                        out_list.add(st1.nextToken());
                    }
                } else {
                    out_list.add(element);
                }
            }
        }
        return out_list;
    }

    /**
     * <p>
     *      Check to see in the composition this service
     *      is required to logically AND inputs before
     *      executing its service
     * </p>
     *
     * @param service_name in the composition
     * @param composition the string of the composition
     * @return true if component name is programmed
     *         as "&<service_name>"
     */
    public boolean is_log_and(String service_name,
                              String composition){
        String ac = "&" + service_name;

        // List that contains composition elements
        List<String> elm_list = new ArrayList<>();

        StringTokenizer st = new StringTokenizer(composition,"+");
        while(st.hasMoreTokens()){
            elm_list.add(st.nextToken());
        }

        for(String s: elm_list){
            if(s.equals(ac)){
                return true;
            }
        }
        return false;
    }

    /**
     * <p>
     *     Sends a request to the xMsg registration service,
     *     asking to return registration information of service/services
     *     based on dpe_host, container and engine names.
     *     Note that character * can be used for any/all container and
     *     engine names. Yet, * is not permitted for the dpe_host specification.
     * </p>
     *
     * @param service_name service canonical name
     * @return set of xMsgR.xMsgRegistrationData objects
     */
    public List<xMsgRegistrationData> find_service(String service_name)
            throws xMsgException, CException, SocketException {

        if (xMsgUtil.getTopicDomain(service_name).equals(xMsgConstants.ANY.getStringValue())) {
            throw new CException("Host name of the DPE must be specified");
        } else{

            // Check the case when the requested service is local
            // Loop over all IP addresses of a node
            for(String ip:xMsgUtil.getLocalHostIps()){
                if(xMsgUtil.getTopicDomain(service_name).equals(ip)){
                    return findLocalSubscribers(name,
                            xMsgUtil.getTopicDomain(service_name),
                            xMsgUtil.getTopicSubject(service_name),
                            xMsgUtil.getTopicType(service_name));
                }
            }

            // This is the case when requested service is remote
            return findSubscribers(name,
                    xMsgUtil.getTopicDomain(service_name),
                    xMsgUtil.getTopicSubject(service_name),
                    xMsgUtil.getTopicType(service_name));
        }
    }

    /**
     * <p>
     *     This method simply calls xMsg subscribe method
     *     passing the reference to user provided call_back method.
     * </p>
     *
     * @param topic Service canonical name that this
     *              method will subscribe or listen
     * @param call_back User provided call_back function
     * @param is_sync if set to true method will block until subscription method is
     *                received and user callback method is returned
     */
    public void receive(String topic,
                        xMsgCallBack call_back,
                        boolean is_sync)
            throws xMsgException {

        subscribe(node_connection,
                xMsgUtil.getTopicDomain(topic),
                xMsgUtil.getTopicSubject(topic),
                xMsgUtil.getTopicType(topic),
                call_back,
                is_sync);
    }

    /**
     * <p>
     *     This method simply calls xMsg subscribe method
     *     passing the reference to user provided call_back method.
     *     The only difference is that this method requires a
     *     connection socket different than the default socket connection
     *     to the local dpe proxy.
     * </p>
     *
     * @param connection zmq connection socket
     * @param topic Service canonical name that this
     *              method will subscribe or listen
     * @param call_back User provided call_back function
     * @param is_sync if set to true method will block until subscription method is
     *                received and user callback method is returned
     */
    public void receive(xMsgConnection connection,
                        String topic,
                        xMsgCallBack call_back,
                        boolean is_sync)
            throws xMsgException {

        subscribe(connection,
                xMsgUtil.getTopicDomain(topic),
                xMsgUtil.getTopicSubject(topic),
                xMsgUtil.getTopicType(topic),
                call_back,
                is_sync);
    }

    /**
     * <p>
     *      Sends xMsgD.Data object to a service defined by:
     * </p>
     * @param topic Clara service canonical name
     * @param data xMsgD.Data object
     */
    public void send(String topic,
                     Object data)
            throws xMsgException {

        publish(node_connection,
                xMsgUtil.getTopicDomain(topic),
                xMsgUtil.getTopicSubject(topic),
                xMsgUtil.getTopicType(topic),
                data);
    }

    /**
     * <p>
     *      Sends xMsgD.Data object to a service.
     *      In this method requires zmq connection object,
     *      and will not use default local dpe proxy connection.
     * </p>
     * @param connection zmq connection socket
     * @param topic Clara service canonical name
     * @param data xMsgD.Data object
     */
    public void send(xMsgConnection connection,
                     String topic,
                     Object data)
            throws xMsgException {

        publish(connection,
                xMsgUtil.getTopicDomain(topic),
                xMsgUtil.getTopicSubject(topic),
                xMsgUtil.getTopicType(topic),
                data);
    }

}
