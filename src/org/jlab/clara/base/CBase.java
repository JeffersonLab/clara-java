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

package org.jlab.clara.base;

import org.jlab.clara.util.CUtility;
import org.jlab.coda.xmsg.core.*;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistrationData;
import org.jlab.coda.xmsg.excp.xMsgDiscoverException;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgAddress;
import org.jlab.coda.xmsg.net.xMsgConnection;

import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.TimeoutException;

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
    private String fe_host_name = xMsgConstants.UNDEFINED.getStringValue();


    /**
     * Constructor
     * @param feHost the host name of the Clara FE
     * @throws xMsgException
     */
    public CBase(String feHost) throws xMsgException, SocketException {
        super(feHost);

        this.fe_host_name = feHost;

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
    public CBase(String dpeHost, String feHost) throws xMsgException, SocketException {
        super(feHost);

        this.fe_host_name = feHost;

        // Create a socket connections to the xMsg node.
        // This is a local DPE, and uses default port number.
        xMsgAddress address = new xMsgAddress(dpeHost);
        this.node_connection = connect(address);
    }

    /**
     * Constructor
     *
     * @param feHost    the host name of the Clara FE
     * @param pool_size thread pool size for servicing subscription callbacks
     * @throws xMsgException
     */
    public CBase(String feHost,
                 int pool_size) throws xMsgException, SocketException {
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
    public CBase() throws xMsgException, SocketException {
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
    public CBase(int pool_size) throws xMsgException, SocketException {
        super("localhost", pool_size);

        // Create a socket connections to the xMsg node.
        // This is a local DPE, and uses default port number.
        xMsgAddress address = new xMsgAddress("localhost");
        this.node_connection = connect(address);
    }

    public String getFeHostName() {
        return fe_host_name;
    }

    /**
     * <p>
     *     Returns given name of this component
     * </p>
     * @return name of the component
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of this component
     * @param name the name of this component
     */
    public void setName(String name) {
        this.name = name;
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
     *                     for which we find input/output links
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
            if(s.startsWith("&")){
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
                return out_list;
            } else if(link_direction > 0){
                index += 1;
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

                return out_list;
            }
        }
        // returns empty list. Most likely this service
        // is the first service in the composition
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
     *     asking to return registration information of container
     *     based on canonical name of the container
     * </p>
     *
     * @param container_name container canonical name
     * @return MsgR.xMsgRegistrationData object
     */
    public xMsgRegistrationData find_container(String container_name)
            throws xMsgException, CException, SocketException {

        if (xMsgUtil.getTopicDomain(container_name).equals(xMsgConstants.ANY.getStringValue())) {
            throw new CException("Host name of the DPE must be specified");
        } else{

            // Check the case when the requested container is local
            // Loop over all IP addresses of a node
            for(String ip:xMsgUtil.getLocalHostIps()){
                if (xMsgUtil.getTopicDomain(container_name).equals(ip)) {
                    return findSubscribers(name,
                            xMsgUtil.getTopicDomain(container_name),
                            xMsgUtil.getTopicSubject(container_name),
                            xMsgConstants.UNDEFINED.getStringValue()).get(0);
                }
            }

            // This is the case when requested container is remote
            return findSubscribers(name,
                    xMsgUtil.getTopicDomain(container_name),
                    xMsgUtil.getTopicSubject(container_name),
                    xMsgConstants.UNDEFINED.getStringValue()).get(0);
        }
    }

    /**
     * <p>
     * Sends a request to the xMsg registration service,
     * asking to return registration information of all containers in the DPE
     * </p>
     *
     * @param dpe_name DPE name
     * @return set of xMsgR.xMsgRegistrationData objects
     */
    public List<xMsgRegistrationData> find_containers(String dpe_name)
            throws xMsgException, CException, SocketException {

        List<xMsgRegistrationData> result = new ArrayList<>();
        List<xMsgRegistrationData> tmpl;

        // Check the case when the requested service is local
        // Loop over all IP addresses of a node
        for (String ip : xMsgUtil.getLocalHostIps()) {
            if (dpe_name.equals(ip)) {
                tmpl = findSubscribers(name,
                        dpe_name,
                        xMsgConstants.UNDEFINED.getStringValue(),
                        xMsgConstants.UNDEFINED.getStringValue());
                if (tmpl != null) {
                    for (xMsgRegistrationData rd : tmpl) {
                        if (rd.getType().equals(xMsgConstants.UNDEFINED.getStringValue()) &&
                                !result.contains(rd)) {
                            result.add(rd);
                        }
                    }
                }
                return result;
            }
        }

        // This is the case when requested service is remote
        tmpl = findSubscribers(name,
                dpe_name,
                xMsgConstants.UNDEFINED.getStringValue(),
                xMsgConstants.UNDEFINED.getStringValue());
        if (tmpl != null) {
            for (xMsgRegistrationData rd : tmpl) {
                if (rd.getType().equals(xMsgConstants.UNDEFINED.getStringValue()) &&
                        !result.contains(rd)) {
                    result.add(rd);
                }
            }
        }
        return result;
    }

    /**
     * <p>
     * Sends a request to the xMsg registration service,
     * asking to return registration information of service/services
     * based on dpe_host, container and engine names.
     * Note that character * can be used for any/all container and
     * engine names. Yet, * is not permitted for the dpe_host specification.
     * </p>
     *
     * @param service_name service canonical name
     * @return set of xMsgR.xMsgRegistrationData objects
     */
    public List<xMsgRegistrationData> find_service(String service_name)
            throws xMsgException, CException, SocketException {

        if (xMsgUtil.getTopicDomain(service_name).equals(xMsgConstants.ANY.getStringValue())) {
            throw new CException("Host name of the DPE must be specified");
        } else {

            // Check the case when the requested service is local
            // Loop over all IP addresses of a node
            for (String ip : xMsgUtil.getLocalHostIps()) {
                if (xMsgUtil.getTopicDomain(service_name).equals(ip)) {
                    return findSubscribers(name,
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
     *      Sends xMsgD.Data object to a service.
     *      In this method requires zmq connection object,
     *      and will not use default local dpe proxy connection.
     * </p>
     * @param connection zmq connection socket
     * @param topic Clara service canonical name
     * @param data xMsgD.Data object
     */
    public void serviceSend(xMsgConnection connection,
                            String topic,
                            Object data)
            throws xMsgException {

        publish(connection, topic, data);

    }

    /**
     * <p>
     *      Sends xMsgD.Data object to a service
     * </p>
     * @param serviceName Clara service canonical name
     * @param data xMsgD.Data object
     */
    public void serviceSend(String serviceName,
                            Object data)
            throws xMsgException, CException, SocketException {

        if(!CUtility.isCanonical(serviceName)){
            throw new CException("service name is not canonical");
        }

        if(CUtility.isRemoteService(serviceName)){
            String dpeHost = CUtility.getDpeName(serviceName);

            // Create a socket connections to the remote dpe.
            xMsgAddress address = new xMsgAddress(dpeHost, true);
            xMsgConnection con = connect(address);
            serviceSend(con, serviceName, data);

        } else {
            serviceSend(node_connection, serviceName, data);
        }
    }

    /**
     * <p>
     *      Sync sends xMsgD.Data object to a service.
     *      In this method requires zmq connection object,
     *      and will not use default local dpe proxy connection.
     * </p>
     * @param connection zmq connection socket
     * @param topic Clara service canonical name
     * @param data xMsgD.Data object
     * @throws TimeoutException
     */
    public Object serviceSyncSend(xMsgConnection connection,
                                  String topic,
                                  Object data,
                                  int timeOut)
            throws xMsgException, TimeoutException {

        String dpe = xMsgUtil.getTopicDomain(topic);
        String container = "*";
        String engine = "*";
        if(!xMsgUtil.getTopicSubject(topic).equals(xMsgConstants.UNDEFINED.getStringValue())){
            container = xMsgUtil.getTopicSubject(topic);
        }
        if(!xMsgUtil.getTopicType(topic).equals(xMsgConstants.UNDEFINED.getStringValue())){
            engine = xMsgUtil.getTopicType(topic);
        }
        return sync_publish(connection,
                dpe,
                container,
                engine,
                data,
                timeOut);
    }

    /**
     * <p>
     *      Sync sends xMsgD.Data object to a service defined by the canonical name
     * </p>
     * @param serviceName Clara service canonical name
     * @param data xMsgD.Data object
     * @param timeOut int in seconds
     * @throws TimeoutException
     */
    public Object serviceSyncSend(String serviceName,
                                  Object data,
                                  int timeOut)
            throws xMsgException, TimeoutException, CException, SocketException {

        if(!CUtility.isCanonical(serviceName)){
            throw new CException("service name is not canonical");
        }

        if(CUtility.isRemoteService(serviceName)){
            String dpeHost = CUtility.getDpeName(serviceName);

            // Create a socket connections to the remote dpe.
            xMsgAddress address = new xMsgAddress(dpeHost, true);
            xMsgConnection con = connect(address);
            return serviceSyncSend(con, serviceName, data, timeOut);

        } else {
            return serviceSyncSend(node_connection, serviceName, data, timeOut);
        }

    }

    /**
     * <p>
     *      Sends xMsgD.Data or a String object to a generic
     *      subscriber of an arbitrary topic.
     *      In this case topic is NOT bound to follow Clara
     *      service naming convention.
     *      In this method requires zmq connection object,
     *      and will not use default local dpe proxy connection.
     * </p>
     * @param connection zmq connection socket
     * @param topic Clara service canonical name
     * @param data xMsgD.Data object
     */
    public void genericSend(xMsgConnection connection,
                            String topic,
                            Object data)
            throws xMsgException {

        publish(connection,
                topic,
                data);
    }

    /**
     * <p>
     *      Sends xMsgD.Data or a String object to a generic
     *      subscriber of an arbitrary topic.
     *      In this case topic is NOT bound to follow Clara
     *      service naming convention.
     * </p>
     *
     * @param dpeHost Clara DPE host IP address
     * @param topic xMsg topic
     * @param data payload (Object of String or xMSgD.Data)
     */
    public void genericSend(String dpeHost,
                            String topic,
                            Object data)
            throws xMsgException, SocketException {

        if(CUtility.isHostLocal(dpeHost)) {
            genericSend(node_connection, topic, data);
        } else {
            // Create a socket connections to the remote dpe.
            xMsgAddress address = new xMsgAddress(dpeHost, true);
            xMsgConnection con = connect(address);
            genericSend(con, topic, data);
        }
    }

    /**
     * <p>
     *      Sync sends xMsgD.Data or a String object to a generic
     *      subscriber of an arbitrary topic.
     *      In this case topic is NOT bound to follow Clara
     *      service naming convention.
     *      In this method requires zmq connection object,
     *      and will not use default local dpe proxy connection.
     * </p>
     * @param connection zmq connection socket
     * @param topic Clara service canonical name
     * @param data payload ( object of String or xMsgD.Data)
     * @param timeOut int in seconds
     * @throws TimeoutException
     */
    public Object genericSyncSend(xMsgConnection connection,
                                  String topic,
                                  Object data,
                                  int timeOut)
            throws xMsgException, TimeoutException {

        return sync_publish(connection,
                topic,
                data,
                timeOut);
    }

    /**
     * <p>
     * Generic sync send
     * </p>
     *
     * @param dpeHost host name of the DPE of interest
     * @param topic   topic of the subscription
     * @param data    payload
     * @param timeOut timeout in seconds
     * @return Object
     * @throws xMsgException
     * @throws TimeoutException
     * @throws SocketException
     */
    public Object genericSyncSend(String dpeHost,
                                  String topic,
                                  Object data,
                                  int timeOut)
            throws xMsgException, TimeoutException, SocketException {

        xMsgConnection connection;
        if (CUtility.isHostLocal(dpeHost)) {
            connection = node_connection;
        } else {
            // Create a socket connections to the remote dpe.
            xMsgAddress address = new xMsgAddress(dpeHost, true);
            connection = connect(address);
        }
        return sync_publish(connection,
                topic,
                data,
                timeOut);
    }

    /**
     * <p>
     *      Sync sends xMsgD.Data or a String object to a generic
     *      subscriber of an arbitrary topic.
     *      In this case topic is NOT bound to follow Clara
     *      service naming convention.
     * </p>
     *
     * @param topic Clara service canonical name
     * @param data xMsgD.Data object
     * @param timeOut int in seconds
     * @throws TimeoutException
     */
    public Object genericSyncSend(String topic,
                                  Object data,
                                  int timeOut)
            throws xMsgException, TimeoutException {

        return genericSyncSend(node_connection,topic,data, timeOut);
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
     */
    public SubscriptionHandler  serviceReceive(xMsgConnection connection,
                                               String topic,
                                               xMsgCallBack call_back)
            throws xMsgException {

        String dpe = xMsgUtil.getTopicDomain(topic);
        String container = "*";
        String engine = "*";
        if(!xMsgUtil.getTopicSubject(topic).equals(xMsgConstants.UNDEFINED.getStringValue())){
            container = xMsgUtil.getTopicSubject(topic);
        }
        if(!xMsgUtil.getTopicType(topic).equals(xMsgConstants.UNDEFINED.getStringValue())){
            engine = xMsgUtil.getTopicType(topic);
        }

        return subscribe(connection,
                dpe,
                container,
                engine,
                call_back);
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
     */
    public SubscriptionHandler  serviceReceive(String topic,
                                               xMsgCallBack call_back)
            throws xMsgException {
        return serviceReceive(node_connection, topic, call_back);
    }

    /**
     * <p>
     *     This method simply calls xMsg subscribe method
     *     passing the reference to user provided call_back method.
     *     The only difference is that this method requires a
     *     connection socket different than the default socket connection
     *     to the local dpe proxy.
     *     In this case topic is NOT bound to follow Clara
     *     service naming convention.
     * </p>
     *
     * @param connection zmq connection socket
     * @param topic Service canonical name that this
     *              method will subscribe or listen
     * @param call_back User provided call_back function
     */
    public SubscriptionHandler genericReceive(xMsgConnection connection,
                                              String topic,
                                              xMsgCallBack call_back)
            throws xMsgException {

        return subscribe(connection,
                topic,
                call_back);
    }

    /**
     * <p>
     *     This method simply calls xMsg subscribe method
     *     passing the reference to user provided call_back method.
     *     In this case topic is NOT bound to follow Clara
     *     service naming convention.
     * </p>
     *
     * @param topic Service canonical name that this
     *              method will subscribe or listen
     * @param call_back User provided call_back function
     */
    public SubscriptionHandler genericReceive(String topic,
                                              xMsgCallBack call_back)
            throws xMsgException {
        return genericReceive(node_connection, topic, call_back);
    }

    public void cancelReceive(SubscriptionHandler handler)
            throws xMsgException {

        unsubscribe(handler);
    }


    /**
     * <p>
     *     Defines if the service is deployed
     * </p>
     * @param requester name of the requester
     * @param dpe dpe IP
     * @param container given name (not canonical)
     * @param engine class name
     * @return true if service is deployed
     * @throws xMsgDiscoverException
     */
    public boolean isServiceDeployed(String requester,
                                     String dpe,
                                     String container,
                                     String engine)
            throws xMsgDiscoverException {
        return isThereLocalSubscriber(requester,
                dpe,
                container,
                engine);
    }

}
