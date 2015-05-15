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

import org.jlab.clara.sys.Container;
import org.jlab.clara.util.CConstants;
import org.jlab.clara.util.CUtility;
import org.jlab.coda.xmsg.core.*;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.excp.xMsgDiscoverException;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgAddress;
import org.jlab.coda.xmsg.net.xMsgConnection;

import java.io.IOException;
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

    private String name = xMsgConstants.UNDEFINED.getStringValue();
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
     *
     * @param container_name container canonical name
     * @return MsgR.xMsgRegistrationData object
     */
    public xMsgRegistration find_container(String container_name)
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
     * @return set of xMsgRegistration objects
     */
    public List<xMsgRegistration> find_containers(String dpe_name)
            throws xMsgException, CException, SocketException {

        List<xMsgRegistration> result = new ArrayList<>();
        List<xMsgRegistration> tmpl;

        // Check the case when the requested service is local
        // Loop over all IP addresses of a node
        for (String ip : xMsgUtil.getLocalHostIps()) {
            if (dpe_name.equals(ip)) {
                tmpl = findSubscribers(name,
                        dpe_name,
                        xMsgConstants.UNDEFINED.getStringValue(),
                        xMsgConstants.UNDEFINED.getStringValue());
                if (tmpl != null) {
                    for (xMsgRegistration rd : tmpl) {
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
            for (xMsgRegistration rd : tmpl) {
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
     * @param serviceName service canonical name
     * @return set of xMsgR.xMsgRegistrationData objects
     */
    public List<xMsgRegistration> find_service(String serviceName)
            throws xMsgException, CException, SocketException {

        if (xMsgUtil.getTopicDomain(serviceName).equals(xMsgConstants.ANY.getStringValue())) {
            throw new CException("Host name of the DPE must be specified");
        } else {

            // Check the case when the requested service is local
            // Loop over all IP addresses of a node
            for (String ip : xMsgUtil.getLocalHostIps()) {
                if (xMsgUtil.getTopicDomain(serviceName).equals(ip)) {
                    return findSubscribers(name,
                            xMsgUtil.getTopicDomain(serviceName),
                            xMsgUtil.getTopicSubject(serviceName),
                            xMsgUtil.getTopicType(serviceName));
                }
            }

            // This is the case when requested service is remote
            return findSubscribers(name,
                    xMsgUtil.getTopicDomain(serviceName),
                    xMsgUtil.getTopicSubject(serviceName),
                    xMsgUtil.getTopicType(serviceName));
        }
    }

    /**
     * <p>
     *     Defines if the service is deployed
     * </p>
     * @param serviceName service canonical name
     * @return true if service is deployed
     * @throws xMsgDiscoverException
     */
    public boolean isServiceDeployed(String serviceName)
            throws xMsgException, CException, SocketException {
        return find_service(serviceName).size() > 0;
    }

    /**
     * <p>
     *      Sends a data object to a generic
     *      subscriber of an arbitrary topic.
     *      In this case topic is NOT bound to follow Clara
     *      service naming convention.
     *      In this method requires zmq connection object,
     *      and will not use default local dpe proxy connection.
     * </p>
     * @param connection zmq connection socket
     */
    public void genericSend(xMsgConnection connection,
                            xMsgMessage msg)
            throws xMsgException, IOException {
        publish(connection, msg);
    }

    /**
     * <p>
     *      Sends data object to a generic
     *      subscriber of an arbitrary topic.
     *      In this case topic is NOT bound to follow Clara
     *      service naming convention.
     *      This method creates a socket connection to the DPE host.
     * </p>
     *
     * @param dpeHost Clara DPE host IP address
     * @param msg xMsgMessage object
     */
    public void genericSend(String dpeHost,
                            xMsgMessage msg)
            throws xMsgException, IOException {

        if(CUtility.isHostLocal(dpeHost)) {
            publish(node_connection, msg);
        } else {
            // Create a socket connections to the remote dpe.
            xMsgAddress address = new xMsgAddress(dpeHost, true);
            xMsgConnection con = connect(address);
            publish(con, msg);
        }
    }

    /**
     * <p>
     *      Sync sends a data object to a generic
     *      subscriber of an arbitrary topic.
     *      In this case topic is NOT bound to follow Clara
     *      service naming convention.
     *      In this method requires zmq connection object.
     * </p>
     * @param connection zmq connection socket
     * @param timeOut int in seconds
     * @throws TimeoutException
     */
    public xMsgMessage genericSyncSend(xMsgConnection connection,
                                       xMsgMessage msg,
                                  int timeOut)
            throws xMsgException, TimeoutException, IOException {

        return sync_publish(connection, msg, timeOut);
    }

    /**
     * <p>
     *      Sync sends data object to a generic
     *      subscriber of an arbitrary topic.
     *      In this case topic is NOT bound to follow Clara
     *      service naming convention.
     *      This method creates a socket connection to the DPE host.
     * </p>
     *
     * @param dpeHost host name of the DPE of interest
     * @param timeOut timeout in seconds
     * @return Object
     * @throws xMsgException
     * @throws TimeoutException
     * @throws SocketException
     */
    public xMsgMessage genericSyncSend(String dpeHost,
                                       xMsgMessage msg,
                                  int timeOut)
            throws xMsgException, TimeoutException, IOException {

        xMsgConnection connection;
        if (CUtility.isHostLocal(dpeHost)) {
            connection = node_connection;
        } else {
            // Create a socket connections to the remote dpe.
            xMsgAddress address = new xMsgAddress(dpeHost, true);
            connection = connect(address);
        }

        return sync_publish(connection, msg, timeOut);
    }

    public void serviceSend(xMsgConnection connection,
                            xMsgMessage msg)
            throws IOException, xMsgException, CException {

        if (!CUtility.isCanonical(msg.getTopic())) {
            throw new CException("service name is not canonical");
        }
        genericSend(connection, msg);

    }

    /**
     * <p>
     *      Sends a data object to a service
     *      Will use default local dpe proxy connection.
     * </p>
     */

    public void serviceSend(xMsgMessage msg)
            throws xMsgException, CException, IOException {

        if (!CUtility.isCanonical(msg.getTopic())) {
            throw new CException("service name is not canonical");
        }

        if (CUtility.isRemoteService(msg.getTopic())) {
            String dpeHost = CUtility.getDpeName(msg.getTopic());

            // Create a socket connections to the remote dpe.
            xMsgAddress address = new xMsgAddress(dpeHost, true);
            xMsgConnection con = connect(address);
            genericSend(con, msg);

        } else {
            genericSend(node_connection, msg);
        }
    }

    /**
     * <p>
     *      Sync sends a data object to a service.
     *      In this method requires zmq connection object.
     * </p>
     * @param connection zmq connection socket
     * @throws TimeoutException
     */
    public xMsgMessage serviceSyncSend(xMsgConnection connection,
                                       xMsgMessage msg,
                                       int timeOut)
            throws xMsgException, TimeoutException, IOException, CException {

        if (!CUtility.isCanonical(msg.getTopic())) {
            throw new CException("service name is not canonical");
        }

        return genericSyncSend(connection, msg, timeOut);

    }

    /**
     * <p>
     *      Sync sends a data object to a service.
     *      In this method requires zmq connection object.
     * </p>
     * @throws TimeoutException
     */
    public xMsgMessage serviceSyncSend(xMsgMessage msg,
                                       int timeOut)
            throws xMsgException, TimeoutException, IOException, CException {

        if (!CUtility.isCanonical(msg.getTopic())) {
            throw new CException("service name is not canonical");
        }
        if (CUtility.isRemoteService(msg.getTopic())) {
            String dpeHost = CUtility.getDpeName(msg.getTopic());

            // Create a socket connections to the remote dpe.
            xMsgAddress address = new xMsgAddress(dpeHost, true);
            xMsgConnection con = connect(address);
            return genericSyncSend(con, msg, timeOut);

        } else {
            return genericSyncSend(node_connection, msg, timeOut);
        }

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

        return subscribe(connection, topic, call_back);
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
     * This method simply calls xMsg subscribe method
     * passing the reference to user provided call_back method.
     * </p>
     *
     * @param serviceName Service canonical name that this
     *                    method will subscribe or listen
     * @param call_back   User provided call_back function
     */
    public SubscriptionHandler serviceReceive(String serviceName,
                                              xMsgCallBack call_back)
            throws xMsgException, CException {
        if (!CUtility.isCanonical(serviceName)) {
            throw new CException("service name is not canonical");
        }

        return genericReceive(node_connection, serviceName, call_back);
    }

    /**
     * <p>
     *     This method simply calls xMsg subscribe method
     *     passing the reference to user provided call_back method.
     *     This method requires a connection socket different than
     *     the default socket connection to the local dpe proxy.
     * </p>
     *
     * @param connection zmq connection socket
     * @param serviceName Service canonical name that this
     *              method will subscribe or listen
     * @param call_back User provided call_back function
     */
    public SubscriptionHandler serviceReceive(xMsgConnection connection,
                                              String serviceName,
                                              xMsgCallBack call_back)
            throws xMsgException, CException {
        if (!CUtility.isCanonical(serviceName)) {
            throw new CException("service name is not canonical");
        }

        return genericReceive(connection, serviceName, call_back);
    }


    // DPE specific methods
    public Object syncPing(String dpeName, int timeOut)
            throws xMsgException, IOException {

        xMsgMessage msg = new xMsgMessage(CConstants.DPE + ":" + dpeName, CConstants.DPE_PING);
        try {
            return genericSyncSend(dpeName, msg, timeOut);
        } catch (TimeoutException e) {
            return null;
        }
    }


    public void reportFE(String command) throws IOException, xMsgException {
        if (!fe_host_name.equals(xMsgConstants.UNDEFINED.getStringValue()) && xMsgUtil.isIP(fe_host_name)) {
            xMsgMessage msg = new xMsgMessage(CConstants.DPE + ":" + fe_host_name, command);

            genericSend(fe_host_name, msg);
        } else {
            throw new xMsgException("FE host is not properly defined.");
        }
    }

    /**
     * <p>
     * possible system call to start DPE on the specified host.
     * unimplemented.
     * </p>
     *
     * @param dpeName
     */
    public void startRemoteDpe(String dpeName) {
        //@todo
    }

    public void removeRemoteDpe(String dpeName) throws IOException, xMsgException {
        dpeName = CUtility.getIPAddress(dpeName);
        xMsgMessage msg = new xMsgMessage(CConstants.DPE + ":" + dpeName,
                CConstants.STOP_DPE + "?" + dpeName);

        genericSend(dpeName, msg);
    }

    public void startContainer(String containerName) throws xMsgException, IOException {
        if (!CUtility.isCanonical(containerName)) {
            throw new xMsgException("Not a canonical name.");
        }
        new Container(containerName);
    }

    public void startContainer(String containerName, String feHsot) throws xMsgException, IOException {
        if (!CUtility.isCanonical(containerName)) {
            throw new xMsgException("Not a canonical name.");
        }
        new Container(containerName, feHsot);
    }

    public void startRemoteContainer(String dpeName, String containerName) throws IOException, xMsgException {
        if (!CUtility.isCanonical(containerName)) {
            throw new xMsgException("Not a canonical name.");
        }
        xMsgMessage msg = new xMsgMessage(CConstants.DPE + ":" + dpeName,
                CConstants.START_CONTAINER + "?" + containerName);

        genericSend(dpeName, msg);

    }

    public void removeContainer(String dpeName, String containerName) throws IOException, xMsgException {
        if (!CUtility.isCanonical(containerName)) {
            throw new xMsgException("Not a canonical name.");
        }
        xMsgMessage msg = new xMsgMessage(CConstants.CONTAINER + ":" + containerName,
                CConstants.REMOVE_CONTAINER);

        genericSend(dpeName, msg);
    }

    public void startService(String dpeName, String serviceName, String serviceClassPath, String poolSize)
            throws xMsgException, IOException, CException {
        if (!CUtility.isCanonical(serviceName)) {
            throw new CException("Not a canonical name.");

        }
        String containerName = CUtility.getContainerName(serviceName);

        xMsgMessage msg = new xMsgMessage(CConstants.CONTAINER + ":" + containerName,
                CConstants.DEPLOY_SERVICE + "?" + serviceClassPath + "?" + poolSize);

        genericSend(dpeName, msg);

    }

    public void removeService(String dpeName, String serviceName) throws CException, IOException, xMsgException {
        if (!CUtility.isCanonical(serviceName)) {
            throw new CException("Not a canonical name.");

        }
        String containerName = CUtility.getContainerName(serviceName);
        xMsgMessage msg = new xMsgMessage(CConstants.CONTAINER + ":" + containerName,
                CConstants.REMOVE_SERVICE + "?" + serviceName);

        genericSend(dpeName, msg);

    }
}
