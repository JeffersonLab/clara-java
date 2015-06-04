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

package org.jlab.clara.sys;

import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.jlab.clara.base.CException;
import org.jlab.clara.util.CConstants;
import org.jlab.clara.util.CUtility;
import org.jlab.coda.xmsg.core.SubscriptionHandler;
import org.jlab.coda.xmsg.core.xMsg;
import org.jlab.coda.xmsg.core.xMsgCallBack;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgAddress;
import org.jlab.coda.xmsg.net.xMsgConnection;

/**
 * Clara base class.
 *
 * @author gurjyan
 * @version 1.x
 * @since 1/31/15
 */
public class CBase extends xMsg {

    private String myName = xMsgConstants.UNDEFINED.getStringValue();
    private xMsgConnection nodeConnection = null;
    private String feHostname = xMsgConstants.UNDEFINED.getStringValue();

    /**
     * Constructor.
     *
     * @param feHost the host name of the Clara front-end
     * @throws xMsgException
     */
    public CBase(String feHost) throws xMsgException, SocketException {
        super(feHost);

        this.feHostname = feHost;

        // Create a socket connections to the xMsg node.
        // This is a local DPE, and uses default port number.
        xMsgAddress address = new xMsgAddress("localhost");
        this.nodeConnection = connect(address);
    }

    /**
     * Constructor.
     *
     * @param dpeHost the host name of the Clara DPE of interest
     * @param feHost the host name of the Clara front-end
     * @throws xMsgException
     * @throws SocketException
     */
    public CBase(String dpeHost, String feHost) throws xMsgException, SocketException {
        super(feHost);

        this.feHostname = feHost;

        // Create a socket connections to the xMsg node.
        // This is a local DPE, and uses default port number.
        xMsgAddress address = new xMsgAddress(dpeHost);
        this.nodeConnection = connect(address);
    }


    /**
     * Constructor.
     *
     * @param feHost   the host name of the Clara front-end
     * @param poolSize thread pool size for servicing subscription callbacks
     * @throws xMsgException
     * @throws SocketException
     */
    public CBase(String feHost, int poolSize) throws xMsgException, SocketException {
        super(feHost, poolSize);

        // Create a socket connections to the xMsg node.
        // This is a local DPE, and uses default port number.
        xMsgAddress address = new xMsgAddress("localhost");
        this.nodeConnection = connect(address);
    }

    /**
     * Constructor.
     *
     * @throws xMsgException
     * @throws SocketException
     */
    public CBase() throws xMsgException, SocketException {
        super("localhost");

        // Create a socket connections to the xMsg node.
        // This is a local DPE, and uses default port number.
        xMsgAddress address = new xMsgAddress("localhost");
        this.nodeConnection = connect(address);
    }

    /**
     * Constructor.
     *
     * @param poolSize thread pool size for servicing subscription callbacks
     * @throws xMsgException
     * @throws SocketException
     */
    public CBase(int poolSize) throws xMsgException, SocketException {
        super("localhost", poolSize);

        // Create a socket connections to the xMsg node.
        // This is a local DPE, and uses default port number.
        xMsgAddress address = new xMsgAddress("localhost");
        this.nodeConnection = connect(address);
    }

    public String getFeHostName() {
        return feHostname;
    }

    /**
     * Returns the given name of this component.
     *
     * @return name of the component
     */
    public String getMyName() {
        return myName;
    }

    /**
     * Sets the name of this component.
     *
     * @param name the name of this component
     */
    public void setMyName(String name) {
        this.myName = name;
    }

    /**
     * Asks the registration information of the given container.
     * Sends a request to the xMsg registration service, asking to return
     * registration information of container based on canonical name of the
     * container.
     *
     * @param containerName container canonical name
     * @return MsgR.xMsgRegistrationData object
     * @throws CException
     * @throws xMsgException
     * @throws SocketException
     */
    public xMsgRegistration findContainer(String containerName)
            throws xMsgException, CException, SocketException {

        if (xMsgUtil.getTopicDomain(containerName).equals(xMsgConstants.ANY.getStringValue())) {
            throw new CException("Host name of the DPE must be specified");
        } else {
            // Check the case when the requested container is local
            // Loop over all IP addresses of a node
            for (String ip : xMsgUtil.getLocalHostIps()) {
                if (xMsgUtil.getTopicDomain(containerName).equals(ip)) {
                    return findSubscribers(myName,
                            xMsgUtil.getTopicDomain(containerName),
                            xMsgUtil.getTopicSubject(containerName),
                            xMsgConstants.UNDEFINED.getStringValue()).get(0);
                }
            }

            // This is the case when requested container is remote
            return findSubscribers(myName,
                    xMsgUtil.getTopicDomain(containerName),
                    xMsgUtil.getTopicSubject(containerName),
                    xMsgConstants.UNDEFINED.getStringValue()).get(0);
        }
    }

    /**
     * Asks the registration information of the containers in the given DPE.
     * Sends a request to the xMsg registration service,
     * asking to return registration information of all containers in the DPE
     *
     * @param dpeName DPE name
     * @return set of xMsgRegistration objects
     * @throws CException
     * @throws xMsgException
     * @throws SocketException
     */
    public List<xMsgRegistration> findContainers(String dpeName)
            throws xMsgException, CException, SocketException {

        List<xMsgRegistration> result = new ArrayList<>();
        List<xMsgRegistration> tmpl;

        // Check the case when the requested service is local
        // Loop over all IP addresses of a node
        for (String ip : xMsgUtil.getLocalHostIps()) {
            if (dpeName.equals(ip)) {
                tmpl = findSubscribers(myName,
                        dpeName,
                        xMsgConstants.UNDEFINED.getStringValue(),
                        xMsgConstants.UNDEFINED.getStringValue());
                if (tmpl != null) {
                    for (xMsgRegistration rd : tmpl) {
                        if (rd.getType().equals(xMsgConstants.UNDEFINED.getStringValue())
                                && !result.contains(rd)) {
                            result.add(rd);
                        }
                    }
                }
                return result;
            }
        }

        // This is the case when requested service is remote
        tmpl = findSubscribers(myName,
                dpeName,
                xMsgConstants.UNDEFINED.getStringValue(),
                xMsgConstants.UNDEFINED.getStringValue());
        if (tmpl != null) {
            for (xMsgRegistration rd : tmpl) {
                if (rd.getType().equals(xMsgConstants.UNDEFINED.getStringValue())
                        && !result.contains(rd)) {
                    result.add(rd);
                }
            }
        }
        return result;
    }

    /**
     * Asks the registration information of the given service.
     * Sends a request to the xMsg registration service,
     * asking to return registration information of service/services
     * based on dpe_host, container and engine names.
     * <p>
     * Note that character {@code *} can be used for any/all container and
     * engine names. Yet, {@code *} is not permitted for the DPE host
     * specification.
     *
     * @param serviceName service canonical name
     * @return set of xMsgR.xMsgRegistrationData objects
     * @throws CException
     * @throws xMsgException
     * @throws SocketException
     */
    public List<xMsgRegistration> findService(String serviceName)
            throws xMsgException, CException, SocketException {

        if (xMsgUtil.getTopicDomain(serviceName).equals(xMsgConstants.ANY.getStringValue())) {
            throw new CException("Host name of the DPE must be specified");
        } else {

            // Check the case when the requested service is local
            // Loop over all IP addresses of a node
            for (String ip : xMsgUtil.getLocalHostIps()) {
                if (xMsgUtil.getTopicDomain(serviceName).equals(ip)) {
                    return findSubscribers(myName,
                            xMsgUtil.getTopicDomain(serviceName),
                            xMsgUtil.getTopicSubject(serviceName),
                            xMsgUtil.getTopicType(serviceName));
                }
            }

            // This is the case when requested service is remote
            return findSubscribers(myName,
                    xMsgUtil.getTopicDomain(serviceName),
                    xMsgUtil.getTopicSubject(serviceName),
                    xMsgUtil.getTopicType(serviceName));
        }
    }

    /**
     * Defines if the service is deployed.
     *
     * @param serviceName service canonical name
     * @return true if service is deployed
     * @throws CException
     * @throws xMsgException
     * @throws SocketException
     */
    public boolean isServiceDeployed(String serviceName)
            throws xMsgException, CException, SocketException {
        return findService(serviceName).size() > 0;
    }

    /**
     * Sends a message to a generic subscriber of an arbitrary topic.
     * In this case topic is NOT bound to follow Clara service naming
     * convention. In this method requires zmq connection object,
     * and will not use default local dpe proxy connection.
     *
     * @param connection zmq connection socket
     * @param msg the message to be sent
     * @throws xMsgException
     * @throws IOException
     */
    public void genericSend(xMsgConnection connection, xMsgMessage msg)
            throws xMsgException, IOException {
        publish(connection, msg);
    }

    /**
     * Sends data object to a generic subscriber of an arbitrary topic.
     * In this case topic is NOT bound to follow Clara service naming
     * convention. This method creates a socket connection to the DPE host.
     *
     * @param dpeHost Clara DPE host IP address
     * @param msg the message to be sent
     * @throws xMsgException
     * @throws IOException
     */
    public void genericSend(String dpeHost, xMsgMessage msg)
            throws xMsgException, IOException {

        if (CUtility.isHostLocal(dpeHost)) {
            publish(nodeConnection, msg);
        } else {
            // Create a socket connections to the remote dpe.
            xMsgAddress address = new xMsgAddress(dpeHost, true);
            xMsgConnection con = connect(address);
            publish(con, msg);
        }
    }

    /**
     * Sync sends a data object to a generic subscriber of an arbitrary topic.
     * In this case topic is NOT bound to follow Clara service naming
     * convention. In this method requires zmq connection object.
     *
     * @param connection zmq connection socket
     * @param msg the message to be sent
     * @param timeOut int in seconds
     * @throws xMsgException
     * @throws TimeoutException
     * @throws IOException
     */
    public xMsgMessage genericSyncSend(xMsgConnection connection,
                                       xMsgMessage msg,
                                       int timeOut)
            throws xMsgException, TimeoutException, IOException {

        return sync_publish(connection, msg, timeOut);
    }

    /**
     * Sync sends data object to a generic subscriber of an arbitrary topic.
     * In this case topic is NOT bound to follow Clara service naming
     * convention. This method creates a socket connection to the DPE host.
     *
     * @param dpeHost host name of the DPE of interest
     * @param timeOut timeout in seconds
     * @return Object
     * @throws xMsgException
     * @throws TimeoutException
     * @throws IOException
     */
    public xMsgMessage genericSyncSend(String dpeHost,
                                       xMsgMessage msg,
                                       int timeOut)
            throws xMsgException, TimeoutException, IOException {

        xMsgConnection connection;
        if (CUtility.isHostLocal(dpeHost)) {
            connection = nodeConnection;
        } else {
            // Create a socket connections to the remote dpe.
            xMsgAddress address = new xMsgAddress(dpeHost, true);
            connection = connect(address);
        }

        return sync_publish(connection, msg, timeOut);
    }

    /**
     * Sends a message to a service.
     * In this method requires zmq connection object.
     *
     * @param connection zmq connection socket
     * @param msg the message to be sent
     * @throws xMsgException
     * @throws CException
     * @throws IOException
     */
    public void serviceSend(xMsgConnection connection,
                            xMsgMessage msg)
            throws IOException, xMsgException, CException {

        if (!CUtility.isCanonical(msg.getTopic())) {
            throw new CException("service name is not canonical");
        }
        genericSend(connection, msg);

    }

    /**
     * Sends a message to a service.
     * Will use default local DPE proxy connection.
     *
     * @param msg the message to be sent
     * @throws xMsgException
     * @throws CException
     * @throws IOException
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
            genericSend(nodeConnection, msg);
        }
    }

    /**
     * Sync sends a message to a service.
     * In this method requires zmq connection object.
     *
     * @param connection zmq connection socket
     * @param msg the message to be sent
     * @param timeOut timeout in seconds
     * @throws xMsgException
     * @throws TimeoutException
     * @throws IOException
     * @throws CException
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
     * Sync sends a message to a service.
     * In this method requires zmq connection object.
     *
     * @param msg the message to be sent
     * @param timeOut timeout in seconds
     * @throws xMsgException
     * @throws TimeoutException
     * @throws IOException
     * @throws CException
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
            return genericSyncSend(nodeConnection, msg, timeOut);
        }

    }

    /**
     * Subscribes to the specific topic with the given callback.
     * This method requires a connection socket different than the default
     * socket connection to the local dpe proxy.
     * This method simply calls xMsg subscribe method
     * passing the reference to user provided callback method.
     * In this case topic is NOT bound to follow Clara
     * service naming convention.
     *
     * @param connection zmq connection socket
     * @param topic service canonical name that this method will subscribe
     * @param callback user provided callback function
     * @throws xMsgException
     */
    public SubscriptionHandler genericReceive(xMsgConnection connection,
                                              String topic,
                                              xMsgCallBack callback)
            throws xMsgException {

        return subscribe(connection, topic, callback);
    }

    /**
     * Subscribes to the specific topic with the given callback.
     * This method simply calls xMsg subscribe method
     * passing the reference to user provided callback method.
     * In this case topic is NOT bound to follow Clara
     * service naming convention.
     *
     * @param topic service canonical name that this method will subscribe
     * @param callback user provided callback function
     * @throws xMsgException
     */
    public SubscriptionHandler genericReceive(String topic,
                                              xMsgCallBack callback)
            throws xMsgException {
        return genericReceive(nodeConnection, topic, callback);
    }


    /**
     * Subscribes to the specific topic with the given callback.
     * This method simply calls xMsg subscribe method
     * passing the reference to user provided callback method.
     * In this case topic is NOT bound to follow Clara
     * service naming convention.
     *
     * @param dpeHost   DPE host IP address
     * @param topic     service canonical name that this method will subscribe
     * @param callback  user provided callback function
     */
    public SubscriptionHandler genericReceive(String dpeHost,
                                              String topic,
                                              xMsgCallBack callback)
            throws xMsgException, SocketException {

        xMsgAddress address = new xMsgAddress(dpeHost, true);
        xMsgConnection con = getNewConnection(address);
        return genericReceive(con, topic, callback);
    }

    /**
     * Calls xMsg unsubscribe method.
     *
     * @param handler the subscription handler
     * @throws xMsgException
     */
    public void cancelReceive(SubscriptionHandler handler)
            throws xMsgException {

        unsubscribe(handler);
    }

    /**
     * Calls xMsg subscribe method the user provided callback.
     *
     * @param serviceName service canonical name that this method will subscribe
     * @param callback    user provided callback function
     * @throws xMsgException
     * @throws CException
     */
    public SubscriptionHandler serviceReceive(String serviceName,
                                              xMsgCallBack callback)
            throws xMsgException, CException {
        if (!CUtility.isCanonical(serviceName)) {
            throw new CException("service name is not canonical");
        }

        return genericReceive(nodeConnection, serviceName, callback);
    }


    /**
     * Calls xMsg subscribe method the user provided callback.
     * This method requires a connection socket different than the default
     * socket connection to the local dpe proxy.
     *
     * @param connection  zmq connection socket
     * @param serviceName service canonical name that this method will subscribe
     * @param callback    user provided callback function
     * @throws xMsgException
     * @throws CException
     */
    public SubscriptionHandler serviceReceive(xMsgConnection connection,
                                              String serviceName,
                                              xMsgCallBack callback)
            throws xMsgException, CException {
        if (!CUtility.isCanonical(serviceName)) {
            throw new CException("service name is not canonical");
        }

        return genericReceive(connection, serviceName, callback);
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
        if (!feHostname.equals(xMsgConstants.UNDEFINED.getStringValue())
                && xMsgUtil.isIP(feHostname)) {
            xMsgMessage msg = new xMsgMessage(CConstants.DPE + ":" + feHostname, command);

            genericSend(feHostname, msg);
        } else {
            throw new xMsgException("FE host is not properly defined.");
        }
    }

    /**
     * Possible system call to start DPE on the specified host.
     * Unimplemented.
     *
     * @param dpeName the name of the DPE
     */
    public void startRemoteDpe(String dpeName) {
        // TODO implement this
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

    public void startContainer(String containerName, String feHost)
            throws xMsgException, IOException {
        if (!CUtility.isCanonical(containerName)) {
            throw new xMsgException("Not a canonical name.");
        }
        new Container(containerName, feHost);
    }

    public void startRemoteContainer(String dpeName, String containerName)
            throws IOException, xMsgException {
        if (!CUtility.isCanonical(containerName)) {
            throw new xMsgException("Not a canonical name.");
        }
        xMsgMessage msg = new xMsgMessage(CConstants.DPE + ":" + dpeName,
                CConstants.START_CONTAINER + "?" + containerName);

        genericSend(dpeName, msg);
    }

    public void removeContainer(String dpeName, String containerName)
            throws IOException, xMsgException {
        if (!CUtility.isCanonical(containerName)) {
            throw new xMsgException("Not a canonical name.");
        }
        xMsgMessage msg = new xMsgMessage(CConstants.CONTAINER + ":" + containerName,
                CConstants.REMOVE_CONTAINER);

        genericSend(dpeName, msg);
    }

    public void startService(String dpeName,
                             String serviceName,
                             String serviceClassPath,
                             String poolSize)
            throws xMsgException, IOException, CException {
        if (!CUtility.isCanonical(serviceName)) {
            throw new CException("Not a canonical name.");

        }
        String containerName = CUtility.getContainerName(serviceName);

        xMsgMessage msg = new xMsgMessage(CConstants.CONTAINER + ":" + containerName,
                CConstants.DEPLOY_SERVICE + "?" + serviceClassPath + "?" + poolSize);

        genericSend(dpeName, msg);
    }

    public void removeService(String dpeName, String serviceName)
            throws CException, IOException, xMsgException {
        if (!CUtility.isCanonical(serviceName)) {
            throw new CException("Not a canonical name.");

        }
        String containerName = CUtility.getContainerName(serviceName);
        xMsgMessage msg = new xMsgMessage(CConstants.CONTAINER + ":" + containerName,
                CConstants.REMOVE_SERVICE + "?" + serviceName);

        genericSend(dpeName, msg);
    }
}
