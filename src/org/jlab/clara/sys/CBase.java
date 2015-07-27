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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.jlab.clara.base.CException;
import org.jlab.clara.util.CConstants;
import org.jlab.clara.util.CUtility;
import org.jlab.coda.xmsg.core.xMsg;
import org.jlab.coda.xmsg.core.xMsgCallBack;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgSubscription;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgAddress;
import org.jlab.coda.xmsg.net.xMsgConnection;
import org.jlab.coda.xmsg.net.xMsgConnectionSetup;
import org.zeromq.ZMQ.Socket;

/**
 * Clara base class.
 *
 * @author gurjyan
 * @version 1.x
 * @since 1/31/15
 */
public class CBase extends xMsg {

    private xMsgConnection nodeConnection = null;
    private String feHostname = xMsgConstants.UNDEFINED.toString();

    private static xMsgConnectionSetup setup = new xMsgConnectionSetup() {
            @Override
            public void preConnection(Socket socket) {
                socket.setRcvHWM(0);
                socket.setSndHWM(0);
            }

            @Override
            public void postConnection() {
                xMsgUtil.sleep(100);
            }
    };

    /**
     * Constructor.
     *
     * @param feHost the host name of the Clara front-end
     * @throws xMsgException
     */
    public CBase(String name, String feHost) throws xMsgException, SocketException {
        super(name, feHost);

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
    public CBase(String name, String dpeHost, String feHost) throws xMsgException, SocketException {
        super(name, feHost);

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
    public CBase(String name, String feHost, int poolSize) throws xMsgException, SocketException {
        super(name, feHost, poolSize);

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
    public CBase(String name) throws xMsgException, SocketException {
        super(name, "localhost");

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
    public CBase(String name, int poolSize) throws xMsgException, SocketException {
        super(name, "localhost", poolSize);

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

        xMsgTopic topic = xMsgTopic.wrap(containerName);
        // Check the case when the requested container is local
        // Loop over all IP addresses of a node
        for (String ip : xMsgUtil.getLocalHostIps()) {
            if (topic.domain().equals(ip)) {
                Set<xMsgRegistration> result = findLocalSubscribers(topic);
                Iterator<xMsgRegistration> it = result.iterator();
                if (it.hasNext()) {
                    return it.next();
                } else {
                    return null;
                }
            }
        }

        // This is the case when requested container is remote
        Set<xMsgRegistration> result = findSubscribers(topic);
        Iterator<xMsgRegistration> it = result.iterator();
        if (it.hasNext()) {
            return it.next();
        } else {
            return null;
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
    public Set<xMsgRegistration> findContainers(String dpeName)
            throws xMsgException, CException, SocketException {

        xMsgTopic topic = xMsgTopic.wrap(dpeName);
        Set<xMsgRegistration> result = new HashSet<xMsgRegistration>();

        // Check the case when the requested service is local
        // Loop over all IP addresses of a node
        for (String ip : xMsgUtil.getLocalHostIps()) {
            if (dpeName.equals(ip)) {
                for (xMsgRegistration rd : findLocalSubscribers(topic)) {
                    if (rd.getType().equals(xMsgConstants.UNDEFINED.toString())) {
                        result.add(rd);
                    }
                }
                return result;
            }
        }

        // This is the case when requested container is remote
        for (xMsgRegistration rd : findSubscribers(topic)) {
            if (rd.getType().equals(xMsgConstants.UNDEFINED.toString())) {
                result.add(rd);
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
    public Set<xMsgRegistration> findService(String serviceName)
            throws xMsgException, CException, SocketException {

        xMsgTopic topic = xMsgTopic.wrap(serviceName);

        // Check the case when the requested service is local
        // Loop over all IP addresses of a node
        for (String ip : xMsgUtil.getLocalHostIps()) {
            if (topic.domain().equals(ip)) {
                return findLocalSubscribers(topic);
            }
        }

        // This is the case when requested container is remote
        return findSubscribers(topic);
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
            xMsgAddress address = new xMsgAddress(dpeHost);
            xMsgConnection con = connect(address, setup);
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

        return syncPublish(connection, msg, timeOut);
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
            xMsgAddress address = new xMsgAddress(dpeHost);
            connection = connect(address, setup);
        }

        return syncPublish(connection, msg, timeOut);
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

        if (!CUtility.isCanonical(msg.getTopic().toString())) {
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

        if (!CUtility.isCanonical(msg.getTopic().toString())) {
            throw new CException("service name is not canonical");
        }

        if (CUtility.isRemoteService(msg.getTopic().toString())) {
            String dpeHost = CUtility.getDpeName(msg.getTopic().toString());

            // Create a socket connections to the remote dpe.
            xMsgAddress address = new xMsgAddress(dpeHost);
            xMsgConnection con = connect(address, setup);
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

        if (!CUtility.isCanonical(msg.getTopic().toString())) {
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

        if (!CUtility.isCanonical(msg.getTopic().toString())) {
            throw new CException("service name is not canonical");
        }
        if (CUtility.isRemoteService(msg.getTopic().toString())) {
            String dpeHost = msg.getTopic().domain();

            // Create a socket connections to the remote dpe.
            xMsgAddress address = new xMsgAddress(dpeHost);
            xMsgConnection con = connect(address, setup);
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
    public xMsgSubscription genericReceive(xMsgConnection connection,
                                           xMsgTopic topic,
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
    public xMsgSubscription genericReceive(xMsgTopic topic,
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
    public xMsgSubscription genericReceive(String dpeHost,
                                           xMsgTopic topic,
                                           xMsgCallBack callback)
            throws xMsgException, SocketException {

        xMsgAddress address = new xMsgAddress(dpeHost);
        xMsgConnection con = getNewConnection(address);
        return genericReceive(con, topic, callback);
    }

    /**
     * Calls xMsg unsubscribe method.
     *
     * @param handler the subscription handler
     * @throws xMsgException
     */
    public void cancelReceive(xMsgSubscription handler)
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
    public xMsgSubscription serviceReceive(String serviceName,
                                           xMsgCallBack callback)
            throws xMsgException, CException {
        if (!CUtility.isCanonical(serviceName)) {
            throw new CException("service name is not canonical");
        }

        return genericReceive(nodeConnection, xMsgTopic.wrap(serviceName), callback);
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
    public xMsgSubscription serviceReceive(xMsgConnection connection,
                                           String serviceName,
                                           xMsgCallBack callback)
            throws xMsgException, CException {
        if (!CUtility.isCanonical(serviceName)) {
            throw new CException("service name is not canonical");
        }

        return genericReceive(connection, xMsgTopic.wrap(serviceName), callback);
    }


    // DPE specific methods
    public Object syncPing(String dpeName, int timeOut)
            throws xMsgException, IOException {

        xMsgTopic topic = xMsgTopic.wrap(CConstants.DPE + ":" + dpeName);
        xMsgMessage msg = new xMsgMessage(topic, CConstants.DPE_PING);
        try {
            return genericSyncSend(dpeName, msg, timeOut);
        } catch (TimeoutException e) {
            return null;
        }
    }


    public void reportFE(String command) throws IOException, xMsgException {
        if (!feHostname.equals(xMsgConstants.UNDEFINED.toString())
                && xMsgUtil.isIP(feHostname)) {
            xMsgTopic topic = xMsgTopic.wrap(CConstants.DPE + ":" + feHostname);
            xMsgMessage msg = new xMsgMessage(topic, command);
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
        xMsgTopic topic = xMsgTopic.wrap(CConstants.DPE + ":" + dpeName);
        String data = CConstants.STOP_DPE + "?" + dpeName;
        xMsgMessage msg = new xMsgMessage(topic, data);

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
        xMsgTopic topic = xMsgTopic.wrap(CConstants.DPE + ":" + dpeName);
        String data = CConstants.START_CONTAINER + "?" + containerName;
        xMsgMessage msg = new xMsgMessage(topic, data);

        genericSend(dpeName, msg);
    }

    public void removeContainer(String dpeName, String containerName)
            throws IOException, xMsgException {
        if (!CUtility.isCanonical(containerName)) {
            throw new xMsgException("Not a canonical name.");
        }
        xMsgTopic topic = xMsgTopic.wrap(CConstants.CONTAINER + ":" + containerName);
        String data = CConstants.REMOVE_CONTAINER;
        xMsgMessage msg = new xMsgMessage(topic, data);
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
        xMsgTopic topic = xMsgTopic.wrap(CConstants.CONTAINER + ":" + containerName);
        String data = CConstants.DEPLOY_SERVICE + "?" + serviceClassPath + "?" + poolSize;
        xMsgMessage msg = new xMsgMessage(topic, data);
        genericSend(dpeName, msg);
    }

    public void removeService(String dpeName, String serviceName)
            throws CException, IOException, xMsgException {
        if (!CUtility.isCanonical(serviceName)) {
            throw new CException("Not a canonical name.");

        }
        String containerName = CUtility.getContainerName(serviceName);
        xMsgTopic topic = xMsgTopic.wrap(CConstants.CONTAINER + ":" + containerName);
        String data = CConstants.REMOVE_SERVICE + "?" + serviceName;
        xMsgMessage msg = new xMsgMessage(topic, data);

        genericSend(dpeName, msg);
    }
}
