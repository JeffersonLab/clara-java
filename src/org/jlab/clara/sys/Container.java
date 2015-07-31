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

import org.jlab.clara.base.CException;
import org.jlab.clara.util.CConstants;
import org.jlab.clara.util.CUtility;
import org.jlab.coda.xmsg.core.xMsgSubscription;
import org.jlab.coda.xmsg.core.xMsgCallBack;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.data.xMsgD.xMsgData;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;
import org.jlab.coda.xmsg.excp.xMsgException;

import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A container for services.
 *
 * @author gurjyan
 * @version 2.x
 * @since 1/30/15
 */
public class Container extends CBase {

    // Unique id for services within the container
    private AtomicInteger uniqueId = new AtomicInteger(0);

    private xMsgSubscription subscriptionHandler;

    private Map<String, Service> _myServices = new HashMap<>();

    /**
     * Constructor.
     *
     * @param name Clara service canonical name (such as dep:container:engine)
     * @param feHost front-end host name. This is the host that holds
     *               centralized registration database.
     * @throws xMsgException
     */
    public Container(String name, String localAddress, String frontEndAddres)
            throws xMsgException, IOException {
        super(name, localAddress, frontEndAddres);

        // Create a socket connections to the local dpe proxy
        connect();

        System.out.println(CUtility.getCurrentTimeInH()+": Started container = "+getName());

        // Subscribe messages published to this container
        xMsgTopic topic = xMsgTopic.wrap(CConstants.CONTAINER + ":" + getName());
        subscriptionHandler = genericReceive(topic, new ContainerCallBack());

        //register container
        registerSubscriber(xMsgTopic.wrap(getName()), "Service Container");
        System.out.println(CUtility.getCurrentTimeInH() + ": Registered container = " + name);
    }

    /**
     * Stops this container.
     * Destroys all services, unsubscribes and unregister.
     *
     * @throws xMsgException
     * @throws IOException
     */
    public void exit() throws CException, xMsgException, IOException {

        reportFE(CConstants.CONTAINER_DOWN + "?" + getName());

        unsubscribe(subscriptionHandler);
        removeSubscriber(xMsgTopic.wrap(getName()));

        for (Service service : _myServices.values()) {
            service.exit();
        }

        uniqueId = null;
        subscriptionHandler = null;
    }

    /**
     * Adds a new service to this container.
     *
     * @param engineName the service engine name
     * @param engineClassPath the service engine class path
     * @param servicePoolSize the size of the engines pool
     */
    public void addService(String engineName,
                           String engineClassPath,
                           int servicePoolSize,
                           String initialState)
            throws CException, xMsgException {

        String serviceName = getName() + ":" + engineName;

        if (_myServices.containsKey(serviceName)) {
            throw new CException("service exists");
        }

        // Define the key in the shared
        // memory map (defined in the DPE).
        int id = uniqueId.incrementAndGet();

        // Object pool size is set to be 2 in case
        // it was requested to be 0 or negative number.
        if (servicePoolSize <= 0) {
            servicePoolSize = 1;
        }

        Service service = new Service(serviceName,
                                      engineClassPath,
                                      getLocalAddress(),
                                      getFrontEndAddress(),
                                      servicePoolSize,
                                      id,
                                      initialState);
        _myServices.put(serviceName, service);
    }

    /**
     * Removes a service from this container.
     *
     * @param serviceName the service canonical name
     */
    public void removeService(String serviceName)
            throws CException {
        if (_myServices.containsKey(serviceName)) {
            Service service = _myServices.remove(serviceName);
            service.exit();
        }
    }


    /**
     * Processes messages published to this container.
     */
    private class ContainerCallBack implements xMsgCallBack {

        @Override
        public xMsgMessage callback(xMsgMessage msg) {

            final xMsgMeta.Builder metadata = msg.getMetaData();
            if (metadata.getDataType().equals("binary/native")) {
                xMsgData data;
                try {
                    data = xMsgData.parseFrom(msg.getData());
                } catch (InvalidProtocolBufferException e1) {
                    e1.printStackTrace();
                    return msg;
                }
                if (data.getType().equals(xMsgData.Type.T_STRING)) {
                    String cmdData = data.getSTRING();
                    String cmd = null, seName = null,
                            objectPoolSize = null,
                            initialState = xMsgConstants.UNDEFINED.toString();
                    try {
                        StringTokenizer st = new StringTokenizer(cmdData, "?");
                        cmd = st.nextToken();
                        seName = st.nextToken();
                        if(st.hasMoreTokens())objectPoolSize = st.nextToken();
                        if(st.hasMoreTokens())initialState = st.nextToken();

                    } catch (NoSuchElementException e) {
                        System.out.println(e.getMessage());
//                    e.printStackTrace();
                    }
                    if (cmd != null && seName != null) {
                        switch (cmd) {
                            case CConstants.DEPLOY_SERVICE:
                                // Note: in this case seName is the pull path to the engine class
                                if (!seName.contains(".")) {
                                    System.out.println("Warning: Deployment failed. " +
                                            "Clara accepts fully qualified class names only.");
                                    return null;
                                }


                                if (objectPoolSize == null) {

                                    // if object pool size is not defined set
                                    // the size equal to the number of cores
                                    // in the node where this container is deployed
                                    int ps = Runtime.getRuntime().availableProcessors();
                                    objectPoolSize = String.valueOf(ps);
                                }
                                try {
                                    String classPath = seName;
                                    String engineName = seName.substring((seName.lastIndexOf(".")) + 1, seName.length());

                                    addService(engineName, classPath, Integer.parseInt(objectPoolSize), initialState);
                                } catch (xMsgException | NumberFormatException | CException e) {
                                    e.printStackTrace();
                                }
                                break;
                            case CConstants.REMOVE_SERVICE:
                                // Note: in this case seName is the canonical name of the service
                                try {
                                    removeService(seName);
                                } catch (CException e) {
                                    e.printStackTrace();
                                }
                                break;
                            case CConstants.REMOVE_CONTAINER:
                                try {
                                    exit();
                                } catch (CException | xMsgException | IOException e) {
                                    e.printStackTrace();
                                }
                                break;
                        }
                    } else {
                        System.out.println("Error: malformed deployment string: " +
                                "command or service name is not defined.");
                    }
                }
            }
            return null;
        }
    }
}
