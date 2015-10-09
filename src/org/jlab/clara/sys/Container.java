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

import org.jlab.clara.base.ClaraException;
import org.jlab.clara.util.CConstants;
import org.jlab.clara.util.ClaraUtil;
import org.jlab.clara.util.xml.RequestParser;
import org.jlab.coda.xmsg.core.xMsgSubscription;
import org.jlab.coda.xmsg.core.xMsgCallBack;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.excp.xMsgException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A container for services.
 *
 * @author gurjyan
 * @version 2.x
 * @since 1/30/15
 */
public class Container extends CBase {

    private xMsgSubscription subscriptionHandler;

    private Map<String, Service> _myServices = new HashMap<>();

    /**
     * Constructor.
     */
    public Container(String name, String localAddress, String frontEndAddres)
            throws xMsgException, IOException {
        super(name, localAddress, frontEndAddres);

        // Create a socket connections to the local dpe proxy
        connect();

        // Subscribe messages published to this container
        xMsgTopic topic = xMsgTopic.wrap(CConstants.CONTAINER + ":" + getName());
        subscriptionHandler = genericReceive(topic, new ContainerCallBack());
        System.out.println(ClaraUtil.getCurrentTimeInH() + ": Started container = " + getName());

        //register container
        registerLocalSubscriber(topic, "Service Container");
        System.out.println(ClaraUtil.getCurrentTimeInH() + ": Registered container = " + name);
    }

    /**
     * Stops this container.
     * Destroys all services, unsubscribes and unregister.
     *
     * @throws xMsgException
     * @throws IOException
     */
    public void exit() throws ClaraException, xMsgException, IOException {

        reportFE(CConstants.CONTAINER_DOWN + "?" + getName());

        unsubscribe(subscriptionHandler);
        removeSubscriber(xMsgTopic.wrap(getName()));

        for (Service service : _myServices.values()) {
            service.exit();
        }

        subscriptionHandler = null;
    }

    /**
     * Adds a new service to this container.
     *
     * @param engineName the service engine name
     * @param engineClassPath the service engine class path
     * @param servicePoolSize the size of the engines pool
     */
    private void addService(String engineName,
                            String engineClassPath,
                            int servicePoolSize,
                            String initialState)
            throws ClaraException, xMsgException, IOException {

        String serviceName = getName() + ":" + engineName;

        if (_myServices.containsKey(serviceName)) {
            String msg = "%s Warning: service %s already exists. No new service is deployed%n";
            System.err.printf(msg, ClaraUtil.getCurrentTimeInH(), serviceName);
            return;
        }

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
                                      initialState);
        _myServices.put(serviceName, service);
    }

    /**
     * Removes a service from this container.
     *
     * @param serviceName the service canonical name
     */
    private void removeService(String serviceName)
            throws ClaraException {
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
            try {
                RequestParser parser = RequestParser.build(msg);
                String command = parser.nextString();
                String serviceName = parser.nextString();

                switch (command) {

                    case CConstants.DEPLOY_SERVICE:
                        String className = parser.nextString();
                        int poolSize = parser.nextInteger();
                        String state = parser.nextString(xMsgConstants.UNDEFINED.toString());
                        addService(serviceName, className, poolSize, state);
                        break;

                    case CConstants.REMOVE_SERVICE:
                        removeService(serviceName);
                        break;

                    case CConstants.REMOVE_CONTAINER:
                        exit();
                        break;

                    default:
                        throw new ClaraException("Invalid request");
                }
            } catch (ClaraException | xMsgException | IOException e) {
                e.printStackTrace();
            }
            return new xMsgMessage(null);
        }
    }
}
