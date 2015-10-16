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

import org.jlab.clara.base.ClaraBase;
import org.jlab.clara.base.ClaraComponent;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.util.CConstants;
import org.jlab.clara.util.ClaraUtil;
import org.jlab.clara.util.xml.RequestParser;
import org.jlab.coda.xmsg.core.*;
import org.jlab.coda.xmsg.excp.xMsgException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Service container
 *
 * @author gurjyan
 * @version 4.x
 */
public class Container extends ClaraBase {

    private xMsgSubscription subscriptionHandler;

    private Map<String, Service> _myServices = new HashMap<>();

    /**
     * Constructor.
     * Note that container runs on a localhost: on a host where
     * dpe is running ( within the dpe process).
     *
     * @param regHost
     * @param regPort
     * @param description
     * @throws xMsgException
     * @throws IOException
     * @throws ClaraException
     */
    public Container(ClaraComponent comp,
                     String regHost,
                     int regPort,
                     String description)
            throws xMsgException, IOException, ClaraException {
        super(comp, regHost, regPort);

        // Create a socket connections to the local dpe proxy
        connect();

        // Subscribe messages published to this container
        xMsgTopic topic = xMsgTopic.wrap(CConstants.CONTAINER + ":" + comp.getName());

        // Register this subscriber
        registerAsSubscriber(topic, description);
        System.out.println(ClaraUtil.getCurrentTimeInH() + ": Registered container = " + comp.getName());

        // Subscribe by passing a callback to the subscription
        subscriptionHandler = listen(topic, new ContainerCallBack());
        System.out.println(ClaraUtil.getCurrentTimeInH() + ": Started container = " + comp.getName());

    }


    /**
     * Stops this container.
     * Destroys all services, un-subscribes and unregister.
     *
     * @throws xMsgException
     * @throws IOException
     */
    public void exit() throws ClaraException, xMsgException, IOException {

        // broadcast to the local proxy
        send(CConstants.CONTAINER_DOWN + "?" + getName());

        stopListening(subscriptionHandler);
        removeRegistration();

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
                if (msg.getMetaData().getReplyTo().equals(xMsgConstants.UNDEFINED.getStringValue())) {

                    RequestParser parser = null;
                    parser = RequestParser.build(msg);
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
                } else {
                    // sync request, updates the received xMsgMessage and sends it to the sender
                    // reset relyTo metadata field
                    msg.getMetaData().setReplyTo(xMsgConstants.UNDEFINED.getStringValue());

                    // sends back "Done" string
                    msg.updateData("Done");
                    send(msg);

                }
            } catch (ClaraException | xMsgException | IOException e) {
                e.printStackTrace();
            }
            return msg;
        }
    }
}
