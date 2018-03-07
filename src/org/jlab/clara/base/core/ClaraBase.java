/*
 *   Copyright (c) 2016.  Jefferson Lab (JLab). All rights reserved. Permission
 *   to use, copy, modify, and distribute  this software and its documentation for
 *   educational, research, and not-for-profit purposes, without fee and without a
 *   signed licensing agreement.
 *
 *   IN NO EVENT SHALL JLAB BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL
 *   INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING
 *   OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF JLAB HAS
 *   BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 *   JLAB SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 *   THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 *   PURPOSE. THE CLARA SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY,
 *   PROVIDED HEREUNDER IS PROVIDED "AS IS". JLAB HAS NO OBLIGATION TO PROVIDE
 *   MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 *   This software was developed under the United States Government license.
 *   For more information contact author at gurjyan@jlab.org
 *   Department of Experimental Nuclear Physics, Jefferson Lab.
 */

package org.jlab.clara.base.core;

import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.util.EnvUtils;
import org.jlab.clara.util.report.ReportType;
import org.jlab.coda.xmsg.core.xMsg;
import org.jlab.coda.xmsg.core.xMsgCallBack;
import org.jlab.coda.xmsg.core.xMsgConnection;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgSetup;
import org.jlab.coda.xmsg.core.xMsgSubscription;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgRegInfo;
import org.jlab.coda.xmsg.data.xMsgRegQuery;
import org.jlab.coda.xmsg.data.xMsgRegRecord;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgRegAddress;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeoutException;

/**
 *  CLARA base class providing methods build services,
 *  service container and orchestrator.
 *
 * @author gurjyan
 * @since 4.x
 */
public class ClaraBase extends xMsg {

    private final String claraHome;
    // reference to this component description
    private final ClaraComponent me;

    // reference to the front end DPE
    private ClaraComponent frontEnd;

    /**
     * A CLARA component that can send and receives messages.
     *
     * @param me        definition of the component
     * @param frontEnd  definition of the front-end
     */
    public ClaraBase(ClaraComponent me, ClaraComponent frontEnd) {
        super(me.getCanonicalName(), setup(me, frontEnd));
        this.me = me;
        this.frontEnd = frontEnd;
        this.claraHome = EnvUtils.claraHome();
    }

    private static xMsgSetup setup(ClaraComponent me, ClaraComponent frontEnd) {
        xMsgSetup.Builder builder = xMsgSetup.newBuilder()
                        .withProxy(me.getProxyAddress())
                        .withRegistrar(getRegAddress(frontEnd))
                        .withPoolSize(me.getSubscriptionPoolSize())
                        .withPreConnectionSetup(s -> {
                            s.setRcvHWM(0);
                            s.setSndHWM(0);
                        })
                        .withPostConnectionSetup(() -> xMsgUtil.sleep(100));
        if (me.isOrchestrator()) {
            builder.checkSubscription(false);
        }
        return builder.build();
    }

    /**
     * @return the path to the Clara_home defined
     * by means of the CLARA_HOME env variable.
     */
    public String getClaraHome() {
        return claraHome;
    }

    /**
     * Returns the definition of this component.
     */
    public ClaraComponent getMe() {
        return me;
    }

    /**
     * Returns the description of this component.
     */
    public String getDescription() {
        return me.getDescription();
    }

    /**
     * Stores a connection to the default proxy in the connection pool.
     *
     * @throws ClaraException if a connection could not be created or connected
     */
    public void cacheLocalConnection() throws ClaraException {
        try {
            cacheConnection();
        } catch (xMsgException e) {
            throw new ClaraException("could not connect to local proxy", e);
        }
    }

    /**
     * Sends a message to the address of the given CLARA component.
     *
     * @param component the component that shall receive the message
     * @param msg the message to be published
     * @throws xMsgException if the message could not be sent
     */
    public void send(ClaraComponent component, xMsgMessage msg)
            throws xMsgException {
        msg.getMetaData().setSender(myName);
        publish(component.getProxyAddress(), msg);
    }

    /**
     * Sends a string to the given CLARA component.
     *
     * @param component the component that shall receive the message
     * @param requestText string of the message
     * @throws xMsgException if the message could not be sent
     */
    public void send(ClaraComponent component, String requestText)
            throws xMsgException {
        xMsgMessage msg = MessageUtil.buildRequest(component.getTopic(), requestText);
        send(component, msg);
    }

    /**
     * Sends a message using the specified connection.
     *
     * @param con the connection that shall be used to publish the message
     * @param msg the message to be published
     * @throws xMsgException if the message could not be sent
     */
    public void send(xMsgConnection con, xMsgMessage msg)
            throws xMsgException {
        msg.getMetaData().setSender(myName);
        publish(con, msg);
    }

    /**
     * Sends a message to the address of this CLARA component.
     *
     * @param msg the message to be published
     * @throws xMsgException if the message could not be sent
     */
    public void send(xMsgMessage msg)
            throws xMsgException {
        send(me, msg);
    }

    /**
     * Sends a text message to this CLARA component.
     *
     * @param msgText string of the message
     * @throws xMsgException if the message could not be sent
     */
    public void send(String msgText)
            throws xMsgException {
        send(me, msgText);
    }

    /**
     * Synchronous sends a message to the address of the given CLARA component.
     *
     * @param component the component that shall receive the message
     * @param msg the message to be published
     * @param timeout in milliseconds
     * @throws xMsgException if the message could not be sent
     * @throws TimeoutException if a response was not received
     */
    public xMsgMessage syncSend(ClaraComponent component, xMsgMessage msg, long timeout)
            throws xMsgException, TimeoutException {
        msg.getMetaData().setSender(myName);
        return syncPublish(component.getProxyAddress(), msg, timeout);
    }

    /**
     * Synchronous sends a string to the given CLARA component.
     *
     * @param component the component that shall receive the message
     * @param requestText string of the message
     * @param timeout in milli seconds
     * @throws xMsgException if the message could not be sent
     * @throws TimeoutException if a response was not received
     */
    public xMsgMessage syncSend(ClaraComponent component, String requestText, long timeout)
            throws xMsgException, TimeoutException {
        xMsgMessage msg = MessageUtil.buildRequest(component.getTopic(), requestText);
        return syncSend(component, msg, timeout);
    }

    /**
     * Listens for messages published to the given component.
     *
     * @param component a component defining the topic of interest
     * @param callback the callback action
     * @return a handler to the subscription
     * @throws ClaraException if the subscription could not be started
     */
    public xMsgSubscription listen(ClaraComponent component, xMsgCallBack callback)
            throws ClaraException {
        return listen(me, component.getTopic(), callback);
    }

    /**
     * Listens for messages of given topic published to the address of the given
     * component.
     *
     * @param component a component defining the address to connect
     * @param topic topic of interest
     * @param callback the callback action
     * @return a handler to the subscription
     * @throws ClaraException if the subscription could not be started
     */
    public xMsgSubscription listen(ClaraComponent component, xMsgTopic topic, xMsgCallBack callback)
            throws ClaraException {
        try {
            return subscribe(component.getProxyAddress(), topic, callback);
        } catch (xMsgException e) {
            throw new ClaraException("could not subscribe to " + topic);
        }
    }

    /**
     * Listens for messages of given topic published to the address of this
     * component.
     *
     * @param topic topic of interest
     * @param callback the callback action
     * @return a handler to the subscription
     * @throws ClaraException if the subscription could not be started
     */
    public xMsgSubscription listen(xMsgTopic topic, xMsgCallBack callback)
            throws ClaraException {
        return listen(me, topic, callback);
    }

    /**
     * Stops listening to a subscription defined by the handler.
     *
     * @param handle the subscription handler
     */
    public void stopListening(xMsgSubscription handle) {
        unsubscribe(handle);
    }

    /**
     * Terminates all callbacks.
     */
    public void stopCallbacks() {
        terminateCallbacks();
    }

    /**
     * Registers this component with the front-end as subscriber to the given topic.
     *
     * @param topic the subscribed topic
     * @param description a description of the component
     * @throws ClaraException if registration failed
     */
    public void register(xMsgTopic topic, String description) throws ClaraException {
        xMsgRegAddress regAddress = getRegAddress(frontEnd);
        try {
            register(xMsgRegInfo.subscriber(topic, description), regAddress);
        } catch (xMsgException e) {
            throw new ClaraException("could not register with front-end = " + regAddress, e);
        }
    }

    /**
     * Remove the registration of this component from the front-end as
     * subscriber to the given topic.
     *
     * @param topic the subscribed topic
     * @throws ClaraException if removing the registration failed
     */
    public void removeRegistration(xMsgTopic topic) throws ClaraException {
        xMsgRegAddress regAddress = getRegAddress(frontEnd);
        try {
            deregister(xMsgRegInfo.subscriber(topic), regAddress);
        } catch (xMsgException e) {
            throw new ClaraException("could not deregister from front-end = " + regAddress, e);
        }
    }

    /**
     * Retrieves CLARA actor registration information from the xMsg registrar service.
     *
     * @param regHost registrar server host
     * @param regPort registrar server port
     * @param topic   the canonical name of an actor: {@link org.jlab.coda.xmsg.core.xMsgTopic}
     * @return set of {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration} objects
     * @throws IOException
     * @throws xMsgException
     */
    public Set<xMsgRegRecord> discover(String regHost, int regPort, xMsgTopic topic)
            throws IOException, xMsgException {
        xMsgRegAddress regAddress = new xMsgRegAddress(regHost, regPort);
        return discover(xMsgRegQuery.subscribers(topic), regAddress, 1000);
    }

    /**
     * Retrieves CLARA actor registration information from the xMsg registrar service,
     * assuming registrar is running using the default port.
     *
     * @param regHost registrar server host
     * @param topic   the canonical name of an actor: {@link org.jlab.coda.xmsg.core.xMsgTopic}
     * @return set of {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration} objects
     * @throws IOException
     * @throws xMsgException
     */
    public Set<xMsgRegRecord> discover(String regHost, xMsgTopic topic)
            throws IOException, xMsgException {
        xMsgRegAddress regAddress = new xMsgRegAddress(regHost);
        return discover(xMsgRegQuery.subscribers(topic), regAddress);
    }

    /**
     * Retrieves CLARA actor registration information from the xMsg registrar service,
     * assuming registrar is running on a local host, using the default port.
     *
     * @param topic the canonical name of an actor: {@link org.jlab.coda.xmsg.core.xMsgTopic}
     * @return set of {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration} objects
     * @throws IOException
     * @throws xMsgException
     */
    public Set<xMsgRegRecord> discover(xMsgTopic topic)
            throws IOException, xMsgException {
        return discover(xMsgRegQuery.subscribers(topic));
    }

    public static xMsgRegAddress getRegAddress(ClaraComponent fe) {
        return new xMsgRegAddress(fe.getDpeHost(), fe.getDpePort() + ClaraConstants.REG_PORT_SHIFT);
    }

    /**
     * Sync asks DPE to report.
     *
     * @param component dpe as a {@link ClaraComponent#dpe()} object
     * @param timeout sync request timeout
     * @return message {@link org.jlab.coda.xmsg.core.xMsgMessage}
     *         back from a dpe.
     * @throws IOException
     * @throws xMsgException
     * @throws TimeoutException
     */
    public xMsgMessage pingDpe(ClaraComponent component, int timeout)
            throws IOException, xMsgException, TimeoutException {

        if (component.isDpe()) {
            String data = MessageUtil.buildData(ReportType.INFO.getValue());
            xMsgTopic topic = component.getTopic();
            xMsgMessage msg = MessageUtil.buildRequest(topic, data);
            return syncSend(component, msg, timeout);
        }
        return null;
    }


    /**
     * Returns the reference to the front-end DPE.
     *
     * @return {@link org.jlab.clara.base.core.ClaraComponent} object
     */
    public ClaraComponent getFrontEnd() {
        return frontEnd;
    }

    /**
     * Sets a DPE CLARA component as a front-end.
     *
     * @param frontEnd {@link org.jlab.clara.base.core.ClaraComponent} object
     */
    public void setFrontEnd(ClaraComponent frontEnd) {
        this.frontEnd = frontEnd;
    }
}
