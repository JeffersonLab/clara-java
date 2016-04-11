/*
 * Copyright (C) 2015. Jefferson Lab, CLARA framework (JLAB). All Rights Reserved.
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for educational, research, and not-for-profit purposes,
 * without fee and without a signed licensing agreement.
 *
 * Contact Vardan Gyurjyan
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

package org.jlab.clara.base.core;

import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.util.report.ReportType;
import org.jlab.coda.xmsg.core.xMsg;
import org.jlab.coda.xmsg.core.xMsgCallBack;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgSubscription;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnection;
import org.jlab.coda.xmsg.net.xMsgConnectionSetup;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.jlab.coda.xmsg.net.xMsgRegAddress;
import org.zeromq.ZMQ.Socket;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Set;
import java.util.concurrent.TimeoutException;

/**
 *  Clara base class providing methods build services,
 *  service container and orchestrator.
 *
 * @author gurjyan
 * @since 4.x
 */
public abstract class ClaraBase extends xMsg {

    private static final EngineDataAccessor DATA_ACCESSOR = EngineDataAccessor.getDefault();
    private final String claraHome;
    // reference to this component description
    private final ClaraComponent me;

    // reference to the front end DPE
    private ClaraComponent frontEnd;

    /**
     * A Clara component that can send and receives messages.
     *
     * @param me        definition of the component
     * @param frontEnd  definition of the front-end
     * @throws ClaraException
     */
    public ClaraBase(ClaraComponent me, ClaraComponent frontEnd) {
        super(me.getCanonicalName(),
              new xMsgProxyAddress(me.getDpeHost(), me.getDpePort()),
              new xMsgRegAddress(),
              me.getSubscriptionPoolSize());

        setConnectionSetup(new xMsgConnectionSetup() {

            @Override
            public void preConnection(Socket socket) {
                socket.setRcvHWM(0);
                socket.setSndHWM(0);
            }

            @Override
            public void postConnection() {
                xMsgUtil.sleep(100);
            }
        });
        this.me = me;
        this.frontEnd = frontEnd;
        this.claraHome = System.getenv("CLARA_HOME");
        if (claraHome == null) {
            throw new IllegalStateException("CLARA_HOME environmental variable is not defined.");
        }
    }

    // abstract methods to start Clara component
    public abstract void start() throws ClaraException;

    // abstract method to gracefully end Clara component
    protected abstract void end();

    @Override
    public void close() {
        end();
        super.close();
    }

    /**
     * @return the path to the Clara_home defined
     * by means of the CLARA_HOME env variable.
     */
    public String getClaraHome() {
        return claraHome;
    }

    /**
     * Returns the description of this component.
     */
    public ClaraComponent getMe() {
        return me;
    }

    /**
     * Stores a connection to the default proxy in the connection pool.
     *
     * @throws ClaraException if a connection could not be created or connected
     */
    public void cacheConnection() throws ClaraException {
        try {
            // Create a socket connection to the local proxy
            releaseConnection(getConnection());
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
        xMsgConnection con = getConnection(component.getProxyAddress());
        publish(con, msg);
        releaseConnection(con);
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
        xMsgMessage msg = MessageUtils.buildRequest(component.getTopic(), requestText);
        xMsgConnection con = getConnection(component.getProxyAddress());
        publish(con, msg);
        releaseConnection(con);
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
    public xMsgMessage syncSend(ClaraComponent component, xMsgMessage msg, int timeout)
            throws xMsgException, TimeoutException {
        xMsgConnection con = getConnection(component.getProxyAddress());
        xMsgMessage m = syncPublish(con, msg, timeout);
        releaseConnection(con);
        return m;
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
    public xMsgMessage syncSend(ClaraComponent component, String requestText, int timeout)
            throws xMsgException, TimeoutException {
        xMsgMessage msg = MessageUtils.buildRequest(component.getTopic(), requestText);
        xMsgConnection con = getConnection(component.getProxyAddress());
        xMsgMessage m = syncPublish(con, msg, timeout);
        releaseConnection(con);
        return m;
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
        xMsgConnection con = null;
        try {
            con = getConnection(component.getProxyAddress());
            return subscribe(con, topic, callback);
        } catch (xMsgException e) {
            if (con != null) {
                releaseConnection(con);
            }
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
     * Registers this component with the front-end as subscriber to the given topic.
     *
     * @param topic the subscribed topic
     * @param description a description of the component
     * @throws ClaraException if registration failed
     */
    public void register(xMsgTopic topic, String description) throws ClaraException {
        xMsgRegAddress regAddress = new xMsgRegAddress(frontEnd.getDpeHost());
        try {
            registerAsSubscriber(regAddress, topic, description);
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
        xMsgRegAddress regAddress = new xMsgRegAddress(frontEnd.getDpeHost());
        try {
            deregisterAsSubscriber(regAddress, topic);
        } catch (xMsgException e) {
            throw new ClaraException("could not deregister from front-end = " + regAddress, e);
        }
    }

    /**
     * Retrieves Clara actor registration information from the xMsg registrar service.
     *
     * @param regHost registrar server host
     * @param regPort registrar server port
     * @param topic   the canonical name of an actor: {@link org.jlab.coda.xmsg.core.xMsgTopic}
     * @return set of {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration} objects
     * @throws IOException
     * @throws xMsgException
     */
    public Set<xMsgRegistration> discover(String regHost, int regPort, xMsgTopic topic)
            throws IOException, xMsgException {
        xMsgRegAddress regAddress = new xMsgRegAddress(regHost, regPort);
        return findSubscribers(regAddress, topic);
    }

    /**
     * Retrieves Clara actor registration information from the xMsg registrar service,
     * assuming registrar is running using the default port.
     *
     * @param regHost registrar server host
     * @param topic   the canonical name of an actor: {@link org.jlab.coda.xmsg.core.xMsgTopic}
     * @return set of {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration} objects
     * @throws IOException
     * @throws xMsgException
     */
    public Set<xMsgRegistration> discover(String regHost, xMsgTopic topic)
            throws IOException, xMsgException {
        xMsgRegAddress regAddress = new xMsgRegAddress(regHost);
        return findSubscribers(regAddress, topic);
    }

    /**
     * Retrieves Clara actor registration information from the xMsg registrar service,
     * assuming registrar is running on a local host, using the default port.
     *
     * @param topic the canonical name of an actor: {@link org.jlab.coda.xmsg.core.xMsgTopic}
     * @return set of {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration} objects
     * @throws IOException
     * @throws xMsgException
     */
    public Set<xMsgRegistration> discover(xMsgTopic topic)
            throws IOException, xMsgException {
        return findSubscribers(topic);
    }

    /**
     * Deploys a Clara actor. Clara component object is used to
     * extract xMsg proxy host and port for a proper connection.
     *
     * @param component Clara actor as a {@link org.jlab.clara.base.core.ClaraComponent} object
     * @throws ClaraException
     * @throws IOException
     * @throws xMsgException
     * @throws TimeoutException
     */
    public void deploy(ClaraComponent component)
            throws ClaraException, IOException, xMsgException, TimeoutException {
        _deploy(component, -1);
    }

    /**
     * Synchronously deploys a Clara actor. Clara component object is used to
     * extract xMsg proxy host and port for a proper connection.
     *
     * @param component Clara actor as a {@link org.jlab.clara.base.core.ClaraComponent} object
     * @param timeout timeout of the sync communication
     * @return message {@link org.jlab.coda.xmsg.core.xMsgMessage}
     *         indicating the status of the sync operation.
     * @throws ClaraException
     * @throws IOException
     * @throws xMsgException
     * @throws TimeoutException
     */
    public xMsgMessage syncDeploy(ClaraComponent component, int timeout)
            throws ClaraException, IOException, xMsgException, TimeoutException {
        return _deploy(component, timeout);
    }

    /**
     * Sends a message to a Clara component/actor telling to exit/destruct.
     *
     * @param component Clara actor as a {@link org.jlab.clara.base.core.ClaraComponent} object
     * @throws ClaraException
     * @throws IOException
     * @throws xMsgException
     * @throws TimeoutException
     */
    public void exit(ClaraComponent component)
            throws ClaraException, IOException, xMsgException, TimeoutException {
        _exit(component, -1);
    }

    /**
     * Sync sends a message to a Clara component/actor telling to exit/destruct.
     *
     * @param component Clara actor as a {@link org.jlab.clara.base.core.ClaraComponent} object
     * @param timeout timeout of the sync communication
     * @return message {@link org.jlab.coda.xmsg.core.xMsgMessage}
     *         indicating the status of the sync operation.
     * @throws ClaraException
     * @throws IOException
     * @throws xMsgException
     * @throws TimeoutException
     */
    public xMsgMessage syncExit(ClaraComponent component, int timeout)
            throws ClaraException, IOException, xMsgException, TimeoutException {
        return _exit(component, timeout);
    }

    /**
     * Sends a message to a Clara component asking to report after processing events = eventCount.
     *
     * @param component component Clara actor as a {@link org.jlab.clara.base.core.ClaraComponent} object
     * @param report report type define as a {@link org.jlab.clara.util.report.ReportType} object
     * @param eventCount number of events after which component
     *                   reports/broadcasts required type of a report
     * @throws IOException
     * @throws xMsgException
     */
    public void startReporting(ClaraComponent component, ReportType report, int eventCount)
            throws IOException, xMsgException {

        if (eventCount < 0) {
            throw new IllegalArgumentException("Clara-Error: Invalid event count: " + eventCount);
        }
        String data = MessageUtils.buildData(report.getValue(), eventCount);
        xMsgTopic topic = component.getTopic();
        xMsgMessage msg = MessageUtils.buildRequest(topic, data);
        send(component, msg);
    }

    /**
     * Sends a message to a Clara component to stop reporting a specific type of report type.
     *
     * @param component component Clara actor as a {@link org.jlab.clara.base.core.ClaraComponent} object
     * @param report report type define as a {@link org.jlab.clara.util.report.ReportType} object
     * @throws IOException
     * @throws xMsgException
     */
    public void stopReporting(ClaraComponent component, ReportType report)
            throws IOException, xMsgException {
        startReporting(component, report, 0);
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
            String data = MessageUtils.buildData(ReportType.INFO.getValue());
            xMsgTopic topic = component.getTopic();
            xMsgMessage msg = MessageUtils.buildRequest(topic, data);
            return syncSend(component, msg, timeout);
        }
        return null;
    }

    /**
     * Builds a message by serializing passed data object using serialization
     * routine defined in one of the data types objects.
     *
     * @param topic     the topic where the data will be published
     * @param data      the data to be serialized
     * @param dataTypes the set of registered data types
     * @throws ClaraException if the data could not be serialized
     */
    public static xMsgMessage serialize(xMsgTopic topic,
                                        EngineData data,
                                        Set<EngineDataType> dataTypes)
            throws ClaraException {

        xMsgMeta.Builder metadata = DATA_ACCESSOR.getMetadata(data);
        String mimeType = metadata.getDataType();
        for (EngineDataType dt : dataTypes) {
            if (dt.mimeType().equals(mimeType)) {
                try {
                    ByteBuffer bb = dt.serializer().write(data.getData());
                    if (bb.order() == ByteOrder.BIG_ENDIAN) {
                        metadata.setByteOrder(xMsgMeta.Endian.Big);
                    } else {
                        metadata.setByteOrder(xMsgMeta.Endian.Little);
                    }
                    return new xMsgMessage(topic, metadata, bb.array());
                } catch (ClaraException e) {
                    throw new ClaraException("Could not serialize " + mimeType, e);
                }
            }
        }
        if (mimeType.equals(EngineDataType.STRING.mimeType())) {
            ByteBuffer bb = EngineDataType.STRING.serializer().write(data.getData());
            return new xMsgMessage(topic, metadata, bb.array());
        }
        throw new ClaraException("Unsupported mime-type = " + mimeType);
    }

    /**
     * De-serializes data of the message {@link org.jlab.coda.xmsg.core.xMsgMessage},
     * represented as a byte[] into an object of az type defined using the mimeType/dataType
     * of the meta-data (also as a part of the xMsgMessage). Second argument is used to
     * pass the serialization routine as a method of the
     * {@link org.jlab.clara.engine.EngineDataType} object.
     *
     * @param msg {@link org.jlab.coda.xmsg.core.xMsgMessage} object
     * @param dataTypes set of {@link org.jlab.clara.engine.EngineDataType} objects
     * @return {@link org.jlab.clara.engine.EngineData} object containing de-serialized data object
     *          and metadata
     * @throws ClaraException
     */
    public EngineData deSerialize(xMsgMessage msg, Set<EngineDataType> dataTypes)
            throws ClaraException {
        xMsgMeta.Builder metadata = msg.getMetaData();
        String mimeType = metadata.getDataType();
        for (EngineDataType dt : dataTypes) {
            if (dt.mimeType().equals(mimeType)) {
                try {
                    ByteBuffer bb = ByteBuffer.wrap(msg.getData());
                    if (metadata.getByteOrder() == xMsgMeta.Endian.Little) {
                        bb.order(ByteOrder.LITTLE_ENDIAN);
                    }
                    Object userData = dt.serializer().read(bb);
                    return DATA_ACCESSOR.build(userData, metadata);
                } catch (ClaraException e) {
                    throw new ClaraException("Clara-Error: Could not deserialize " + mimeType, e);
                }
            }
        }
        throw new ClaraException("Clara-Error: Unsupported mime-type = " + mimeType);
    }

    /**
     * Creates system exception data (EngineData object).
     *
     * @param msg         the exception message
     * @param severity    severity ID of the exception
     * @param description More thorough description of the source of the exception
     * @return {@link org.jlab.clara.engine.EngineData} object
     */
    public EngineData buildSystemErrorData(String msg, int severity, String description) {
        EngineData outData = new EngineData();
        outData.setData(EngineDataType.STRING.mimeType(), msg);
        outData.setDescription(description);

        xMsgMeta.Builder outMeta = getMetadata(outData);
        outMeta.setStatus(xMsgMeta.Status.ERROR);
        outMeta.setSeverityId(severity);

        return outData;
    }

    /**
     * Convoluted way to access the internal EngineData metadata,
     * which is hidden to users.
     *
     * @param data {@link org.jlab.clara.engine.EngineData} object
     * @return {@link org.jlab.coda.xmsg.data.xMsgM.xMsgMeta.Builder} object
     */
    public xMsgMeta.Builder getMetadata(EngineData data) {
        return DATA_ACCESSOR.getMetadata(data);
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
     * Sets a DPE Clara component as a front-end.
     *
     * @param frontEnd {@link org.jlab.clara.base.core.ClaraComponent} object
     */
    public void setFrontEnd(ClaraComponent frontEnd) {
        this.frontEnd = frontEnd;
    }

    /**
     * *********************** Private Methods *****************************
     */

    /**
     * xMsg send wrapper.
     *
     * @param component Clara component
     * @param msg xMsgMessage object
     * @param timeout timeout in case sync send. Note timeout <=0 indicates async send
     * @return xMsgMessage in case sync request, null otherwise
     * @throws TimeoutException
     * @throws xMsgException
     */
    private xMsgMessage _send(ClaraComponent component, xMsgMessage msg, int timeout)
            throws TimeoutException, xMsgException {
        if (timeout > 0) {
            return syncSend(component, msg, timeout);
        } else {
            send(component, msg);
            return null;
        }
    }

    /**
     * Deploys container or service Clara component on a DPE.
     *
     * @param component {@link org.jlab.clara.base.core.ClaraComponent} of the type container or service
     * @param timeout timeout in case of sync operation. Note timeout <=0 indicates async deploy
     * @return xMsgMessage in case sync request, null otherwise
     * @throws ClaraException
     * @throws IOException
     * @throws xMsgException
     * @throws TimeoutException
     */
    private xMsgMessage _deploy(ClaraComponent component, int timeout)
            throws ClaraException, IOException, xMsgException, TimeoutException {
        if (component.isOrchestrator() || component.isDpe()) {
            throw new IllegalArgumentException("Clara-Error: illegal component to deploy");
        }
        String data;
        xMsgTopic topic = MessageUtils.buildTopic(ClaraConstants.DPE, component.getDpeCanonicalName());
        if (component.isContainer()) {
            data = MessageUtils.buildData(ClaraConstants.START_CONTAINER,
                        component.getDpeHost(),
                        component.getDpePort(),
                        component.getDpeLang(),
                        component.getContainerName(),
                        component.getSubscriptionPoolSize(),
                        component.getDescription());
        } else if (component.isService()) {
            data = MessageUtils.buildData(ClaraConstants.START_SERVICE,
                    component.getDpeHost(),
                    component.getDpePort(),
                    component.getDpeLang(),
                    component.getContainerName(),
                    component.getEngineName(),
                    component.getEngineClass(),
                    component.getSubscriptionPoolSize(),
                    component.getDescription());

        } else {
            throw new ClaraException("Clara-Error: unknown or undefined component type. ");
        }
        xMsgMessage msg = MessageUtils.buildRequest(topic, data);
        return _send(component, msg, timeout);
    }

    /**
     * Request to stop dpe, container or service component.
     *
     * @param component the type dpe, container or service
     * @param timeout timeout in case of sync operation. Note timeout <=0 indicates async exit
     * @return xMsgMessage in case sync request, null otherwise
     * @throws IOException
     * @throws xMsgException
     * @throws TimeoutException
     * @throws ClaraException
     */
    private xMsgMessage _exit(ClaraComponent component, int timeout)
            throws IOException, xMsgException, TimeoutException, ClaraException {
        if (component.isOrchestrator()) {
            throw new IllegalArgumentException("Cannot deploy nor exit an orchestrator.");
        }
        String data;
        xMsgTopic topic = MessageUtils.buildTopic(ClaraConstants.DPE, component.getDpeCanonicalName());
        if (component.isDpe()) {
            data = ClaraConstants.STOP_DPE;

        } else if (component.isContainer()) {
            data = MessageUtils.buildData(ClaraConstants.STOP_CONTAINER,
                    component.getContainerName());
        } else if (component.isService()) {
            data = MessageUtils.buildData(ClaraConstants.STOP_SERVICE,
                    component.getContainerName(),
                    component.getEngineName());
        } else {
            throw new ClaraException("Clara-Error: unknown or undefined component type. ");
        }
        xMsgMessage msg = MessageUtils.buildRequest(topic, data);
        return _send(component, msg, timeout);
    }


    public abstract static class EngineDataAccessor {

        // CHECKSTYLE.OFF: StaticVariableName
        private static volatile EngineDataAccessor DEFAULT;
        // CHECKSTYLE.ON: StaticVariableName

        public static EngineDataAccessor getDefault() {
            new EngineData(); // Load the accessor
            EngineDataAccessor a = DEFAULT;
            if (a == null) {
                throw new IllegalStateException("EngineDataAccessor should not be null");
            }
            return a;
        }

        public static void setDefault(EngineDataAccessor accessor) {
            if (DEFAULT != null) {
                throw new IllegalStateException("EngineDataAccessor should be null");
            }
            DEFAULT = accessor;
        }

        protected abstract xMsgMeta.Builder getMetadata(EngineData data);

        protected abstract EngineData build(Object data, xMsgMeta.Builder metadata);
    }
}
