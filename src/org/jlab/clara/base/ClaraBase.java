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

package org.jlab.clara.base;

import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.util.CConstants;
import org.jlab.clara.util.report.CReportTypes;
import org.jlab.coda.xmsg.core.xMsg;
import org.jlab.coda.xmsg.core.xMsgCallBack;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgSubscription;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgM;
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

    private final String claraHome;

    private static final EngineDataAccessor dataAccessor = EngineDataAccessor.getDefault();

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
    public ClaraBase(ClaraComponent me,
                     ClaraComponent frontEnd) {
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

    // abstract methods to start and gracefully end Clara components
    public abstract void end();

    public abstract void start(ClaraComponent component);

    /**
     * @return the path to the Clara_home defined
     * by means of the CLARA_HOME env variable.
     */
    public String getClaraHome() {
        return claraHome;
    }

    /**
     * @return the description of this component:
     * {@link org.jlab.clara.base.ClaraComponent} object
     */
    public ClaraComponent getMe() {
        return me;
    }

    /**
     * Sends xMsgMessage message to a component.
     *
     * @param component {@link org.jlab.clara.base.ClaraComponent} object
     * @param msg message: {@link org.jlab.coda.xmsg.core.xMsgMessage} object
     * @throws xMsgException
     */
    public void send(ClaraComponent component, xMsgMessage msg)
            throws xMsgException {
        xMsgConnection con = connect(component.getProxyAddress());
        publish(con, msg);
        release(con);
    }

    /**
     * Sends a string to a component.
     *
     * @param component {@link org.jlab.clara.base.ClaraComponent} object
     * @param requestText string of the message
     * @throws IOException
     * @throws xMsgException
     */
    public void send(ClaraComponent component, String requestText)
            throws IOException, xMsgException {
        xMsgMessage msg = createRequest(component.getTopic(), requestText);
        xMsgConnection con = connect(component.getProxyAddress());
        publish(con, msg);
        release(con);
    }

    /**
     * Sending a message using the defined connection.
     *
     * @param con connection: {@link org.jlab.coda.xmsg.net.xMsgConnection} object
     * @param msg message: {@link org.jlab.coda.xmsg.core.xMsgMessage} object
     * @throws xMsgException
     */
    public void send(xMsgConnection con, xMsgMessage msg)
            throws xMsgException {
        publish(con, msg);
    }

    /**
     * Sending a message using the dpe host and port of this component.
     *
     * @param msg message: {@link org.jlab.coda.xmsg.core.xMsgMessage} object
     * @throws IOException
     * @throws xMsgException
     */
    public void send(xMsgMessage msg)
            throws IOException, xMsgException {
        send(me, msg);
    }

    /**
     * Sending a text message using the dpe host and port of this component.
     *
     * @param msgText String of the message
     * @throws IOException
     * @throws xMsgException
     */
    public void send(String msgText)
            throws IOException, xMsgException {
        send(me, msgText);
    }

    /**
     * Synchronous xMsgMessage send to a component.
     *
     * @param component {@link org.jlab.clara.base.ClaraComponent} object
     * @param msg message: {@link org.jlab.coda.xmsg.core.xMsgMessage} object
     * @param timeout in milli seconds
     * @return message: {@link org.jlab.coda.xmsg.core.xMsgMessage} object
     * @throws xMsgException
     * @throws TimeoutException
     */
    public xMsgMessage syncSend(ClaraComponent component, xMsgMessage msg, int timeout)
            throws xMsgException, TimeoutException {
        xMsgConnection con = connect(component.getProxyAddress());
        xMsgMessage m = syncPublish(con, msg, timeout);
        release(con);
        return m;
    }

    /**
     * Synchronous string send to a component.
     *
     * @param component {@link org.jlab.clara.base.ClaraComponent} object
     * @param requestText String of the message
     * @param timeout in milli seconds
     * @return message: {@link org.jlab.coda.xmsg.core.xMsgMessage} object
     * @throws IOException
     * @throws xMsgException
     * @throws TimeoutException
     */
    public xMsgMessage syncSend(ClaraComponent component, String requestText, int timeout)
            throws IOException, xMsgException, TimeoutException {
        xMsgMessage msg = createRequest(component.getTopic(), requestText);
        xMsgConnection con = connect(component.getProxyAddress());
        xMsgMessage m = syncPublish(con, msg, timeout);
        release(con);
        return m;
    }


    /**
     * Listens messages from the defined component.
     * Connection is done to the dpe of the passed component.
     * The topic is the name of the define component.
     *
     * @param component {@link org.jlab.clara.base.ClaraComponent} object
     * @param callback {@link org.jlab.coda.xmsg.core.xMsgCallBack} object
     * @return subscription handler {@link org.jlab.coda.xmsg.core.xMsgMessage} object
     * @throws xMsgException
     */
    public xMsgSubscription listen(ClaraComponent component, xMsgCallBack callback)
            throws xMsgException {
        xMsgConnection con = connect(component.getProxyAddress());
        return subscribe(con, component.getTopic(), callback);
    }

    /**
     * Listens messages from a defined component to a specified topic.
     * Connection is done to the dpe of the passed component.
     *
     * @param component {@link org.jlab.clara.base.ClaraComponent} object
     * @param topic topic of the subscription
     * @param callback {@link org.jlab.coda.xmsg.core.xMsgCallBack} object
     * @return subscription handler {@link org.jlab.coda.xmsg.core.xMsgMessage} object
     * @throws xMsgException
     */
    public xMsgSubscription listen(ClaraComponent component, xMsgTopic topic, xMsgCallBack callback)
            throws xMsgException {
        xMsgConnection con = connect(component.getProxyAddress());
        return subscribe(con, topic, callback);
    }

    /**
     * Listens messages coming to a dpe of this component (i.e. connection
     * is done to the local dpe) to a specified topic.
     *
     * @param topic topic of the subscription
     * @param callback {@link org.jlab.coda.xmsg.core.xMsgCallBack} object
     * @return subscription handler {@link org.jlab.coda.xmsg.core.xMsgMessage} object
     * @throws xMsgException
     */
    public xMsgSubscription listen(xMsgTopic topic, xMsgCallBack callback)
            throws xMsgException {
        xMsgConnection con = connect(me.getProxyAddress());
        return subscribe(con, topic, callback);
    }

    /**
     * Stops listening to a subscription defined by the handler.
     *
     * @param handle subscription handler {@link org.jlab.coda.xmsg.core.xMsgMessage} object
     * @throws xMsgException
     */
    public void stopListening(xMsgSubscription handle)
            throws xMsgException {
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
        try {
            xMsgRegAddress regAddress = new xMsgRegAddress(frontEnd.getDpeHost());
            registerAsSubscriber(regAddress, topic, description);
        } catch (xMsgException e) {
            throw new ClaraException("Could not register with front-end registrar", e);
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
        try {
            xMsgRegAddress regAddress = new xMsgRegAddress(frontEnd.getDpeHost());
            deregisterAsSubscriber(regAddress, topic);
        } catch (xMsgException e) {
            throw new ClaraException("Could not register with front-end registrar", e);
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
     * @param component Clara actor as a {@link org.jlab.clara.base.ClaraComponent} object
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
     * @param component Clara actor as a {@link org.jlab.clara.base.ClaraComponent} object
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
     * @param component Clara actor as a {@link org.jlab.clara.base.ClaraComponent} object
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
     * @param component Clara actor as a {@link org.jlab.clara.base.ClaraComponent} object
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
     * @param component component Clara actor as a {@link org.jlab.clara.base.ClaraComponent} object
     * @param report report type define as a {@link org.jlab.clara.util.report.CReportTypes} object
     * @param eventCount number of events after which component
     *                   reports/broadcasts required type of a report
     * @throws IOException
     * @throws xMsgException
     */
    public void startReporting(ClaraComponent component, CReportTypes report, int eventCount)
            throws IOException, xMsgException {

        if (eventCount < 0) {
            throw new IllegalArgumentException("Clara-Error: Invalid event count: " + eventCount);
        }
        String data = ClaraUtil.buildData(report.getValue(), eventCount);
        xMsgTopic topic = component.getTopic();
        xMsgMessage msg = createRequest(topic, data);
        send(component, msg);
    }

    /**
     * Sends a message to a Clara component to stop reporting a specific type of report type.
     *
     * @param component component Clara actor as a {@link org.jlab.clara.base.ClaraComponent} object
     * @param report report type define as a {@link org.jlab.clara.util.report.CReportTypes} object
     * @throws IOException
     * @throws xMsgException
     */
    public void stopReporting(ClaraComponent component, CReportTypes report)
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
            String data = ClaraUtil.buildData(CReportTypes.INFO.getValue());
            xMsgTopic topic = component.getTopic();
            xMsgMessage msg = createRequest(topic, data);
            return syncSend(component, msg, timeout);
        }
        return null;
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
                    Object userData = dt.serializer().read(bb);
                    return dataAccessor.build(userData, metadata);
                } catch (ClaraException e) {
                    throw new ClaraException("Clara-Error: Could not deserialize " + mimeType, e);
                }
            }
        }
        throw new ClaraException("Clara-Error: Unsupported mime-type = " + mimeType);
    }


    /**
     * Builds a message by serializing passed data object using serialization
     * routine defined in one of the data types objects.
     *
     * @param topic the topic where the data will be published
     * @param data the data to be serialized
     * @param dataTypes the set of registered data types
     * @throws ClaraException if the data could not be serialized
     */
    public static xMsgMessage serialize(xMsgTopic topic,
                                        EngineData data,
                                        Set<EngineDataType> dataTypes)
            throws ClaraException {

        xMsgMeta.Builder metadata = dataAccessor.getMetadata(data);
        String mimeType = metadata.getDataType();
        for (EngineDataType dt : dataTypes) {
            if (dt.mimeType().equals(mimeType)) {
                try {
                    ByteBuffer bb = dt.serializer().write(data.getData());
                    return new xMsgMessage(topic, metadata, bb.array());
                } catch (ClaraException e) {
                    throw new ClaraException("Could not serialize " + mimeType, e);
                }
            }
        }
        throw new ClaraException("Unsupported mime-type = " + mimeType);
    }

    public static xMsgMessage createRequest(xMsgTopic topic, String data) {
        return new xMsgMessage(topic, xMsgConstants.MimeType.STRING, data.getBytes());
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
        return dataAccessor.getMetadata(data);
    }

    /**
     * Returns the reference to the front-end DPE.
     *
     * @return {@link org.jlab.clara.base.ClaraComponent} object
     */
    public ClaraComponent getFrontEnd() {
        return frontEnd;
    }

    /**
     * Sets a DPE Clara component as a front-end.
     *
     * @param frontEnd {@link org.jlab.clara.base.ClaraComponent} object
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
     * @param component {@link org.jlab.clara.base.ClaraComponent} of the type container or service
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
        xMsgTopic topic = ClaraUtil.buildTopic(CConstants.DPE, component.getDpeCanonicalName());
        if (component.isContainer()) {
            data = ClaraUtil.buildData(CConstants.START_CONTAINER,
                        component.getDpeHost(),
                        component.getDpePort(),
                        component.getDpeLang(),
                        component.getContainerName(),
                        component.getSubscriptionPoolSize(),
                        component.getDescription());
        } else if (component.isService()) {
            data = ClaraUtil.buildData(CConstants.START_SERVICE,
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
        xMsgMessage msg = createRequest(topic, data);
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
        xMsgTopic topic = ClaraUtil.buildTopic(CConstants.DPE, component.getDpeCanonicalName());
        if (component.isDpe()) {
            data = CConstants.STOP_DPE;

        } else if (component.isContainer()) {
            data = ClaraUtil.buildData(CConstants.STOP_CONTAINER,
                    component.getContainerName());
        } else if (component.isService()) {
            data = ClaraUtil.buildData(CConstants.STOP_SERVICE,
                    component.getContainerName(),
                    component.getEngineName());
        } else {
            throw new ClaraException("Clara-Error: unknown or undefined component type. ");
        }
        xMsgMessage msg = createRequest(topic, data);
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

        protected abstract xMsgM.xMsgMeta.Builder getMetadata(EngineData data);

        protected abstract EngineData build(Object data, xMsgM.xMsgMeta.Builder metadata);
    }
}
