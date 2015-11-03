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

import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataAccessor;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.util.CConstants;
import org.jlab.clara.util.ClaraUtil;
import org.jlab.clara.util.report.CReportTypes;
import org.jlab.coda.xmsg.core.*;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnection;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.jlab.coda.xmsg.net.xMsgRegAddress;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
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

    private String claraHome;

    private EngineDataAccessor dataAccessor;

    // reference to this component description
    private ClaraComponent me;

    // reference to the front end DPE
    private ClaraComponent frontEnd = null;

    /**
     * Constructor of this Clara component
     *
     * @param me                   Definition of the component: {@link org.jlab.clara.base.ClaraComponent} object
     * @param defaultRegistrarHost host name of the xMsg registrar
     * @param defaultRegistrarPort port number of the xMsg registrar
     * @throws IOException
     * @throws ClaraException
     */
    public ClaraBase(ClaraComponent me,
                     String defaultRegistrarHost,
                     int defaultRegistrarPort)
            throws IOException, ClaraException {
        super(me.getCanonicalName(), new xMsgProxyAddress(me.getDpeHost(), me.getDpePort()),
                new xMsgRegAddress(defaultRegistrarHost, defaultRegistrarPort),
                me.getSubscriptionPoolSize());
        dataAccessor = EngineDataAccessor.getDefault();
        this.me = me;
        claraHome = System.getenv("CLARA_HOME");
        if(claraHome ==null) {
            throw new ClaraException("Clara-Error: CLARA_HOME environmental variable is not defined.");
        }
    }

    /**
     * Constructor
     *
     * @param me Definition of the component: {@link org.jlab.clara.base.ClaraComponent} object
     * @throws IOException
     * @throws ClaraException
     */
    public ClaraBase(ClaraComponent me)
            throws IOException, ClaraException {
        this(me, xMsgUtil.localhost(),
                xMsgConstants.REGISTRAR_PORT);
    }

    // abstract methods
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
        publish(con,msg);
        release(con);
    }

    /**
     * Sends a string to a component
     *
     * @param component {@link org.jlab.clara.base.ClaraComponent} object
     * @param requestText string of the message
     * @throws IOException
     * @throws xMsgException
     */
    public void send(ClaraComponent component, String requestText)
            throws IOException, xMsgException {
        xMsgMessage msg = new xMsgMessage(component.getTopic(), requestText);
        xMsgConnection con = connect(component.getProxyAddress());
        publish(con, msg);
        release(con);
    }

    /**
     * Synchronous xMsgMessage send to a component
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
     * Synchronous string send to a component
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
        xMsgMessage msg = new xMsgMessage(component.getTopic(), requestText);
        xMsgConnection con = connect(component.getProxyAddress());
        xMsgMessage m = syncPublish(con, msg, timeout);
        release(con);
        return m;
    }

    /**
     * Sending a message using the defined connection
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
     * Sending a message using the dpe host and port of this component
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
     * Sending a text message using the dpe host and port of this component
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
     * Stops listening to a subscription defined by the handler
     *
     * @param handle subscription handler {@link org.jlab.coda.xmsg.core.xMsgMessage} object
     * @throws xMsgException
     */
    public void stopListening(xMsgSubscription handle)
            throws xMsgException {
        unsubscribe(handle);
    }

    /**
     * Method registers a Clara actor with the xMsg in-memory registration service.
     * Note that Clara service registers as an xMsg subscriber.
     *
     * @param regHost registrar server host
     * @param regPort registrar server port
     * @param description service description
     * @throws IOException
     * @throws xMsgException
     */
    public void register(String regHost, int regPort, String description )
            throws IOException, xMsgException {
        xMsgRegAddress regAddress = new xMsgRegAddress(regHost, regPort);
        registerAsSubscriber(regAddress, me.getTopic(), description);
    }

    /**
     * Registers a Clara actor with the xMsg in-memory registration service.
     * This will assume that registration service is running on a default port.
     *
     * @param regHost registrar server host
     * @param description service description
     * @throws IOException
     * @throws xMsgException
     */
    public void register(String regHost, String description )
            throws IOException, xMsgException {
        xMsgRegAddress regAddress = new xMsgRegAddress(regHost);
        registerAsSubscriber(regAddress, me.getTopic(), description);
    }

    /**
     * Registers a Clara actor with the xMsg in-memory registration service.
     * This assumes registration service is running on a same node with a default port number.
     *
     * @param description service description
     * @throws IOException
     * @throws xMsgException
     */
    public void register(String description )
            throws IOException, xMsgException {
        registerAsSubscriber(me.getTopic(), description);
    }

    /**
     * Removes actor registration
     *
     * @param regHost registrar server host
     * @param regPort registrar server port
     * @throws IOException
     * @throws xMsgException
     */
    public void removeRegistration(String regHost, int regPort)
            throws IOException, xMsgException {
        xMsgRegAddress regAddress = new xMsgRegAddress(regHost, regPort);
        removeSubscriberRegistration(regAddress, me.getTopic());
    }

    /**
     * Removes actor registration, assuming registrar is running on a default port.
     *
     * @param regHost registrar server host
     * @throws IOException
     * @throws xMsgException
     */
    public void removeRegistration(String regHost)
            throws IOException, xMsgException {
        xMsgRegAddress regAddress = new xMsgRegAddress(regHost);
        removeSubscriberRegistration(regAddress, me.getTopic());
    }

    /**
     * Removes actor registration, assuming registrar is running
     * on a local host and with a default port.
     *
     * @throws IOException
     * @throws xMsgException
     */
    public void removeRegistration()
            throws IOException, xMsgException {
        removeSubscriberRegistration(me.getTopic());
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
    public Set<xMsgRegistration> discover(String regHost, int regPort, xMsgTopic topic )
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
    public Set<xMsgRegistration> discover(xMsgTopic topic )
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
     * Sends a message to a Clara component/actor telling to exit/destruct
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
     * Sync sends a message to a Clara component/actor telling to exit/destruct
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

    public void configureService(ClaraComponent component, EngineData data)
            throws ClaraException, TimeoutException, xMsgException, IOException {
        _configure(component, data, -1);
    }

    public xMsgMessage syncConfigureService(ClaraComponent component, EngineData data, int timeout)
            throws ClaraException, TimeoutException, xMsgException, IOException {
        return _configure(component, data, timeout);
    }

    public void executeService(ClaraComponent component, EngineData data)
            throws ClaraException, TimeoutException, xMsgException, IOException {
        _execute(component, data, -1);
    }

    public xMsgMessage syncExecuteService(ClaraComponent component, EngineData data, int timeout)
            throws ClaraException, TimeoutException, xMsgException, IOException {
        return _execute(component, data, timeout);
    }

    public void executeComposition(Composition composition, EngineData data)
            throws ClaraException, TimeoutException, xMsgException, IOException {
        _executeComp(composition, data, -1);
    }

    public xMsgMessage syncExecuteComposition(Composition composition, EngineData data, int timeout)
            throws ClaraException, TimeoutException, xMsgException, IOException {
        return _executeComp(composition, data, timeout);
    }

    public void startReporting(ClaraComponent component, CReportTypes report, int eventCount)
            throws IOException, xMsgException {

        if (eventCount < 0) {
            throw new IllegalArgumentException("Clara-Error: Invalid event count: " + eventCount);
        }
        String data = ClaraUtil.buildData(report.getValue(), eventCount);
        xMsgTopic topic = component.getTopic();
        xMsgMessage msg = new xMsgMessage(topic, data);
        send(component,msg);
    }

    public void stopReporting(ClaraComponent component, CReportTypes report)
            throws IOException, xMsgException {
        startReporting(component, report, 0);
    }

    public xMsgMessage pingDpe(ClaraComponent component, int timeout)
            throws IOException, xMsgException, TimeoutException {

        if (component.isDpe()) {
            String data = ClaraUtil.buildData(CReportTypes.INFO.getValue());
            xMsgTopic topic = component.getTopic();
            xMsgMessage msg = new xMsgMessage(topic, data);
            return syncSend(component, msg, timeout);
        }
        return null;
    }


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


    public void serialize(EngineData data, xMsgMessage msg, Set<EngineDataType> dataTypes)
            throws ClaraException {
        xMsgMeta.Builder metadata = dataAccessor.getMetadata(data);
        String mimeType = metadata.getDataType();
        for (EngineDataType dt : dataTypes) {
            if (dt.mimeType().equals(mimeType)) {
                try {
                    ByteBuffer bb = dt.serializer().write(data.getData());
                    msg.setMetaData(metadata);
                    msg.setData(bb.array());
                    return;
                } catch (ClaraException e) {
                    throw new ClaraException("Could not serialize " + mimeType, e);
                }
            }
        }
        throw new ClaraException("Unsupported mime-type = " + mimeType);
    }

    public xMsgMessage buildMessage(xMsgTopic topic, EngineData data, Set<EngineDataType> dataTypes)
            throws ClaraException, xMsgException, IOException {
        try {
            xMsgMessage msg = new xMsgMessage(topic, data);
            serialize(data, msg, dataTypes);
            return msg;
        } catch (ClaraException e) {
            throw new ClaraException("Clara-Error: Could not serialize data", e);
        }
    }

    public EngineData reportSystemError(String msg, int severity, String description) {
        EngineData outData = new EngineData();
        outData.setData(EngineDataType.STRING.mimeType(), msg);
        outData.setDescription(description);

        xMsgMeta.Builder outMeta = getMetadata(outData);
        outMeta.setStatus(xMsgMeta.Status.ERROR);
        outMeta.setSeverityId(severity);

        return outData;
    }

    /*
 * Convoluted way to access the internal EngineData metadata,
 * which is hidden to users.
 */
    public xMsgMeta.Builder getMetadata(EngineData data) {
        return dataAccessor.getMetadata(data);
    }

    public ClaraComponent getFrontEnd() {
        return frontEnd;
    }

    public void setFrontEnd(ClaraComponent frontEnd) {
        this.frontEnd = frontEnd;
    }

    /**
     * *********************** Private Methods *****************************
     */

    private xMsgMessage __send(ClaraComponent component, xMsgMessage msg, int timeout)
            throws TimeoutException, xMsgException {
        if (timeout > 0) {
            return syncSend(component, msg, timeout);
        } else {
            send(component, msg);
            return null;
        }
    }

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
            xMsgMessage msg = new xMsgMessage(topic, data);
            return __send(component, msg, timeout);
    }

    private xMsgMessage _exit(ClaraComponent component, int timeout)
            throws IOException, xMsgException, TimeoutException, ClaraException {
        if(component.isOrchestrator()) {
            throw new IllegalArgumentException("Clara-Error: can not deploy nor exit an orchestrator.");
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
            xMsgMessage msg = new xMsgMessage(topic, data);
            return __send(component, msg, timeout);
    }

    private xMsgMessage _configure(ClaraComponent component, EngineData data, int timeout)
            throws ClaraException, IOException, xMsgException, TimeoutException {
        if(component.isService()){

            xMsgTopic topic = component.getTopic();
            xMsgMessage msg = new xMsgMessage(topic, data);
            xMsgMeta.Builder msgMeta = msg.getMetaData();
            msgMeta.setComposition(topic.toString());
            msgMeta.setAction(xMsgMeta.ControlAction.CONFIGURE);
            if(timeout>0){
                return syncSend(component, msg, timeout);
            } else {
                send(component, msg);
                return null;
            }
        } else {
            throw new ClaraException("Clara-Error: configure is not supported for this component ");
        }
    }

    private xMsgMessage _execute(ClaraComponent component, EngineData data, int timeout)
            throws ClaraException, IOException, xMsgException, TimeoutException {
        if(component.isOrchestrator() || component.isDpe() || component.isContainer() ) {
            throw new ClaraException("Clara-Error: orchestrator, dpe and container configurations are not supported");
        }
        if(component.isService()){

            xMsgTopic topic = component.getTopic();
            xMsgMessage msg = new xMsgMessage(topic, data);
            xMsgMeta.Builder msgMeta = msg.getMetaData();
            msgMeta.setComposition(topic.toString());
            msgMeta.setAction(xMsgMeta.ControlAction.EXECUTE);
            if(timeout>0){
                return syncSend(component, msg, timeout);
            } else {
                send(component, msg);
                return null;
            }
        } else {
            throw new ClaraException("Clara-Error: unknown or undefined component type. ");
        }

    }

    private xMsgMessage _executeComp(Composition composition, EngineData data, int timeout)
            throws ClaraException, xMsgException, IOException, TimeoutException {
        Objects.requireNonNull(composition, "Null service composition");
        Objects.requireNonNull(data, "Null input data");

        String firstService = composition.firstService();

        return _execute(ClaraComponent.service(firstService), data, timeout);
    }
}
