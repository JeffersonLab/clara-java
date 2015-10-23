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
import org.jlab.coda.xmsg.data.xMsgM;
import org.jlab.coda.xmsg.data.xMsgR;
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
    private ClaraComponent me;

    private ClaraComponent frontEnd = null;

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

    public ClaraBase(ClaraComponent me)
            throws IOException, ClaraException {
        this(me, xMsgUtil.localhost(),
                xMsgConstants.REGISTRAR_PORT);
    }

    public abstract void exit();

    public abstract void start(ClaraComponent component);

    public String getClaraHome() {
        return claraHome;
    }

    public ClaraComponent getMe() {
        return me;
    }

    public void send(ClaraComponent component, xMsgMessage msg)
            throws xMsgException {
        xMsgConnection con = connect(component.getProxyAddress());
        publish(con,msg);
        release(con);
    }

    public void send(ClaraComponent component, String requestText)
            throws IOException, xMsgException {
        xMsgMessage msg = new xMsgMessage(component.getTopic(), requestText);
        xMsgConnection con = connect(component.getProxyAddress());
        publish(con, msg);
        release(con);
    }

    public xMsgMessage syncSend(ClaraComponent component, xMsgMessage msg, int timeout)
            throws xMsgException, TimeoutException {
        xMsgConnection con = connect(component.getProxyAddress());
        xMsgMessage m = syncPublish(con, msg, timeout);
        release(con);
        return m;
    }

    public xMsgMessage syncSend(ClaraComponent component, String requestText, int timeout)
            throws IOException, xMsgException, TimeoutException {
        xMsgMessage msg = new xMsgMessage(component.getTopic(), requestText);
        xMsgConnection con = connect(component.getProxyAddress());
        xMsgMessage m = syncPublish(con, msg, timeout);
        release(con);
        return m;
    }

    public void send(xMsgConnection con, xMsgMessage msg)
            throws xMsgException {
        publish(con, msg);
    }

    public void send(xMsgMessage msg)
            throws IOException, xMsgException {
        send(me, msg);
    }

    public void send(String msgText)
            throws IOException, xMsgException {
        send(me, msgText);
    }

    public xMsgSubscription listen(ClaraComponent component, xMsgCallBack callback)
            throws xMsgException {
        xMsgConnection con = connect(component.getProxyAddress());
        return subscribe(con, component.getTopic(), callback);
    }

    public xMsgSubscription listen(ClaraComponent component, xMsgTopic topic, xMsgCallBack callback)
            throws xMsgException {
        xMsgConnection con = connect(component.getProxyAddress());
        return subscribe(con, topic, callback);
    }

    public xMsgSubscription listen(xMsgTopic topic, xMsgCallBack callback)
            throws xMsgException {
        xMsgConnection con = connect(me.getProxyAddress());
        return subscribe(con, topic, callback);
    }

    public void stopListening(xMsgSubscription handle)
            throws xMsgException {
        unsubscribe(handle);
    }

    public void register(String regHost, int regPort, String description )
            throws IOException, xMsgException {
        xMsgRegAddress regAddress = new xMsgRegAddress(regHost, regPort);
        registerAsSubscriber(regAddress, me.getTopic(), description);
    }

    public void register(String regHost, String description )
            throws IOException, xMsgException {
        xMsgRegAddress regAddress = new xMsgRegAddress(regHost);
        registerAsSubscriber(regAddress, me.getTopic(), description);
    }

    public void register(String description )
            throws IOException, xMsgException {
        registerAsSubscriber(me.getTopic(), description);
    }

    public void removeRegistration(String regHost, int regPort)
            throws IOException, xMsgException {
        xMsgRegAddress regAddress = new xMsgRegAddress(regHost, regPort);
        removeSubscriberRegistration(regAddress, me.getTopic());
    }

    public void removeRegistration(String regHost)
            throws IOException, xMsgException {
        xMsgRegAddress regAddress = new xMsgRegAddress(regHost);
        removeSubscriberRegistration(regAddress, me.getTopic());
    }

    public void removeRegistration()
            throws IOException, xMsgException {
        removeSubscriberRegistration(me.getTopic());
    }

    public Set<xMsgR.xMsgRegistration> discover(String regHost, int regPort, xMsgTopic topic )
            throws IOException, xMsgException {
        xMsgRegAddress regAddress = new xMsgRegAddress(regHost, regPort);
        return findSubscribers(regAddress, topic);
    }

    public Set<xMsgR.xMsgRegistration> discover(String regHost, xMsgTopic topic)
            throws IOException, xMsgException {
        xMsgRegAddress regAddress = new xMsgRegAddress(regHost);
        return findSubscribers(regAddress, topic);
    }

    public Set<xMsgR.xMsgRegistration> discover(xMsgTopic topic )
            throws IOException, xMsgException {
        return findSubscribers(topic);
    }

    public void deploy(ClaraComponent component, boolean isRemote)
            throws ClaraException, IOException, xMsgException, TimeoutException {
        _deploy(component, isRemote, -1);
    }

    public xMsgMessage syncDeploy(ClaraComponent component, boolean isRemote, int timeout)
            throws ClaraException, IOException, xMsgException, TimeoutException {
        return _deploy(component, isRemote, timeout);
    }

    public void exitComponent(ClaraComponent component, boolean isRemote)
            throws ClaraException, IOException, xMsgException, TimeoutException {
        _exit(component, isRemote, -1);
    }

    public xMsgMessage syncExitComponent(ClaraComponent component, boolean isRemote, int timeout)
            throws ClaraException, IOException, xMsgException, TimeoutException {
        return _exit(component, isRemote, timeout);
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

    public xMsgMessage ping(ClaraComponent component, int timeout)
            throws IOException, xMsgException, TimeoutException {

        String data = ClaraUtil.buildData(CReportTypes.INFO.getValue());
        xMsgTopic topic = component.getTopic();
        xMsgMessage msg = new xMsgMessage(topic, data);
        return syncSend(component, msg, timeout);
    }


    public EngineData deSerialize(xMsgMessage msg, Set<EngineDataType> dataTypes)
            throws ClaraException {
        xMsgM.xMsgMeta.Builder metadata = msg.getMetaData();
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
        xMsgM.xMsgMeta.Builder metadata = dataAccessor.getMetadata(data);
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

        xMsgM.xMsgMeta.Builder outMeta = getMetadata(outData);
        outMeta.setStatus(xMsgM.xMsgMeta.Status.ERROR);
        outMeta.setSeverityId(severity);

        return outData;
    }

    /*
 * Convoluted way to access the internal EngineData metadata,
 * which is hidden to users.
 */
    public xMsgM.xMsgMeta.Builder getMetadata(EngineData data) {
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

    private xMsgMessage _deploy(ClaraComponent component, boolean isRemote, int timeout)
            throws ClaraException, IOException, xMsgException, TimeoutException {
        if(component.isOrchestrator()) {
            throw new IllegalArgumentException("Clara-Error: can not deploy nor exit an orchestrator.");
        }
        if (!isRemote) {
            start(component);
            return null;
        } else {
            String data;
            xMsgTopic topic = ClaraUtil.buildTopic(CConstants.DPE, component.getCanonicalName());
            if (component.isDpe()) {
                data = ClaraUtil.buildData(CConstants.START_DPE,
                        component.getDpeHost(),
                        component.getDpePort(),
                        component.getDpeLang(),
                        component.getSubscriptionPoolSize(),
                        getDefaultRegistrarAddress().host(),
                        getDefaultRegistrarAddress().port(),
                        component.getDescription());

            } else if (component.isContainer()) {
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
    }

    private xMsgMessage _exit(ClaraComponent component, boolean isRemote, int timeout)
            throws IOException, xMsgException, TimeoutException, ClaraException {
        if(component.isOrchestrator()) {
            throw new IllegalArgumentException("Clara-Error: can not deploy nor exitComponent an orchestrator.");
        }
        if (!isRemote) {
            exit();
            return null;
        } else {
            String data;

            xMsgTopic topic = ClaraUtil.buildTopic(CConstants.DPE, component.getCanonicalName());
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
    }

    private xMsgMessage _configure(ClaraComponent component, EngineData data, int timeout)
            throws ClaraException, IOException, xMsgException, TimeoutException {
        if(component.isOrchestrator() || component.isDpe() || component.isContainer() ) {
            throw new ClaraException("Clara-Error: orchestrator, dpe and container configurations are not supported");
        }
        if(component.isService()){

            xMsgTopic topic = component.getTopic();
            xMsgMessage msg = new xMsgMessage(topic, data);
            xMsgM.xMsgMeta.Builder msgMeta = msg.getMetaData();
            msgMeta.setComposition(topic.toString());
            msgMeta.setAction(xMsgM.xMsgMeta.ControlAction.CONFIGURE);
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

    private xMsgMessage _execute(ClaraComponent component, EngineData data, int timeout)
            throws ClaraException, IOException, xMsgException, TimeoutException {
        if(component.isOrchestrator() || component.isDpe() || component.isContainer() ) {
            throw new ClaraException("Clara-Error: orchestrator, dpe and container configurations are not supported");
        }
        if(component.isService()){

            xMsgTopic topic = component.getTopic();
            xMsgMessage msg = new xMsgMessage(topic, data);
            xMsgM.xMsgMeta.Builder msgMeta = msg.getMetaData();
            msgMeta.setComposition(topic.toString());
            msgMeta.setAction(xMsgM.xMsgMeta.ControlAction.EXECUTE);
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
