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
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.engine.EngineStatus;
import org.jlab.clara.util.CConstants;
import org.jlab.clara.util.ClaraUtil;
import org.jlab.clara.util.report.CReportTypes;
import org.jlab.coda.xmsg.core.*;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.excp.xMsgException;

import javax.annotation.ParametersAreNonnullByDefault;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeoutException;


/**
 * Base class for orchestration of applications.
 */
@ParametersAreNonnullByDefault
public class BaseOrchestrator {

    private final Set<EngineDataType> dataTypes = new HashSet<>();
    private final Map<String, xMsgSubscription> subscriptions = new HashMap<>();
    private ClaraBase base = null;


    public BaseOrchestrator() throws ClaraException, IOException {
        this(ClaraUtil.getUniqueName(),
                xMsgUtil.localhost(),
                xMsgConstants.REGISTRAR_PORT,

                xMsgUtil.localhost(),
                xMsgConstants.DEFAULT_PORT,
                CConstants.JAVA_LANG,
                xMsgConstants.DEFAULT_POOL_SIZE,
                CConstants.UNDEFINED);
    }

    public BaseOrchestrator(int subPoolSize) throws ClaraException, IOException {
        this(ClaraUtil.getUniqueName(),
                xMsgUtil.localhost(),
                xMsgConstants.REGISTRAR_PORT,
                xMsgUtil.localhost(),
                xMsgConstants.DEFAULT_PORT,
                CConstants.JAVA_LANG,
                subPoolSize,
                CConstants.UNDEFINED);
    }

    public BaseOrchestrator(String dpeHost, int subPoolSize, String description) throws ClaraException {
        this(ClaraUtil.getUniqueName(),
                dpeHost,
                xMsgConstants.REGISTRAR_PORT,
                dpeHost,
                xMsgConstants.DEFAULT_PORT,
                CConstants.JAVA_LANG,
                subPoolSize, description);
    }

    /**
     * @param name        the name of this orchestrator
     * @param regHost     registration service host
     * @param regPort     registration service port
     * @param dpeHost     front-end host
     * @param dpePort     front-end port
     * @param dpeLang     front-en d lang
     * @param subPoolSize thread pool size for subscriptions
     * @param description description of this orchestrator
     * @throws ClaraException
     */
    public BaseOrchestrator(String name,
                            String regHost,
                            int regPort,
                            String dpeHost,
                            int dpePort,
                            String dpeLang,
                            int subPoolSize,
                            String description)
            throws ClaraException {
        try {
            base = new ClaraBase(ClaraComponent.orchestrator(name, dpeHost, dpePort, dpeLang,
                    subPoolSize, description), regHost, regPort) {
                @Override
                public void end() {
                }

                @Override
                public void start(ClaraComponent component) {
                }
            };
            base.setFrontEnd(ClaraComponent.dpe(dpeHost, dpePort, dpeLang, 1, "FrontEnd"));
        } catch (IOException e) {
            throw new ClaraException("Clara-Error: Could not start orchestrator", e);
        }
    }

    /**
     * Returns the map of subscriptions for testing purposes.
     */
    Map<String, xMsgSubscription> getSubscriptions() {
        return subscriptions;
    }


    /**
     * Registers the necessary data-types to communicate with to services.
     *
     * @param dataTypes the data-types used by the services
     */
    public void registerDataTypes(EngineDataType... dataTypes) {
        Collections.addAll(this.dataTypes, dataTypes);
    }

    /**
     * Registers the necessary data-types to communicate with to services.
     *
     * @param dataTypes the data-types used by the services
     */
    public void registerDataTypes(Set<EngineDataType> dataTypes) {
        this.dataTypes.addAll(dataTypes);
    }


    /**
     * @param dpe
     * @param frontEnd
     */
    public void setFrontEnd(ClaraComponent dpe, ClaraComponent frontEnd)
            throws IOException, xMsgException {

        xMsgTopic topic = ClaraUtil.buildTopic(CConstants.DPE, base.getFrontEnd().getCanonicalName());

        String data = ClaraUtil.buildData(CConstants.SET_FRONT_END_REMOTE, dpe.getDpeHost(),
                dpe.getDpePort(), dpe.getDpeLang(), frontEnd.getDpeHost(),
                frontEnd.getDpePort(), frontEnd.getDpeLang());
        base.send(base.getFrontEnd(), new xMsgMessage(topic, data));
    }

    public void deployDpe(ClaraComponent comp, String regHost, int regPort)
            throws ClaraException, xMsgException, IOException, TimeoutException {
        if (comp.isDpe()) {
            xMsgTopic topic = ClaraUtil.buildTopic(CConstants.DPE, base.getFrontEnd().getCanonicalName());


            String data = ClaraUtil.buildData(CConstants.START_DPE,
                    comp.getDpeHost(),
                    comp.getDpePort(),
                    comp.getDpeLang(),
                    comp.getSubscriptionPoolSize(),
                    regHost, regPort,
                    comp.getDescription());
            base.send(base.getFrontEnd(), new xMsgMessage(topic, data));
        }
    }

    /**
     * Requests a DPE to exit.
     * The request is sent to a running DPE of the given language.
     * If no DPE is running in the node, the message is lost.
     *
     * @throws ClaraException if the request could not be sent
     */
    public void exitFrontEnd()
            throws ClaraException, xMsgException, IOException, TimeoutException {
        xMsgTopic topic = ClaraUtil.buildTopic(CConstants.DPE, base.getFrontEnd().getCanonicalName());

        String data = CConstants.STOP_DPE;
        base.send(base.getFrontEnd(), new xMsgMessage(topic, data));
    }

    public void deploy(ClaraComponent comp)
            throws ClaraException, TimeoutException, xMsgException, IOException {
        base.deploy(comp);
    }

    public xMsgMessage syncDeploy(ClaraComponent comp, int timeout)
            throws ClaraException, TimeoutException, xMsgException, IOException {
        return base.syncDeploy(comp, timeout);
    }


    public void feDeploy(ClaraComponent comp) throws IOException, xMsgException {
        xMsgTopic topic = ClaraUtil.buildTopic(CConstants.DPE, base.getFrontEnd().getDpeCanonicalName());
        String data = null;
        if (comp.isContainer()) {
            data = ClaraUtil.buildData(CConstants.START_REMOTE_CONTAINER,
                    comp.getDpeHost(),
                    comp.getDpePort(),
                    comp.getDpeLang(),
                    comp.getContainerName(),
                    comp.getSubscriptionPoolSize(),
                    comp.getDescription()
            );
        } else if (comp.isService()) {
            data = ClaraUtil.buildData(CConstants.START_REMOTE_SERVICE,
                    comp.getDpeHost(),
                    comp.getDpePort(),
                    comp.getDpeLang(),
                    comp.getContainerName(),
                    comp.getEngineName(),
                    comp.getEngineClass(),
                    comp.getSubscriptionPoolSize(),
                    comp.getDescription(),
                    comp.getInitialState()
            );
        }
        if (data == null) {
            throw new IllegalArgumentException("Clara-Error: illegal component to deploy");
        }
        base.send(base.getFrontEnd(), new xMsgMessage(topic, data));
    }

    public xMsgMessage feSyncDeploy(ClaraComponent comp, int timeout) throws IOException, xMsgException, TimeoutException {
        xMsgTopic topic = ClaraUtil.buildTopic(CConstants.DPE, base.getFrontEnd().getDpeCanonicalName());
        String data = null;
        if (comp.isContainer()) {
            data = ClaraUtil.buildData(CConstants.START_REMOTE_CONTAINER,
                    comp.getDpeHost(),
                    comp.getDpePort(),
                    comp.getDpeLang(),
                    comp.getContainerName(),
                    comp.getSubscriptionPoolSize(),
                    comp.getDescription()
            );
        } else if (comp.isService()) {
            data = ClaraUtil.buildData(CConstants.START_REMOTE_SERVICE,
                    comp.getDpeHost(),
                    comp.getDpePort(),
                    comp.getDpeLang(),
                    comp.getContainerName(),
                    comp.getEngineName(),
                    comp.getEngineClass(),
                    comp.getSubscriptionPoolSize(),
                    comp.getDescription(),
                    comp.getInitialState()
            );
        }
        if (data == null) {
            throw new IllegalArgumentException("Clara-Error: illegal component to deploy");
        }
        return base.syncSend(base.getFrontEnd(), new xMsgMessage(topic, data), timeout);
    }

    public void exit(ClaraComponent comp)
            throws ClaraException, TimeoutException, xMsgException, IOException {
        base.exit(comp);
    }

    public xMsgMessage syncExit(ClaraComponent comp, int timeout)
            throws ClaraException, TimeoutException, xMsgException, IOException {
        return base.syncExit(comp, timeout);
    }

    public void feExit(ClaraComponent comp)
            throws ClaraException, TimeoutException, xMsgException, IOException {
        xMsgTopic topic = ClaraUtil.buildTopic(CConstants.DPE, base.getFrontEnd().getDpeCanonicalName());
        String data = null;
        if (comp.isDpe()) {
            data = ClaraUtil.buildData(CConstants.STOP_REMOTE_DPE,
                    comp.getDpeHost(),
                    comp.getDpePort(),
                    comp.getDpeLang());

        } else if (comp.isContainer()) {
            data = ClaraUtil.buildData(CConstants.STOP_REMOTE_CONTAINER,
                    comp.getDpeHost(),
                    comp.getDpePort(),
                    comp.getDpeLang(),
                    comp.getContainerName()
            );
        } else if (comp.isService()) {
            data = ClaraUtil.buildData(CConstants.STOP_REMOTE_SERVICE,
                    comp.getDpeHost(),
                    comp.getDpePort(),
                    comp.getDpeLang(),
                    comp.getContainerName(),
                    comp.getEngineName()
            );
        }
        if (data == null) {
            throw new IllegalArgumentException("Clara-Error: illegal component to exit");
        }
        base.send(base.getFrontEnd(), new xMsgMessage(topic, data));
    }

    public xMsgMessage feSyncExit(ClaraComponent comp, int timeout)
            throws ClaraException, TimeoutException, xMsgException, IOException {
        xMsgTopic topic = ClaraUtil.buildTopic(CConstants.DPE, base.getFrontEnd().getDpeCanonicalName());
        String data = null;
        if (comp.isDpe()) {
            data = ClaraUtil.buildData(CConstants.STOP_REMOTE_DPE,
                    comp.getDpeHost(),
                    comp.getDpePort(),
                    comp.getDpeLang());

        } else if (comp.isContainer()) {
            data = ClaraUtil.buildData(CConstants.STOP_REMOTE_CONTAINER,
                    comp.getDpeHost(),
                    comp.getDpePort(),
                    comp.getDpeLang(),
                    comp.getContainerName()
            );
        } else if (comp.isService()) {
            data = ClaraUtil.buildData(CConstants.STOP_REMOTE_SERVICE,
                    comp.getDpeHost(),
                    comp.getDpePort(),
                    comp.getDpeLang(),
                    comp.getContainerName(),
                    comp.getEngineName()
            );
        }
        if (data == null) {
            throw new IllegalArgumentException("Clara-Error: illegal component to exit");
        }
        return base.syncSend(base.getFrontEnd(), new xMsgMessage(topic, data), timeout);
    }

    public xMsgMessage pingDpe(ClaraComponent dpe, int timeout)
            throws ClaraException, xMsgException, IOException, TimeoutException {
        return base.pingDpe(dpe, timeout);
    }

    public xMsgMessage fePingDpe(ClaraComponent comp, int timeout)
            throws ClaraException, xMsgException, IOException, TimeoutException {
        xMsgTopic topic = ClaraUtil.buildTopic(CConstants.DPE, base.getFrontEnd().getDpeCanonicalName());
        String data = null;
        if (comp.isDpe()) {
            data = ClaraUtil.buildData(CConstants.PING_REMOTE_DPE,
                    comp.getDpeHost(),
                    comp.getDpePort(),
                    comp.getDpeLang());

        }
        if (data == null) {
            throw new IllegalArgumentException("Clara-Error: component is not a dpe");
        }
        return base.syncSend(base.getFrontEnd(), new xMsgMessage(topic, data), timeout);
    }

    public void configureService(String serviceCanonicalName, EngineData data)
            throws ClaraException, xMsgException, TimeoutException, IOException {
        base.configureService(ClaraComponent.service(serviceCanonicalName), data);
    }

    public void configureService(ClaraComponent comp, EngineData data)
            throws ClaraException, xMsgException, TimeoutException, IOException {
        base.configureService(comp, data);
    }

    public xMsgMessage syncConfigureService(String serviceCanonicalName, EngineData data, int timeout)
            throws ClaraException, xMsgException, TimeoutException, IOException {
        return base.syncConfigureService(ClaraComponent.service(serviceCanonicalName), data, timeout);
    }

    public xMsgMessage syncConfigureService(ClaraComponent comp, EngineData data, int timeout)
            throws ClaraException, xMsgException, TimeoutException, IOException {
        return base.syncConfigureService(comp, data, timeout);
    }


    public void executeService(String serviceCanonicalName, EngineData data)
            throws ClaraException, xMsgException, TimeoutException, IOException {
        base.executeService(ClaraComponent.service(serviceCanonicalName), data);
    }

    public void executeService(ClaraComponent comp, EngineData data)
            throws ClaraException, xMsgException, TimeoutException, IOException {
        base.executeService(comp, data);
    }


    public EngineData syncExecuteService(String serviceCanonicalName, EngineData data, int timeout)
            throws ClaraException, TimeoutException, IOException, xMsgException {
        xMsgMessage response = base.syncExecuteService(ClaraComponent.service(serviceCanonicalName), data, timeout);
        return base.deSerialize(response, dataTypes);
    }

    public EngineData syncExecuteService(ClaraComponent comp, EngineData data, int timeout)
            throws ClaraException, TimeoutException, IOException, xMsgException {
        xMsgMessage response = base.syncExecuteService(comp, data, timeout);
        return base.deSerialize(response, dataTypes);
    }


    /**
     * Sends a request to execute a composition.
     * If any service does not exist, the messages are lost.
     *
     * @param composition the composition of services
     * @param data the input data for the composition
     * @throws ClaraException if the request could not be sent
     */
    public void executeComposition(String composition, EngineData data)
            throws ClaraException, xMsgException, TimeoutException, IOException {
        base.executeComposition(composition, data);
    }


    /**
     * Sends a request to execute a composition and receives the result.
     * If any service does not exist, the messages are lost.
     * A response is received with the output data of the entire execution.
     *
     * @param composition the composition of services
     * @param data the input data for the composition
     * @param timeout the time to wait for a response, in milliseconds
     * @return the output data of the last service in the composition
     * @throws ClaraException if the request could not be sent
     * @throws TimeoutException if a response is not received
     */
    public EngineData syncExecuteComposition(String composition,
                                             EngineData data,
                                             int timeout)
            throws ClaraException, TimeoutException, IOException, xMsgException {
        xMsgMessage response = base.syncExecuteComposition(composition, data, timeout);
        return base.deSerialize(response, dataTypes);
    }


    /**
     * Sends a request to start reporting "done" on service executions.
     * Configures the service to repeatedly publish "done" messages after
     * a number of <code>eventCount</code> executions have been completed.
     * If the service does not exist, the message is lost.
     *
     * @param serviceCanonicalName the canonical name of the service
     * @param eventCount the interval of executions to be completed to publish the report
     * @throws ClaraException if the request could not be sent
     */
    public void startReportingDone(String serviceCanonicalName, int eventCount)
            throws ClaraException, IOException, xMsgException {

        base.startReporting(ClaraComponent.service(serviceCanonicalName), CReportTypes.DONE, eventCount);
    }


    /**
     * Sends a request to stop reporting "done" on service executions.
     * Configures the service to stop publishing "done" messages.
     * If the service does not exist, the message is lost.
     *
     * @param serviceCanonicalName the canonical name of the service
     * @throws ClaraException if the request could not be sent
     */
    public void stopReportingDone(String serviceCanonicalName)
            throws ClaraException, IOException, xMsgException {
        base.stopReporting(ClaraComponent.service(serviceCanonicalName), CReportTypes.DONE);
    }


    /**
     * Sends a request to start reporting the output data on service executions.
     * Configures the service to repeatedly publish the resulting output data after
     * a number of <code>eventCount</code> executions have been completed.
     * If the service does not exist, the message is lost.
     *
     * @param serviceCanonicalName the canonical name of the service
     * @param eventCount the interval of executions to be completed to publish the report
     * @throws ClaraException if the request could not be sent
     */
    public void startReportingData(String serviceCanonicalName, int eventCount)
            throws ClaraException, IOException, xMsgException {

        base.startReporting(ClaraComponent.service(serviceCanonicalName), CReportTypes.DATA, eventCount);

    }


    /**
     * Sends a request to stop reporting the output data on service executions.
     * Configures the service to stop publishing the resulting output data.
     * If the service does not exist, the message is lost.
     *
     * @param serviceCanonicalName the canonical name of the service
     * @throws ClaraException if the request could not be sent
     */
    public void stopReportingData(String serviceCanonicalName)
            throws ClaraException, IOException, xMsgException {
        base.stopReporting(ClaraComponent.service(serviceCanonicalName), CReportTypes.DATA);
    }


    /**
     * Subscribes to the specified status reports of the selected service.
     * <p>
     * A background thread is started to receive messages from the service.
     * Every time a report is received, the provided callback will be executed.
     * The messages are received sequentially, but the callback may run
     * in extra background threads, so it must be thread-safe.
     * <p>
     * Services will publish status reports after every execution that results
     * on error or warning.
     *
     * @param serviceCanonicalName the service to be listened
     * @param status the status to be listened
     * @param callback the action to be run when a message is received
     * @throws ClaraException if there was an error starting the subscription
     */
    public void listenServiceStatus(String serviceCanonicalName,
                                    EngineStatus status,
                                    EngineCallback callback) throws ClaraException, xMsgException {

        xMsgTopic topic = ClaraUtil.buildTopic(ClaraUtil.getStatusText(status), serviceCanonicalName);
        String key = ClaraUtil.getDpeHost(serviceCanonicalName) + CConstants.MAPKEY_SEP + topic;
        if (subscriptions.containsKey(key)) {
            throw new IllegalStateException("Clara-Error: Duplicated subscription to: " + serviceCanonicalName);
        }
        xMsgCallBack wrapperCallback = wrapEngineCallback(callback, status);
        xMsgSubscription handler = base.listen(ClaraComponent.service(serviceCanonicalName), topic, wrapperCallback);
        subscriptions.put(key, handler);
    }


    /**
     * Un-subscribes from the specified status reports of the selected service.
     *
     * @param serviceCanonicalName the service being listened
     * @param status the status being listened
     * @throws ClaraException if there was an error stopping the subscription
     */
    public void unListenServiceStatus(String serviceCanonicalName, EngineStatus status)
            throws ClaraException, xMsgException {
        xMsgTopic topic = ClaraUtil.buildTopic(ClaraUtil.getStatusText(status), serviceCanonicalName);
        String key = ClaraUtil.getDpeHost(serviceCanonicalName) + CConstants.MAPKEY_SEP + topic;
        xMsgSubscription handler = subscriptions.remove(key);
        if (handler != null) {
            base.unsubscribe(handler);
        }
    }


    /**
     * Subscribes to the data reports of the selected service.
     * <p>
     * A background thread is started to receive messages from the service.
     * Every time a report is received, the provided callback will be executed.
     * The messages are received sequentially, but the callback may run
     * in extra background threads, so it must be thread-safe.
     * <p>
     * Services will publish "data" reports if they are configured to do so
     * with {@link #startReportingData}. The messages will contain the full
     * output result of the service.
     *
     * @param serviceCanonicalName the service to be listened
     * @param callback the action to be run when a message is received
     * @throws ClaraException if there was an error starting the subscription
     */
    public void listenServiceData(String serviceCanonicalName, EngineCallback callback)
            throws ClaraException, xMsgException {

        xMsgTopic topic = ClaraUtil.buildTopic(xMsgConstants.DATA.toString(), serviceCanonicalName);
        String key = ClaraUtil.getDpeHost(serviceCanonicalName) + CConstants.MAPKEY_SEP + topic;
        if (subscriptions.containsKey(key)) {
            throw new IllegalStateException("Clara-Error: Duplicated subscription to: " + serviceCanonicalName);
        }
        xMsgCallBack wrapperCallback = wrapEngineCallback(callback, null);
        xMsgSubscription handler = base.listen(ClaraComponent.service(serviceCanonicalName), topic, wrapperCallback);
        subscriptions.put(key, handler);
    }


    /**
     * Un-subscribes from the data reports of the selected service.
     *
     * @param serviceCanonicalName the service being listened
     * @throws ClaraException if there was an error stopping the subscription
     */
    public void unListenServiceData(String serviceCanonicalName)
            throws ClaraException, xMsgException {

        xMsgTopic topic = ClaraUtil.buildTopic(xMsgConstants.DATA.toString(), serviceCanonicalName);
        String key = ClaraUtil.getDpeHost(serviceCanonicalName) + CConstants.MAPKEY_SEP + topic;

        xMsgSubscription handler = subscriptions.remove(key);
        if (handler != null) {
            base.unsubscribe(handler);
        }
    }


    /**
     * Subscribes to the "done" reports of the selected service.
     * <p>
     * A background thread is started to receive messages from the service.
     * Every time a report is received, the provided callback will be executed.
     * The messages are received sequentially, but the callback may run
     * in extra background threads, so it must be thread-safe.
     * <p>
     * Services will publish "done" reports if they are configured to do so
     * with {@link #startReportingDone}. The messages will not contain the full
     * output result of the service, but just a few stats about the execution.
     *
     * @param serviceCanonicalName the service to be listened
     * @param callback the action to be run when a message is received
     * @throws ClaraException if there was an error starting the subscription
     */
    public void listenServiceDone(String serviceCanonicalName, EngineCallback callback)
            throws ClaraException, xMsgException {
        xMsgTopic topic = ClaraUtil.buildTopic(xMsgConstants.DONE.toString(), serviceCanonicalName);
        String key = ClaraUtil.getDpeHost(serviceCanonicalName) + CConstants.MAPKEY_SEP + topic;
        if (subscriptions.containsKey(key)) {
            throw new IllegalStateException("Clara-Error: Duplicated subscription to: " + serviceCanonicalName);
        }
        xMsgCallBack wrapperCallback = wrapEngineCallback(callback, null);
        xMsgSubscription handler = base.listen(ClaraComponent.service(serviceCanonicalName), topic, wrapperCallback);
        subscriptions.put(key, handler);
    }


    /**
     * Un-subscribes from the "done" reports of the selected service.
     *
     * @param serviceCanonicalName the service being listened
     * @throws ClaraException if there was an error stopping the subscription
     */
    public void unListenServiceDone(String serviceCanonicalName)
            throws ClaraException, xMsgException {

        xMsgTopic topic = ClaraUtil.buildTopic(xMsgConstants.DONE.toString(), serviceCanonicalName);
        String key = ClaraUtil.getDpeHost(serviceCanonicalName) + CConstants.MAPKEY_SEP + topic;

        xMsgSubscription handler = subscriptions.remove(key);
        if (handler != null) {
            base.unsubscribe(handler);
        }
    }


    /**
     * Subscribes to the periodic alive message reported by the running DPEs.
     *
     * @param callback the action to be run when a report is received
     * @throws ClaraException if there was an error starting the subscription
     */
    public void listenDpe(String FrontEndDpeCanonicalName, GenericCallback callback)
            throws ClaraException, xMsgException {
        ClaraComponent dpe = ClaraComponent.dpe(FrontEndDpeCanonicalName);
        xMsgTopic topic = ClaraUtil.buildTopic(CConstants.DPE_ALIVE);
        String key = dpe.getDpeHost() + CConstants.MAPKEY_SEP + topic;
        if (subscriptions.containsKey(key)) {
            throw new IllegalStateException("Clara-Error: Duplicated subscription to: " + topic);
        }
        xMsgCallBack wrapperCallback = wrapGenericCallback(callback);
        xMsgSubscription handler = base.listen(dpe, topic, wrapperCallback);
        subscriptions.put(key, handler);
    }


    /**
     * Un-subscribes from the alive reports of the running DPEs.
     *
     * @throws ClaraException if there was an error stopping the subscription
     */
    public void unListenDpe(String FrontEndDpeCanonicalName)
            throws ClaraException, xMsgException {

        ClaraComponent dpe = ClaraComponent.dpe(FrontEndDpeCanonicalName);
        xMsgTopic topic = ClaraUtil.buildTopic(CConstants.DPE_ALIVE);
        String key = dpe.getDpeHost() + CConstants.MAPKEY_SEP + topic;

            xMsgSubscription handler = subscriptions.remove(key);
            if (handler != null) {
                base.unsubscribe(handler);
            }
    }


    public Set<String> getDpeNames() throws ClaraException, xMsgException {
        xMsgTopic topic = xMsgTopic.build("xyz)");
        String rs = base.findSubscriberDomainNames(topic);
        StringTokenizer st = new StringTokenizer(rs);
        HashSet<String> result = new HashSet<>();
        while (st.hasMoreTokens()) {
            result.add(st.nextToken());
        }
        return result;
    }

    public Set<String> getContainerNames(String dpeName) throws ClaraException, xMsgException {
        xMsgTopic topic = xMsgTopic.build(dpeName);
        String rs = base.findSubscriberSubjectNames(topic);
        StringTokenizer st = new StringTokenizer(rs);
        HashSet<String> result = new HashSet<>();
        while (st.hasMoreTokens()) {
            result.add(st.nextToken());
        }
        return result;
    }

    public Set<String> getEngineNames(String dpeName, String containerName) throws ClaraException, xMsgException {
        xMsgTopic topic = xMsgTopic.build(dpeName, containerName);
        String rs = base.findSubscriberTypeNames(topic);
        StringTokenizer st = new StringTokenizer(rs);
        HashSet<String> result = new HashSet<>();
        while (st.hasMoreTokens()) {
            result.add(st.nextToken());
        }
        return result;
    }


    /**
     * Returns the registration information of the selected Clara actor.
     * The actor can be a DPE, a container or a service.
     *
     * @param canonicalName the name of the actor
     */
    public Set<xMsgRegistration> getRegistrationInfo(String canonicalName)
            throws ClaraException, xMsgException {
        xMsgTopic topic = xMsgTopic.wrap(canonicalName);
        return base.findSubscribers(topic);
    }


    public String meta2Json(xMsgMeta meta) throws ClaraException {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public String reg2Json(xMsgRegistration regData) throws ClaraException {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    /**
     * Returns the assigned orchestrator name.
     */
    public String getName() {
        return base.getName();
    }


    /**
     * Extracts the EngineData from the received message and calls the user
     * callback.
     *
     * The EngineStatus is passed also for testing purposes.
     */
    xMsgCallBack wrapEngineCallback(final EngineCallback userCallback,
                                    final EngineStatus userStatus) {
        return new xMsgCallBack() {
            @Override
            public xMsgMessage callback(xMsgMessage msg) {
                try {
                    userCallback.callback(base.deSerialize(msg, dataTypes));
                } catch (ClaraException e) {
                    System.out.println("Error receiving data to " + msg.getTopic());
                    e.printStackTrace();
                }
                return null;
            }
        };
    }


    /**
     * Extracts the JSON data from the received message and calls the user
     * callback.
     */
    xMsgCallBack wrapGenericCallback(final GenericCallback userCallback) {
        return new xMsgCallBack() {
            @Override
            public xMsgMessage callback(xMsgMessage msg) {
                try {
                    String mimeType = msg.getMetaData().getDataType();
                    if (mimeType.equals("text/string")) {
                        userCallback.callback(new String(msg.getData()));
                    } else {
                        throw new ClaraException("Unexpected mime-type: " + mimeType);
                    }
                } catch (ClaraException e) {
                    System.out.println("Error receiving data to " + msg.getTopic());
                    e.printStackTrace();
                }
                return null;
            }
        };
    }



}
