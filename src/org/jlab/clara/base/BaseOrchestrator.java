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

    private final ClaraBase base;
    private final Set<EngineDataType> dataTypes = new HashSet<>();
    private final Map<String, xMsgSubscription> subscriptions = new HashMap<>();


    public BaseOrchestrator() throws ClaraException, IOException {
        this(ClaraUtil.getUniqueName(),
                xMsgUtil.localhost(),
                xMsgConstants.REGISTRAR_PORT,

                xMsgUtil.localhost(),
                xMsgConstants.DEFAULT_PORT,
                CConstants.JAVA_LANG,
                xMsgConstants.DEFAULT_POOL_SIZE);
    }

    public BaseOrchestrator(int subPoolSize) throws ClaraException, IOException {
        this(ClaraUtil.getUniqueName(),
                xMsgUtil.localhost(),
                xMsgConstants.REGISTRAR_PORT,
                xMsgUtil.localhost(),
                xMsgConstants.DEFAULT_PORT,
                CConstants.JAVA_LANG,
                subPoolSize);
    }

    public BaseOrchestrator(String dpeHost, int subPoolSize) throws ClaraException {
        this(ClaraUtil.getUniqueName(),
                dpeHost,
                xMsgConstants.REGISTRAR_PORT,
                dpeHost,
                xMsgConstants.DEFAULT_PORT,
                CConstants.JAVA_LANG,
                subPoolSize);
    }

    public BaseOrchestrator(String name,
                            String regHost,
                            int regPort,
                            String dpeHost,
                            int dpePort,
                            String dpeLang,
                            int subPoolSize)
            throws ClaraException {
        try {
            base = new ClaraBase(ClaraComponent.orchestrator(name, dpeHost, dpePort, dpeLang, subPoolSize), regHost, regPort);
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
        for (EngineDataType dt : dataTypes) {
            this.dataTypes.add(dt);
        }
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
     * Requests a start a DPE.
     *
     * @param dpeCanonicalName the name of the DPE
     * @throws ClaraException if the request could not be sent
     */
    public void deployDpe(String dpeCanonicalName)
            throws ClaraException, xMsgException, IOException, TimeoutException {

        ClaraComponent dpe = ClaraComponent.dpe(dpeCanonicalName);
        base.deploy(dpe);
    }

    /**
     * Requests a DPE to exit.
     * The request is sent to a running DPE of the given language.
     * If no DPE is running in the node, the message is lost.
     *
     * @param dpeCanonicalName the name of the DPE
     * @throws ClaraException if the request could not be sent
     */
    public void exitDpe(String dpeCanonicalName)
            throws ClaraException, xMsgException, IOException, TimeoutException {

        ClaraComponent dpe = ClaraComponent.dpe(dpeCanonicalName);
        base.exit(dpe);
    }

    /**
     * Sends a request to deploy a container.
     * The request is sent to a running DPE of the given language.
     * If no DPE is running in the node, the message is lost.
     * If there is a container with the given name in the DPE, the request is ignored.
     *
     * @param contCanonicalName the canonical name of the container
     * @throws ClaraException if the request could not be sent
     */
    public void deployContainer(String contCanonicalName)
            throws ClaraException, xMsgException, IOException, TimeoutException {
        ClaraComponent container = ClaraComponent.container(contCanonicalName);
        base.deploy(container);
    }


    /**
     * Sends a request to deploy a container and waits until it is deployed.
     * The request is sent to a running DPE of the given language.
     * If no DPE is running in the node, the message is lost.
     * If there is a container with the given name in the DPE, the request is ignored.
     * A response is received once the container has been deployed.
     *
     * @param contCanonicalName the canonical name of the container
     * @param timeout the time to wait for a response, in milliseconds
     * @throws ClaraException if the request could not be sent
     * @throws TimeoutException if a response is not received
     */
    public xMsgMessage deployContainerSync(String contCanonicalName, int timeout)
            throws ClaraException, xMsgException, IOException, TimeoutException {

        ClaraComponent container = ClaraComponent.container(contCanonicalName);
        return base.syncDeploy(container, timeout);
    }


    /**
     * Sends a request to remove a container.
     * The request is sent to a running DPE of the given language.
     * If no DPE is running in the node, the message is lost.
     * If there is no container of the given name in the DPE, the request is ignored.
     *
     * @param contCanonicalName the canonical name of the container
     * @throws ClaraException if the request could not be sent
     */
    public void removeContainer(String contCanonicalName)
            throws ClaraException, xMsgException, IOException, TimeoutException {

        ClaraComponent container = ClaraComponent.container(contCanonicalName);
        base.exit(container);
    }


    /**
     * Sends a request to remove a container and waits until it is removed.
     * The request is sent to a running DPE of the given language.
     * If no DPE is running in the node, the message is lost.
     * If there is no container of the given name in the DPE, the request is ignored.
     * A response is received once the container has been removed.
     *
     * @param contCanonicalName the canonical name of the container
     * @param timeout the time to wait for a response, in milliseconds
     * @throws ClaraException if the request could not be sent
     * @throws TimeoutException if a response is not received
     */
    public xMsgMessage removeContainerSync(String contCanonicalName, int timeout)
            throws ClaraException, TimeoutException, xMsgException, IOException {

        ClaraComponent container = ClaraComponent.container(contCanonicalName);
        return base.syncExit(container, timeout);
    }


    /**
     * Sends a request to deploy a service.
     * If the container does not exist, the message is lost.
     * If there is a service with the given name in the container, the request is ignored.
     *
     * @param serviceCanonicalName the canonical name of the service
     * @param serviceClass the classpath to the service engine
     * @param poolSize the maximum number of parallel engines to be created
     *                 to process multi-threading requests
     * @throws ClaraException if the request could not be sent
     */
    public void deployService(String serviceCanonicalName, String serviceClass, int poolSize)
            throws ClaraException, xMsgException, IOException, TimeoutException {
        ClaraComponent service = ClaraComponent.service(serviceCanonicalName);
        service.setEngineClass(serviceClass);
        service.setSubscriptionPoolSize(poolSize);

        base.deploy(service);
    }


    /**
     * Sends a request to deploy a service and waits until it is deployed.
     * If the container does not exist, the message is lost.
     * If there is a service with the given name in the container, the request is ignored.
     * A response is received once the service has been deployed.
     *
     * @param serviceCanonicalName the canonical name of the service
     * @param serviceClass the classpath to the service engine
     * @param poolSize the maximum number of parallel engines to be created
     *                 to process multi-threading requests
     * @param timeout the time to wait for a response, in milliseconds
     * @throws ClaraException if the request could not be sent
     * @throws TimeoutException if a response is not received
     */
    public xMsgMessage deployServiceSync(String serviceCanonicalName, String serviceClass,
                                         int poolSize, int timeout)
            throws ClaraException, TimeoutException, IOException, xMsgException {

        ClaraComponent service = ClaraComponent.service(serviceCanonicalName);
        service.setEngineClass(serviceClass);
        service.setSubscriptionPoolSize(poolSize);

        return base.syncDeploy(service, timeout);
    }


    /**
     * Sends a request to remove a service.
     * If the container does not exist, the message is lost.
     * If there is no service of the given name in the container, the request is ignored.
     *
     * @param serviceCanonicalName the canonical name of the service
     * @throws ClaraException if the request could not be sent
     */
    public void removeService(String serviceCanonicalName)
            throws ClaraException, xMsgException, IOException, TimeoutException {
        base.exit(ClaraComponent.service(serviceCanonicalName));
    }


    /**
     * Sends a request to remove a service and waits until it is removed.
     * If the container does not exist, the message is lost.
     * If there is no service of the given name in the container, the request is ignored.
     * A response is received once the service has been removed.
     *
     * @param serviceCanonicalName the canonical name of the service
     * @param timeout the time to wait for a response, in milliseconds
     * @throws ClaraException if the request could not be sent
     * @throws TimeoutException if a response is not received
     */
    public xMsgMessage removeServiceSync(String serviceCanonicalName, int timeout)
            throws ClaraException, TimeoutException, IOException, xMsgException {
        return base.syncExit(ClaraComponent.service(serviceCanonicalName), timeout);
    }


    /**
     * Sends a request to configure a service.
     * If the service does not exist, the message is lost.
     * @param serviceCanonicalName the canonical name of the service
     * @param data the configuration data for the service
     * @throws ClaraException if the request could not be sent
     */
    public void configureService(String serviceCanonicalName, EngineData data)
            throws ClaraException, xMsgException, TimeoutException, IOException {
        base.configureService(ClaraComponent.service(serviceCanonicalName), data);
    }


    /**
     * Sends a request to configure a service and waits until it is done.
     * If the service does not exist, the message is lost.
     * A response is received once the service has been configured.
     *
     * @param serviceCanonicalName the canonical name of the service
     * @param data the configuration data for the service
     * @param timeout the time to wait for a response, in milliseconds
     * @throws ClaraException if the request could not be sent
     * @throws TimeoutException if a response is not received
     */
    public xMsgMessage configureServiceSync(String serviceCanonicalName, EngineData data, int timeout)
            throws ClaraException, TimeoutException, IOException, xMsgException {
        return base.syncConfigureService(ClaraComponent.service(serviceCanonicalName), data, timeout);
    }


    /**
     * Sends a request to execute a service.
     * If the service does not exist, the message is lost.
     *
     * @param serviceCanonicalName the canonical name of the service
     * @param data the input data for the service
     * @throws ClaraException if the request could not be sent
     */
    public void executeService(String serviceCanonicalName, EngineData data)
            throws ClaraException, xMsgException, TimeoutException, IOException {
        base.executeService(ClaraComponent.service(serviceCanonicalName), data);
    }


    /**
     * Sends a request to execute a service and receives the result.
     * If the service does not exist, the message is lost.
     * A response is received with the output data of the execution.
     *
     * @param serviceCanonicalName the canonical name of the service
     * @param data the input data for the service
     * @param timeout the time to wait for a response, in milliseconds
     * @return the service output data
     * @throws ClaraException if the request could not be sent
     * @throws TimeoutException if a response is not received
     */
    public EngineData executeServiceSync(String serviceCanonicalName, EngineData data, int timeout)
            throws ClaraException, TimeoutException, IOException, xMsgException {
        xMsgMessage response = base.syncExecuteService(ClaraComponent.service(serviceCanonicalName), data, timeout);

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
    public void executeComposition(Composition composition, EngineData data)
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
    public EngineData executeCompositionSync(Composition composition,
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
    public void listenDpes(String FrontEndDpeCanonicalName, GenericCallback callback)
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
    public void unListenDpes(String FrontEndDpeCanonicalName)
            throws ClaraException, xMsgException {

        ClaraComponent dpe = ClaraComponent.dpe(FrontEndDpeCanonicalName);
        xMsgTopic topic = ClaraUtil.buildTopic(CConstants.DPE_ALIVE);
        String key = dpe.getDpeHost() + CConstants.MAPKEY_SEP + topic;

            xMsgSubscription handler = subscriptions.remove(key);
            if (handler != null) {
                base.unsubscribe(handler);
            }
    }


    /**
     * Checks if the given DPE is up and running.
     *
     * @param dpeCanonicalName the name of the DPE
     * @return true if the DPE is running
     * @throws ClaraException if there was a problem with the request
     */
    public xMsgMessage pingDpe(String dpeCanonicalName, int timeout)
            throws ClaraException, xMsgException, IOException, TimeoutException {
        return base.ping(ClaraComponent.dpe(dpeCanonicalName), timeout);
    }


    /**
     * Checks if the given service is up and running.
     *
     * @param serviceCanonicalName the name of the service
     * @return true if the service is running
     * @throws ClaraException if there was a problem with the request
     */
    public xMsgMessage pingService(String serviceCanonicalName, int timeout)
            throws ClaraException, xMsgException, IOException, TimeoutException {
        return base.ping(ClaraComponent.service(serviceCanonicalName), timeout);
    }


    /**
     * Returns the runtime information of the selected Clara actor.
     * The actor can be a DPE, a container or a service.
     *
     * @param serviceCanonicalName the name of the actor
     * @return a JSON object with the runtime information of the actor
     * @throws ClaraException if there was a problem with the request
     */
    public String getServiceState(String serviceCanonicalName, int timeout)
            throws ClaraException, xMsgException, IOException, TimeoutException {
        xMsgMessage response = base.ping(ClaraComponent.service(serviceCanonicalName), timeout);
        return response.getMetaData().getSenderState();
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
