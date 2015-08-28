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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import javax.annotation.ParametersAreNonnullByDefault;

import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.engine.EngineStatus;
import org.jlab.clara.sys.CBase;
import org.jlab.clara.util.CConstants;
import org.jlab.coda.xmsg.core.xMsgCallBack;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgSubscription;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;
import org.jlab.coda.xmsg.excp.xMsgException;


/**
 * Base class for orchestration of applications.
 */
@ParametersAreNonnullByDefault
public class BaseOrchestrator {

    private final CBase base;
    private final Set<EngineDataType> dataTypes = new HashSet<>();
    private final Map<String, xMsgSubscription> subscriptions = new HashMap<>();

    /**
     * Creates a new orchestrator. Uses localhost as front-end node.
     *
     * @throws ClaraException in case of connection errors
     */
    public BaseOrchestrator() throws ClaraException {
        try {
            base = getClaraBase(xMsgUtil.localhost());
        } catch (IOException | xMsgException e) {
            throw new ClaraException("Could not start orchestrator", e);
        }
    }


    /**
     * Creates a new orchestrator. Receives the location of the front-end node.
     *
     * @param frontEndHost the IP of the front-end node
     * @throws ClaraException in case of connection errors
     */
    public BaseOrchestrator(String frontEndHost) throws ClaraException {
        try {
            base = getClaraBase(frontEndHost);
        } catch (IOException | xMsgException e) {
            throw new ClaraException("Could not start orchestrator", e);
        }
    }


    /**
     * Creates the internal Clara object.
     * It can be overridden to return a mock for testing purposes.
     * @throws IOException
     */
    CBase getClaraBase(String frontEndHost) throws xMsgException, IOException {
        return new CBase(generateName(), xMsgUtil.localhost(), frontEndHost);
    }


    /**
     * Returns the map of subscriptions for testing purposes.
     */
    Map<String, xMsgSubscription> getSubscriptions() {
        return subscriptions;
    }


    private String generateName() {
        Random rand = new Random();
        return "orchestrator" + rand.nextInt(1000) + ":" + "localhost";
    }


    private xMsgTopic buildTopic(Object... args) {
        StringBuilder topic  = new StringBuilder();
        topic.append(args[0]);
        for (int i = 1; i < args.length; i++) {
            topic.append(CConstants.TOPIC_SEP);
            topic.append(args[i]);
        }
        return xMsgTopic.wrap(topic.toString());
    }


    private String buildData(Object... args) {
        StringBuilder topic  = new StringBuilder();
        topic.append(args[0]);
        for (int i = 1; i < args.length; i++) {
            topic.append(CConstants.DATA_SEP);
            topic.append(args[i]);
        }
        return topic.toString();
    }


    private xMsgMessage buildMessage(xMsgTopic topic, String data) {
        return new xMsgMessage(topic, data);
    }


    private xMsgMessage buildMessage(xMsgTopic topic, EngineData data) throws ClaraException {
        try {
            xMsgMessage msg = new xMsgMessage(topic);
            base.serialize(data, msg, dataTypes);
            return msg;
        } catch (CException e) {
            throw new ClaraException("Could not serialize data", e);
        }
    }


    private void validateTimeout(int timeout) {
        if (timeout <= 0) {
            throw new IllegalArgumentException("Invalid timeout: " + timeout);
        }
    }


    /**
     * Registers a new data-type to be sent to services.
     *
     * @param dataType the information about the type
     */
    public void registerDataType(EngineDataType dataType) {
        dataTypes.add(dataType);
    }


    /**
     * Requests a DPE to exit.
     * The request is sent to a running DPE of the given language.
     * If no DPE is running in the node, the message is lost.
     *
     * @param dpeName the name of the DPE
     * @throws ClaraException if the request could not be sent
     */
    public void exitDpe(String dpeName) throws ClaraException {
        try {
            Objects.requireNonNull(dpeName, "Null Dpe name");
            if (!ClaraUtil.isDpeName(dpeName)) {
                throw new IllegalArgumentException("Malformed DPE name: " + dpeName);
            }
            String host = ClaraUtil.getHostName(dpeName);
            xMsgTopic topic = buildTopic(CConstants.DPE, dpeName);
            String data = CConstants.DPE_EXIT;
            xMsgMessage msg = buildMessage(topic, data);
            base.genericSend(host, msg);
        } catch (xMsgException | IOException e) {
            throw new ClaraException("Could not send request", e);
        }
    }


    /**
     * Sends a request to deploy a container.
     * The request is sent to a running DPE of the given language.
     * If no DPE is running in the node, the message is lost.
     * If there is a container with the given name in the DPE, the request is ignored.
     *
     * @param containerName the canonical name of the container
     * @throws ClaraException if the request could not be sent
     */
    public void deployContainer(String containerName) throws ClaraException {
        try {
            Objects.requireNonNull(containerName, "Null container name");
            if (!ClaraUtil.isContainerName(containerName)) {
                throw new IllegalArgumentException("Malformed container name: " + containerName);
            }
            String host = ClaraUtil.getHostName(containerName);
            String dpe = ClaraUtil.getDpeName(containerName);
            String name = ClaraUtil.getContainerName(containerName);
            xMsgTopic topic = buildTopic(CConstants.DPE, dpe);
            String data = buildData(CConstants.START_CONTAINER, name);
            xMsgMessage msg = buildMessage(topic, data);
            base.genericSend(host, msg);
        } catch (IOException | xMsgException e) {
            throw new ClaraException("Could not send request", e);
        }
    }


    /**
     * Sends a request to deploy a container and waits until it is deployed.
     * The request is sent to a running DPE of the given language.
     * If no DPE is running in the node, the message is lost.
     * If there is a container with the given name in the DPE, the request is ignored.
     * A response is received once the container has been deployed.
     *
     * @param containerName the canonical name of the container
     * @param timeout the time to wait for a response, in milliseconds
     * @throws ClaraException if the request could not be sent
     * @throws TimeoutException if a response is not received
     */
    public void deployContainerSync(String containerName, int timeout)
            throws ClaraException, TimeoutException {
        try {
            Objects.requireNonNull(containerName, "Null container name");
            if (!ClaraUtil.isContainerName(containerName)) {
                throw new IllegalArgumentException("Malformed container name: " + containerName);
            }
            validateTimeout(timeout);

            String host = ClaraUtil.getHostName(containerName);
            String dpe = ClaraUtil.getDpeName(containerName);
            String name = ClaraUtil.getContainerName(containerName);
            xMsgTopic topic = buildTopic(CConstants.DPE, dpe);
            String data = buildData(CConstants.START_CONTAINER, name);
            xMsgMessage msg = buildMessage(topic, data);
            base.genericSyncSend(host, msg, timeout);
        } catch (IOException | xMsgException e) {
            throw new ClaraException("Could not send request", e);
        }
    }


    /**
     * Sends a request to remove a container.
     * The request is sent to a running DPE of the given language.
     * If no DPE is running in the node, the message is lost.
     * If there is no container of the given name in the DPE, the request is ignored.
     *
     * @param containerName the canonical name of the container
     * @throws ClaraException if the request could not be sent
     */
    public void removeContainer(String containerName) throws ClaraException {
        try {
            Objects.requireNonNull(containerName, "Null container name");
            if (!ClaraUtil.isContainerName(containerName)) {
                throw new IllegalArgumentException("Malformed container name: " + containerName);
            }
            String host = ClaraUtil.getHostName(containerName);
            String dpe = ClaraUtil.getDpeName(containerName);
            String name = ClaraUtil.getContainerName(containerName);
            xMsgTopic topic = buildTopic(CConstants.DPE, dpe);
            String data = buildData(CConstants.REMOVE_CONTAINER, name);
            xMsgMessage msg = buildMessage(topic, data);
            base.genericSend(host, msg);
        } catch (IOException | xMsgException e) {
            throw new ClaraException("Could not send request", e);
        }
    }


    /**
     * Sends a request to remove a container and waits until it is removed.
     * The request is sent to a running DPE of the given language.
     * If no DPE is running in the node, the message is lost.
     * If there is no container of the given name in the DPE, the request is ignored.
     * A response is received once the container has been removed.
     *
     * @param containerName the canonical name of the container
     * @param timeout the time to wait for a response, in milliseconds
     * @throws ClaraException if the request could not be sent
     * @throws TimeoutException if a response is not received
     */
    public void removeContainerSync(String containerName, int timeout)
            throws ClaraException, TimeoutException {
        try {
            Objects.requireNonNull(containerName, "Null container name");
            if (!ClaraUtil.isContainerName(containerName)) {
                throw new IllegalArgumentException("Malformed container name: " + containerName);
            }
            validateTimeout(timeout);

            String host = ClaraUtil.getHostName(containerName);
            String dpe = ClaraUtil.getDpeName(containerName);
            String name = ClaraUtil.getContainerName(containerName);
            xMsgTopic topic = buildTopic(CConstants.DPE, dpe);
            String data = buildData(CConstants.REMOVE_CONTAINER, name);
            xMsgMessage msg = buildMessage(topic, data);
            base.genericSyncSend(host, msg, timeout);
        } catch (IOException | xMsgException e) {
            throw new ClaraException("Could not send request", e);
        }
    }


    /**
     * Sends a request to deploy a service.
     * If the container does not exist, the message is lost.
     * If there is a service with the given name in the container, the request is ignored.
     *
     * @param serviceName the canonical name of the service
     * @param serviceClass the classpath to the service engine
     * @param poolSize the maximum number of parallel engines to be created
     *                 to process multi-threading requests
     * @throws ClaraException if the request could not be sent
     */
    public void deployService(String serviceName, String serviceClass, int poolSize)
            throws ClaraException {
        try {
            Objects.requireNonNull(serviceName, "Null service name");
            if (!ClaraUtil.isServiceName(serviceName)) {
                throw new IllegalArgumentException("Malformed service name: " + serviceName);
            }
            if (poolSize <= 0) {
                throw new IllegalArgumentException("Invalid pool size:" + poolSize);
            }

            String host = ClaraUtil.getHostName(serviceName);
            String containerName = ClaraUtil.getContainerCanonicalName(serviceName);
            String engineName = ClaraUtil.getEngineName(serviceName);
            xMsgTopic topic = buildTopic(CConstants.CONTAINER, containerName);
            String data = buildData(CConstants.DEPLOY_SERVICE, engineName, serviceClass, poolSize);
            xMsgMessage msg = buildMessage(topic, data);
            base.genericSend(host, msg);
        } catch (IOException | xMsgException e) {
            throw new ClaraException("Could not send request", e);
        }
    }


    /**
     * Sends a request to deploy a service and waits until it is deployed.
     * If the container does not exist, the message is lost.
     * If there is a service with the given name in the container, the request is ignored.
     * A response is received once the service has been deployed.
     *
     * @param serviceName the canonical name of the service
     * @param serviceClass the classpath to the service engine
     * @param poolSize the maximum number of parallel engines to be created
     *                 to process multi-threading requests
     * @param timeout the time to wait for a response, in milliseconds
     * @throws ClaraException if the request could not be sent
     * @throws TimeoutException if a response is not received
     */
    public void deployServiceSync(String serviceName, String serviceClass,
                                  int poolSize, int timeout)
           throws ClaraException, TimeoutException {
        try {
            Objects.requireNonNull(serviceName, "Null service name");
            if (!ClaraUtil.isServiceName(serviceName)) {
                throw new IllegalArgumentException("Malformed service name: " + serviceName);
            }
            if (poolSize <= 0) {
                throw new IllegalArgumentException("Invalid pool size:" + poolSize);
            }
            validateTimeout(timeout);

            String host = ClaraUtil.getHostName(serviceName);
            String containerName = ClaraUtil.getContainerCanonicalName(serviceName);
            String engineName = ClaraUtil.getEngineName(serviceName);
            xMsgTopic topic = buildTopic(CConstants.CONTAINER, containerName);
            String data = buildData(CConstants.DEPLOY_SERVICE, engineName, serviceClass, poolSize);
            xMsgMessage msg = buildMessage(topic, data);
            base.genericSyncSend(host, msg, timeout);
        } catch (IOException | xMsgException e) {
            throw new ClaraException("Could not send request", e);
        }
    }


    /**
     * Sends a request to remove a service.
     * If the container does not exist, the message is lost.
     * If there is no service of the given name in the container, the request is ignored.
     *
     * @param serviceName the canonical name of the service
     * @throws ClaraException if the request could not be sent
     */
    public void removeService(String serviceName) throws ClaraException {
        try {
            Objects.requireNonNull(serviceName, "Null service name");
            if (!ClaraUtil.isServiceName(serviceName)) {
                throw new IllegalArgumentException("Malformed service name: " + serviceName);
            }
            String host = ClaraUtil.getHostName(serviceName);
            String containerName = ClaraUtil.getContainerCanonicalName(serviceName);
            String engineName = ClaraUtil.getEngineName(serviceName);
            xMsgTopic topic = buildTopic(CConstants.CONTAINER, containerName);
            String data = buildData(CConstants.REMOVE_SERVICE, engineName);
            xMsgMessage msg = buildMessage(topic, data);
            base.genericSend(host, msg);
        } catch (IOException | xMsgException e) {
            throw new ClaraException("Could not send request", e);
        }
    }


    /**
     * Sends a request to remove a service and waits until it is removed.
     * If the container does not exist, the message is lost.
     * If there is no service of the given name in the container, the request is ignored.
     * A response is received once the service has been removed.
     *
     * @param serviceName the canonical name of the service
     * @param timeout the time to wait for a response, in milliseconds
     * @throws ClaraException if the request could not be sent
     * @throws TimeoutException if a response is not received
     */
    public void removeServiceSync(String serviceName, int timeout)
            throws ClaraException, TimeoutException {
        try {
            Objects.requireNonNull(serviceName, "Null service name");
            if (!ClaraUtil.isServiceName(serviceName)) {
                throw new IllegalArgumentException("Malformed service name: " + serviceName);
            }
            validateTimeout(timeout);

            String host = ClaraUtil.getHostName(serviceName);
            String containerName = ClaraUtil.getContainerCanonicalName(serviceName);
            String engineName = ClaraUtil.getEngineName(serviceName);
            xMsgTopic topic = buildTopic(CConstants.CONTAINER, containerName);
            String data = buildData(CConstants.REMOVE_SERVICE, engineName);
            xMsgMessage msg = buildMessage(topic, data);
            base.genericSyncSend(host, msg, timeout);
        } catch (IOException | xMsgException e) {
            throw new ClaraException("Could not send request", e);
        }
    }


    /**
     * Sends a request to configure a service.
     * If the service does not exist, the message is lost.
     * @param serviceName the canonical name of the service
     * @param data the configuration data for the service
     * @throws ClaraException if the request could not be sent
     */
    public void configureService(String serviceName, EngineData data)
            throws ClaraException {
        try {
            Objects.requireNonNull(serviceName, "Null service name");
            Objects.requireNonNull(data, "Null input data");
            if (!ClaraUtil.isServiceName(serviceName)) {
                throw new IllegalArgumentException("Malformed service name: " + serviceName);
            }
            String host = ClaraUtil.getHostName(serviceName);
            xMsgTopic topic = xMsgTopic.wrap(serviceName);
            xMsgMessage msg = buildMessage(topic, data);
            xMsgMeta.Builder msgMeta = msg.getMetaData();
            msgMeta.setComposition(serviceName);
            msgMeta.setAction(xMsgMeta.ControlAction.CONFIGURE);
            base.genericSend(host, msg);
        } catch (IOException | xMsgException e) {
            throw new ClaraException("Could not send request", e);
        }
    }


    /**
     * Sends a request to configure a service and waits until it is done.
     * If the service does not exist, the message is lost.
     * A response is received once the service has been configured.
     *
     * @param serviceName the canonical name of the service
     * @param data the configuration data for the service
     * @param timeout the time to wait for a response, in milliseconds
     * @throws ClaraException if the request could not be sent
     * @throws TimeoutException if a response is not received
     */
    public void configureServiceSync(String serviceName, EngineData data, int timeout)
            throws ClaraException, TimeoutException {
        try {
            Objects.requireNonNull(serviceName, "Null service name");
            Objects.requireNonNull(data, "Null input data");
            if (!ClaraUtil.isServiceName(serviceName)) {
                throw new IllegalArgumentException("Malformed service name: " + serviceName);
            }
            validateTimeout(timeout);

            String host = ClaraUtil.getHostName(serviceName);
            xMsgTopic topic = xMsgTopic.wrap(serviceName);
            xMsgMessage msg = buildMessage(topic, data);
            xMsgMeta.Builder msgMeta = msg.getMetaData();
            msgMeta.setComposition(serviceName);
            msgMeta.setAction(xMsgMeta.ControlAction.CONFIGURE);
            base.genericSyncSend(host, msg, timeout);
        } catch (IOException | xMsgException e) {
            throw new ClaraException("Could not send request", e);
        }
    }


    /**
     * Sends a request to execute a service.
     * If the service does not exist, the message is lost.
     *
     * @param serviceName the canonical name of the service
     * @param data the input data for the service
     * @throws ClaraException if the request could not be sent
     */
    public void executeService(String serviceName, EngineData data)
            throws ClaraException {
        try {
            Objects.requireNonNull(serviceName, "Null service name");
            Objects.requireNonNull(data, "Null input data");
            if (!ClaraUtil.isServiceName(serviceName)) {
                throw new IllegalArgumentException("Malformed service name: " + serviceName);
            }
            String host = ClaraUtil.getHostName(serviceName);
            xMsgTopic topic = xMsgTopic.wrap(serviceName);
            xMsgMessage msg = buildMessage(topic, data);
            xMsgMeta.Builder msgMeta = msg.getMetaData();
            msgMeta.setComposition(serviceName);
            msgMeta.setAction(xMsgMeta.ControlAction.EXECUTE);
            base.genericSend(host, msg);
        } catch (IOException | xMsgException e) {
            throw new ClaraException("Could not send request", e);
        }
    }


    /**
     * Sends a request to execute a service and receives the result.
     * If the service does not exist, the message is lost.
     * A response is received with the output data of the execution.
     *
     * @param serviceName the canonical name of the service
     * @param data the input data for the service
     * @param timeout the time to wait for a response, in milliseconds
     * @return the service output data
     * @throws ClaraException if the request could not be sent
     * @throws TimeoutException if a response is not received
     */
    public EngineData executeServiceSync(String serviceName, EngineData data, int timeout)
            throws ClaraException, TimeoutException {
        try {
            Objects.requireNonNull(serviceName, "Null service name");
            Objects.requireNonNull(data, "Null input data");
            if (!ClaraUtil.isServiceName(serviceName)) {
                throw new IllegalArgumentException("Malformed service name: " + serviceName);
            }
            validateTimeout(timeout);

            String host = ClaraUtil.getHostName(serviceName);
            xMsgTopic topic = xMsgTopic.wrap(serviceName);
            xMsgMessage msg = buildMessage(topic, data);
            xMsgMeta.Builder msgMeta = msg.getMetaData();
            msgMeta.setComposition(serviceName);
            msgMeta.setAction(xMsgMeta.ControlAction.EXECUTE);
            xMsgMessage response = base.genericSyncSend(host, msg, timeout);
            return base.parseFrom(response, dataTypes);
        } catch (IOException | xMsgException e) {
            throw new ClaraException("Could not send request", e);
        } catch (CException e) {
            throw new ClaraException("Could not deserialize response", e);
        }
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
            throws ClaraException {
        try {
            Objects.requireNonNull(composition, "Null service composition");
            Objects.requireNonNull(data, "Null input data");

            String firstService = composition.firstService();
            String host = ClaraUtil.getHostName(firstService);
            xMsgTopic topic = xMsgTopic.wrap(firstService);
            xMsgMessage msg = buildMessage(topic, data);
            xMsgMeta.Builder msgMeta = msg.getMetaData();
            msgMeta.setComposition(composition.toString());
            msgMeta.setAction(xMsgMeta.ControlAction.EXECUTE);
            base.genericSend(host, msg);
        } catch (IOException | xMsgException e) {
            throw new ClaraException("Could not send request", e);
        }
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
            throws ClaraException, TimeoutException {
        throw new UnsupportedOperationException("Not implemented yet");
    }


    /**
     * Sends a request to start reporting "done" on service executions.
     * Configures the service to repeatedly publish "done" messages after
     * a number of <code>eventCount</code> executions have been completed.
     * If the service does not exist, the message is lost.
     *
     * @param serviceName the canonical name of the service
     * @param eventCount the interval of executions to be completed to publish the report
     * @throws ClaraException if the request could not be sent
     */
    public void startReportingDone(String serviceName, int eventCount) throws ClaraException {
        try {
            Objects.requireNonNull(serviceName, "Null service name");
            if (!ClaraUtil.isServiceName(serviceName)) {
                throw new IllegalArgumentException("Malformed service name: " + serviceName);
            }
            if (eventCount < 0) {
                throw new IllegalArgumentException("Invalid event count: " + eventCount);
            }
            String host = ClaraUtil.getHostName(serviceName);
            String data = buildData(CConstants.SERVICE_REPORT_DONE, eventCount);
            xMsgTopic topic = xMsgTopic.wrap(serviceName);
            xMsgMessage msg = buildMessage(topic, data);
            base.genericSend(host, msg);
        } catch (IOException | xMsgException e) {
            throw new ClaraException("Could not send request", e);
        }
    }


    /**
     * Sends a request to stop reporting "done" on service executions.
     * Configures the service to stop publishing "done" messages.
     * If the service does not exist, the message is lost.
     *
     * @param serviceName the canonical name of the service
     * @throws ClaraException if the request could not be sent
     */
    public void stopReportingDone(String serviceName) throws ClaraException {
        startReportingDone(serviceName, 0);
    }


    /**
     * Sends a request to start reporting the output data on service executions.
     * Configures the service to repeatedly publish the resulting output data after
     * a number of <code>eventCount</code> executions have been completed.
     * If the service does not exist, the message is lost.
     *
     * @param serviceName the canonical name of the service
     * @param eventCount the interval of executions to be completed to publish the report
     * @throws ClaraException if the request could not be sent
     */
    public void startReportingData(String serviceName, int eventCount) throws ClaraException {
        try {
            Objects.requireNonNull(serviceName, "Null service name");
            if (!ClaraUtil.isServiceName(serviceName)) {
                throw new IllegalArgumentException("Malformed service name: " + serviceName);
            }
            if (eventCount < 0) {
                throw new IllegalArgumentException("Invalid event count: " + eventCount);
            }
            String host = ClaraUtil.getHostName(serviceName);
            String data = buildData(CConstants.SERVICE_REPORT_DATA, eventCount);
            xMsgTopic topic = xMsgTopic.wrap(serviceName);
            xMsgMessage msg = buildMessage(topic, data);
            base.genericSend(host, msg);
        } catch (IOException | xMsgException e) {
            throw new ClaraException("Could not send request", e);
        }
    }


    /**
     * Sends a request to stop reporting the output data on service executions.
     * Configures the service to stop publishing the resulting output data.
     * If the service does not exist, the message is lost.
     *
     * @param serviceName the canonical name of the service
     * @throws ClaraException if the request could not be sent
     */
    public void stopReportingData(String serviceName) throws ClaraException {
        startReportingData(serviceName, 0);
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
     * @param serviceName the service to be listened
     * @param status the status to be listened
     * @param callback the action to be run when a message is received
     * @throws ClaraException if there was an error starting the subscription
     */
    public void listenServiceStatus(String serviceName,
                                    EngineStatus status,
                                    EngineCallback callback) throws ClaraException {
        try {
            Objects.requireNonNull(serviceName, "Null service name");
            Objects.requireNonNull(status, "Null status");
            Objects.requireNonNull(callback, "Null callback");
            if (!ClaraUtil.isCanonicalName(serviceName)) {
                throw new IllegalArgumentException("Not a Clara name: " + serviceName);
            }
            String host = base.getFrontEndAddress();
            xMsgTopic topic = buildTopic(getStatusText(status), serviceName);
            String key = host + "#" + topic;
            if (subscriptions.containsKey(key)) {
                throw new IllegalStateException("Duplicated subscription to: " + serviceName);
            }
            xMsgCallBack wrapperCallback = wrapEngineCallback(callback, status);
            xMsgSubscription handler = base.genericReceive(host, topic, wrapperCallback);
            subscriptions.put(key, handler);
        } catch (IOException | xMsgException e) {
            throw new ClaraException("Could not subscribe to service status", e);
        }
    }


    /**
     * Unsubscribes from the specified status reports of the selected service.
     *
     * @param serviceName the service being listened
     * @param status the status being listened
     * @throws ClaraException if there was an error stopping the subscription
     */
    public void unlistenServiceStatus(String serviceName, EngineStatus status)
            throws ClaraException {
        try {
            Objects.requireNonNull(serviceName, "Null service name");
            Objects.requireNonNull(status, "Null status");
            if (!ClaraUtil.isCanonicalName(serviceName)) {
                throw new IllegalArgumentException("Not a Clara name: " + serviceName);
            }
            String host = base.getFrontEndAddress();
            xMsgTopic topic = buildTopic(getStatusText(status), serviceName);
            String key = host + "#" + topic;
            xMsgSubscription handler = subscriptions.remove(key);
            if (handler != null) {
                base.unsubscribe(handler);
            }
        } catch (xMsgException e) {
            throw new ClaraException("Could not unsubscribe to service status", e);
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
      * @param serviceName the service to be listened
      * @param callback the action to be run when a message is received
     * @throws ClaraException if there was an error starting the subscription
      */
    public void listenServiceData(String serviceName, EngineCallback callback)
            throws ClaraException {
        try {
            Objects.requireNonNull(serviceName, "Null service name");
            Objects.requireNonNull(callback, "Null callback");
            if (!ClaraUtil.isCanonicalName(serviceName)) {
                throw new IllegalArgumentException("Not a Clara name: " + serviceName);
            }
            String host = base.getFrontEndAddress();
            xMsgTopic topic = buildTopic(xMsgConstants.DATA.toString(), serviceName);
            String key = host + "#" + topic;
            if (subscriptions.containsKey(key)) {
                throw new IllegalStateException("Duplicated subscription to: " + serviceName);
            }
            xMsgCallBack wrapperCallback = wrapEngineCallback(callback, null);
            xMsgSubscription handler = base.genericReceive(host, topic, wrapperCallback);
            subscriptions.put(key, handler);
        } catch (IOException | xMsgException e) {
            throw new ClaraException("Could not subscribe to service data", e);
        }
    }


    /**
     * Unsubscribes from the data reports of the selected service.
     *
     * @param serviceName the service being listened
     * @throws ClaraException if there was an error stopping the subscription
     */
    public void unlistenServiceData(String serviceName)
            throws ClaraException {
        try {
            Objects.requireNonNull(serviceName, "Null service name");
            if (!ClaraUtil.isCanonicalName(serviceName)) {
                throw new IllegalArgumentException("Not a Clara name: " + serviceName);
            }
            String host = base.getFrontEndAddress();
            xMsgTopic topic = buildTopic(xMsgConstants.DATA.toString(), serviceName);
            String key = host + "#" + topic;
            xMsgSubscription handler = subscriptions.remove(key);
            if (handler != null) {
                base.unsubscribe(handler);
            }
        } catch (xMsgException e) {
            throw new ClaraException("Could not unsubscribe to service data", e);
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
     * @param serviceName the service to be listened
     * @param callback the action to be run when a message is received
     * @throws ClaraException if there was an error starting the subscription
     */
    public void listenServiceDone(String serviceName, EngineCallback callback)
            throws ClaraException {
        try {
            Objects.requireNonNull(serviceName, "Null service name");
            Objects.requireNonNull(callback, "Null callback");
            if (!ClaraUtil.isCanonicalName(serviceName)) {
                throw new IllegalArgumentException("Not a Clara name: " + serviceName);
            }
            String host = base.getFrontEndAddress();
            xMsgTopic topic = buildTopic(xMsgConstants.DONE.toString(), serviceName);
            String key = host + "#" + topic;
            if (subscriptions.containsKey(key)) {
                throw new IllegalStateException("Duplicated subscription to: " + serviceName);
            }
            xMsgCallBack wrapperCallback = wrapEngineCallback(callback, null);
            xMsgSubscription handler = base.genericReceive(host, topic, wrapperCallback);
            subscriptions.put(key, handler);
        } catch (IOException | xMsgException e) {
            throw new ClaraException("Could not subscribe to service done", e);
        }
    }


    /**
     * Unsubscribes from the "done" reports of the selected service.
     *
     * @param serviceName the service being listened
     * @throws ClaraException if there was an error stopping the subscription
     */
    public void unlistenServiceDone(String serviceName)
            throws ClaraException {
        try {
            Objects.requireNonNull(serviceName, "Null service name");
            if (!ClaraUtil.isCanonicalName(serviceName)) {
                throw new IllegalArgumentException("Not a Clara name: " + serviceName);
            }
            String host = base.getFrontEndAddress();
            xMsgTopic topic = buildTopic(xMsgConstants.DONE.toString(), serviceName);
            String key = host + "#" + topic;
            xMsgSubscription handler = subscriptions.remove(key);
            if (handler != null) {
                base.unsubscribe(handler);
            }
        } catch (xMsgException e) {
            throw new ClaraException("Could not unsubscribe to service done", e);
        }
    }


    /**
     * Subscribes to the periodic alive message reported by the running DPEs.
     *
     * @param callback the action to be run when a report is received
     * @throws ClaraException if there was an error starting the subscription
     */
    public void listenDpes(GenericCallback callback) throws ClaraException {
        try {
            Objects.requireNonNull(callback, "Null callback");
            String host = base.getFrontEndAddress();
            xMsgTopic topic = buildTopic(CConstants.DPE_ALIVE);
            String key = host + "#" + topic;
            if (subscriptions.containsKey(key)) {
                throw new IllegalStateException("Duplicated subscription to: " + topic);
            }
            xMsgCallBack wrapperCallback = wrapGenericCallback(callback);
            xMsgSubscription handler = base.genericReceive(host, topic, wrapperCallback);
            subscriptions.put(key, handler);
        } catch (IOException | xMsgException e) {
            throw new ClaraException("Could not subscribe to DPEs", e);
        }
    }


    /**
     * Unsubscribes from the alive reports of the running DPEs.
     *
     * @throws ClaraException if there was an error stopping the subscription
     */
    public void unlistenDpes() throws ClaraException {
        try {
            xMsgTopic topic = buildTopic(CConstants.DPE_ALIVE);
            String host = base.getFrontEndAddress();
            String key = host + "#" + topic;
            xMsgSubscription handler = subscriptions.remove(key);
            if (handler != null) {
                base.unsubscribe(handler);
            }
        } catch (xMsgException e) {
            throw new ClaraException("Could not unsubscribe to DPEs", e);
        }
    }


    /**
     * Returns the canonical names of all the actors that match the given query.
     * The returned actors (DPEs, containers or services) depend of the type of
     * the query.
     *
     * @param query the search filter
     * @return the canonical names that match the filter
     * @throws ClaraException if there was a problem with the request
     */
    public Set<String> getCanonicalNames(ClaraFilter query) throws ClaraException {
        throw new UnsupportedOperationException("Not implemented yet");
    }


    /**
     * Returns the registration information of the selected Clara actor.
     * The actor can be a DPE, a container or a service.
     *
     * @param canonicalName the name of the actor
     * @return a JSON object with the registration information of the actor
     * @throws ClaraException if there was a problem with the request
     */
    public String getRegistrationInfo(String canonicalName) throws ClaraException {
        throw new UnsupportedOperationException("Not implemented yet");
    }


    /**
     * Returns the registration information of all the actors that match the
     * given query.
     * The returned actors (DPEs, containers or services) depend of the type of
     * the query.
     *
     * @param query the search filter
     * @return a JSON array with the registration information of all selected actorss
     * @throws ClaraException if there was a problem with the request
     */
    public String getRegistrationInfo(ClaraFilter query) throws ClaraException {
        throw new UnsupportedOperationException("Not implemented yet");
    }


    /**
     * Checks if the given DPE is up and running.
     *
     * @param dpeName the name of the DPE
     * @return true if the DPE is running
     * @throws ClaraException if there was a problem with the request
     */
    public boolean pingDpe(String dpeName) throws ClaraException {
        throw new UnsupportedOperationException("Not implemented yet");
    }


    /**
     * Checks if the given service is up and running.
     *
     * @param serviceName the name of the service
     * @return true if the service is running
     * @throws ClaraException if there was a problem with the request
     */
    public boolean pingService(String serviceName) throws ClaraException {
        throw new UnsupportedOperationException("Not implemented yet");
    }


    /**
     * Returns the runtime information of the selected Clara actor.
     * The actor can be a DPE, a container or a service.
     *
     * @param canonicalName the name of the actor
     * @return a JSON object with the runtime information of the actor
     * @throws ClaraException if there was a problem with the request
     */
    public String getRuntimeState(String canonicalName) throws ClaraException {
        throw new UnsupportedOperationException("Not implemented yet");
    }


    /**
     * Returns the runtime information of all the actors that match the
     * given query.
     * The returned actors (DPEs, containers or services) depend of the type of
     * the query.
     *
     * @param query the search filter
     * @return a JSON array with the runtime information of all selected actorss
     * @throws ClaraException if there was a problem with the request
     */
    public String getRuntimeState(ClaraFilter query) throws ClaraException {
        throw new UnsupportedOperationException("Not implemented yet");
    }


    /**
     * Returns the assigned orchestrator name.
     */
    public String getName() {
        return base.getName();
    }


    /**
     * Returns the registered local address.
     */
    public String getLocalAddress() {
        return base.getLocalAddress();
    }


    /**
     * Returns the registered front-end address.
     */
    public String getFrontEndAddress() {
        return base.getFrontEndAddress();
    }


    private String getStatusText(EngineStatus status) {
        switch (status) {
            case INFO:
                return xMsgConstants.INFO.toString();
            case WARNING:
                return xMsgConstants.WARNING.toString();
            case ERROR:
                return xMsgConstants.ERROR.toString();
            default:
                throw new IllegalStateException("Unknown status " + status);
        }
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
                    userCallback.callback(base.parseFrom(msg, dataTypes));
                } catch (CException e) {
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
                    String mimeType = msg.getMimeType();
                    if (mimeType.equals("text/string")) {
                        userCallback.callback(new String(msg.getData()));
                    } else {
                        throw new CException("Unexpected mime-type: " + mimeType);
                    }
                } catch (CException e) {
                    System.out.println("Error receiving data to " + msg.getTopic());
                    e.printStackTrace();
                }
                return null;
            }
        };
    }
}
