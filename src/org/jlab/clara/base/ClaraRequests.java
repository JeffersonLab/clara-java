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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.util.CConstants;
import org.jlab.clara.util.ClaraUtil;
import org.jlab.clara.util.report.CReportTypes;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;
import org.jlab.coda.xmsg.excp.xMsgException;

public final class ClaraRequests {

    private ClaraRequests() { }

    /**
     * A request to a Clara component.
     *
     * @param <D> The specific subclass
     * @param <T> The type returned when a result is expected
     */
    public abstract static class BaseRequest<D extends BaseRequest<D, T>, T> {

        protected final ClaraBase base;

        protected ClaraComponent frontEnd;
        protected xMsgTopic topic;
        protected xMsgMeta.Builder meta;

        BaseRequest(ClaraBase base,
                    ClaraComponent frontEnd,
                    String topic) {
            this.base = base;
            this.frontEnd = frontEnd;
            this.topic = xMsgTopic.wrap(topic);
            this.meta = xMsgMeta.newBuilder();
        }

        /**
         * Sends the request.
         *
         * @throws ClaraException if the request could not be sent
         */
        public void run() throws ClaraException {
            try {
                base.send(frontEnd, msg());
            } catch (xMsgException e) {
                throw new ClaraException("Cannot send message", e);
            }
        }

        /**
         * Sends the request and wait for a response.
         *
         * @param wait the amount of time units to wait for a response
         * @param unit the unit of time
         * @throws ClaraException if the request could not be sent or received
         * @throws TimeoutException if the response is not received
         * @returns the data of the response
         */
        public T syncRun(int wait, TimeUnit unit) throws ClaraException, TimeoutException {
            try {
                int timeout = (int) unit.toMillis(wait);
                xMsgMessage response = base.syncSend(frontEnd, msg(), timeout);
                return parseData(response);
            } catch (xMsgException e) {
                throw new ClaraException("Cannot sync send message", e);
            }
        }

        @SuppressWarnings("unchecked")
        protected D self() {
            return (D) this;
        }

        /**
         * Creates the message to be sent to the component.
         *
         * @throws ClaraException if the message could not be created
         */
        protected abstract xMsgMessage msg() throws ClaraException;

        /**
         * Parses the data returned by a sync request.
         *
         * @throws ClaraException if the data could not be parsed
         */
        protected abstract T parseData(xMsgMessage msg) throws ClaraException;
    }


    /**
     * Base class for sending a string-encoded request
     * and parsing the status of the operation.
     */
    public abstract static class DataRequest<D extends DataRequest<D>>
            extends BaseRequest<D, Boolean> {

        DataRequest(ClaraBase base, ClaraComponent frontEnd, String topic) {
            super(base, frontEnd, topic);
        }

        /**
         * Creates the data to be sent to the component.
         */
        protected abstract String getData();

        @Override
        protected xMsgMessage msg() throws ClaraException {
            xMsgMessage msg = createMessage(topic, getData());
            return msg;
        }

        @Override
        protected Boolean parseData(xMsgMessage msg) {
            // TODO Auto-generated method stub
            return true;
        }
    }


    /**
     * Base class to deploy a Clara component.
     * Each subclass presents the optional fields specific to each component.
     */
    public abstract static class DeployRequest<D extends DeployRequest<D>>
            extends DataRequest<D> {

        protected int poolSize = 1;
        protected String description = CConstants.UNDEFINED;

        DeployRequest(ClaraBase base, ClaraComponent frontEnd, String topic) {
            super(base, frontEnd, topic);
        }

        /**
         * Defines a custom pool size for the started component.
         * The pool size sets how many parallel requests can be processed
         * by the component.
         *
         * @return this object, so methods can be chained
         */
        public D withPoolsize(int poolSize) {
            this.poolSize = poolSize;
            return self();
        }

        /**
         * Defines a description for the started component.
         * The description will be used when the component is registered.
         *
         * @return this object, so methods can be chained
         */
        public D withDescription(String description) {
            this.description = description;
            return self();
        }
    }


    /**
     * A request to start a container.
     */
    public static class DeployContainerRequest extends DeployRequest<DeployContainerRequest> {

        private final ContainerName container;

        DeployContainerRequest(ClaraBase base, ClaraComponent frontEnd, ContainerName container) {
            super(base, frontEnd, getDpeTopic(container));
            this.container = container;
        }

        @Override
        protected String getData() {
            return ClaraUtil.buildData(CConstants.START_CONTAINER,
                                       container.name(),
                                       poolSize,
                                       description);
        }
    }


    /**
     * A request to start a service.
     */
    public static class DeployServiceRequest extends DeployRequest<DeployServiceRequest> {

        private final ServiceName service;
        private final String classPath;

        private String initialState = CConstants.UNDEFINED;

        DeployServiceRequest(ClaraBase base, ClaraComponent frontEnd,
                             ServiceName service, String classPath) {
            super(base, frontEnd, getDpeTopic(service));
            this.service = service;
            this.classPath = classPath;
        }

        /**
         * Defines an initial state for the started service.
         *
         * @return this object, so methods can be chained
         */
        public DeployServiceRequest withInitialState(String initialState) {
            this.initialState = initialState;
            return self();
        }

        @Override
        protected String getData() {
            return ClaraUtil.buildData(CConstants.START_SERVICE,
                                       getContainerName(service),
                                       service.name(),
                                       classPath,
                                       poolSize,
                                       description,
                                       initialState);
        }
    }


    /**
     * A request to stop a running Clara component.
     */
    public static class ExitRequest extends DataRequest<ExitRequest> {

        private final String data;

        /**
         * A request to stop a DPE.
         */
        ExitRequest(ClaraBase base, ClaraComponent frontEnd, DpeName dpe) {
            super(base, frontEnd, getDpeTopic(dpe));
            data = ClaraUtil.buildData(CConstants.STOP_DPE);
        }

        /**
         * A request to stop a container.
         */
        ExitRequest(ClaraBase base, ClaraComponent frontEnd, ContainerName container) {
            super(base, frontEnd, getDpeTopic(container));
            data = ClaraUtil.buildData(CConstants.STOP_CONTAINER, container.name());
        }

        /**
         * A request to stop a service.
         */
        ExitRequest(ClaraBase base, ClaraComponent frontEnd, ServiceName service) {
            super(base, frontEnd, getDpeTopic(service));
            String containerName = getContainerName(service);
            data = ClaraUtil.buildData(CConstants.STOP_SERVICE, containerName, service.name());
        }

        @Override
        protected String getData() {
            return data;
        }
    }


    /**
     * Base class to send a control request to a service, and return a response.
     *
     * @param <T> The type of data returned to the client by the request.
     */
    public abstract static class ServiceRequest<D extends ServiceRequest<D, T>, T>
                extends BaseRequest<D, T> {

        protected final EngineData userData;
        protected Set<EngineDataType> dataTypes;
        protected final xMsgMeta.ControlAction action;
        protected final String composition;

        ServiceRequest(ClaraBase base, ClaraComponent frontEnd, ServiceName service,
                       xMsgMeta.ControlAction action,
                       EngineData data, Set<EngineDataType> dataTypes) {
            super(base, frontEnd, service.canonicalName());
            this.userData = data;
            this.dataTypes = dataTypes;
            this.action = action;
            this.composition = service.canonicalName();
        }

        ServiceRequest(ClaraBase base, ClaraComponent frontEnd, Composition composition,
                       xMsgMeta.ControlAction action,
                       EngineData data, Set<EngineDataType> dataTypes) {
            super(base, frontEnd, composition.firstService());
            this.userData = data;
            this.dataTypes = dataTypes;
            this.action = action;
            this.composition = composition.toString();
        }

        /**
         * Overwrites the data types used for serializing the data to the service,
         * and deserializing its response if needed.
         *
         * @param dataType the custom data-type of the configuration data
         * @return this object, so methods can be chained
         */
        public D withDataTypes(Set<EngineDataType> dataTypes) {
            this.dataTypes = dataTypes;
            return self();
        }

        /**
         * Overwrites the data types used for serializing the data to the service,
         * and deserializing its response if needed.
         *
         * @param dataType the custom data-type of the configuration data
         * @return this object, so methods can be chained
         */
        public D withDataTypes(EngineDataType... dataTypes) {
            Set<EngineDataType> newTypes = new HashSet<>();
            for (EngineDataType dt : dataTypes) {
                newTypes.add(dt);
            }
            this.dataTypes = newTypes;
            return self();
        }

        @Override
        protected xMsgMessage msg() throws ClaraException {
            try {
                xMsgMessage msg = new xMsgMessage(topic, null);
                base.serialize(userData, msg, dataTypes);
                msg.getMetaData().setAction(action);
                msg.getMetaData().setComposition(composition);
                return msg;
            } catch (xMsgException | IOException e) {
                throw new ClaraException("Cannot create message", e);
            }
        }
    }


    /**
     * A request to configure a service.
     */
    public static class ServiceConfigRequest
                extends ServiceRequest<ServiceConfigRequest, Boolean> {

        ServiceConfigRequest(ClaraBase base, ClaraComponent frontEnd, ServiceName service,
                             EngineData data, Set<EngineDataType> dataTypes) {
            super(base, frontEnd, service,
                  xMsgMeta.ControlAction.CONFIGURE, data, dataTypes);
        }

        @Override
        protected Boolean parseData(xMsgMessage msg) {
            // TODO Auto-generated catch block
            return true;
        }
    }


    /**
     * A request to execute a service composition.
     */
    public static class ServiceExecuteRequest
                extends ServiceRequest<ServiceExecuteRequest, EngineData> {

        ServiceExecuteRequest(ClaraBase base, ClaraComponent frontEnd,
                              Composition composition,
                              EngineData data, Set<EngineDataType> dataTypes)
                throws ClaraException {
            super(base, frontEnd, composition,
                  xMsgMeta.ControlAction.EXECUTE, data, dataTypes);
        }

        @Override
        protected EngineData parseData(xMsgMessage msg) throws ClaraException {
            return base.deSerialize(msg, dataTypes);
        }
    }


    /**
     * A request to setup the reports of a service.
     */
    public static class ServiceReportRequest extends DataRequest<ServiceReportRequest> {

        private final String data;

        ServiceReportRequest(ClaraBase base, ClaraComponent frontEnd,
                             ServiceName service, CReportTypes type, int eventCount) {
            super(base, frontEnd, service.canonicalName());
            data = ClaraUtil.buildData(type.getValue(), eventCount);
        }

        @Override
        protected String getData() {
            return data;
        }
    }


    /**
     * Builds a request to configure a service.
     * A service can be configured with data,
     * or by setting the event count to publish data/done reports.
     */
    public static class ServiceConfigRequestBuilder {

        private final ClaraBase base;
        private final ClaraComponent frontEnd;
        private final ServiceName service;
        private final Set<EngineDataType> dataTypes;

        ServiceConfigRequestBuilder(ClaraBase base, ClaraComponent frontEnd,
                                    ServiceName service, Set<EngineDataType> dataTypes) {
            this.base = base;
            this.frontEnd = frontEnd;
            this.service = service;
            this.dataTypes = dataTypes;
        }

        /**
         * Creates a request to configure the specified service
         * with the given data.
         * If the service does not exist, the message is lost.
         *
         * @param data the data for configuring the service
         * @returns a service configuration request to be run
         */
        public ServiceConfigRequest withData(EngineData data) {
            return new ServiceConfigRequest(base, frontEnd, service, data, dataTypes);
        }

        /**
         * Creates a request to start reporting "done" on executions of the
         * specified service.
         * Configures the service to repeatedly publish "done" messages after
         * a number of <code>eventCount</code> executions have been completed.
         * If the service does not exist, the message is lost.
         *
         * @param eventCount the interval of executions to be completed to publish the report
         * @returns a service configuration request to be run
         */
        public ServiceReportRequest startDoneReporting(int eventCount) {
            return new ServiceReportRequest(base, frontEnd, service, CReportTypes.DONE, eventCount);
        }

        /**
         * Creates a request to stop reporting "done" on executions of the
         * specified service.
         * Configures the service to stop publishing "done" messages.
         * If the service does not exist, the message is lost.
         *
         * @returns a service configuration request to be run
         */
        public ServiceReportRequest stopDoneReporting() {
            return new ServiceReportRequest(base, frontEnd, service, CReportTypes.DONE, 0);
        }

        /**
         * Creates a request to start reporting the output data on executions of the
         * specified service.
         * Configures the service to repeatedly publish the resulting output data after
         * a number of <code>eventCount</code> executions have been completed.
         * If the service does not exist, the message is lost.
         *
         * @param eventCount the interval of executions to be completed to publish the report
         * @returns a service configuration request to be run
         */
        public ServiceReportRequest startDataReporting(int eventCount) {
            return new ServiceReportRequest(base, frontEnd, service, CReportTypes.DATA, eventCount);
        }

        /**
         * Creates a request to stop reporting the output data on executions of the
         * specified service.
         * Configures the service to stop publishing the resulting output data.
         * If the service does not exist, the message is lost.
         *
         * @returns a service configuration request to be run
         */
        public ServiceReportRequest stopDataReporting() {
            return new ServiceReportRequest(base, frontEnd, service, CReportTypes.DATA, 0);
        }
    }



    /**
     * Builds a request to execute a service or a composition.
     */
    public static class ServiceExecuteRequestBuilder {

        private final ClaraBase base;
        private final ClaraComponent frontEnd;
        private final Composition composition;
        private final Set<EngineDataType> dataTypes;

        ServiceExecuteRequestBuilder(ClaraBase base, ClaraComponent frontEnd,
                                     ServiceName service, Set<EngineDataType> dataTypes) {
            this(base, frontEnd, new Composition(service.canonicalName()), dataTypes);
        }

        ServiceExecuteRequestBuilder(ClaraBase base, ClaraComponent frontEnd,
                                     Composition composition, Set<EngineDataType> dataTypes) {
            this.base = base;
            this.frontEnd = frontEnd;
            this.composition = composition;
            this.dataTypes = dataTypes;
        }


        /**
         * Creates a request to execute the specified service/composition with
         * the given data.
         * If any service does not exist, the message is lost.
         *
         * @param data the input data to execute the service/composition
         * @returns a service execute request to be run
         */
        public ServiceExecuteRequest withData(EngineData data) throws ClaraException {
            return new ServiceExecuteRequest(base, frontEnd, composition, data, dataTypes);
        }
    }



    private static ClaraComponent getDpeComponent(ClaraName claraName) {
        try {
            return ClaraComponent.dpe(ClaraUtil.getDpeName(claraName.canonicalName()));
        } catch (ClaraException e) {
            throw new IllegalArgumentException("Invalid Clara name: " + claraName);
        }
    }

    private static String getDpeTopic(ClaraName claraName) {
        return "dpe:" + getDpeComponent(claraName).getTopic();
    }

    private static String getContainerName(ServiceName service) {
        try {
            return ClaraUtil.getContainerName(service.canonicalName());
        } catch (ClaraException e) {
            throw new IllegalArgumentException("Invalid service name: " + service);
        }
    }

    private static xMsgMessage createMessage(xMsgTopic topic, String data) throws ClaraException {
        try {
            return new xMsgMessage(topic, xMsgConstants.STRING, data.getBytes());
        } catch (xMsgException | IOException e) {
            throw new ClaraException("Cannot create message", e);
        }
    }
}
