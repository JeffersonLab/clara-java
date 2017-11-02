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

package org.jlab.clara.base;

import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.clara.base.core.DataUtil;
import org.jlab.clara.base.core.ClaraBase;
import org.jlab.clara.base.core.ClaraComponent;
import org.jlab.clara.base.core.MessageUtil;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.util.report.ReportType;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;
import org.jlab.coda.xmsg.excp.xMsgException;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Requests to running CLARA components.
 */
public final class ClaraRequests {

    private ClaraRequests() { }

    /**
     * A request to a CLARA component.
     *
     * @param <D> The specific subclass
     * @param <T> The type returned when a result is expected
     */
    abstract static class BaseRequest<D extends BaseRequest<D, T>, T> {

        final ClaraBase base;

        final ClaraComponent frontEnd;
        final xMsgTopic topic;

        BaseRequest(ClaraBase base,
                    ClaraComponent frontEnd,
                    String topic) {
            this.base = base;
            this.frontEnd = frontEnd;
            this.topic = xMsgTopic.wrap(topic);
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
         * @return the data of the response
         */
        public T syncRun(long wait, TimeUnit unit) throws ClaraException, TimeoutException {
            try {
                if (wait <= 0) {
                    throw new IllegalArgumentException("Invalid timeout: " + wait);
                }
                long timeout = unit.toMillis(wait);
                xMsgMessage response = base.syncSend(frontEnd, msg(), timeout);
                return parseData(response);
            } catch (xMsgException e) {
                throw new ClaraException("Cannot sync send message", e);
            }
        }

        @SuppressWarnings("unchecked")
        D self() {
            return (D) this;
        }

        /**
         * Creates the message to be sent to the component.
         *
         * @throws ClaraException if the message could not be created
         */
        abstract xMsgMessage msg() throws ClaraException;

        /**
         * Parses the data returned by a sync request.
         *
         * @throws ClaraException if the data could not be parsed
         */
        abstract T parseData(xMsgMessage msg) throws ClaraException;
    }

    /**
     * Base class for sending a string-encoded request
     * and parsing the status of the operation.
     */
    abstract static class DataRequest<D extends DataRequest<D>>
            extends BaseRequest<D, Boolean> {

        DataRequest(ClaraBase base, ClaraComponent frontEnd, String topic) {
            super(base, frontEnd, topic);
        }

        /**
         * Creates the data to be sent to the component.
         */
        abstract String getData();

        @Override
        xMsgMessage msg() throws ClaraException {
            xMsgMessage msg = MessageUtil.buildRequest(topic, getData());
            msg.getMetaData().setAuthor(base.getName());
            return msg;
        }

        @Override
        Boolean parseData(xMsgMessage msg) throws ClaraException {
            xMsgMeta.Status status = msg.getMetaData().getStatus();
            if (status == xMsgMeta.Status.ERROR) {
                // TODO: use specific "request" exception
                throw new ClaraException(new String(msg.getData()));
            }
            return true;
        }
    }

    /**
     * Base class to deploy a CLARA component.
     * Each subclass presents the optional fields specific to each component.
     */
    abstract static class DeployRequest<D extends DeployRequest<D>>
            extends DataRequest<D> {

        protected int poolSize = 1;
        protected String description = ClaraConstants.UNDEFINED;

        DeployRequest(ClaraBase base, ClaraComponent frontEnd, String topic) {
            super(base, frontEnd, topic);
        }

        /**
         * Defines a custom pool size for the started component.
         * The pool size sets how many parallel requests can be processed
         * by the component.
         *
         * @param poolSize the poolSize for the component
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
         * @param description a description for the component
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
        String getData() {
            return MessageUtil.buildData(ClaraConstants.START_CONTAINER,
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

        private String initialState = ClaraConstants.UNDEFINED;

        DeployServiceRequest(ClaraBase base, ClaraComponent frontEnd,
                             ServiceName service, String classPath) {
            super(base, frontEnd, getDpeTopic(service));
            this.service = service;
            this.classPath = classPath;
        }

        /**
         * Defines an initial state for the started service.
         *
         * @param initialState the initial state for the service
         * @return this object, so methods can be chained
         */
        public DeployServiceRequest withInitialState(String initialState) {
            this.initialState = initialState;
            return self();
        }

        @Override
        String getData() {
            return MessageUtil.buildData(ClaraConstants.START_SERVICE,
                                         service.container().name(),
                                         service.name(),
                                         classPath,
                                         poolSize,
                                         description,
                                         initialState);
        }
    }

    /**
     * A request to stop a running CLARA component.
     */
    public static class ExitRequest extends DataRequest<ExitRequest> {

        private final String data;

        /**
         * A request to stop a DPE.
         */
        ExitRequest(ClaraBase base, ClaraComponent frontEnd, DpeName dpe) {
            super(base, frontEnd, getDpeTopic(dpe));
            data = MessageUtil.buildData(ClaraConstants.STOP_DPE);
        }

        /**
         * A request to stop a container.
         */
        ExitRequest(ClaraBase base, ClaraComponent frontEnd, ContainerName container) {
            super(base, frontEnd, getDpeTopic(container));
            data = MessageUtil.buildData(ClaraConstants.STOP_CONTAINER, container.name());
        }

        /**
         * A request to stop a service.
         */
        ExitRequest(ClaraBase base, ClaraComponent frontEnd, ServiceName service) {
            super(base, frontEnd, getDpeTopic(service));
            data = MessageUtil.buildData(ClaraConstants.STOP_SERVICE,
                                          service.container().name(), service.name());
        }

        @Override
        String getData() {
            return data;
        }
    }

    /**
     * Base class to send a control request to a service, and return a response.
     *
     * @param <T> The type of data returned to the client by the request.
     */
    abstract static class ServiceRequest<D extends ServiceRequest<D, T>, T>
                extends BaseRequest<D, T> {

        private final EngineData userData;
        private final xMsgMeta.ControlAction action;
        private final Composition composition;

        protected Set<EngineDataType> dataTypes;

        ServiceRequest(ClaraBase base, ClaraComponent frontEnd, ServiceName service,
                       xMsgMeta.ControlAction action,
                       EngineData data, Set<EngineDataType> dataTypes) {
            super(base, frontEnd, service.canonicalName());
            this.userData = data;
            this.dataTypes = dataTypes;
            this.action = action;
            this.composition = getComposition(service);
        }

        ServiceRequest(ClaraBase base, ClaraComponent frontEnd, Composition composition,
                       xMsgMeta.ControlAction action,
                       EngineData data, Set<EngineDataType> dataTypes) {
            super(base, frontEnd, composition.firstService());
            this.userData = data;
            this.dataTypes = dataTypes;
            this.action = action;
            this.composition = composition;
        }

        /**
         * Overwrites the data types used for serializing the data to the service,
         * and deserializing its response if needed.
         *
         * @param dataTypes the custom data-type of the configuration data
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
         * @param dataTypes the custom data-type of the configuration data
         * @return this object, so methods can be chained
         */
        public D withDataTypes(EngineDataType... dataTypes) {
            Set<EngineDataType> newTypes = new HashSet<>();
            Collections.addAll(newTypes, dataTypes);
            this.dataTypes = newTypes;
            return self();
        }

        @Override
        xMsgMessage msg() throws ClaraException {
            xMsgMessage msg = DataUtil.serialize(topic, userData, dataTypes);
            xMsgMeta.Builder meta = msg.getMetaData();
            meta.setAuthor(base.getName());
            meta.setAction(action);
            meta.setComposition(composition.toString());
            return msg;
        }
    }

    /**
     * A request to configure a service.
     */
    public static class ServiceConfigRequest
                extends ServiceRequest<ServiceConfigRequest, EngineData> {

        ServiceConfigRequest(ClaraBase base, ClaraComponent frontEnd, ServiceName service,
                             EngineData data, Set<EngineDataType> dataTypes) {
            super(base, frontEnd, service,
                  xMsgMeta.ControlAction.CONFIGURE, data, dataTypes);
        }

        @Override
        EngineData parseData(xMsgMessage msg) throws ClaraException {
            return DataUtil.deserialize(msg, dataTypes);
        }
    }

    /**
     * A request to execute a service composition.
     */
    public static class ServiceExecuteRequest
                extends ServiceRequest<ServiceExecuteRequest, EngineData> {

        ServiceExecuteRequest(ClaraBase base, ClaraComponent frontEnd,
                              Composition composition,
                              EngineData data, Set<EngineDataType> dataTypes) {
            super(base, frontEnd, composition,
                  xMsgMeta.ControlAction.EXECUTE, data, dataTypes);
        }

        @Override
        EngineData parseData(xMsgMessage msg) throws ClaraException {
            return DataUtil.deserialize(msg, dataTypes);
        }
    }

    /**
     * A request to setup the reports of a service.
     */
    public static class ServiceReportRequest extends DataRequest<ServiceReportRequest> {

        private final String data;

        ServiceReportRequest(ClaraBase base, ClaraComponent frontEnd,
                             ServiceName service, ReportType type, int eventCount) {
            super(base, frontEnd, service.canonicalName());
            data = MessageUtil.buildData(type.getValue(), eventCount);
        }

        @Override
        String getData() {
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
         * @return a service configuration request to be run
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
         * @return a service configuration request to be run
         */
        public ServiceReportRequest startDoneReporting(int eventCount) {
            return new ServiceReportRequest(base, frontEnd, service, ReportType.DONE, eventCount);
        }

        /**
         * Creates a request to stop reporting "done" on executions of the
         * specified service.
         * Configures the service to stop publishing "done" messages.
         * If the service does not exist, the message is lost.
         *
         * @return a service configuration request to be run
         */
        public ServiceReportRequest stopDoneReporting() {
            return new ServiceReportRequest(base, frontEnd, service, ReportType.DONE, 0);
        }

        /**
         * Creates a request to start reporting the output data on executions of the
         * specified service.
         * Configures the service to repeatedly publish the resulting output data after
         * a number of <code>eventCount</code> executions have been completed.
         * If the service does not exist, the message is lost.
         *
         * @param eventCount the interval of executions to be completed to publish the report
         * @return a service configuration request to be run
         */
        public ServiceReportRequest startDataReporting(int eventCount) {
            return new ServiceReportRequest(base, frontEnd, service, ReportType.DATA, eventCount);
        }

        /**
         * Creates a request to stop reporting the output data on executions of the
         * specified service.
         * Configures the service to stop publishing the resulting output data.
         * If the service does not exist, the message is lost.
         *
         * @return a service configuration request to be run
         */
        public ServiceReportRequest stopDataReporting() {
            return new ServiceReportRequest(base, frontEnd, service, ReportType.DATA, 0);
        }

        /**
         * Creates a request to start reporting result of executions of the
         * specified service to the CLARA Data Ring.
         * If the service does not exist, the message is lost.
         *
         * @return a service configuration request to be run
         */
        public ServiceReportRequest startDataRingReporting() {
            return new ServiceReportRequest(base, frontEnd, service, ReportType.RING, 1);
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
            this(base, frontEnd, getComposition(service), dataTypes);
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
         * @return a service execute request to be run
         */
        public ServiceExecuteRequest withData(EngineData data) {
            return new ServiceExecuteRequest(base, frontEnd, composition, data, dataTypes);
        }
    }



    private static ClaraComponent getDpeComponent(ClaraName claraName) {
        return ClaraComponent.dpe(ClaraUtil.getDpeName(claraName.canonicalName()));
    }

    private static String getDpeTopic(ClaraName claraName) {
        return getDpeComponent(claraName).getTopic().toString();
    }

    private static Composition getComposition(ServiceName service) {
        return new Composition(service.canonicalName() + ";");
    }
}
