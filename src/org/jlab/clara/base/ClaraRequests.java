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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.util.CConstants;
import org.jlab.clara.util.ClaraUtil;
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
