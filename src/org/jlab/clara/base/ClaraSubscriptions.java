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
import org.jlab.clara.base.core.ClaraBase;
import org.jlab.clara.base.core.ClaraComponent;
import org.jlab.clara.base.core.MessageUtil;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.engine.EngineStatus;
import org.jlab.coda.xmsg.core.xMsgCallBack;
import org.jlab.coda.xmsg.core.xMsgSubscription;
import org.jlab.coda.xmsg.core.xMsgTopic;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ClaraSubscriptions {

    /**
     * Starts and stops a Clara subscription.
     *
     * @param <D> The specific subclass
     * @param <C> The user callback
     */
    public abstract static class BaseSubscription<D extends BaseSubscription<D, C>, C> {

        protected final ClaraBase base;

        protected ClaraComponent frontEnd;

        protected xMsgTopic topic;

        private Map<String, xMsgSubscription> subscriptions;

        BaseSubscription(ClaraBase base,
                         Map<String, xMsgSubscription> subscriptions,
                         ClaraComponent frontEnd,
                         xMsgTopic topic) {
            this.base = base;
            this.frontEnd = frontEnd;
            this.topic = topic;
            this.subscriptions = subscriptions;
        }

        /**
         * A background thread is started to receive messages from the service.
         * Every time a report is received, the provided callback will be executed.
         * The messages are received sequentially, but the callback may run
         * in extra background threads, so it must be thread-safe.
         */
        public void start(C callback) throws ClaraException {
            String key = frontEnd.getDpeHost() + ClaraConstants.MAPKEY_SEP + topic;
            if (subscriptions.containsKey(key)) {
                throw new IllegalStateException("duplicated subscription to: " + frontEnd);
            }
            xMsgCallBack wrapperCallback = wrap(callback);
            xMsgSubscription handler = base.listen(frontEnd, topic, wrapperCallback);
            subscriptions.put(key, handler);
        }

        public void stop() {
            String key = frontEnd.getDpeHost() + ClaraConstants.MAPKEY_SEP + topic;
            xMsgSubscription handler = subscriptions.remove(key);
            if (handler != null) {
                base.unsubscribe(handler);
            }
        }

        @SuppressWarnings("unchecked")
        protected D self() {
            return (D) this;
        }

        protected abstract xMsgCallBack wrap(C callback);
    }


    public static class ServiceSubscription
            extends BaseSubscription<ServiceSubscription, EngineCallback> {

        private Set<EngineDataType> dataTypes;

        ServiceSubscription(ClaraBase base,
                            Map<String, xMsgSubscription> subscriptions,
                            Set<EngineDataType> dataTypes,
                            ClaraComponent frontEnd,
                            xMsgTopic topic) {
            super(base, subscriptions, frontEnd, topic);
            this.dataTypes = dataTypes;
        }

        public ServiceSubscription withDataTypes(Set<EngineDataType> dataTypes) {
            this.dataTypes = dataTypes;
            return this;
        }

        public ServiceSubscription withDataTypes(EngineDataType... dataTypes) {
            Set<EngineDataType> newTypes = new HashSet<>();
            Collections.addAll(newTypes, dataTypes);
            this.dataTypes = newTypes;
            return this;
        }

        @Override
        protected xMsgCallBack wrap(final EngineCallback userCallback) {
            return msg -> {
                try {
                    userCallback.callback(base.deSerialize(msg, dataTypes));
                } catch (ClaraException e) {
                    System.out.println("Error receiving data to " + msg.getTopic());
                    e.printStackTrace();
                }
            };
        }
    }


    public static class JsonReportSubscription
            extends BaseSubscription<JsonReportSubscription, GenericCallback> {

        JsonReportSubscription(ClaraBase base,
                               Map<String, xMsgSubscription> subscriptions,
                               ClaraComponent frontEnd,
                               xMsgTopic topic) {
            super(base, subscriptions, frontEnd, topic);
        }

        @Override
        protected xMsgCallBack wrap(final GenericCallback userCallback) {
            return msg -> {
                try {
                    String mimeType = msg.getMimeType();
                    if (mimeType.equals("text/string")) {
                        userCallback.callback(new String(msg.getData()));
                    } else {
                        throw new ClaraException("Unexpected mime-type: " + mimeType);
                    }
                } catch (ClaraException e) {
                    System.out.println("Error receiving data to " + msg.getTopic());
                    e.printStackTrace();
                }
            };
        }
    }


    public static class ServiceSubscriptionBuilder {
        private final ClaraBase base;
        private final Map<String, xMsgSubscription> subscriptions;
        private final Set<EngineDataType> dataTypes;
        private final ClaraComponent frontEnd;
        private final ClaraName component;

        ServiceSubscriptionBuilder(ClaraBase base,
                                   Map<String, xMsgSubscription> subscriptions,
                                   Set<EngineDataType> dataTypes,
                                   ClaraComponent frontEnd,
                                   ClaraName service) {
            this.base = base;
            this.subscriptions = subscriptions;
            this.dataTypes = dataTypes;
            this.frontEnd = frontEnd;
            this.component = service;
        }

        /**
         * A subscription to the specified status reports of the selected service.
         * <p>
         * Services will publish status reports after every execution that results
         * on error or warning.
         *
         * @param status the status to be listened
         */
        public ServiceSubscription status(EngineStatus status) {
            return new ServiceSubscription(base, subscriptions, dataTypes, frontEnd,
                                           getTopic(status.toString(), component));
        }

        /**
         * A subscription to the "done" reports of the selected service.
         * <p>
         * Services will publish "done" reports if they are configured to do so
         * with a given event count. The messages will not contain the full
         * output result of the service, but just a few stats about the execution.
         */
        public ServiceSubscription done() {
            return new ServiceSubscription(base, subscriptions, dataTypes, frontEnd,
                                           getTopic(ClaraConstants.DONE, component));
        }

        /**
         * A subscription to the data reports of the selected service.
         * <p>
         * Services will publish "data" reports if they are configured to do so
         * with a given event count. The messages will contain the full
         * output result of the service.
         */
        public ServiceSubscription data() {
            return new ServiceSubscription(base, subscriptions, dataTypes, frontEnd,
                                           getTopic(ClaraConstants.DATA, component));
        }

        private xMsgTopic getTopic(String prefix, ClaraName service) {
            return MessageUtil.buildTopic(prefix, service.canonicalName());
        }
    }


    public static class GlobalSubscriptionBuilder {
        private final ClaraBase base;
        private final Map<String, xMsgSubscription> subscriptions;
        private final ClaraComponent frontEnd;

        GlobalSubscriptionBuilder(ClaraBase base,
                               Map<String, xMsgSubscription> subscriptions,
                               ClaraComponent frontEnd) {
            this.base = base;
            this.subscriptions = subscriptions;
            this.frontEnd = frontEnd;
        }

        /**
         * A subscription to the periodic alive message reported by
         * the running DPEs.
         */
        public JsonReportSubscription aliveDpes() {
            return new JsonReportSubscription(base, subscriptions, frontEnd,
                                              MessageUtil.buildTopic(ClaraConstants.DPE_ALIVE));
        }
    }
}
