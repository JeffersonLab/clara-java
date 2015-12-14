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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.util.CConstants;
import org.jlab.coda.xmsg.core.xMsgCallBack;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgSubscription;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.excp.xMsgException;

public class ClaraSubscriptions {

    /**
     * Starts and stops a Clara subscription.
     *
     * @param <D> The specific subclass
     * @param <T> The type returned when a result is expected
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
            String key = frontEnd.getDpeHost() + CConstants.MAPKEY_SEP + topic;
            if (subscriptions.containsKey(key)) {
                throw new IllegalStateException("duplicated subscription to: " + frontEnd);
            }
            try {
                xMsgCallBack wrapperCallback = wrap(callback);
                xMsgSubscription handler = base.listen(frontEnd, topic, wrapperCallback);
                subscriptions.put(key, handler);
            } catch (xMsgException e) {
                throw new ClaraException("Could not start subscription", e);
            }
        }

        public void stop() throws ClaraException {
            String key = frontEnd.getDpeHost() + CConstants.MAPKEY_SEP + topic;
            xMsgSubscription handler = subscriptions.remove(key);
            if (handler != null) {
                try {
                    base.unsubscribe(handler);
                } catch (xMsgException e) {
                    throw new ClaraException("Could not stop subscription", e);
                }
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
                            ClaraComponent frontEnd,
                            xMsgTopic topic) {
            super(base, subscriptions, frontEnd, topic);
        }

        public ServiceSubscription withDataTypes(Set<EngineDataType> dataTypes) {
            this.dataTypes = dataTypes;
            return this;
        }

        public ServiceSubscription withDataTypes(EngineDataType... dataTypes) {
            Set<EngineDataType> newTypes = new HashSet<>();
            for (EngineDataType dt : dataTypes) {
                newTypes.add(dt);
            }
            this.dataTypes = newTypes;
            return this;
        }

        @Override
        protected xMsgCallBack wrap(final EngineCallback userCallback) {
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
    }
}
