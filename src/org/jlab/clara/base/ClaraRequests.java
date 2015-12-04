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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.jlab.clara.base.error.ClaraException;
import org.jlab.coda.xmsg.core.xMsgMessage;
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
}
