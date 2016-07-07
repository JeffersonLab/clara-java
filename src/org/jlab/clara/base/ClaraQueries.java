/*
 *   Copyright (c) 2017.  Jefferson Lab (JLab). All rights reserved. Permission
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

import org.jlab.clara.base.core.ClaraBase;
import org.jlab.clara.base.core.ClaraComponent;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.coda.xmsg.data.xMsgRegRecord;
import org.jlab.coda.xmsg.excp.xMsgException;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

/**
 * Queries to the CLARA registration/runtime database.
 */
public final class ClaraQueries {

    private ClaraQueries() { }


    private static class WrappedException extends RuntimeException {

        final Throwable cause;

        WrappedException(Throwable cause) {
            this.cause = cause;
        }
    }


    /**
     * A query to CLARA registration.
     *
     * @param <D> The specific subclass
     * @param <T> The type returned by the query
     */
    abstract static class BaseQuery<D extends BaseQuery<D, T>, T> {

        protected final ClaraBase base;
        protected final ClaraComponent frontEnd;
        protected final ClaraFilter filter;

        protected BaseQuery(ClaraBase base, ClaraComponent frontEnd, ClaraFilter filter) {
            this.base = base;
            this.frontEnd = frontEnd;
            this.filter = filter;
        }

        /**
         * Sends the query and waits for a response.
         *
         * @param wait the amount of time units to wait for a response
         * @param unit the unit of time
         * @throws ClaraException if the query could not be sent or received
         * @throws TimeoutException if the response is not received
         * @return the result of the query
         */
        public T syncRun(long wait, TimeUnit unit) throws ClaraException, TimeoutException {
            try {
                if (wait <= 0) {
                    throw new IllegalArgumentException("Invalid timeout: " + wait);
                }
                long timeout = (int) unit.toMillis(wait);
                long start = System.currentTimeMillis();
                Stream<xMsgRegRecord> regData = queryRegistrar(timeout);
                long end = System.currentTimeMillis();
                return collect(regData, timeout - (end - start));
            } catch (xMsgException e) {
                throw new ClaraException("Cannot send query", e);
            } catch (WrappedException e) {
                throw new ClaraException("Canend query ", e.cause);
            }
        }

        private Stream<xMsgRegRecord> queryRegistrar(long timeout) throws xMsgException {
            return base.discover(filter.regQuery(), ClaraBase.getRegAddress(frontEnd), timeout)
                       .stream()
                       .filter(filter.regFilter());
        }

        protected abstract T collect(Stream<xMsgRegRecord> regData, long timeout);

        @SuppressWarnings("unchecked")
        protected D self() {
            return (D) this;
        }
    }


    /**
     * Builds a request to query the CLARA registration and runtime database.
     */
    public static class ClaraQueryBuilder {

        private final ClaraBase base;
        private final ClaraComponent frontEnd;

        ClaraQueryBuilder(ClaraBase base, ClaraComponent frontEnd) {
            this.base = base;
            this.frontEnd = frontEnd;
        }
    }
}
