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

package org.jlab.clara.std.services;

import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineStatus;

/**
 * A collection of utilities to write services.
 */
public final class ServiceUtils {

    private ServiceUtils() { }

    /**
     * Sets the given engine data with an error status.
     *
     * @param output the engine data that will be returned by the service
     * @param msg a description for the error
     */
    public static void setError(EngineData output, String msg) {
        output.setDescription(msg);
        output.setStatus(EngineStatus.ERROR, 1);
    }

    /**
     * Sets the given engine data with an error status.
     *
     * @param output the engine data that will be returned by the service
     * @param msg a description for the error
     * @param severity the severity of the error, as a positive integer
     */
    public static void setError(EngineData output, String msg, int severity) {
        output.setDescription(msg);
        output.setStatus(EngineStatus.ERROR, severity);
    }

    /**
     * Sets the given engine data with an error status.
     *
     * @param output the engine data that will be returned by the service
     * @param format a format string with a description for the error
     * @param args arguments referenced by the format specifiers in the format string
     */
    public static void setError(EngineData output, String format, Object... args) {
        ServiceUtils.setError(output, String.format(format, args));
    }
}
