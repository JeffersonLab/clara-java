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

import org.jlab.clara.base.core.ClaraConstants;

/**
 * The address of a CLARA data-ring.
 */
public class DataRingAddress extends ClaraAddress {

    /**
     * Identify a CLARA data-ring.
     *
     * @param host the host address of the data ring.
     */
    public DataRingAddress(String host) {
        super(host, ClaraConstants.MONITOR_PORT);
    }

    /**
     * Identify a CLARA data-ring.
     *
     * @param host the host address of the data ring.
     * @param port the port used by the data ring.
     */
    public DataRingAddress(String host, int port) {
        super(host, port);
    }

    /**
     * Identify a CLARA data-ring.
     *
     * @param dpe the DPE acting as a data ring.
     */
    public DataRingAddress(DpeName dpe) {
        super(dpe.address().host(), dpe.address().pubPort());
    }
}
