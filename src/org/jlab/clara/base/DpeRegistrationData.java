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

import org.jlab.clara.util.report.JsonUtils;
import org.json.JSONObject;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The registration data of a running DPE.
 */
public class DpeRegistrationData implements ClaraReportData<DpeName> {

    private final DpeName name;
    private final LocalDateTime startTime;
    private final String claraHome;
    private final String session;

    private final int numCores;
    private final long memorySize;

    private final Set<ContainerRegistrationData> containers;

    DpeRegistrationData(JSONObject json) {
        name = new DpeName(json.getString("name"));
        this.startTime = JsonUtils.getDate(json, "start_time");
        this.claraHome = json.optString("clara_home");
        this.session = json.optString("session");
        this.numCores = json.optInt("n_cores");
        this.memorySize = json.optLong("memory_size");

        this.containers = JsonUtils.containerStream(json)
                                   .map(ContainerRegistrationData::new)
                                   .collect(Collectors.toSet());
    }

    @Override
    public DpeName name() {
        return name;
    }

    /**
     * Gets the local time when the DPE was started.
     *
     * @return the start time of the DPE
     */
    public LocalDateTime startTime() {
        return startTime;
    }

    /**
     * Gets an identification of who started the DPE.
     *
     * @return the author that started the DPE
     */
    public String startedBy() {
        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Gets the local value of the <code>$CLARA_HOME</code> environment variable
     * used by the DPE.
     *
     * @return the local path of the CLARA installation for the DPE
     */
    public String claraHome() {
        return claraHome;
    }

    /**
     * Gets the session used by the DPE to publish reports.
     *
     * @return the session ID of the DPE
     */
    public String session() {
        return session;
    }

    /**
     * Gets the maximum number of cores assigned to the DPE.
     *
     * @return the number of cores the can be used by the DPE.
     */
    public int numCores() {
        return numCores;
    }

    /**
     * Gets the maximum amount of memory available to the DPE.
     *
     * @return the maximum amount of memory that the DPE will attempt to use,
     *         measured in bytes
     */
    public long memorySize() {
        return memorySize;
    }

    /**
     * Gets all the containers running on the DPE.
     *
     * @return the registration data of the containers
     */
    public Set<ContainerRegistrationData> containers() {
        return Collections.unmodifiableSet(containers);
    }
}
