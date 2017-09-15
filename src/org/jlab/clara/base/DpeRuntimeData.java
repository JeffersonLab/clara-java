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

import org.jlab.clara.util.report.JsonUtils;
import org.json.JSONObject;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The runtime data of a running DPE.
 */
public class DpeRuntimeData implements ClaraReportData<DpeName> {

    private final DpeName name;
    private final LocalDateTime snapshotTime;
    private final double cpuUsage;
    private final long memUsage;
    private final double sysLoad;

    private String session;
    private String description;

    private final Set<ContainerRuntimeData> containers;

    DpeRuntimeData(JSONObject json) {
        this.name = new DpeName(json.getString("name"));
        this.snapshotTime = JsonUtils.getDate(json, "snapshot_time");
        this.cpuUsage = json.optDouble("cpu_usage", Double.NaN);
        this.memUsage = json.optLong("memory_usage");
        this.sysLoad = json.optDouble("load", -1);

        this.containers = JsonUtils.containerStream(json)
                                   .map(ContainerRuntimeData::new)
                                   .collect(Collectors.toSet());
        this.session = json.getString("session");
        this.description = json.getString("description");
    }

    @Override
    public DpeName name() {
        return name;
    }

    /**
     * Gets the local time when the runtime data was collected.
     *
     * @return the snapshot time of the report
     */
    public LocalDateTime snapshotTime() {
        return snapshotTime;
    }

    /**
     * Gets the reported CPU usage of the DPE.
     * <p>
     * A value of 1.0 is equivalent to 100% CPU usage.
     *
     * @return the CPU usage or NaN if it could not be obtained
     */
    public double cpuUsage() {
        return cpuUsage;
    }

    /**
     * Gets the amount of memory in use by the DPE.
     *
     * @return the amount of memory that the DPE is using, measured in bytes
     */
    public long memoryUsage() {
        return memUsage;
    }

    /**
     * Gets the system load average of the node where the DPE is running.
     *
     * @return the system load average, or a negative value if not available
     */
    public double systemLoad() {
        return sysLoad;
    }

    /**
     * Gets the runtime report of all the containers running on the DPE.
     *
     * @return the runtime data of the containers
     */
    public Set<ContainerRuntimeData> containers() {
        return Collections.unmodifiableSet(containers);
    }

    /**
     * Gets the data processing session.
     * Note: we can run multiple sessions within the same registered DPE
     * @return runtime session
     */
    public String session() {
        return session;
    }

    /**
     * Gets the data processing description.
     * @return runtime session
     */
    public String description() {
        return description;
    }
}
