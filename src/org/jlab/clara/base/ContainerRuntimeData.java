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
 * The runtime data of a running container.
 */
public class ContainerRuntimeData implements ClaraReportData<ContainerName> {

    private final ContainerName name;
    private final LocalDateTime snapshotTime;
    private final long numRequests;

    private final Set<ServiceRuntimeData> services;

    ContainerRuntimeData(JSONObject json) {
        this.name = new ContainerName(json.getString("name"));
        this.snapshotTime = JsonUtils.getDate(json, "snapshot_time");
        this.numRequests = json.optLong("n_requests");

        this.services = JsonUtils.serviceStream(json)
                                 .map(ServiceRuntimeData::new)
                                 .collect(Collectors.toSet());
    }

    @Override
    public ContainerName name() {
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
     * Gets the sum of all requests received by all the services running in the
     * container.
     *
     * @return the total number of requests received by the services of the container
     */
    public long numRequests() {
        return numRequests;
    }

    /**
     * Gets the runtime report of all the services running on the container.
     *
     * @return the runtime data of the services
     */
    public Set<ServiceRuntimeData> services() {
        return Collections.unmodifiableSet(services);
    }
}
