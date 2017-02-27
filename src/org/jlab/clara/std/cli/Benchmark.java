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

package org.jlab.clara.std.cli;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.jlab.clara.base.ContainerRuntimeData;
import org.jlab.clara.base.DpeRuntimeData;
import org.jlab.clara.base.ServiceName;
import org.jlab.clara.base.ServiceRuntimeData;

class Benchmark {

    private double cpuUsageSum = 0;
    private long memoryUsageSum = 0;
    private int dataPoints = 0;
    private DpeRuntimeData firstSnapshot = null;
    private DpeRuntimeData lastSnapshot = null;

    public void addSnapshot(DpeRuntimeData dpeData) {
        cpuUsageSum += dpeData.cpuUsage();
        memoryUsageSum += dpeData.memoryUsage();
        dataPoints++;

        if (firstSnapshot == null) {
            firstSnapshot = dpeData;
        } else {
            lastSnapshot = dpeData;
        }
    }

    public double getCPUAverage() {
        if (dataPoints == 0) {
            return Double.NaN;
        }
        return cpuUsageSum / dataPoints;
    }

    public long getMemoryAverage() {
        if (dataPoints == 0) {
            return 0;
        }
        return memoryUsageSum / dataPoints;
    }

    public Map<ServiceName, ServiceBenchmark> getServiceBenchmark() {
        if (firstSnapshot == null || lastSnapshot == null) {
            throw new IllegalStateException("missing snapshots");
        }
        Map<ServiceName, ServiceRuntimeData> servicesFirstSnapshot = parseServices(firstSnapshot);
        Map<ServiceName, ServiceRuntimeData> servicesLastSnapshot = parseServices(lastSnapshot);
        Map<ServiceName, ServiceBenchmark> servicesBenchmark = new HashMap<>();
        Set<ServiceName> serviceNames = servicesFirstSnapshot.keySet();
        if (!serviceNames.equals(servicesLastSnapshot.keySet())) {
            throw new IllegalStateException("services do not match");
        }
        for (ServiceName name : serviceNames) {
            ServiceRuntimeData first = servicesFirstSnapshot.get(name);
            ServiceRuntimeData last = servicesLastSnapshot.get(name);
            ServiceBenchmark benchmark = new ServiceBenchmark(first, last);
            servicesBenchmark.put(name, benchmark);
        }
        return servicesBenchmark;
    }


    private static Map<ServiceName, ServiceRuntimeData> parseServices(DpeRuntimeData dpeData) {
        return dpeData.containers().stream()
                .map(ContainerRuntimeData::services)
                .flatMap(Set::stream)
                .collect(Collectors.toMap(ServiceRuntimeData::name, Function.identity()));
    }
}
