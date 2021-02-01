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

package org.jlab.clara.std.orchestrators;

import org.jlab.clara.base.ServiceName;
import org.jlab.clara.base.ServiceRuntimeData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Benchmark {

    final public Map<ServiceInfo, Runtime> runtimeStats = new HashMap<>();

    public Benchmark(ApplicationInfo application) {
        List<ServiceInfo> services = allServices(application);
        services.forEach(s -> {
            runtimeStats.put(s, new Runtime());
        });
    }

    private List<ServiceInfo> allServices(ApplicationInfo application) {
        List<ServiceInfo> services = new ArrayList<>();
        services.add(application.getReaderService());
        services.addAll(application.getDataProcessingServices());
        services.add(application.getWriterService());
        return services;
    }

    public void initialize(Set<ServiceRuntimeData> data) {
        data.forEach(s -> {
            Runtime r = runtimeStats.get(key(s.name()));
            if (r != null) {
                r.initialTime = s.executionTime();
            }
        });
    }

    public void update(Set<ServiceRuntimeData> data) {
        data.forEach(s -> {
            Runtime r = runtimeStats.get(key(s.name()));
            if (r != null) {
                r.totalTime = s.executionTime();
            }
        });
    }

    public long time(ServiceInfo service) {
        Runtime r = runtimeStats.get(service);
        if (r != null) {
            return r.totalTime - r.initialTime;
        }
        throw new OrchestratorException("Invalid runtime report: missing " + service.name);
    }

    private static ServiceInfo key(ServiceName service) {
        return new ServiceInfo("", service.container().name(), service.name(), service.language());
    }

    private static class Runtime {
        long initialTime = 0;
        long totalTime = 0;
    }
}
