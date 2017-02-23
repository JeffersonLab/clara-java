package org.jlab.clara.cli;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.jlab.clara.base.ContainerRuntimeData;
import org.jlab.clara.base.DpeRuntimeData;
import org.jlab.clara.base.ServiceName;
import org.jlab.clara.base.ServiceRuntimeData;

public class Benchmark {

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
