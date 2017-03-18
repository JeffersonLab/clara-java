package org.jlab.clara.std.cli;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.Map;

import org.jlab.clara.base.RuntimeDataFactory;
import org.jlab.clara.base.ServiceName;
import org.junit.Test;

public class BenchmarkTest {

    private static final ServiceName S1 = new ServiceName("10.1.1.10_java:trevor:Engine1");
    private static final ServiceName S2 = new ServiceName("10.1.1.10_java:trevor:Engine2");
    private static final ServiceName S3 = new ServiceName("10.1.1.10_java:trevor:Engine3");

    @Test
    public void getCPUAverageReturnsNaNWithoutRuntimeData() throws Exception {
        Benchmark b = new Benchmark();

        assertThat(b.getCPUAverage(), is(Double.NaN));
    }

    @Test
    public void getCPUAverageReturnsSameValueForOneRuntimeDataValue() throws Exception {
        Benchmark b = new Benchmark();
        b.addSnapshot(RuntimeDataFactory.parseRuntime("/resources/runtime-snapshots-1.json"));

        assertThat(b.getCPUAverage(), is(closeTo(45.2, 0.01)));
    }

    @Test
    public void getCPUAverageReturnsForManyRuntimeDataValues() throws Exception {
        Benchmark b = new Benchmark();
        b.addSnapshot(RuntimeDataFactory.parseRuntime("/resources/runtime-snapshots-1.json"));
        b.addSnapshot(RuntimeDataFactory.parseRuntime("/resources/runtime-snapshots-2.json"));
        b.addSnapshot(RuntimeDataFactory.parseRuntime("/resources/runtime-snapshots-3.json"));

        double expected = (45.2 + 47.2 + 43.8) / 3;

        assertThat(b.getCPUAverage(), is(closeTo(expected, 0.01)));
    }

    @Test
    public void getMemoryAverageReturnsZeroWithoutRuntimeData() throws Exception {
        Benchmark b = new Benchmark();

        assertThat(b.getMemoryAverage(), is(0L));
    }

    @Test
    public void getMemoryAverageReturnsSameValueForOneRuntimeDataValue() throws Exception {
        Benchmark b = new Benchmark();
        b.addSnapshot(RuntimeDataFactory.parseRuntime("/resources/runtime-snapshots-1.json"));

        assertThat(b.getMemoryAverage(), is(631222786L));
    }

    @Test
    public void getMemoryAverageReturnsForManyRuntimeDataValues() throws Exception {
        Benchmark b = new Benchmark();
        b.addSnapshot(RuntimeDataFactory.parseRuntime("/resources/runtime-snapshots-1.json"));
        b.addSnapshot(RuntimeDataFactory.parseRuntime("/resources/runtime-snapshots-2.json"));
        b.addSnapshot(RuntimeDataFactory.parseRuntime("/resources/runtime-snapshots-3.json"));

        long expected = (631222786 + 631222132 + 631226872) / 3;


        assertThat(b.getMemoryAverage(), is(expected));
    }

    @Test(expected = IllegalStateException.class)
    public void getServiceReturnsNothingWithoutData() throws Exception {
        Benchmark b = new Benchmark();

        b.getServiceBenchmark();
    }

    @Test(expected = IllegalStateException.class)
    public void getServiceReturnsSameValueForOneRuntimeDataValue() throws Exception {
        Benchmark b = new Benchmark();
        b.addSnapshot(RuntimeDataFactory.parseRuntime("/resources/runtime-snapshots-1.json"));

        b.getServiceBenchmark();
    }

    @Test
    public void getServiceReturnsSameValueForManyRuntimeDataValues() throws Exception {
        Benchmark b = new Benchmark();
        b.addSnapshot(RuntimeDataFactory.parseRuntime("/resources/runtime-snapshots-1.json"));
        b.addSnapshot(RuntimeDataFactory.parseRuntime("/resources/runtime-snapshots-2.json"));
        b.addSnapshot(RuntimeDataFactory.parseRuntime("/resources/runtime-snapshots-3.json"));

        Map<ServiceName, ServiceBenchmark> stats = b.getServiceBenchmark();

        assertThat(stats.keySet(), containsInAnyOrder(S1, S2, S3));

        assertThat(stats.get(S1).numRequests(), is(1300L - 1000L));
        assertThat(stats.get(S2).numRequests(), is(800L - 500L));
        assertThat(stats.get(S3).numRequests(), is(2300L - 2000L));
    }

}
