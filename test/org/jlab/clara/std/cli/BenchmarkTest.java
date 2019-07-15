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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;

import org.jlab.clara.base.RuntimeDataFactory;
import org.jlab.clara.base.ServiceName;
import org.junit.jupiter.api.Test;

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
        b.addSnapshot(RuntimeDataFactory.parseRuntime("/runtime-snapshots-1.json"));

        assertThat(b.getCPUAverage(), is(closeTo(45.2, 0.01)));
    }

    @Test
    public void getCPUAverageReturnsForManyRuntimeDataValues() throws Exception {
        Benchmark b = new Benchmark();
        b.addSnapshot(RuntimeDataFactory.parseRuntime("/runtime-snapshots-1.json"));
        b.addSnapshot(RuntimeDataFactory.parseRuntime("/runtime-snapshots-2.json"));
        b.addSnapshot(RuntimeDataFactory.parseRuntime("/runtime-snapshots-3.json"));

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
        b.addSnapshot(RuntimeDataFactory.parseRuntime("/runtime-snapshots-1.json"));

        assertThat(b.getMemoryAverage(), is(631222786L));
    }

    @Test
    public void getMemoryAverageReturnsForManyRuntimeDataValues() throws Exception {
        Benchmark b = new Benchmark();
        b.addSnapshot(RuntimeDataFactory.parseRuntime("/runtime-snapshots-1.json"));
        b.addSnapshot(RuntimeDataFactory.parseRuntime("/runtime-snapshots-2.json"));
        b.addSnapshot(RuntimeDataFactory.parseRuntime("/runtime-snapshots-3.json"));

        long expected = (631222786 + 631222132 + 631226872) / 3;


        assertThat(b.getMemoryAverage(), is(expected));
    }

    @Test
    public void getServiceReturnsNothingWithoutData() throws Exception {
        Benchmark b = new Benchmark();

        assertThrows(IllegalStateException.class, () -> b.getServiceBenchmark());
    }

    @Test
    public void getServiceReturnsSameValueForOneRuntimeDataValue() throws Exception {
        Benchmark b = new Benchmark();
        b.addSnapshot(RuntimeDataFactory.parseRuntime("/runtime-snapshots-1.json"));

        assertThrows(IllegalStateException.class, () -> b.getServiceBenchmark());
    }

    @Test
    public void getServiceReturnsSameValueForManyRuntimeDataValues() throws Exception {
        Benchmark b = new Benchmark();
        b.addSnapshot(RuntimeDataFactory.parseRuntime("/runtime-snapshots-1.json"));
        b.addSnapshot(RuntimeDataFactory.parseRuntime("/runtime-snapshots-2.json"));
        b.addSnapshot(RuntimeDataFactory.parseRuntime("/runtime-snapshots-3.json"));

        Map<ServiceName, ServiceBenchmark> stats = b.getServiceBenchmark();

        assertThat(stats.keySet(), containsInAnyOrder(S1, S2, S3));

        assertThat(stats.get(S1).numRequests(), is(1300L - 1000L));
        assertThat(stats.get(S2).numRequests(), is(800L - 500L));
        assertThat(stats.get(S3).numRequests(), is(2300L - 2000L));
    }

}
