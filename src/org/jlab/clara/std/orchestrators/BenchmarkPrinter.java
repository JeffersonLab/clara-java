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

public class BenchmarkPrinter {

    private final Benchmark benchmark;

    private long totalTime = 0;
    private long totalRequests = 0;

    public BenchmarkPrinter(Benchmark benchmark, long totalRequests) {
        this.benchmark = benchmark;
        this.totalRequests = totalRequests;
    }

    public void printBenchmark(ApplicationInfo application) {
        Logging.info("Benchmark results:");
        printService(application.getReaderService(), "READER");
        for (ServiceInfo service : application.getDataProcessingServices()) {
            printService(service, service.name);
        }
        printService(application.getWriterService(), "WRITER");
        printTotal();
    }

    private void printService(ServiceInfo service, String label) {
        long time = benchmark.time(service);
        totalTime += time;
        print(label, time, totalRequests);
    }

    private void printTotal() {
        print("TOTAL", totalTime, totalRequests);
    }

    private void print(String name, long time, long requests) {
        double timePerEvent = (time / (double) requests) / 1e3;
        Logging.info("  %-12.12s   %6d events    total time = %8.2f s    "
                + "average event time = %7.2f ms",
                name, requests, time / 1e6,
                timePerEvent);
    }
}
