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

import org.jlab.clara.base.ServiceName;
import org.jlab.clara.base.ServiceRuntimeData;

class ServiceBenchmark {

    private final ServiceName name;
    private final long numRequest;
    private final long numFailures;
    private final long shmReads;
    private final long shmWrites;
    private final long bytesRecv;
    private final long bytesSent;
    private final long execTime;

    ServiceBenchmark(ServiceRuntimeData first, ServiceRuntimeData last) {
        this.name = first.name();
        this.numRequest = last.numRequests() - first.numRequests();
        this.numFailures = last.numFailures() - first.numFailures();
        this.shmReads = last.sharedMemoryReads() - first.sharedMemoryReads();
        this.shmWrites = last.sharedMemoryWrites() - first.sharedMemoryWrites();
        this.bytesRecv = last.bytesReceived() - first.bytesReceived();
        this.bytesSent = last.bytesSent() - first.bytesSent();
        this.execTime = last.executionTime() - first.executionTime();
    }

    public ServiceName name() {
        return name;
    }

    public long numRequests() {
        return numRequest;
    }

    public long numFailures() {
        return numFailures;
    }

    public long sharedMemoryReads() {
        return shmReads;
    }

    public long sharedMemoryWrites() {
        return shmWrites;
    }

    public long bytesReceived() {
        return bytesRecv;
    }

    public long bytesSent() {
        return bytesSent;
    }

    public long executionTime() {
        return execTime;
    }
}
