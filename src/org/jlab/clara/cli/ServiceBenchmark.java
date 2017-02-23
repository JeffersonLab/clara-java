package org.jlab.clara.cli;

import org.jlab.clara.base.ServiceName;
import org.jlab.clara.base.ServiceRuntimeData;

public class ServiceBenchmark {

    private final ServiceName name;
    private final long numRequest;
    private final long numFailures;
    private final long shmReads;
    private final long shmWrites;
    private final long bytesRecv;
    private final long bytesSent;
    private final long execTime;

    public ServiceBenchmark(ServiceRuntimeData first, ServiceRuntimeData last) {
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
