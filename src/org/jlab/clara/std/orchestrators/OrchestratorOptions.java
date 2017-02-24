package org.jlab.clara.std.orchestrators;

final class OrchestratorOptions {

    static final int DEFAULT_POOLSIZE = 32;
    static final int MAX_NODES = 512;
    static final int MAX_THREADS = 64;

    final boolean useFrontEnd;
    final boolean stageFiles;

    final int poolSize;
    final int maxNodes;
    final int maxThreads;

    final int skipEvents;
    final int maxEvents;
    final int reportFreq;


    static Builder builder() {
        return new Builder();
    }


    static final class Builder {

        private boolean useFrontEnd = false;
        private boolean stageFiles = false;

        private int poolSize = DEFAULT_POOLSIZE;
        private int maxNodes = MAX_NODES;
        private int maxThreads = MAX_THREADS;

        private int skipEvents = 0;
        private int maxEvents = 0;
        private int reportFreq = 0;

        Builder useFrontEnd() {
            this.useFrontEnd = true;
            return this;
        }

        Builder stageFiles() {
            this.stageFiles = true;
            return this;
        }

        Builder withPoolSize(int poolSize) {
            if (poolSize <= 0) {
                throw new IllegalArgumentException("Invalid pool size: " + poolSize);
            }
            this.poolSize = poolSize;
            return this;
        }

        Builder withMaxNodes(int maxNodes) {
            if (maxNodes <= 0) {
                throw new IllegalArgumentException("Invalid max number of nodes: " + maxNodes);
            }
            this.maxNodes = maxNodes;
            return this;
        }

        Builder withMaxThreads(int maxThreads) {
            if (maxThreads <= 0) {
                throw new IllegalArgumentException("Invalid max number of threads: " + maxThreads);
            }
            this.maxThreads = maxThreads;
            return this;
        }

        Builder withSkipEvents(int skipEvents) {
            if (skipEvents < 0) {
                throw new IllegalArgumentException("Invalid skip events value: " + skipEvents);
            }
            this.skipEvents = skipEvents;
            return this;
        }

        Builder withMaxEvents(int maxEvents) {
            if (maxEvents < 0) {
                throw new IllegalArgumentException("Invalid max events value: " + maxEvents);
            }
            this.maxEvents = maxEvents;
            return this;
        }

        Builder withReportFrequency(int reportFreq) {
            if (reportFreq <= 0) {
                throw new IllegalArgumentException("Invalid report frequency: " + reportFreq);
            }
            this.reportFreq = reportFreq;
            return this;
        }

        OrchestratorOptions build() {
            return new OrchestratorOptions(this);
        }
    }


    private OrchestratorOptions(Builder builder) {
        this.useFrontEnd = builder.useFrontEnd;
        this.stageFiles = builder.stageFiles;
        this.poolSize = builder.poolSize;
        this.maxNodes = builder.maxNodes;
        this.maxThreads = builder.maxThreads;
        this.skipEvents = builder.skipEvents;
        this.maxEvents = builder.maxEvents;
        this.reportFreq = builder.reportFreq;
    }
}
