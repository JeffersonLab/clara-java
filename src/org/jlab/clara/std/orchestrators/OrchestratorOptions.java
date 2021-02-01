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

final public class OrchestratorOptions {

    static final public int DEFAULT_POOLSIZE = 32;
    static final public int DEFAULT_REPORT_FREQ = 500;
    static final public int MAX_NODES = 512;
    static final public int MAX_THREADS = 64;

    final public OrchestratorMode orchMode;
    final public boolean useFrontEnd;
    final public boolean stageFiles;

    final public int poolSize;
    final public int maxNodes;
    final public int maxThreads;

    final public int skipEvents;
    final public int maxEvents;
    final public int reportFreq;


    public static Builder builder() {
        return new Builder();
    }


    static final public class Builder {

        private OrchestratorMode orchMode = OrchestratorMode.LOCAL;
        private boolean useFrontEnd = false;
        private boolean stageFiles = false;

        private int poolSize = DEFAULT_POOLSIZE;
        private int maxNodes = MAX_NODES;
        private int maxThreads = MAX_THREADS;

        private int skipEvents = 0;
        private int maxEvents = 0;
        private int reportFreq = DEFAULT_REPORT_FREQ;

        public Builder() {
            if (System.getenv("CLARA_USE_DOCKER") != null) {
                orchMode = OrchestratorMode.DOCKER;
            }
        }

        public Builder cloudMode() {
            this.orchMode = OrchestratorMode.CLOUD;
            return this;
        }

        public Builder useFrontEnd() {
            this.useFrontEnd = true;
            return this;
        }

        public Builder stageFiles() {
            this.stageFiles = true;
            return this;
        }

        public Builder withPoolSize(int poolSize) {
            if (poolSize <= 0) {
                throw new IllegalArgumentException("Invalid pool size: " + poolSize);
            }
            this.poolSize = poolSize;
            return this;
        }

        public Builder withMaxNodes(int maxNodes) {
            if (maxNodes <= 0) {
                throw new IllegalArgumentException("Invalid max number of nodes: " + maxNodes);
            }
            this.maxNodes = maxNodes;
            return this;
        }

        public Builder withMaxThreads(int maxThreads) {
            if (maxThreads <= 0) {
                throw new IllegalArgumentException("Invalid max number of threads: " + maxThreads);
            }
            this.maxThreads = maxThreads;
            return this;
        }

        public Builder withSkipEvents(int skipEvents) {
            if (skipEvents < 0) {
                throw new IllegalArgumentException("Invalid skip events value: " + skipEvents);
            }
            this.skipEvents = skipEvents;
            return this;
        }

        public Builder withMaxEvents(int maxEvents) {
            if (maxEvents < 0) {
                throw new IllegalArgumentException("Invalid max events value: " + maxEvents);
            }
            this.maxEvents = maxEvents;
            return this;
        }

        public Builder withReportFrequency(int reportFreq) {
            if (reportFreq <= 0) {
                throw new IllegalArgumentException("Invalid report frequency: " + reportFreq);
            }
            this.reportFreq = reportFreq;
            return this;
        }

        public OrchestratorOptions build() {
            return new OrchestratorOptions(this);
        }
    }


    private OrchestratorOptions(Builder builder) {
        this.orchMode = builder.orchMode;
        this.useFrontEnd = builder.orchMode != OrchestratorMode.CLOUD || builder.useFrontEnd;
        this.stageFiles = builder.stageFiles;
        this.poolSize = builder.poolSize;
        this.maxNodes = builder.orchMode != OrchestratorMode.CLOUD ? 1 : builder.maxNodes;
        this.maxThreads = builder.maxThreads;
        this.skipEvents = builder.skipEvents;
        this.maxEvents = builder.maxEvents;
        this.reportFreq = builder.reportFreq;
    }
}
