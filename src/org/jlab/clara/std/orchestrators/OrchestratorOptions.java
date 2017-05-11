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

final class OrchestratorOptions {

    static final int DEFAULT_POOLSIZE = 32;
    static final int MAX_NODES = 512;
    static final int MAX_THREADS = 64;

    final OrchestratorMode orchMode;
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

        private OrchestratorMode orchMode = OrchestratorMode.LOCAL;
        private boolean useFrontEnd = false;
        private boolean stageFiles = false;

        private int poolSize = DEFAULT_POOLSIZE;
        private int maxNodes = MAX_NODES;
        private int maxThreads = MAX_THREADS;

        private int skipEvents = 0;
        private int maxEvents = 0;
        private int reportFreq = 0;

        Builder cloudMode() {
            this.orchMode = OrchestratorMode.CLOUD;
            return this;
        }

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
        this.orchMode = builder.orchMode;
        this.useFrontEnd = builder.orchMode == OrchestratorMode.LOCAL || builder.useFrontEnd;
        this.stageFiles = builder.stageFiles;
        this.poolSize = builder.poolSize;
        this.maxNodes = builder.orchMode == OrchestratorMode.LOCAL ? 1 : builder.maxNodes;
        this.maxThreads = builder.maxThreads;
        this.skipEvents = builder.skipEvents;
        this.maxEvents = builder.maxEvents;
        this.reportFreq = builder.reportFreq;
    }
}
