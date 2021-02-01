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

import org.jlab.clara.base.DpeName;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.ClaraSerializer;
import org.jlab.clara.engine.EngineDataType;
import org.json.JSONObject;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class OrchestratorSetup {

    final public ApplicationInfo application;
    final public DpeName frontEnd;
    final public String session;
    final public JSONObject configuration;
    final public OrchestratorConfigMode configMode;
    final public Set<EngineDataType> dataTypes;


    public static class Builder {

        private final ApplicationInfo application;

        private DpeName frontEnd = OrchestratorConfigParser.localDpeName();
        private String session = "";

        private JSONObject config = new JSONObject();
        private OrchestratorConfigMode configMode = OrchestratorConfigMode.DATASET;
        private Set<String> dataTypes = new HashSet<>();

        public Builder(Map<String, ServiceInfo> ioServices,
                List<ServiceInfo> dataServices,
                List<ServiceInfo> monServices) {
            this.application = new ApplicationInfo(ioServices, dataServices, monServices);
        }

        public Builder withFrontEnd(DpeName frontEnd) {
            Objects.requireNonNull(frontEnd, "frontEnd parameter is null");
            this.frontEnd = frontEnd;
            return this;
        }

        public Builder withSession(String session) {
            Objects.requireNonNull(session, "session parameter is null");
            this.session = session;
            return this;
        }

        public Builder withConfig(JSONObject config) {
            this.config = config;
            return this;
        }

        public Builder withConfigMode(OrchestratorConfigMode mode) {
            this.configMode = mode;
            return this;
        }

        public Builder withDataTypes(Set<String> dataTypes) {
            this.dataTypes = dataTypes;
            return this;
        }

        public OrchestratorSetup build() {
            return new OrchestratorSetup(this);
        }
    }


    protected OrchestratorSetup(Builder builder) {
        this.frontEnd = builder.frontEnd;
        this.session = builder.session;
        this.application = builder.application;
        this.configuration = builder.config;
        this.configMode = builder.configMode;
        this.dataTypes = builder.dataTypes.stream()
                    .map(OrchestratorSetup::dummyDataType)
                    .collect(Collectors.toSet());
    }


    private static EngineDataType dummyDataType(String mimeType) {
        return new EngineDataType(mimeType, new ClaraSerializer() {

            @Override
            public ByteBuffer write(Object data) throws ClaraException {
                throw new IllegalStateException("orchestrator should not publish: " + mimeType);
            }

            @Override
            public Object read(ByteBuffer buffer) throws ClaraException {
                // ignore serialization
                return buffer;
            }
        });
    }
}
