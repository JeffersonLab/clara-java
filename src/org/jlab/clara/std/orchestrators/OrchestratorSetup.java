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

class OrchestratorSetup {

    final ApplicationInfo application;
    final DpeName frontEnd;
    final String session;
    final JSONObject configuration;
    final Set<EngineDataType> dataTypes;


    static class Builder {

        private final ApplicationInfo application;

        private DpeName frontEnd = OrchestratorConfigParser.localDpeName();
        private String session = "";

        private JSONObject config = new JSONObject();
        private Set<String> dataTypes = new HashSet<>();

        Builder(Map<String, ServiceInfo> ioServices, List<ServiceInfo> recChain) {
            this.application = new ApplicationInfo(ioServices, recChain);
        }

        Builder withFrontEnd(DpeName frontEnd) {
            Objects.requireNonNull(frontEnd, "frontEnd parameter is null");
            this.frontEnd = frontEnd;
            return this;
        }

        Builder withSession(String session) {
            Objects.requireNonNull(session, "session parameter is null");
            this.session = session;
            return this;
        }

        Builder withConfig(JSONObject config) {
            this.config = config;
            return this;
        }

        Builder withDataTypes(Set<String> dataTypes) {
            this.dataTypes = dataTypes;
            return this;
        }

        OrchestratorSetup build() {
            return null;
        }
    }


    protected OrchestratorSetup(Builder builder) {
        this.frontEnd = builder.frontEnd;
        this.session = builder.session;
        this.application = builder.application;
        this.configuration = builder.config;
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
