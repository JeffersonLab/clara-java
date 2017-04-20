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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class OrchestratorSetup {

    final DpeName frontEnd;
    final String session;

    final ApplicationInfo application;
    final Set<EngineDataType> dataTypes;
    final JSONObject globalConfig;

    OrchestratorSetup(DpeName frontEnd,
                      Map<String, ServiceInfo> ioServices,
                      List<ServiceInfo> recChain,
                      Set<String> dataTypes,
                      JSONObject globalConfig,
                      String session) {
        this.frontEnd = frontEnd;
        this.session = session;
        this.application = new ApplicationInfo(ioServices, recChain);
        this.dataTypes = dataTypes.stream().map(this::dummyDataType).collect(Collectors.toSet());
        this.globalConfig = globalConfig;
    }

    private EngineDataType dummyDataType(String mimeType) {
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
