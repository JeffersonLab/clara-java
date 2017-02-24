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
