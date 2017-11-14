package org.jlab.clara.examples.callbacks;

import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.std.orchestrators.EngineReportHandler;

import java.util.Set;

/**
 * Engine report handler example.
 */
public class M1 implements EngineReportHandler {

    @Override
    public void handleEvent(EngineData event) {
        System.out.printf("%s: received %s [%s] from %s%n",
                ClaraUtil.getCurrentTime(),
                event.getExecutionState(),
                event.getMimeType(),
                event.getEngineName());
    }

    @Override
    public Set<EngineDataType> dataTypes() {
        return ClaraUtil.buildDataTypes(EngineDataType.STRING, EngineDataType.JSON);
    }
}
