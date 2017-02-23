package org.jlab.clara.base;

import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.clara.util.report.JsonUtils;
import org.json.JSONObject;

// CHECKSTYLE.OFF: Javadoc
public final class RuntimeDataFactory {

    private RuntimeDataFactory() { }

    public static DpeRuntimeData parseRuntime(String resource) {
        JSONObject json = JsonUtils.readJson(resource).getJSONObject(ClaraConstants.RUNTIME_KEY);
        return new DpeRuntimeData(json);
    }
}
