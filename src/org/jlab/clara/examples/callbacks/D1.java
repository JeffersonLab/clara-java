package org.jlab.clara.examples.callbacks;

import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.base.DpeRegistrationData;
import org.jlab.clara.base.DpeRuntimeData;
import org.jlab.clara.std.orchestrators.DpeReportHandler;

/**
 * DPE report handler example.
 */
public class D1 implements DpeReportHandler {

    @Override
    public void handleReport(DpeRegistrationData dpeRegistration, DpeRuntimeData dpeRuntime) {
        System.out.printf("%s: received DPE report from %s%n",
                ClaraUtil.getCurrentTime(), dpeRegistration.name());
    }
}
