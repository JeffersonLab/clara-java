package org.jlab.clara.std.orchestrators;

import org.jlab.clara.base.ClaraLang;
import org.jlab.clara.base.DpeName;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class AppData {

    static final int CORES = 5;

    static final String CONT1 = "master";
    static final String CONT2 = "slave";

    static final ServiceInfo S1 = service(CONT1, "S1", ClaraLang.JAVA);
    static final ServiceInfo R1 = service(CONT1, "R1", ClaraLang.JAVA);
    static final ServiceInfo W1 = service(CONT1, "W1", ClaraLang.JAVA);

    static final ServiceInfo J1 = service(CONT1, "J1", ClaraLang.JAVA);
    static final ServiceInfo J2 = service(CONT1, "J2", ClaraLang.JAVA);
    static final ServiceInfo J3 = service(CONT1, "J3", ClaraLang.JAVA);

    static final ServiceInfo K1 = service(CONT2, "K1", ClaraLang.JAVA);
    static final ServiceInfo K2 = service(CONT2, "K2", ClaraLang.JAVA);

    static final ServiceInfo C1 = service(CONT1, "C1", ClaraLang.CPP);
    static final ServiceInfo C2 = service(CONT1, "C2", ClaraLang.CPP);

    static final ServiceInfo P1 = service(CONT2, "P1", ClaraLang.PYTHON);

    static final DpeInfo DPE1 = dpe("10.1.1.10_java");
    static final DpeInfo DPE2 = dpe("10.1.1.10_cpp");
    static final DpeInfo DPE3 = dpe("10.1.1.10_python");

    private AppData() { }


    static final class AppBuilder {

        private ApplicationInfo app;
        private Map<ClaraLang, DpeInfo> dpes;

        private AppBuilder() {
            app = defaultAppInfo();
            dpes = new HashMap<>();
            dpes.put(DPE1.name.language(), DPE1);
        }

        AppBuilder withServices(ServiceInfo... services) {
            this.app = new ApplicationInfo(ioServices(), recServices(services));
            return this;
        }

        AppBuilder withDpes(DpeInfo... dpes) {
            for (DpeInfo dpe : dpes) {
                this.dpes.put(dpe.name.language(), dpe);
            }
            return this;
        }

        WorkerApplication build() {
            return new WorkerApplication(app, dpes);
        }
    }


    static AppBuilder builder() {
        return new AppBuilder();
    }


    static ApplicationInfo defaultAppInfo() {
        return newAppInfo(J1, J2, J3);
    }


    static ApplicationInfo newAppInfo(ServiceInfo... services) {
        return new ApplicationInfo(ioServices(), recServices(services));
    }


    static Map<String, ServiceInfo> ioServices() {
        Map<String, ServiceInfo> map = new HashMap<>();
        map.put(ApplicationInfo.STAGE, S1);
        map.put(ApplicationInfo.READER, R1);
        map.put(ApplicationInfo.WRITER, W1);
        return map;
    }


    static List<ServiceInfo> recServices(ServiceInfo... elem) {
        return Arrays.asList(elem);
    }


    static ServiceInfo service(String cont, String engine, ClaraLang lang) {
        return new ServiceInfo("org.test." + engine, cont, engine, lang);
    }


    static DpeInfo dpe(String name) {
        return new DpeInfo(new DpeName(name), CORES, "");
    }
}
