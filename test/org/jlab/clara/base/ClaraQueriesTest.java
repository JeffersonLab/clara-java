package org.jlab.clara.base;

import org.jlab.clara.base.core.ClaraBase;
import org.jlab.clara.base.core.ClaraComponent;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgRegAddress;
import org.jlab.coda.xmsg.net.xMsgSocketFactory;
import org.jlab.coda.xmsg.sys.xMsgRegistrar;
import org.jlab.coda.xmsg.sys.regdis.xMsgRegDriver;
import org.jlab.coda.xmsg.sys.regdis.xMsgRegFactory;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zeromq.ZContext;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class ClaraQueriesTest {

    private static final xMsgRegistration.OwnerType TYPE = xMsgRegistration.OwnerType.SUBSCRIBER;

    private static TestData data;

    private ClaraBase base = base();
    private ClaraQueries.ClaraQueryBuilder queryBuilder;


    private static class Data<T extends ClaraName> {

        final T name;

        Data(T name) {
            this.name = name;
        }
    }


    private static class TestData {

        private final ZContext context;
        private final xMsgRegistrar server;
        private final xMsgRegDriver driver;

        private final Map<String, Data<DpeName>> dpes = new HashMap<>();
        private final Map<String, Data<ContainerName>> containers = new HashMap<>();
        private final Map<String, Data<ServiceName>> services = new HashMap<>();

        TestData() throws xMsgException {

            xMsgRegAddress addr = new xMsgRegAddress("localhost", 7775);
            context = new ZContext();
            server = new xMsgRegistrar(context, addr);
            driver = new xMsgRegDriver(addr, new xMsgSocketFactory(context.getContext()));

            server.start();
            driver.connect();

            createDpe("dpeJ1", "10.2.9.1", ClaraLang.JAVA);
            createDpe("dpeJ2", "10.2.9.2", ClaraLang.JAVA);
            createDpe("dpeC1", "10.2.9.1", ClaraLang.CPP);
            createDpe("dpeC2", "10.2.9.2", ClaraLang.CPP);

            createContainer("contAJ1", "dpeJ1", "A");
            createContainer("contAJ2", "dpeJ2", "A");
            createContainer("contAC1", "dpeC1", "A");
            createContainer("contAC2", "dpeC2", "A");

            createContainer("contBJ1", "dpeJ1", "B");

            createContainer("contCJ1", "dpeJ1", "C");
            createContainer("contCC1", "dpeC1", "C");

            createService("E1", "contAJ1", "E");
            createService("E2", "contAJ2", "E");
            createService("E3", "contBJ1", "E");

            createService("F1", "contAJ1", "F");
            createService("F2", "contAJ2", "F");

            createService("G1", "contAJ1", "G");

            createService("H1", "contBJ1", "H");
            createService("H2", "contCJ1", "H");

            createService("M1", "contAC1", "M");
            createService("M2", "contAC2", "M");

            createService("N1", "contAC1", "N");
            createService("N2", "contCC1", "N");

            dpes.forEach((k, n) -> register(regData(n.name)));
            containers.forEach((k, n) -> register(regData(n.name)));
            services.forEach((k, n) -> register(regData(n.name)));
        }

        private void createDpe(String key, String host, ClaraLang lang) {
            DpeName name = new DpeName(host, lang);

            dpes.put(key, new Data<>(name));
        }

        private void createContainer(String key, String dpeKey, String contName) {
            Data<DpeName> dpe = dpes.get(dpeKey);
            ContainerName name = new ContainerName(dpe.name, contName);

            containers.put(key, new Data<ContainerName>(name));
        }

        private void createService(String key, String contKey, String engine) {
            Data<ContainerName> container = containers.get(contKey);
            ServiceName name = new ServiceName(container.name, engine);

            services.put(key, new Data<ServiceName>(name));
        }

        void close() {
            driver.close();
            context.close();
            server.shutdown();
        }

        DpeName dpe(String name) {
            return dpes.get(name).name;
        }

        ContainerName cont(String name) {
            return containers.get(name).name;
        }

        ServiceName service(String name) {
            return services.get(name).name;
        }

        Set<DpeName> dpes(String... names) {
            return names(dpes, names);
        }

        Set<ContainerName> containers(String... names) {
            return names(containers, names);
        }

        Set<ServiceName> services(String... names) {
            return names(services, names);
        }

        private <T extends ClaraName> Set<T> names(Map<String, Data<T>> map,
                                                   String... names) {
            Set<T> set = new HashSet<>();
            for (String name : names) {
                set.add(map.get(name).name);
            }
            return set;
        }

        private void register(xMsgRegistration data) {
            try {
                driver.addRegistration("test", data);
            } catch (xMsgException e) {
                throw new RuntimeException(e);
            }
        }
    }


    @BeforeClass
    public static void prepare() throws xMsgException {
        data = new TestData();
    }


    @AfterClass
    public static void end() {
        data.close();
    }


    @Before
    public void setup() throws Exception {
        queryBuilder = new ClaraQueries.ClaraQueryBuilder(base, ClaraComponent.dpe());
    }



    private static ClaraBase base() {
        return new ClaraBase(ClaraComponent.dpe(), ClaraComponent.dpe()) {
            @Override
            public void start() throws ClaraException { }

            @Override
            protected void end() { }
        };
    }


    private static xMsgRegistration regData(DpeName name) {
        return registration(name, xMsgTopic.build("dpe", name.canonicalName()));
    }


    private static xMsgRegistration regData(ContainerName name) {
        return registration(name, xMsgTopic.build("container", name.canonicalName()));
    }


    private static xMsgRegistration regData(ServiceName name) {
        return registration(name, xMsgTopic.wrap(name.canonicalName()));
    }


    private static xMsgRegistration registration(ClaraName name, xMsgTopic topic) {
        return xMsgRegFactory.newRegistration(name.canonicalName(), name.address(), TYPE, topic)
                             .build();
    }
}
