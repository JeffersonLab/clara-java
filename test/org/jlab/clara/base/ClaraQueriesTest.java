package org.jlab.clara.base;

import org.jlab.clara.base.core.ClaraBase;
import org.jlab.clara.base.core.ClaraComponent;
import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.clara.base.core.MessageUtil;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.jlab.coda.xmsg.net.xMsgRegAddress;
import org.jlab.coda.xmsg.net.xMsgSocketFactory;
import org.jlab.coda.xmsg.sys.xMsgRegistrar;
import org.jlab.coda.xmsg.sys.regdis.xMsgRegDriver;
import org.jlab.coda.xmsg.sys.regdis.xMsgRegFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zeromq.ZContext;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

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
        final JSONObject registration;
        final JSONObject runtime;

        Data(T name, JSONObject registration, JSONObject runtime) {
            this.name = name;
            this.registration = registration;
            this.runtime = runtime;
        }
    }


    private static class TestData {

        private static final String DATE = LocalDateTime.now()
                .format(DateTimeFormatter.ofPattern(ClaraConstants.DATE_FORMAT));

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

            createService("E1", "contAJ1", "E", "Trevor", "Calculate error");
            createService("E2", "contAJ2", "E", "Trevor", "Calculate error");
            createService("E3", "contBJ1", "E", "Trevor", "Calculate error");

            createService("F1", "contAJ1", "F", "Franklin", "Find term");
            createService("F2", "contAJ2", "F", "Franklin", "Find term");

            createService("G1", "contAJ1", "G", "Michael", "Grep term");

            createService("H1", "contBJ1", "H", "Franklin", "Calculate height");
            createService("H2", "contCJ1", "H", "Franklin", "Calculate height");

            createService("M1", "contAC1", "M", "Michael", "Get max");
            createService("M2", "contAC2", "M", "Michael", "Get max");

            createService("N1", "contAC1", "N", "Michael", "Sum N");
            createService("N2", "contCC1", "N", "Michael", "Sum N");

            dpes.forEach((k, n) -> register(regData(n.name)));
            containers.forEach((k, n) -> register(regData(n.name)));
            services.forEach((k, n) -> register(regData(n.name)));
        }

        private void createDpe(String key, String host, ClaraLang lang) {
            DpeName name = new DpeName(host, lang);

            JSONObject regData = new JSONObject();
            regData.put("name", name.canonicalName());
            regData.put("start_time", DATE);
            regData.put("containers", new JSONArray());

            JSONObject runData = new JSONObject();
            runData.put("name", name.canonicalName());
            runData.put("snapshot_time", DATE);
            runData.put("containers", new JSONArray());

            dpes.put(key, new Data<>(name, regData, runData));
        }

        private void createContainer(String key, String dpeKey, String contName) {
            Data<DpeName> dpe = dpes.get(dpeKey);
            ContainerName name = new ContainerName(dpe.name, contName);

            JSONObject regData = new JSONObject();
            regData.put("name", name.canonicalName());
            regData.put("start_time", DATE);
            regData.put("services", new JSONArray());

            JSONObject runData = new JSONObject();
            runData.put("name", name.canonicalName());
            runData.put("snapshot_time", DATE);
            runData.put("services", new JSONArray());

            dpe.registration.getJSONArray("containers").put(regData);
            dpe.runtime.getJSONArray("containers").put(runData);

            containers.put(key, new Data<ContainerName>(name, regData, runData));
        }

        private void createService(String key, String contKey, String engine,
                                   String author, String description) {
            Data<ContainerName> container = containers.get(contKey);
            ServiceName name = new ServiceName(container.name, engine);

            JSONObject regData = new JSONObject();
            regData.put("name", name.canonicalName());
            regData.put("class_name", name.canonicalName());
            regData.put("start_time", DATE);
            regData.put("author", author);
            regData.put("description", description);

            JSONObject runData = new JSONObject();
            runData.put("name", name.canonicalName());
            runData.put("snapshot_time", DATE);

            container.registration.getJSONArray("services").put(regData);
            container.runtime.getJSONArray("services").put(runData);

            services.put(key, new Data<ServiceName>(name, regData, runData));
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

        public JSONObject json(String dpeName) {
            return dpes.values()
                       .stream()
                       .filter(d -> dpeName.equals(d.name.canonicalName()))
                       .map(d -> {
                           JSONObject r = new JSONObject();
                           r.put(ClaraConstants.REGISTRATION_KEY, d.registration);
                           r.put(ClaraConstants.RUNTIME_KEY, d.runtime);
                           return r;
                       })
                       .findFirst()
                       .get();
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


    @Test
    public void getNamesOfAllDpes() throws Exception {
        Set<DpeName> result = queryBuilder.canonicalNames(ClaraFilters.allDpes())
                                          .syncRun(3, TimeUnit.SECONDS);
        Set<DpeName> expected = data.dpes("dpeJ1", "dpeJ2", "dpeC1", "dpeC2");

        assertThat(result, is(expected));
    }


    @Test
    public void getNamesOfAllContainers() throws Exception {
        Set<ContainerName> result = queryBuilder.canonicalNames(ClaraFilters.allContainers())
                                                .syncRun(3, TimeUnit.SECONDS);
        Set<ContainerName> expected = data.containers("contAJ1", "contAJ2", "contAC1", "contAC2",
                                                      "contBJ1", "contCJ1", "contCC1");

        assertThat(result, is(expected));
    }


    @Test
    public void getNamesOfAllServices() throws Exception {
        Set<ServiceName> result = queryBuilder.canonicalNames(ClaraFilters.allServices())
                                                .syncRun(3, TimeUnit.SECONDS);
        Set<ServiceName> expected = data.services("E1", "E2", "E3",
                                                  "F1", "F2", "G1", "H1", "H2",
                                                  "M1", "M2", "N1", "N2");
        assertThat(result, is(expected));
    }


    @Test
    public void getNamesOfDpesByHost() throws Exception {
        Set<DpeName> result = queryBuilder.canonicalNames(ClaraFilters.dpesByHost("10.2.9.1"))
                                          .syncRun(3, TimeUnit.SECONDS);
        Set<DpeName> expected = data.dpes("dpeJ1", "dpeC1");

        assertThat(result, is(expected));
    }


    @Test
    public void getNamesOfDpesByLanguage() throws Exception {
        ClaraLang lang = ClaraLang.JAVA;
        Set<DpeName> result = queryBuilder.canonicalNames(ClaraFilters.dpesByLanguage(lang))
                                          .syncRun(3, TimeUnit.SECONDS);
        Set<DpeName> expected = data.dpes("dpeJ1", "dpeJ2");

        assertThat(result, is(expected));
    }


    @Test
    public void getNamesOfContainersByHost() throws Exception {
        Set<ContainerName> result = queryBuilder
                .canonicalNames(ClaraFilters.containersByHost("10.2.9.1"))
                .syncRun(3, TimeUnit.SECONDS);
        Set<ContainerName> expected = data.containers("contAJ1", "contAC1",
                                                      "contBJ1", "contCJ1", "contCC1");

        assertThat(result, is(expected));
    }


    @Test
    public void getNamesOfContainersByDpe() throws Exception {
        Set<ContainerName> result = queryBuilder
                .canonicalNames(ClaraFilters.containersByDpe(data.dpe("dpeJ1")))
                .syncRun(3, TimeUnit.SECONDS);
        Set<ContainerName> expected = data.containers("contAJ1", "contBJ1", "contCJ1");

        assertThat(result, is(expected));
    }


    @Test
    public void getNamesOfContainersByLanguage() throws Exception {
        Set<ContainerName> result = queryBuilder
                .canonicalNames(ClaraFilters.containersByLanguage(ClaraLang.CPP))
                .syncRun(3, TimeUnit.SECONDS);
        Set<ContainerName> expected = data.containers("contAC1", "contAC2", "contCC1");

        assertThat(result, is(expected));
    }


    @Test
    public void getNamesOfContainersByName() throws Exception {
        Set<ContainerName> result = queryBuilder
                .canonicalNames(ClaraFilters.containersByName("A"))
                .syncRun(3, TimeUnit.SECONDS);
        Set<ContainerName> expected = data.containers("contAJ1", "contAJ2", "contAC1", "contAC2");

        assertThat(result, is(expected));
    }


    @Test
    public void getNamesOfServicesByHost() throws Exception {
        Set<ServiceName> result = queryBuilder
                .canonicalNames(ClaraFilters.servicesByHost("10.2.9.1"))
                .syncRun(3, TimeUnit.SECONDS);
        Set<ServiceName> expected = data.services("E1", "E3", "F1", "G1", "H1", "H2",
                                                  "M1", "N1", "N2");

        assertThat(result, is(expected));
    }


    @Test
    public void getNamesOfServicesByDpe() throws Exception {
        Set<ServiceName> result = queryBuilder
                .canonicalNames(ClaraFilters.servicesByDpe(data.dpe("dpeC1")))
                .syncRun(3, TimeUnit.SECONDS);
        Set<ServiceName> expected = data.services("M1", "N1", "N2");

        assertThat(result, is(expected));
    }


    @Test
    public void getNamesOfServicesByContainer() throws Exception {
        Set<ServiceName> result = queryBuilder
                .canonicalNames(ClaraFilters.servicesByContainer(data.cont("contAJ1")))
                .syncRun(3, TimeUnit.SECONDS);
        Set<ServiceName> expected = data.services("E1", "F1", "G1");

        assertThat(result, is(expected));
    }


    @Test
    public void getNamesOfServicesByLanguage() throws Exception {
        Set<ServiceName> result = queryBuilder
                .canonicalNames(ClaraFilters.servicesByLanguage(ClaraLang.CPP))
                .syncRun(3, TimeUnit.SECONDS);
        Set<ServiceName> expected = data.services("M1", "M2", "N1", "N2");

        assertThat(result, is(expected));
    }


    @Test
    public void getNamesOfServicesByName() throws Exception {
        Set<ServiceName> result = queryBuilder
                .canonicalNames(ClaraFilters.servicesByName("E"))
                .syncRun(3, TimeUnit.SECONDS);
        Set<ServiceName> expected = data.services("E1", "E2", "E3");

        assertThat(result, is(expected));
    }


    @Test
    public void getNamesOfServicesByAuthor() throws Exception {
        Set<ServiceName> result = queryBuilder
                .canonicalNames(ClaraFilters.servicesByAuthor("Trevor"))
                .syncRun(3, TimeUnit.SECONDS);
        Set<ServiceName> expected = data.services("E1", "E2", "E3");

        assertThat(result, is(expected));
    }


    @Test
    public void getNamesOfServicesByDescription() throws Exception {
        Set<ServiceName> result = queryBuilder
                .canonicalNames(ClaraFilters.servicesByDescription(".*[Cc]alculate.*"))
                .syncRun(3, TimeUnit.SECONDS);
        Set<ServiceName> expected = data.services("E1", "E2", "E3", "H1", "H2");

        assertThat(result, is(expected));
    }


    @Test
    public void getNamesOfMissingDpes() throws Exception {
        Set<DpeName> result = queryBuilder
                .canonicalNames(ClaraFilters.dpesByHost("10.2.9.3"))
                .syncRun(3, TimeUnit.SECONDS);

        assertThat(result, is(empty()));
    }


    @Test
    public void getNamesOfMissingContainers() throws Exception {
        Set<ContainerName> result = queryBuilder
                .canonicalNames(ClaraFilters.containersByName("Z"))
                .syncRun(3, TimeUnit.SECONDS);

        assertThat(result, is(empty()));
    }


    @Test
    public void getNamesOfMissingServices() throws Exception {
        Set<ServiceName> result = queryBuilder
                .canonicalNames(ClaraFilters.servicesByAuthor("CJ"))
                .syncRun(3, TimeUnit.SECONDS);

        assertThat(result, is(empty()));
    }


    @Test
    public void discoverRegisteredDpe() throws Exception {
        Boolean result = queryBuilder.discover(new DpeName("10.2.9.1_java"))
                                     .syncRun(3, TimeUnit.SECONDS);

        assertThat(result, is(true));
    }


    @Test
    public void discoverRegisteredContainer() throws Exception {
        Boolean result = queryBuilder.discover(new ContainerName("10.2.9.1_cpp:C"))
                                     .syncRun(3, TimeUnit.SECONDS);

        assertThat(result, is(true));
    }


    @Test
    public void discoverRegisteredService() throws Exception {
        Boolean result = queryBuilder.discover(new ServiceName("10.2.9.1_java:B:H"))
                                     .syncRun(3, TimeUnit.SECONDS);

        assertThat(result, is(true));
    }


    @Test
    public void discoverMissingDpe() throws Exception {
        Boolean result = queryBuilder.discover(new DpeName("10.2.9.1_python"))
                                     .syncRun(3, TimeUnit.SECONDS);

        assertThat(result, is(false));
    }


    @Test
    public void discoverMissingContainer() throws Exception {
        Boolean result = queryBuilder.discover(new ContainerName("10.2.9.1_cpp:B"))
                                     .syncRun(3, TimeUnit.SECONDS);

        assertThat(result, is(false));
    }


    @Test
    public void discoverMissingService() throws Exception {
        Boolean result = queryBuilder.discover(new ServiceName("10.2.9.1_java:A:H"))
                                     .syncRun(3, TimeUnit.SECONDS);

        assertThat(result, is(false));
    }


    private static ClaraBase base() {
        return new ClaraBase(ClaraComponent.dpe(), ClaraComponent.dpe()) {
            @Override
            public void start() throws ClaraException { }

            @Override
            protected void end() { }

            @Override
            public xMsgMessage syncPublish(xMsgProxyAddress address, xMsgMessage msg, long timeout)
                    throws xMsgException, TimeoutException {
                JSONObject report = data.json(msg.getTopic().subject());
                return MessageUtil.buildRequest(msg.getTopic(), report.toString());
            }
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


    private static <T extends ClaraName> Set<T> names(Set<? extends ClaraReportData<T>> regData) {
        return regData.stream().map(ClaraReportData::name).collect(Collectors.toSet());
    }
}
