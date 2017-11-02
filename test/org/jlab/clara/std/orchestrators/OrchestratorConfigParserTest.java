package org.jlab.clara.std.orchestrators;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.jlab.clara.IntegrationTest;
import org.jlab.clara.base.ClaraLang;
import org.json.JSONObject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.hasEntry;


@Category(IntegrationTest.class)
public class OrchestratorConfigParserTest {

    private static final String CONT = OrchestratorConfigParser.getDefaultContainer();

    private final List<ServiceInfo> servicesList = new ArrayList<>();

    public OrchestratorConfigParserTest() {
        servicesList.add(service("org.jlab.clas12.ec.services.ECReconstruction",
                                 "ECReconstruction"));
        servicesList.add(service("org.clas12.services.tracking.SeedFinder",
                                 "SeedFinder"));
        servicesList.add(service("org.jlab.clas12.ftof.services.FTOFReconstruction",
                                 "FTOFReconstruction"));
    }


    @Rule
    public ExpectedException expectedEx = ExpectedException.none();


    private OrchestratorConfigParser parseFile(String resource) {
        URL path = getClass().getResource(resource);
        return new OrchestratorConfigParser(path.getPath());
    }


    @Test
    public void parseGoodServicesFileYaml() {
        OrchestratorConfigParser parser = parseFile("/resources/services-ok.yml");

        assertThat(parser.parseDataProcessingServices(), is(servicesList));
    }


    @Test
    public void parseBadServicesFileYaml() {
        expectedEx.expect(OrchestratorConfigException.class);
        expectedEx.expectMessage("missing name or class of service");

        OrchestratorConfigParser parser = parseFile("/resources/services-bad.yml");

        parser.parseDataProcessingServices();
    }


    @Test
    public void parseIOServices() throws Exception {
        OrchestratorConfigParser parser = parseFile("/resources/services-custom.yml");

        Map<String, ServiceInfo> services = parser.parseInputOutputServices();

        ServiceInfo reader = new ServiceInfo("org.jlab.clas12.convertors.CustomReader",
                                             CONT, "CustomReader", ClaraLang.JAVA);
        ServiceInfo writer = new ServiceInfo("org.jlab.clas12.convertors.CustomWriter",
                                             CONT, "CustomWriter", ClaraLang.CPP);

        assertThat(services, hasEntry(equalTo("reader"), equalTo(reader)));
        assertThat(services, hasEntry(equalTo("writer"), equalTo(writer)));
    }


    @Test
    public void parseMultiLangServices() throws Exception {
        OrchestratorConfigParser parser = parseFile("/resources/services-custom.yml");

        List<ServiceInfo> expected = Arrays.asList(
                new ServiceInfo("org.jlab.clas12.convertors.ECReconstruction",
                                CONT, "ECReconstruction", ClaraLang.JAVA),
                new ServiceInfo("org.jlab.clas12.convertors.SeedFinder",
                                CONT, "SeedFinder", ClaraLang.JAVA),
                new ServiceInfo("org.jlab.clas12.convertors.HeaderFilter",
                                CONT, "HeaderFilter", ClaraLang.CPP),
                new ServiceInfo("org.jlab.clas12.convertors.FTOFReconstruction",
                                CONT, "FTOFReconstruction", ClaraLang.JAVA)
        );

        assertThat(parser.parseDataProcessingServices(), is(expected));
    }


    @Test
    public void parseMonitoringServices() throws Exception {
        OrchestratorConfigParser parser = parseFile("/resources/services-custom.yml");

        List<ServiceInfo> expected = Arrays.asList(
                service("org.jlab.clas12.services.ECMonitoring", "ECMonitor"),
                service("org.jlab.clas12.services.DCMonitoring", "DCMonitor")
        );

        assertThat(parser.parseMonitoringServices(), is(expected));
    }


    @Test
    public void parseLanguagesSingleLang() throws Exception {
        OrchestratorConfigParser parser = parseFile("/resources/services-ok.yml");

        assertThat(parser.parseLanguages(), containsInAnyOrder(ClaraLang.JAVA));
    }


    @Test
    public void parseLanguagesMultiLang() throws Exception {
        OrchestratorConfigParser parser = parseFile("/resources/services-custom.yml");

        assertThat(parser.parseLanguages(), containsInAnyOrder(ClaraLang.JAVA, ClaraLang.CPP));
    }


    @Test
    public void parseEmptyMimeTypes() {
        OrchestratorConfigParser parser = parseFile("/resources/services-ok.yml");

        assertThat(parser.parseDataTypes(), is(empty()));
    }


    @Test
    public void parseUserDefinedMimeTypes() {
        OrchestratorConfigParser parser = parseFile("/resources/services-custom.yml");

        String[] expected = new String[] {"binary/data-evio", "binary/data-hipo"};
        assertThat(parser.parseDataTypes(), containsInAnyOrder(expected));
    }


    @Test
    public void parseEmptyConfig() {
        OrchestratorConfigParser parser = parseFile("/resources/services-ok.yml");

        JSONObject config = parser.parseConfiguration();

        assertThat(config.toString(), is("{}"));
    }


    @Test
    public void parseCustomConfig() {
        OrchestratorConfigParser parser = parseFile("/resources/services-custom.yml");

        JSONObject config = parser.parseConfiguration();

        assertThat(config.keySet(), containsInAnyOrder("global", "io-services", "services"));

        assertThat(config.getJSONObject("global").keySet(),
                   containsInAnyOrder("ccdb", "magnet", "kalman"));
        assertThat(config.getJSONObject("io-services").keySet(),
                   containsInAnyOrder("reader", "writer"));
        assertThat(config.getJSONObject("services").keySet(),
                   containsInAnyOrder("ECReconstruction", "HeaderFilter"));
    }


    @Test
    public void parseInputFilesList() {
        URL files = getClass().getResource("/resources/files.list");

        List<String> expected = Arrays.asList("file1.ev", "file2.ev", "file3.ev",
                                              "file4.ev", "file5.ev");

        assertThat(OrchestratorConfigParser.readInputFiles(files.getPath()), is(expected));
    }


    private static ServiceInfo service(String classPath, String name) {
        return new ServiceInfo(classPath, CONT, name, ClaraLang.JAVA);
    }
}
