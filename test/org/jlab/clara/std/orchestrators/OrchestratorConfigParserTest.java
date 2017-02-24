package org.jlab.clara.std.orchestrators;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.jlab.clara.base.ClaraLang;
import org.json.JSONObject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.hasEntry;


public class OrchestratorConfigParserTest {

    private static final String CONT = OrchestratorConfigParser.getDefaultContainer();

    private final List<ServiceInfo> servicesList = new ArrayList<ServiceInfo>();
    private final List<DpeInfo> recNodesList = new ArrayList<DpeInfo>();
    private final List<DpeInfo> ioNodesList = new ArrayList<DpeInfo>();


    public OrchestratorConfigParserTest() {
        servicesList.add(service("org.jlab.clas12.ec.services.ECReconstruction",
                                 "ECReconstruction"));
        servicesList.add(service("org.clas12.services.tracking.SeedFinder",
                                 "SeedFinder"));
        servicesList.add(service("org.jlab.clas12.ftof.services.FTOFReconstruction",
                                 "FTOFReconstruction"));

        String servicesDir = "/home/user/services";
        recNodesList.add(new DpeInfo("10.1.3.1_java", 12, servicesDir));
        recNodesList.add(new DpeInfo("10.1.3.2_java", 10, servicesDir));
        recNodesList.add(new DpeInfo("10.1.3.3_java", 12, servicesDir));

        ioNodesList.add(new DpeInfo("10.1.3.254_java", 0, "/home/user/clas12/services"));
    }


    @Rule
    public ExpectedException expectedEx = ExpectedException.none();


    @Test
    public void parseGoodServicesFileYaml() {
        URL path = getClass().getResource("/resources/services-ok.yaml");
        OrchestratorConfigParser parser = new OrchestratorConfigParser(path.getPath());

        assertThat(parser.parseReconstructionChain(), is(servicesList));
    }


    @Test
    public void parseBadServicesFileYaml() {
        expectedEx.expect(OrchestratorConfigError.class);
        expectedEx.expectMessage("missing name or class of service");

        URL path = getClass().getResource("/resources/services-bad.yaml");
        OrchestratorConfigParser parser = new OrchestratorConfigParser(path.getPath());
        parser.parseReconstructionChain();
    }


    @Test
    public void parseIOServices() throws Exception {
        URL path = getClass().getResource("/resources/services-custom.yaml");
        OrchestratorConfigParser parser = new OrchestratorConfigParser(path.getPath());
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
        URL path = getClass().getResource("/resources/services-custom.yaml");
        OrchestratorConfigParser parser = new OrchestratorConfigParser(path.getPath());

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

        assertThat(parser.parseReconstructionChain(), is(expected));
    }


    @Test
    public void parseEmptyMimeTypes() {
        URL path = getClass().getResource("/resources/services-ok.yaml");
        OrchestratorConfigParser parser = new OrchestratorConfigParser(path.getPath());

        assertThat(parser.parseDataTypes(), is(empty()));
    }


    @Test
    public void parseUserDefinedMimeTypes() {
        URL path = getClass().getResource("/resources/services-custom.yaml");
        OrchestratorConfigParser parser = new OrchestratorConfigParser(path.getPath());

        String[] expected = new String[] { "binary/data-evio", "binary/data-hipo" };
        assertThat(parser.parseDataTypes(), containsInAnyOrder(expected));
    }


    @Test
    public void parseEmptyConfig() {
        URL path = getClass().getResource("/resources/services-ok.yaml");
        OrchestratorConfigParser parser = new OrchestratorConfigParser(path.getPath());

        JSONObject config = parser.parseReconstructionConfig();

        assertThat(config.toString(), is("{}"));
    }


    @Test
    public void parseCustomConfig() {
        URL path = getClass().getResource("/resources/services-custom.yaml");
        OrchestratorConfigParser parser = new OrchestratorConfigParser(path.getPath());

        JSONObject config = parser.parseReconstructionConfig();

        assertThat(config.keySet(), containsInAnyOrder("ccdb", "magnet", "kalman"));
    }


    @Test
    public void parseInputFilesList() {
        URL config = getClass().getResource("/resources/services-ok.yaml");
        URL files = getClass().getResource("/resources/files.list");

        OrchestratorConfigParser parser = new OrchestratorConfigParser(config.getPath());

        List<String> expected = Arrays.asList("file1.ev", "file2.ev", "file3.ev",
                                              "file4.ev", "file5.ev");

        assertThat(parser.readInputFiles(files.getPath()), is(expected));
    }


    private static ServiceInfo service(String classPath, String name) {
        return new ServiceInfo(classPath, CONT, name, ClaraLang.JAVA);
    }
}
