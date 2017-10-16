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


    @Test
    public void parseGoodServicesFileYaml() {
        URL path = getClass().getResource("/resources/services-ok.yaml");
        OrchestratorConfigParser parser = new OrchestratorConfigParser(path.getPath());

        assertThat(parser.parseReconstructionChain(), is(servicesList));
    }


    @Test
    public void parseBadServicesFileYaml() {
        expectedEx.expect(OrchestratorConfigException.class);
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
    public void parseLanguagesSingleLang() throws Exception {
        URL path = getClass().getResource("/resources/services-ok.yaml");
        OrchestratorConfigParser parser = new OrchestratorConfigParser(path.getPath());

        assertThat(parser.parseLanguages(), containsInAnyOrder(ClaraLang.JAVA));
    }


    @Test
    public void parseLanguagesMultiLang() throws Exception {
        URL path = getClass().getResource("/resources/services-custom.yaml");
        OrchestratorConfigParser parser = new OrchestratorConfigParser(path.getPath());

        assertThat(parser.parseLanguages(), containsInAnyOrder(ClaraLang.JAVA, ClaraLang.CPP));
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

        String[] expected = new String[] {"binary/data-evio", "binary/data-hipo"};
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
