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

import org.jlab.clara.base.ClaraLang;
import org.jlab.clara.base.ContainerName;
import org.jlab.clara.base.DpeName;
import org.jlab.clara.base.ServiceName;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class WorkerApplicationTest {

    @Test
    public void getStageService() throws Exception {
        WorkerApplication app = AppData.builder().build();

        assertThat(app.stageService().canonicalName(), is("10.1.1.10_java:master:S1"));
    }


    @Test
    public void getReaderService() throws Exception {
        WorkerApplication app = AppData.builder().build();

        assertThat(app.readerService().canonicalName(), is("10.1.1.10_java:master:R1"));
    }


    @Test
    public void getWriterService() throws Exception {
        WorkerApplication app = AppData.builder().build();

        assertThat(app.writerService().canonicalName(), is("10.1.1.10_java:master:W1"));
    }


    @Test
    public void getReconstructionServicesForSingleLangApplication() throws Exception {
        WorkerApplication app = AppData.builder().build();

        ServiceName[] expected = toServices("10.1.1.10_java:master:J1",
                                            "10.1.1.10_java:master:J2",
                                            "10.1.1.10_java:master:J3");

        assertThat(app.processingServices(), containsInAnyOrder(expected));
    }


    @Test
    public void getReconstructionServicesForMultiLangApplication() throws Exception {
        WorkerApplication app = AppData.builder()
                .withServices(AppData.J1, AppData.C1, AppData.C2, AppData.P1)
                .withDpes(AppData.DPE1, AppData.DPE2, AppData.DPE3)
                .build();

        ServiceName[] expected = toServices("10.1.1.10_java:master:J1",
                                            "10.1.1.10_cpp:master:C1",
                                            "10.1.1.10_cpp:master:C2",
                                            "10.1.1.10_python:slave:P1");

        assertThat(app.processingServices(), containsInAnyOrder(expected));
    }


    @Test
    public void getUniqueContainer() throws Exception {
        WorkerApplication app = AppData.builder().build();

        ContainerName[] expected = toContainers("10.1.1.10_java:master");

        assertThat(flatContainers(app.allContainers()), containsInAnyOrder(expected));
    }


    @Test
    public void getAllContainersForSingleLangApplication() throws Exception {
        WorkerApplication app = AppData.builder()
                .withServices(AppData.J1, AppData.J2, AppData.K1)
                .build();

        ContainerName[] expected = toContainers("10.1.1.10_java:master", "10.1.1.10_java:slave");

        assertThat(flatContainers(app.allContainers()), containsInAnyOrder(expected));
    }


    @Test
    public void getAllContainersForMultiLangApplication() throws Exception {
        WorkerApplication app = AppData.builder()
                .withServices(AppData.J1, AppData.K1, AppData.C1, AppData.P1)
                .withDpes(AppData.DPE1, AppData.DPE2, AppData.DPE3)
                .build();

        ContainerName[] expected = toContainers("10.1.1.10_java:master",
                                                "10.1.1.10_java:slave",
                                                "10.1.1.10_cpp:master",
                                                "10.1.1.10_python:slave");

        assertThat(flatContainers(app.allContainers()), containsInAnyOrder(expected));
    }


    @Test
    public void getLanguageForSingleLangApplication() throws Exception {
        ApplicationInfo info = AppData.newAppInfo(AppData.J1, AppData.J2, AppData.J3);

        assertThat(info.getLanguages(), containsInAnyOrder(ClaraLang.JAVA));
    }


    @Test
    public void getLanguagesForMultiLangApplication() throws Exception {
        ApplicationInfo info = AppData.newAppInfo(AppData.J1, AppData.C1, AppData.P1);

        ClaraLang[] expected = {ClaraLang.JAVA, ClaraLang.CPP, ClaraLang.PYTHON};

        assertThat(info.getLanguages(), containsInAnyOrder(expected));
    }


    private static ServiceName[] toServices(String... elem) {
        return Stream.of(elem)
                     .map(ServiceName::new)
                     .toArray(ServiceName[]::new);
    }


    private static ContainerName[] toContainers(String... elem) {
        return Stream.of(elem)
                     .map(ContainerName::new)
                     .toArray(ContainerName[]::new);
    }


    private static List<ContainerName> flatContainers(Map<DpeName, Set<ContainerName>> all) {
        return all.values().stream()
                  .flatMap(Set::stream)
                  .collect(Collectors.toList());
    }
}
