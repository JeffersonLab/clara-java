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
import org.jlab.clara.base.Composition;
import org.jlab.clara.base.ContainerName;
import org.jlab.clara.base.DpeName;
import org.jlab.clara.base.ServiceName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class WorkerApplication {

    private final ApplicationInfo application;
    private final Map<ClaraLang, DpeInfo> dpes;


    WorkerApplication(ApplicationInfo application, DpeInfo dpe) {
        this.application = application;
        this.dpes = new HashMap<>();
        this.dpes.put(dpe.name.language(), dpe);
    }


    WorkerApplication(ApplicationInfo application, Map<ClaraLang, DpeInfo> dpes) {
        this.application = application;
        this.dpes = new HashMap<>(dpes);
    }


    public Set<ClaraLang> languages() {
        return application.getLanguages();
    }


    public ServiceName stageService() {
        return toName(application.getStageService());
    }


    public ServiceName readerService() {
        return toName(application.getReaderService());
    }


    public ServiceName writerService() {
        return toName(application.getWriterService());
    }


    public List<ServiceName> recServices() {
        return application.getRecServices().stream()
                .map(this::toName)
                .collect(Collectors.toList());
    }


    public Composition composition() {
        String composition = readerService().canonicalName();
        for (ServiceName service : recServices()) {
            composition += "+" + service.canonicalName();
        }
        composition += "+" + writerService().canonicalName();
        composition += "+" + readerService().canonicalName();
        composition += ";";
        return new Composition(composition);
    }


    private ServiceName toName(ServiceInfo service) {
        DpeInfo dpe = dpes.get(service.lang);
        if (dpe == null) {
            String msg = String.format("Missing %s DPE for service %s", service.lang, service.name);
            throw new IllegalStateException(msg);
        }
        return new ServiceName(dpe.name, service.cont, service.name);
    }


    Stream<DeployInfo> getIODeployInfo() {
        return application.getIOServices().stream()
                          .map(s -> new DeployInfo(toName(s), s.classpath, 1));
    }


    Stream<DeployInfo> getRecDeployInfo() {
        int recCores = maxCores();
        return application.getRecServices().stream()
                          .distinct()
                          .map(s -> new DeployInfo(toName(s), s.classpath, recCores));
    }


    Map<DpeName, Set<ServiceName>> allServices() {
        return application.getAllServices().stream()
                          .map(this::toName)
                          .collect(Collectors.groupingBy(ServiceName::dpe, Collectors.toSet()));
    }


    Map<DpeName, Set<ContainerName>> allContainers() {
        return application.getAllServices().stream()
                          .map(this::toName)
                          .map(ServiceName::container)
                          .collect(Collectors.groupingBy(ContainerName::dpe, Collectors.toSet()));
    }


    Set<DpeName> dpes() {
        return dpes.values().stream()
                   .map(dpe -> dpe.name)
                   .collect(Collectors.toSet());
    }


    public int maxCores() {
        return dpes.values().stream().mapToInt(dpe -> dpe.cores).min().getAsInt();
    }


    public String hostName() {
        DpeInfo firstDpe = dpes.values().iterator().next();
        return firstDpe.name.address().host();
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + dpes.hashCode();
        return result;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        WorkerApplication other = (WorkerApplication) obj;
        if (!dpes.equals(other.dpes)) {
            return false;
        }
        return true;
    }


    @Override
    public String toString() {
        String dpeNames = dpes.values().stream()
                .map(dpe -> dpe.name.canonicalName())
                .collect(Collectors.joining(","));
        return "[" + dpeNames + "]";
    }
}
