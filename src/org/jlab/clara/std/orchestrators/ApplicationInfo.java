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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class ApplicationInfo {

    static final String STAGE = "stage";
    static final String READER = "reader";
    static final String WRITER = "writer";

    private final Map<String, ServiceInfo> ioServices;
    private final List<ServiceInfo> recServices;
    private final Set<ClaraLang> languages;

    ApplicationInfo(Map<String, ServiceInfo> ioServices, List<ServiceInfo> recServices) {
        this.ioServices = copyServices(ioServices);
        this.recServices = copyServices(recServices);
        this.languages = parseLanguages(ioServices.values(), recServices);
    }

    private static Map<String, ServiceInfo> copyServices(Map<String, ServiceInfo> ioServices) {
        if (ioServices.get(STAGE) == null) {
            throw new IllegalArgumentException("missing stage service");
        }
        if (ioServices.get(READER) == null) {
            throw new IllegalArgumentException("missing reader service");
        }
        if (ioServices.get(WRITER) == null) {
            throw new IllegalArgumentException("missing writer service");
        }
        return new HashMap<>(ioServices);
    }

    private static List<ServiceInfo> copyServices(List<ServiceInfo> recChain) {
        if (recChain == null) {
            throw new IllegalArgumentException("null reconstruction chain");
        }
        if (recChain.isEmpty()) {
            throw new IllegalArgumentException("empty reconstruction chain");
        }
        return new ArrayList<>(recChain);
    }

    private Set<ClaraLang> parseLanguages(Collection<ServiceInfo> ioServices,
                                          Collection<ServiceInfo> recServices) {
        return Stream.concat(ioServices.stream(), recServices.stream())
                     .map(s -> s.lang)
                     .collect(Collectors.toSet());
    }

    ServiceInfo getStageService() {
        return ioServices.get(STAGE);
    }

    ServiceInfo getReaderService() {
        return ioServices.get(READER);
    }

    ServiceInfo getWriterService() {
        return ioServices.get(WRITER);
    }

    List<ServiceInfo> getIOServices() {
        return Arrays.asList(getStageService(), getReaderService(), getWriterService());
    }

    List<ServiceInfo> getRecServices() {
        return recServices;
    }

    Set<ServiceInfo> getAllServices() {
        return Stream.concat(ioServices.values().stream(), recServices.stream())
                     .collect(Collectors.toSet());
    }

    Set<ClaraLang> getLanguages() {
        return languages;
    }
}
