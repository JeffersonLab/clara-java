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
