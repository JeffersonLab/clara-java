package org.jlab.clara.std.orchestrators;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jlab.clara.base.ClaraLang;
import org.jlab.clara.base.DpeName;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.yaml.snakeyaml.Yaml;

/**
 * Helper class to read configuration for the standard orchestrators.
 * <p>
 * Currently, the user can set:
 * <ul>
 * <li>A default container name and language for the services
 * <li>The set of I/O services (required)
 * <li>The list of processing services (required)
 * <li>The list of mime-types used by the services (required)
 * <li>The global configuration for all services
 * </ul>
 *
 * The <i>services</i> description is provided in a YAML file,
 * which format is the following:
 * <pre>
 * container: default # Optional: change default container, otherwise it is $USER
 * io-services:
 *   reader:
 *     class: org.jlab.clas12.ana.ReaderService
 *     name: ReaderService
 *   reader:
 *     class: org.jlab.clas12.ana.WriterService
 *     name: WriterService
 * services:
 *   - class: org.jlab.clas12.ana.ServiceA
 *     name: ServiceA
 *   - class: org.jlab.clas12.rec.ServiceB
 *     name: ServiceB
 *     container: containerB # Optional: change container for this service
 *   - class: service_c
 *     name: ServiceC
 *     lang: cpp # a C++ service
 * mime-types:
 *   - binary/data-hipo
 * config:
 *   param1: "some_string"
 *   param2:
 *      key1: 31
 *      key2: 50
 * </pre>
 * By default, all processing and I/O services will be deployed in a
 * container named as the {@code $USER} running the orchestrator. This can be
 * changed by including a {@code container} key with the desired container name.
 * The container can be overwritten for individual services too. There is no
 * need to include I/O services in this file. They are controlled by the
 * orchestrators.
 */
public class OrchestratorConfigParser {

    private static final String DEFAULT_CONTAINER = System.getProperty("user.name");

    private final JSONObject config;

    /**
     * Creates a parser for the given configuration file.
     *
     * @param configFilePath the path to the configuration file
     */
    public OrchestratorConfigParser(String configFilePath) {
        try (InputStream input = new FileInputStream(configFilePath)) {
            Yaml yaml = new Yaml();
            @SuppressWarnings("unchecked")
            Map<String, Object> config = (Map<String, Object>) yaml.load(input);
            this.config = new JSONObject(config);
        } catch (FileNotFoundException e) {
            throw error("missing configuration file " + configFilePath);
        } catch (IOException e) {
            throw error(e);
        }
    }


    static String getDefaultContainer() {
        return DEFAULT_CONTAINER;
    }


    /**
     * Returns the languages of all services defined in the application.
     *
     * @return a set with the languages of the services
     */
    public Set<ClaraLang> parseLanguages() {
        Stream<ServiceInfo> io = parseInputOutputServices().values().stream();
        Stream<ServiceInfo> rec = parseReconstructionChain().stream();

        return Stream.concat(io, rec).map(s -> s.lang).collect(Collectors.toSet());
    }


    /**
     * Returns the mime-types required to receive messages from the services.
     * <p>
     * The orchestrator will create fake {@link org.jlab.clara.engine.EngineDataType
     * EngineDataType} objects for each mime-type. These will not deserialize
     * the user-data contained in the messages, they will be used to access just
     * the metadata.
     *
     * @return the mime-types of the data returned by the services
     */
    public Set<String> parseDataTypes() {
        Set<String> types = new HashSet<>();
        JSONArray mimeTypes = config.optJSONArray("mime-types");
        if (mimeTypes != null) {
            for (int i = 0; i < mimeTypes.length(); i++) {
                try {
                    types.add(mimeTypes.getString(i));
                } catch (JSONException e) {
                    throw error("Invalid array of mime-types");
                }
            }
        }
        return types;
    }


    Map<String, ServiceInfo> parseInputOutputServices() {
        Map<String, ServiceInfo> services = new HashMap<>();
        JSONObject io = config.optJSONObject("io-services");
        if (io == null) {
            throw error("Missing I/O services");
        }

        Consumer<String> getTypes = key -> {
            JSONObject data = io.optJSONObject(key);
            if (data == null) {
                throw error("Missing " + key + " I/O service");
            }
            services.put(key, parseService(data));
        };
        getTypes.accept(ApplicationInfo.READER);
        getTypes.accept(ApplicationInfo.WRITER);

        services.put(ApplicationInfo.STAGE, getStageService());

        return services;
    }


    private ServiceInfo getStageService() {
        return new ServiceInfo("org.jlab.clara.std.services.DataManager",
                               parseDefaultContainer(), "DataManager", ClaraLang.JAVA);
    }


    List<ServiceInfo> parseReconstructionChain() {
        List<ServiceInfo> services = new ArrayList<>();
        JSONArray sl = config.optJSONArray("services");
        if (sl == null) {
            throw error("missing list of services");
        }
        for (int i = 0; i < sl.length(); i++) {
            ServiceInfo service = parseService(sl.getJSONObject(i));
            if (services.contains(service)) {
                throw error(String.format("duplicated service  name = '%s' container = '%s'",
                                          service.name, service.cont));
            }
            services.add(service);
        }
        return services;
    }


    JSONObject parseReconstructionConfig() {
        if (config.has("configuration")) {
            return config.getJSONObject("configuration");
        }
        return new JSONObject();
    }


    private String parseDefaultContainer() {
        return config.optString("container", DEFAULT_CONTAINER);
    }


    private String parseDefaultLanguage() {
        return config.optString("lang", ClaraLang.JAVA.toString());
    }


    private ServiceInfo parseService(JSONObject data) {
        String name = data.optString("name");
        String classPath = data.optString("class");
        String container = data.optString("container", parseDefaultContainer());
        ClaraLang lang = ClaraLang.fromString(data.optString("lang", parseDefaultLanguage()));
        if (name.isEmpty() || classPath.isEmpty()) {
            throw error("missing name or class of service");
        }
        return new ServiceInfo(classPath, container, name, lang);
    }


    static DpeInfo getDefaultDpeInfo(String hostName) {
        String dpeIp = hostAddress(hostName);
        DpeName dpeName = new DpeName(dpeIp, ClaraLang.JAVA);
        return new DpeInfo(dpeName, 0, DpeInfo.DEFAULT_CLARA_HOME);
    }


    static DpeName localDpeName() {
        return new DpeName(hostAddress("localhost"), ClaraLang.JAVA);
    }


    static String hostAddress(String host) {
        try {
            return xMsgUtil.toHostAddress(host);
        } catch (UncheckedIOException e) {
            throw error("node name not known: " + host);
        }
    }


    List<String> readInputFiles(String inputFilesList) {
        try {
            Pattern pattern = Pattern.compile("^\\s*#.*$");
            return Files.lines(Paths.get(inputFilesList))
                        .filter(line -> !line.isEmpty())
                        .filter(line -> !pattern.matcher(line).matches())
                        .collect(Collectors.toList());
        } catch (IOException e) {
            throw error("Could not read file " + inputFilesList);
        }
    }


    private static OrchestratorConfigError error(String msg) {
        return new OrchestratorConfigError(msg);
    }


    private static OrchestratorConfigError error(Throwable cause) {
        return new OrchestratorConfigError(cause);
    }
}
