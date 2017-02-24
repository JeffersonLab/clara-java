package org.jlab.clara.std.orchestrators;

import java.io.FileInputStream;
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
 * <li>The list of services in the reconstruction chain
 * <li>The name of the container for the services
 * <li>The list of I/O and reconstruction nodes
 * <li>The list of input files
 * </ul>
 *
 * The <i>reconstruction services</i> description is provided in a YAML file,
 * which format is the following:
 * <pre>
 * container: my-default # Optional: change default container, otherwise it is $USER
 * services:
 *   - class: org.jlab.clas12.ana.serviceA
 *     name: serviceA
 *   - class: org.jlab.clas12.rec.serviceB
 *     name: serviceB
 *     container: containerB # Optional: change container for this service
 * </pre>
 * By default, all processing and I/O services will be deployed in a
 * container named as the {@code $USER} running the orchestrator. This can be
 * changed by including a {@code container} key with the desired container name.
 * The container can be overwritten for individual services too. There is no
 * need to include I/O services in this file. They are controlled by the
 * orchestrators.
 * <p>
 * The <i>input files</i> description is just a simple text file with the list
 * of all the input files, one per line:
 * <pre>
 * input-file1.ev
 * input-file2.ev
 * input-file3.ev
 * input-file4.ev
 * </pre>
 */
class OrchestratorConfigParser {

    private static final String DEFAULT_CONTAINER = System.getProperty("user.name");

    private final JSONObject config;


    OrchestratorConfigParser(String configFilePath) {
        try (InputStream input = new FileInputStream(configFilePath)) {
            Yaml yaml = new Yaml();
            @SuppressWarnings("unchecked")
            Map<String, Object> config = (Map<String, Object>) yaml.load(input);
            this.config = new JSONObject(config);
        } catch (IOException e) {
            throw error(e);
        }
    }


    public static String getDefaultContainer() {
        return DEFAULT_CONTAINER;
    }


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


    public Map<String, ServiceInfo> parseInputOutputServices() {
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


    public List<ServiceInfo> parseReconstructionChain() {
        List<ServiceInfo> services = new ArrayList<ServiceInfo>();
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


    public JSONObject parseReconstructionConfig() {
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


    public static DpeInfo getDefaultDpeInfo(String hostName) {
        String dpeIp = hostAddress(hostName);
        DpeName dpeName = new DpeName(dpeIp, ClaraLang.JAVA);
        return new DpeInfo(dpeName, 0, DpeInfo.DEFAULT_CLARA_HOME);
    }


    public static DpeName localDpeName() {
        return new DpeName(hostAddress("localhost"), ClaraLang.JAVA);
    }


    public static String hostAddress(String host) {
        try {
            return xMsgUtil.toHostAddress(host);
        } catch (UncheckedIOException e) {
            throw error("node name not known: " + host);
        }
    }


    public List<String> readInputFiles(String inputFilesList) {
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
