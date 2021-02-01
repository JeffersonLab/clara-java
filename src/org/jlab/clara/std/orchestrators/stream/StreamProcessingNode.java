package org.jlab.clara.std.orchestrators.stream;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.jlab.clara.base.ClaraLang;
import org.jlab.clara.base.DpeName;
import org.jlab.clara.base.EngineCallback;
import org.jlab.clara.base.ServiceName;
import org.jlab.clara.base.ServiceRuntimeData;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.std.orchestrators.*;
import org.json.JSONObject;


class StreamProcessingNode {


    private final CoreOrchestrator orchestrator;
    private final WorkerApplication application;

    private final ServiceName stageName;
    private final ServiceName readerName;
    private final ServiceName writerName;

    private volatile JSONObject userConfig = new JSONObject();

    private volatile String currentInputFileName;
    private volatile String currentInputFile;
    private volatile String currentOutputFile;

    AtomicInteger currentFileCounter = new AtomicInteger();
    AtomicInteger totalFilesCounter = new AtomicInteger();

    AtomicInteger skipEvents = new AtomicInteger();
    AtomicInteger maxEvents = new AtomicInteger();

    AtomicInteger totalEvents = new AtomicInteger();
    AtomicInteger eventNumber = new AtomicInteger();
    AtomicInteger eofCounter = new AtomicInteger();

    AtomicLong startTime = new AtomicLong();
    AtomicLong lastReportTime = new AtomicLong();


    static class Builder {

        private final ApplicationInfo app;
        private final Map<ClaraLang, DpeInfo> dpes;

        private boolean ready = false;

        Builder(ApplicationInfo application) {
            this.app = application;
            this.dpes = new HashMap<>();

            this.app.getLanguages().forEach(lang -> this.dpes.put(lang, null));
        }

        public void addDpe(DpeInfo dpe) {
            ClaraLang lang = dpe.name.language();
            if (!dpes.containsKey(lang)) {
                Logging.info("Ignoring DPE %s (language not needed)", dpe.name);
                return;
            }
            DpeInfo prev = dpes.put(dpe.name.language(), dpe);
            if (prev != null && !prev.equals(dpe)) {
                Logging.info("Replacing existing DPE %s with %s", prev.name, dpe.name);
            }
            ready = checkReady();
        }

        public boolean isReady() {
            return ready;
        }

        public StreamProcessingNode build(CoreOrchestrator orchestrator) {
            return new StreamProcessingNode(orchestrator, new WorkerApplication(app, dpes));
        }

        private boolean checkReady() {
            for (Map.Entry<ClaraLang, DpeInfo> e : dpes.entrySet()) {
                if (e.getValue() == null) {
                    return false;
                }
            }
            return true;
        }
    }


    StreamProcessingNode(CoreOrchestrator orchestrator, WorkerApplication application) {
        if (orchestrator == null) {
            throw new IllegalArgumentException("Null orchestrator parameter");
        }
        if (application == null) {
            throw new IllegalArgumentException("Null application parameter");
        }

        this.application = application;
        this.orchestrator = orchestrator;

        this.stageName = application.stageService();
        this.readerName = application.readerService();
        this.writerName = application.writerService();
    }


    void deployServices() {
        application.getInputOutputServicesDeployInfo().forEach(orchestrator::deployService);
        application.getProcessingServicesDeployInfo().forEach(orchestrator::deployService);
        application.getMonitoringServicesDeployInfo().forEach(orchestrator::deployService);

        application.allServices().forEach(orchestrator::checkServices);
    }


    boolean checkServices() {
        return application.allServices().entrySet().stream()
            .allMatch(e -> orchestrator.findServices(e.getKey(), e.getValue()));
    }


    void subscribeErrors(Function<StreamProcessingNode, EngineCallback> callbackFn) {
        EngineCallback callback = callbackFn.apply(this);
        application.allContainers().values().stream()
            .flatMap(set -> set.stream())
            .forEach(cont -> orchestrator.subscribeErrors(cont, callback));
    }


    void subscribeDone(Function<StreamProcessingNode, EngineCallback> callbackFn) {
        orchestrator.subscribeDone(writerName, callbackFn.apply(this));
    }

    void setConfiguration(JSONObject configData) {
        this.userConfig = configData;
    }

    private ServiceConfig createServiceConfig(boolean fillDataModel) {
        Map<String, Object> model = new HashMap<>();
        if (fillDataModel && currentInputFile != null) {
            model.put("input_file", currentInputFile);
            model.put("output_file", currentOutputFile);
        }

        return new ServiceConfig(userConfig, model);
    }

    void setPaths(Path inputPath, Path outputPath, Path stagePath, String outFilePrefix) {
        try {
            JSONObject data = new JSONObject();
            data.put("input_path", inputPath);
            data.put("output_path", outputPath);
            data.put("stage_path", stagePath);
            data.put("out_prefix", outFilePrefix);
            orchestrator.syncConfig(stageName, data, 2, TimeUnit.MINUTES);
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorException("Could not configure directories", e);
        }
    }




    void setEventLimits(int skipEvents, int maxEvents) {
        this.skipEvents.set(skipEvents);
        this.maxEvents.set(maxEvents);
    }


    void setReportFrequency(int frequency) {
        // set "report done" frequency
        if (frequency <= 0) {
            return;
        }
        try {
            orchestrator.startDoneReporting(writerName, frequency);
        } catch (ClaraException e) {
            throw new OrchestratorException("Could not configure writer", e);
        }
    }



    private int requestNumberOfEvents() {
        try {
            EngineData output = orchestrator.syncSend(readerName, "count", 1, TimeUnit.MINUTES);
            return (Integer) output.getData();
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorException("Could not get number of input events", e);
        }
    }


    void configureServices() {
        ServiceConfig configuration = createServiceConfig(true);

        for (ServiceName service : application.services()) {
            try {
                orchestrator.syncConfig(service, configuration.get(service), 2, TimeUnit.MINUTES);
            } catch (OrchestratorConfigException e) {
                throw new OrchestratorException("Could not configure " + service, e);
            } catch (ClaraException | TimeoutException e) {
                throw new OrchestratorException("Could not configure " + service, e);
            }
        }

        for (ServiceName service : application.monitoringServices()) {
            try {
                orchestrator.syncEnableRing(service, 1, TimeUnit.MINUTES);
            } catch (ClaraException | TimeoutException e) {
                throw new OrchestratorException("Could not configure " + service, e);
            }
        }
    }


    void sendEvents(int maxCores) {
        long currentTime = System.currentTimeMillis();
        startTime.compareAndSet(0, currentTime);
        lastReportTime.compareAndSet(0, currentTime);

        int requestCores = numCores(maxCores);
        int requestId = 1;

        Logging.info("Using %d cores on %s to process %d events of %s [%d/%d]",
            requestCores, name(), totalEvents.get(), currentInputFileName,
            currentFileCounter.get(), totalFilesCounter.get());

        for (int i = 0; i < requestCores; i++) {
            requestEvent(requestId++, "next");
        }
    }


    void requestEvent(int requestId, String type) {
        try {
            EngineData data = new EngineData();
            data.setData(EngineDataType.STRING.mimeType(), type);
            data.setCommunicationId(requestId);
            orchestrator.send(application.composition(), data);
        } catch (ClaraException e) {
            throw new OrchestratorException("Could not send an event request to = " + name(), e);
        }
    }


    private int numCores(int maxCores) {
        int appCores = application.maxCores();
        return appCores <= maxCores ? appCores : maxCores;
    }


    int maxCores() {
        return application.maxCores();
    }


    Set<ServiceRuntimeData> getRuntimeData() {
        return application.dpes().stream()
            .map(orchestrator::getReport)
            .flatMap(Set::stream)
            .collect(Collectors.toSet());
    }


    boolean isFrontEnd() {
        String frontEndHost = orchestrator.getFrontEnd().address().host();
        return frontEndHost.equals(name());
    }


    String name() {
        return application.hostName();
    }


    Set<DpeName> dpes() {
        return Collections.unmodifiableSet(application.dpes());
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + application.hashCode();
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
        if (!(obj instanceof StreamProcessingNode)) {
            return false;
        }
        StreamProcessingNode other = (StreamProcessingNode) obj;
        if (!application.equals(other.application)) {
            return false;
        }
        return true;
    }


    @Override
    public String toString() {
        return application.toString();
    }
}
