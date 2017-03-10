package org.jlab.clara.std.orchestrators;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
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
import org.jlab.clara.engine.EngineStatus;
import org.json.JSONObject;


class WorkerNode {

    private static final String NO_NAME = "undefined";

    private final CoreOrchestrator orchestrator;
    private final WorkerApplication application;

    private final ServiceName stageName;
    private final ServiceName readerName;
    private final ServiceName writerName;

    volatile WorkerFile recFile;

    volatile String currentInputFileName;
    volatile String currentInputFile;
    volatile String currentOutputFile;

    AtomicInteger currentFileCounter = new AtomicInteger();
    AtomicInteger totalFilesCounter = new AtomicInteger();

    AtomicInteger skipEvents = new AtomicInteger();
    AtomicInteger maxEvents = new AtomicInteger();

    AtomicInteger totalEvents = new AtomicInteger();
    AtomicInteger eventNumber = new AtomicInteger();
    AtomicLong startTime = new AtomicLong();


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

        public WorkerNode build(CoreOrchestrator orchestrator) {
            return new WorkerNode(orchestrator, new WorkerApplication(app, dpes));
        }

        private boolean checkReady() {
            for (Entry<ClaraLang, DpeInfo> e : dpes.entrySet()) {
                if (e.getValue() == null) {
                    return false;
                }
            }
            return true;
        }
    }


    WorkerNode(CoreOrchestrator orchestrator, WorkerApplication application) {
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
        application.getIODeployInfo().forEach(orchestrator::deployService);
        application.getRecDeployInfo().forEach(orchestrator::deployService);

        application.allServices().forEach(orchestrator::checkServices);
    }


    boolean checkServices() {
        return application.allServices().entrySet().stream()
                .allMatch(e -> orchestrator.findServices(e.getKey(), e.getValue()));
    }


    void subscribeErrors(Function<WorkerNode, EngineCallback> callbackFn) {
        EngineCallback callback = callbackFn.apply(this);
        application.allContainers().values().stream()
                   .flatMap(set -> set.stream())
                   .forEach(cont -> orchestrator.subscribeErrors(cont, callback));
    }


    void subscribeDone(Function<WorkerNode, EngineCallback> callbackFn) {
        orchestrator.subscribeDone(writerName, callbackFn.apply(this));
    }


    void setPaths(String inputPath, String outputPath, String stagePath) {
        try {
            JSONObject data = new JSONObject();
            data.put("input_path", inputPath);
            data.put("output_path", outputPath);
            data.put("stage_path", stagePath);
            orchestrator.syncConfig(stageName, data, 2, TimeUnit.MINUTES);
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorError("Could not configure directories", e);
        }
    }


    boolean setFiles(String inputFileName) {
        try {
            currentInputFileName = inputFileName;
            currentInputFile = NO_NAME;
            currentOutputFile = NO_NAME;

            JSONObject data = new JSONObject();
            data.put("type", "exec");
            data.put("action", "stage_input");
            data.put("file", currentInputFileName);

            Logging.info("Staging file %s on %s", currentInputFileName, name());
            EngineData result = orchestrator.syncSend(stageName, data, 5, TimeUnit.MINUTES);

            if (!result.getStatus().equals(EngineStatus.ERROR)) {
                String rs = (String) result.getData();
                JSONObject rd = new JSONObject(rs);
                currentInputFile = rd.getString("input_file");
                currentOutputFile = rd.getString("output_file");
                return true;
            } else {
                System.err.println(result.getDescription());
                currentInputFileName = NO_NAME;
                return false;
            }
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorError("Could not configure directories", e);
        }
    }


    void setFiles(String inputFile, String outputFile) {
        currentInputFile = inputFile;
        currentOutputFile = outputFile;
        currentInputFileName = new File(inputFile).getName();
    }


    void setFileCounter(int currentFile, int totalFiles) {
        currentFileCounter.set(currentFile);
        totalFilesCounter.set(totalFiles);
    }


    boolean saveOutputFile() {
        try {
            JSONObject cleanRequest = new JSONObject();
            cleanRequest.put("type", "exec");
            cleanRequest.put("action", "remove_input");
            cleanRequest.put("file", currentInputFileName);
            EngineData rr = orchestrator.syncSend(stageName, cleanRequest, 5, TimeUnit.MINUTES);

            JSONObject saveRequest = new JSONObject();
            saveRequest.put("type", "exec");
            saveRequest.put("action", "save_output");
            saveRequest.put("file", currentInputFileName);
            EngineData rs = orchestrator.syncSend(stageName, saveRequest, 5, TimeUnit.MINUTES);

            currentInputFileName = NO_NAME;
            currentInputFile = NO_NAME;
            currentOutputFile = NO_NAME;

            boolean status = true;
            if (rr.getStatus().equals(EngineStatus.ERROR)) {
                System.err.println(rr.getDescription());
                status = false;
            }
            if (rs.getStatus().equals(EngineStatus.ERROR)) {
                status = false;
                System.err.println(rs.getDescription());
            }

            return status;
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorError("Could not save output", e);
        }
    }


    boolean removeStageDir() {
        try {
            JSONObject request = new JSONObject();
            request.put("type", "exec");
            request.put("action", "clear_stage");
            request.put("file", currentInputFileName);
            EngineData rr = orchestrator.syncSend(stageName, request, 5, TimeUnit.MINUTES);
            if (rr.getStatus().equals(EngineStatus.ERROR)) {
                System.err.println(rr.getDescription());
                return false;
            }
            return true;
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorError("Could not remove stage directory", e);
        }
    }


    void setEventLimits(int skipEvents, int maxEvents) {
        this.skipEvents.set(skipEvents);
        this.maxEvents.set(maxEvents);
    }


    void openFiles() {
        startTime.set(0);
        eventNumber.set(0);
        totalEvents.set(0);

        int skipEv = skipEvents.get();
        int maxEv = maxEvents.get();

        // open input file
        try {
            Logging.info("Opening file %s on %s", currentInputFileName, name());
            JSONObject inputConfig = new JSONObject();
            inputConfig.put("action", "open");
            inputConfig.put("file", currentInputFile);
            if (skipEv > 0) {
                inputConfig.put("skip", skipEv);
            }
            if (maxEv > 0) {
                inputConfig.put("max", maxEv);
            }
            orchestrator.syncConfig(readerName, inputConfig, 5, TimeUnit.MINUTES);
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorError("Could not open input file", e);
        }

        // total number of events in the file
        int numEv = requestNumberOfEvents() - skipEv;
        if (maxEv > 0 && maxEv < numEv) {
            numEv = maxEv;
        }
        totalEvents.set(numEv);

        // endiannes of the file
        String fileOrder = requestFileOrder();

        // open output file
        try {
            JSONObject outputConfig = new JSONObject();
            outputConfig.put("action", "open");
            outputConfig.put("file", currentOutputFile);
            outputConfig.put("order", fileOrder);
            outputConfig.put("overwrite", true);
            orchestrator.syncConfig(writerName, outputConfig, 5, TimeUnit.MINUTES);
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorError("Could not open output file", e);
        }
    }


    void setReportFrequency(int frequency) {
        // set "report done" frequency
        if (frequency <= 0) {
            return;
        }
        try {
            orchestrator.startDoneReporting(writerName, frequency);
        } catch (ClaraException e) {
            throw new OrchestratorError("Could not configure writer", e);
        }
    }


    void closeFiles() {
        try {
            JSONObject closeInput = new JSONObject();
            closeInput.put("action", "close");
            closeInput.put("file", currentInputFile);
            orchestrator.syncConfig(readerName, closeInput, 5, TimeUnit.MINUTES);
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorError("Could not close input file", e);
        }

        try {
            JSONObject closeOutput = new JSONObject();
            closeOutput.put("action", "close");
            closeOutput.put("file", currentOutputFile);
            orchestrator.syncConfig(writerName, closeOutput, 5, TimeUnit.MINUTES);
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorError("Could not close output file", e);
        }
    }


    private String requestFileOrder() {
        try {
            EngineData output = orchestrator.syncSend(readerName, "order", 1, TimeUnit.MINUTES);
            return (String) output.getData();
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorError("Could not get input file order", e);
        }
    }


    private int requestNumberOfEvents() {
        try {
            EngineData output = orchestrator.syncSend(readerName, "count", 1, TimeUnit.MINUTES);
            return (Integer) output.getData();
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorError("Could not get number of input events", e);
        }
    }


    void configureServices(JSONObject configData) {
        for (ServiceName service : application.recServices()) {
            try {
                orchestrator.syncConfig(service, configData, 2, TimeUnit.MINUTES);
            } catch (ClaraException | TimeoutException e) {
                throw new OrchestratorError("Could not configure " + service, e);
            }
        }
    }


    void sendEvents(int maxCores) {
        startTime.compareAndSet(0, System.currentTimeMillis());

        int requestCores = numCores(maxCores);
        int requestId = 1;

        Logging.info("Using %d cores on %s to reconstruct %d events of %s [%d/%d]",
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
            throw new OrchestratorError("Could not request reconstruction on = " + name(), e);
        }
    }


    private int numCores(int maxCores) {
        int appCores = application.maxCores();
        return appCores <= maxCores ? appCores : maxCores;
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
        if (!(obj instanceof WorkerNode)) {
            return false;
        }
        WorkerNode other = (WorkerNode) obj;
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
