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

import java.nio.file.Path;
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
        application.getInputOutputServicesDeployInfo().forEach(orchestrator::deployService);
        application.getProcessingServicesDeployInfo().forEach(orchestrator::deployService);
        application.getMonitoringServicesDeployInfo().forEach(orchestrator::deployService);

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


    void setFiles(WorkerFile currentFile) {
        try {
            JSONObject data = new JSONObject();
            data.put("type", "exec");
            data.put("action", "stage_input");
            data.put("file", currentFile.inputName);

            Logging.info("Staging file %s on %s", currentFile.inputName, name());
            EngineData result = orchestrator.syncSend(stageName, data, 15, TimeUnit.MINUTES);

            if (!result.getStatus().equals(EngineStatus.ERROR)) {
                String rs = (String) result.getData();
                JSONObject rd = new JSONObject(rs);
                currentInputFile = rd.getString("input_file");
                currentOutputFile = rd.getString("output_file");
                currentInputFileName = currentFile.inputName;
            } else {
                String msg = "Could not stage input file: " + result.getDescription();
                throw new OrchestratorException(msg);
            }
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorException("Could not stage input file", e);
        }
    }


    void setFiles(OrchestratorPaths paths, WorkerFile currentFile) {
        currentInputFile = paths.inputFilePath(currentFile).toString();
        currentOutputFile = paths.outputFilePath(currentFile).toString();
        currentInputFileName = currentFile.inputName;
    }


    void setFileCounter(int currentFile, int totalFiles) {
        currentFileCounter.set(currentFile);
        totalFilesCounter.set(totalFiles);
    }


    void clearFiles() {
        currentInputFile = null;
        currentOutputFile = null;
        currentInputFileName = null;
    }


    String currentFile() {
        return currentInputFileName;
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
            throw new OrchestratorException("Could not save output", e);
        }
    }


    boolean removeStageDir() {
        try {
            JSONObject request = new JSONObject();
            request.put("type", "exec");
            request.put("action", "clear_stage");
            EngineData rr = orchestrator.syncSend(stageName, request, 5, TimeUnit.MINUTES);
            if (rr.getStatus().equals(EngineStatus.ERROR)) {
                Logging.error("Failed to remove stage directory on %s: %s",
                        name(), rr.getDescription());
                return false;
            }
            return true;
        } catch (ClaraException | TimeoutException e) {
            Logging.error("Failed to remove stage directory on %s: %s", name(), e.getMessage());
            return false;
        }
    }


    void setEventLimits(int skipEvents, int maxEvents) {
        this.skipEvents.set(skipEvents);
        this.maxEvents.set(maxEvents);
    }


    void openFiles() {
        startTime.set(0);
        lastReportTime.set(0);
        eofCounter.set(0);
        eventNumber.set(0);
        totalEvents.set(0);

        ServiceConfig configuration = createServiceConfig(false);

        int skipEv = skipEvents.get();
        int maxEv = maxEvents.get();

        // open input file
        try {
            Logging.info("Opening file %s on %s", currentInputFileName, name());
            JSONObject inputConfig = configuration.reader();
            inputConfig.put("action", "open");
            inputConfig.put("file", currentInputFile);
            if (skipEv > 0) {
                inputConfig.put("skip", skipEv);
            }
            if (maxEv > 0) {
                inputConfig.put("max", maxEv);
            }
            orchestrator.syncConfig(readerName, inputConfig, 5, TimeUnit.MINUTES);
        } catch (OrchestratorConfigException e) {
            throw new OrchestratorException("Could not configure reader", e);
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorException("Could not open input file", e);
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
            JSONObject outputConfig = configuration.writer();
            outputConfig.put("action", "open");
            outputConfig.put("file", currentOutputFile);
            outputConfig.put("order", fileOrder);
            outputConfig.put("overwrite", true);
            orchestrator.syncConfig(writerName, outputConfig, 5, TimeUnit.MINUTES);
        } catch (OrchestratorConfigException e) {
            throw new OrchestratorException("Could not configure writer", e);
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorException("Could not open output file", e);
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
            throw new OrchestratorException("Could not configure writer", e);
        }
    }


    void closeFiles() {
        try {
            JSONObject closeInput = new JSONObject();
            closeInput.put("action", "close");
            closeInput.put("file", currentInputFile);
            orchestrator.syncConfig(readerName, closeInput, 5, TimeUnit.MINUTES);
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorException("Could not close input file", e);
        }

        try {
            JSONObject closeOutput = new JSONObject();
            closeOutput.put("action", "close");
            closeOutput.put("file", currentOutputFile);
            orchestrator.syncConfig(writerName, closeOutput, 5, TimeUnit.MINUTES);
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorException("Could not close output file", e);
        }
    }


    private String requestFileOrder() {
        try {
            EngineData output = orchestrator.syncSend(readerName, "order", 1, TimeUnit.MINUTES);
            return (String) output.getData();
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorException("Could not get input file order", e);
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
