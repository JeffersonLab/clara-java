package org.jlab.clara.std.orchestrators;

import java.nio.file.Path;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.jlab.clara.base.EngineCallback;
import org.jlab.clara.base.ServiceName;
import org.jlab.clara.engine.EngineData;


abstract class AbstractOrchestrator {

    final CoreOrchestrator orchestrator;

    final OrchestratorSetup setup;
    final OrchestratorPaths paths;
    final OrchestratorOptions options;
    final ReconstructionStats stats;

    private final BlockingQueue<WorkerNode> freeNodes;
    private final ExecutorService nodesExecutor;

    private final BlockingQueue<WorkerFile> processingQueue = new LinkedBlockingQueue<>();

    private final AtomicInteger startedFilesCounter = new AtomicInteger();
    private final AtomicInteger processedFilesCounter = new AtomicInteger();

    private final Semaphore recSem;
    private volatile boolean recStatus;
    private volatile String recMsg = "Could not run reconstruction!";

    static class ReconstructionStats {

        private final Map<WorkerNode, NodeStats> recStats = new ConcurrentHashMap<>();
        private final AtomicLong startTime = new AtomicLong();
        private final AtomicLong endTime = new AtomicLong();

        private static class NodeStats {
            private int events = 0;
            private long totalTime = 0;
        }

        void add(WorkerNode node) {
            recStats.put(node, new NodeStats());
        }

        void startClock() {
            startTime.compareAndSet(0, System.currentTimeMillis());
        }

        void stopClock() {
            endTime.set(System.currentTimeMillis());
        }

        void update(WorkerNode node, int recEvents, long recTime) {
            NodeStats nodeStats = recStats.get(node);
            synchronized (nodeStats) {
                nodeStats.events += recEvents;
                nodeStats.totalTime += recTime;
            }
        }

        long totalEvents() {
            long sum = 0;
            for (Entry<WorkerNode, NodeStats> entry : recStats.entrySet()) {
                NodeStats stat = entry.getValue();
                synchronized (stat) {
                    if (stat.events > 0) {
                        sum += stat.events;
                    }
                }
            }
            return sum;
        }

        long totalTime() {
            long sum = 0;
            for (Entry<WorkerNode, NodeStats> entry : recStats.entrySet()) {
                NodeStats stat = entry.getValue();
                synchronized (stat) {
                    if (stat.events > 0) {
                        sum += stat.totalTime;
                    }
                }
            }
            return sum;
        }

        double globalTime() {
            return endTime.get() - startTime.get();
        }

        double localAverage() {
            double avgSum = 0;
            int avgCount = 0;
            for (Entry<WorkerNode, NodeStats> entry : recStats.entrySet()) {
                NodeStats stat = entry.getValue();
                synchronized (stat) {
                    if (stat.events > 0) {
                        avgSum += stat.totalTime / (double) stat.events;
                        avgCount++;
                    }
                }
            }
            return avgSum / avgCount;
        }

        double globalAverage() {
            return globalTime() / totalEvents();
        }
    }


    AbstractOrchestrator(OrchestratorSetup setup,
                         OrchestratorPaths paths,
                         OrchestratorOptions options) {
        this.orchestrator = new CoreOrchestrator(setup, options.poolSize);

        this.setup = setup;
        this.paths = paths;
        this.options = options;

        this.freeNodes = new LinkedBlockingQueue<>();
        this.nodesExecutor = Executors.newCachedThreadPool();

        this.recSem = new Semaphore(1);
        this.stats = new ReconstructionStats();
    }


    /**
     * Runs the reconstruction.
     *
     * @return status of the reconstruction.
     * @throws OrchestratorError in case of any error that aborted the reconstruction
     */
    public boolean run() {
        try {
            start();
            checkFiles();
            startRec();
            waitRec();
            end();
            destroy();
            return recStatus;
        } catch (OrchestratorError e) {
            // cleanup();
            destroy();
            throw e;
        }
    }


    protected abstract void start();

    protected abstract void end();


    private void startRec() {
        try {
            recSem.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException("Could not block processing.");
        }
        try {
            processAllFiles();
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted");
        }
    }


    private void waitRec() {
        try {
            recSem.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
            recMsg = "Processing interrupted...";
        }
    }


    void exitRec(boolean status, String msg) {
        recStatus = status;
        recMsg = msg;
        recSem.release();
    }


    void destroy() {
        nodesExecutor.shutdown();
        Logging.info(recMsg);
    }


    /**
     * Deploy and configure services in the node using a worker thread.
     *
     * @throws RejectedExecutionException
     */
    void executeSetup(WorkerNode node) {
        nodesExecutor.execute(() -> {
            try {
                setupNode(node);
            } catch (OrchestratorError e) {
                Logging.error("Could not use %s for reconstruction:%n%s",
                              node.name(), e.getMessage());
            }
        });
    }


    /**
     * Deploy and configure services in the node.
     *
     * @throws OrchestratorError
     */
    void setupNode(WorkerNode node) {
        try {
            Logging.info("Start processing on %s...", node.name());
            if (!checkChain(node)) {
                deploy(node);
            }
            subscribe(node);

            Logging.info("Configuring services on %s...", node.name());
            if (options.stageFiles) {
                node.setPaths(paths.inputDir, paths.outputDir, paths.stageDir);
            }
            node.configureServices(setup.globalConfig);
            node.setEventLimits(options.skipEvents, options.maxEvents);
            Logging.info("All services configured on %s", node.name());

            freeNodes.add(node);
            stats.add(node);
        } catch (OrchestratorError e) {
            // TODO cleanup
            throw e;
        }
    }


    boolean checkChain(WorkerNode node) {
        Logging.info("Searching services in %s...", node.name());
        if (node.checkServices()) {
            Logging.info("All services already deployed on %s", node.name());
            return true;
        }
        return false;
    }


    void deploy(WorkerNode node) {
        Logging.info("Deploying services in %s...", node.name());
        node.deployServices();
        Logging.info("All services deployed on %s", node.name());
    }


    void subscribe(WorkerNode node) {
        node.subscribeErrors(ErrorHandlerCB::new);
    }


    private void checkFiles() {
        if (options.stageFiles) {
            Logging.info("Monitoring files on input directory...");
            new Thread(new FileMonitoringWorker(), "file-monitoring-thread").start();
        } else {
            for (WorkerFile file : paths.allFiles) {
                processingQueue.add(file);
            }
        }
    }


    private class FileMonitoringWorker implements Runnable {

        @Override
        public void run() {
            BlockingQueue<WorkerFile> requestedFiles = new LinkedBlockingDeque<>();
            for (WorkerFile file : paths.allFiles) {
                requestedFiles.add(file);
            }
            while (!requestedFiles.isEmpty()) {
                WorkerFile recFile = requestedFiles.element();
                Path filePath = paths.inputFilePath(recFile);
                if (filePath.toFile().exists()) {
                    processingQueue.add(recFile);
                    requestedFiles.remove();
                    Logging.info("File %s is cached", filePath);
                } else {
                    orchestrator.sleep(100);
                }
            }
        }
    }


    private void processAllFiles() throws InterruptedException {
        if (options.maxNodes < OrchestratorOptions.MAX_NODES) {
            while (freeNodes.size() < options.maxNodes) {
                orchestrator.sleep(100);
            }
        }
        while (processedFilesCounter.get() < paths.numFiles()) {
            WorkerFile recFile = processingQueue.peek();
            if (recFile == null) {
                orchestrator.sleep(100);
                continue;
            }
            // TODO check if file exists
            final WorkerNode node = freeNodes.poll(60, TimeUnit.SECONDS);
            if (node != null) {
                try {
                    nodesExecutor.execute(() -> processFile(node, recFile));
                    processingQueue.remove();
                } catch (RejectedExecutionException e) {
                    freeNodes.add(node);
                }
            }
        }
    }


    void processFile(WorkerNode node, WorkerFile recFile) {
        try {
            stats.startClock();
            // TODO check DPE is alive
            openFiles(node, recFile);
            startFile(node);
        } catch (OrchestratorError e) {
            Logging.error("Could not use %s for reconstruction:%n%s",
                    node.name(), e.getMessage());
        }
    }


    void openFiles(WorkerNode node, WorkerFile recFile) {
        if (options.stageFiles) {
            node.setFiles(recFile);
        } else {
            node.setFiles(paths, recFile);
        }
        node.openFiles();
    }


    void startFile(WorkerNode node) {
        node.setReportFrequency(options.reportFreq);

        int fileCounter = startedFilesCounter.incrementAndGet();
        int totalFiles = paths.numFiles();
        node.setFileCounter(fileCounter, totalFiles);

        node.sendEvents(options.maxThreads);
    }


    void printAverage(WorkerNode node) {
        long endTime = System.currentTimeMillis();
        long recTime = endTime - node.startTime.get();
        double timePerEvent = recTime / (double) node.totalEvents.get();
        stats.update(node, node.totalEvents.get(), recTime);
        Logging.info("Finished file %s on %s. Average event time = %.2f ms",
                     node.recFile.inputName, node.name(), timePerEvent);
    }


    void processFinishedFile(WorkerNode node) {
        try {
            String currentFile = node.recFile.inputName;
            node.closeFiles();
            if (options.stageFiles) {
                node.saveOutputFile();
                Logging.info("Saved file %s on %s", currentFile, node.name());
            }
        } catch (OrchestratorError e) {
            Logging.error("Could not close files on %s:%n%s", node.name(), e.getMessage());
        } finally {
            incrementFinishedFile();
            freeNodes.add(node);
        }
    }

    private boolean incrementFinishedFile() {
        int counter = processedFilesCounter.incrementAndGet();
        boolean finished = counter == paths.numFiles();
        if (finished) {
            stats.stopClock();
            exitRec(true, "Processing is complete.");
        }
        return finished;
    }


    void removeStageDirectories() {
        if (options.stageFiles) {
            freeNodes.stream().parallel().forEach(n -> {
                try {
                    n.removeStageDir();
                } catch (OrchestratorError e) {
                    Logging.error("Could not remove stage directory from %s: %s",
                            n.name(), e.getMessage());
                }
            });
        }
    }


    private class ErrorHandlerCB implements EngineCallback {

        private final WorkerNode node;

        ErrorHandlerCB(WorkerNode node) {
            this.node = node;
        }

        @Override
        public void callback(EngineData data) {
            ServiceName source = new ServiceName(data.getEngineName());
            int requestId = data.getCommunicationId();
            int severity = data.getStatusSeverity();
            String description = data.getDescription();
            if (description.equalsIgnoreCase("End of File")) {
                if (severity == 2) {
                    printAverage(node);
                    processFinishedFile(node);
                }
            } else if (description.startsWith("Error opening the file")) {
                Logging.error(description);
            } else {
                try {
                    Logging.error("Error in %s (ID: %d):%n%s", source, requestId, description);
                    node.requestEvent(requestId, "next-rec");
                } catch (OrchestratorError e) {
                    Logging.error(e.getMessage());
                }
            }
        }
    }
}
