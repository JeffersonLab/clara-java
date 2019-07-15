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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
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
    private volatile String recMsg = "Could not run data processing!";

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
     * Runs the data processing.
     *
     * @return status of the processing.
     * @throws OrchestratorException in case of any error that aborted the processing.
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
        } catch (OrchestratorException e) {
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
            } catch (OrchestratorException e) {
                Logging.error("Could not use %s for processing:%n%s",
                    node.name(), e.getMessage());
            }
        });
    }


    /**
     * Deploy and configure services in the node.
     *
     * @throws OrchestratorException
     */
    void setupNode(WorkerNode node) {
        try {
            Logging.info("Start processing on %s...", node.name());
            if (!checkChain(node)) {
                deploy(node);
            }
            subscribe(node);

            if (options.stageFiles) {
                node.setPaths(paths.inputDir, paths.outputDir, paths.stageDir, paths.prefix);
                clearLocalStage(node);
            }
            node.setConfiguration(setup.configuration);
            if (setup.configMode == OrchestratorConfigMode.DATASET) {
                Logging.info("Configuring services on %s...", node.name());
                node.configureServices();
                Logging.info("All services configured on %s", node.name());
            }
            node.setEventLimits(options.skipEvents, options.maxEvents);

            freeNodes.add(node);
            stats.add(node);
        } catch (OrchestratorException e) {
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
            int count = 0;
            for (WorkerFile file : paths.allFiles) {
                if (Files.exists(paths.inputFilePath(file))) {
                    processingQueue.add(file);
                    count++;
                }
            }
            if (count == 0) {
                throw new OrchestratorException("Input files do not exist");
            }
        }
    }


    private class FileMonitoringWorker implements Runnable {

        @Override
        public void run() {
            BlockingQueue<WorkerFile> requestedFiles = new LinkedBlockingDeque<>(paths.allFiles);
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


    private void clearLocalStage(WorkerNode node) {
        // XXX: only remove files in case the node is used exclusively
        if (options.maxThreads >= node.maxCores()) {
            node.removeStageDir();
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

    private void exitAll() {
        // check to see if .pid file exists in the log directory
        // NOTE: looks in $CLARA_USER_DATA/log dir only
        String logDir = System.getenv("CLARA_USER_DATA");
        if (logDir != null) {
            String fileName = logDir
                + File.separator
                + "log"
                + File.separator
                + "."
                + setup.session
                + "_dpe.pid";

            Path path = Paths.get(fileName);
            if (Files.exists(path)) {
                // open and read the pid
                try {
                    BufferedReader reader = new BufferedReader(new FileReader(fileName));
                    String pid = reader.readLine();
                    reader.close();
                    System.err.println("Severe Error. Exiting DPE ( PID = "
                        + pid
                        + ") and Orchestrator. Data processing will be terminated.");
                    // running external process to kill the DPE
                } catch (IOException e) {
                    e.printStackTrace();
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
        } catch (OrchestratorException e) {
            Logging.error("Could not use %s for processing:%n%s",
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
        if (setup.configMode == OrchestratorConfigMode.FILE) {
            Logging.info("Configuring services on %s...", node.name());
            node.configureServices();
        }
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
            node.currentFile(), node.name(), timePerEvent);
    }


    void processFinishedFile(WorkerNode node) {
        try {
            node.closeFiles();
            if (options.stageFiles) {
                node.saveOutputFile();
                Logging.info("Saved file %s on %s", node.currentFile(), node.name());
            }
        } catch (OrchestratorException e) {
            Logging.error("Could not close files on %s:%n%s", node.name(), e.getMessage());
        } finally {
            node.clearFiles();
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
                n.removeStageDir();
            });
        }
    }


    private class ErrorHandlerCB implements EngineCallback {

        private final WorkerNode node;

        private Timer timer;

        ErrorHandlerCB(WorkerNode node) {
            this.node = node;
        }

        @Override
        public void callback(EngineData data) {
            int severity = data.getStatusSeverity();
            String description = data.getDescription();
            if (description.equalsIgnoreCase("End of File")) {
                int eof = node.eofCounter.incrementAndGet();
                if (eof == 1) {
                    startTimer();
                }
                if (severity == 2) {
                    finishCurrentFile();
                }
            } else if (description.startsWith("Error opening the file")) {
                Logging.error(description);
            } else {
                handleEngineError(data);
            }
        }

        private synchronized void finishCurrentFile() {
            stopTimer();
            if (node.currentFile() != null) {
                printAverage(node);
                processFinishedFile(node);
            }
        }

        private synchronized void handleEngineError(EngineData data) {
            if (node.currentFile() == null) {
                return;
            }
            try {
                Logging.error("Error in %s (ID: %d):%n%s",
                    data.getEngineName(), data.getCommunicationId(), data.getDescription());
                node.requestEvent(data.getCommunicationId(), "next-rec");
            } catch (OrchestratorException e) {
                Logging.error(e.getMessage());
            }
        }

        private synchronized void startTimer() {
            TimerTask task = new EndOfFileTimerTask(30, 300, () -> {
                finishCurrentFile();
            });

            timer = new Timer();
            timer.scheduleAtFixedRate(task, 30_000, 30_000);
        }

        private synchronized void stopTimer() {
            if (timer != null) {
                timer.cancel();
            }
        }
    }


    private class EndOfFileTimerTask extends TimerTask {

        private final int delta;
        private final int timeout;
        private final Runnable timeoutAction;

        private AtomicInteger timeLeft;

        EndOfFileTimerTask(int delta, int timeout, Runnable timeoutAction) {
            this.delta = delta;
            this.timeout = timeout;
            this.timeoutAction = timeoutAction;
            this.timeLeft = new AtomicInteger(timeout);
        }

        @Override
        public void run() {
            int timeLeft = this.timeLeft.addAndGet(-delta);
            if (options.orchMode == OrchestratorMode.LOCAL) {
                Logging.info("All events read. Waiting output events for %3d seconds...",
                    timeout - timeLeft);
            }
            if (timeLeft <= 0) {
                if (options.orchMode == OrchestratorMode.LOCAL) {
                    Logging.info("Last output events are taking too long. Closing files...");
                }
                timeoutAction.run();
            }
        }
    }
}
