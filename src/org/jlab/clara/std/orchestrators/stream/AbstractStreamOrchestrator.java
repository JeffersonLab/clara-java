package org.jlab.clara.std.orchestrators.stream;

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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.jlab.clara.base.EngineCallback;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.std.orchestrators.*;


abstract class AbstractStreamOrchestrator {

    final CoreOrchestrator orchestrator;

    final OrchestratorSetup setup;
    final OrchestratorPaths paths;
    final OrchestratorOptions options;
    final AbstractStreamOrchestrator.ReconstructionStats stats;

    private final BlockingQueue<StreamProcessingNode> freeNodes;
    private final ExecutorService nodesExecutor;

    private final AtomicInteger startedFilesCounter = new AtomicInteger();
    private final AtomicInteger processedFilesCounter = new AtomicInteger();

    private final Semaphore recSem;
    private volatile boolean recStatus;
    private volatile String recMsg = "Could not run data processing!";

    static class ReconstructionStats {

        private final Map<StreamProcessingNode, AbstractStreamOrchestrator.ReconstructionStats.NodeStats> recStats = new ConcurrentHashMap<>();
        private final AtomicLong startTime = new AtomicLong();
        private final AtomicLong endTime = new AtomicLong();

        private static class NodeStats {
            private int events = 0;
            private long totalTime = 0;
        }

        void add(StreamProcessingNode node) {
            recStats.put(node, new AbstractStreamOrchestrator.ReconstructionStats.NodeStats());
        }

        void startClock() {
            startTime.compareAndSet(0, System.currentTimeMillis());
        }

        void stopClock() {
            endTime.set(System.currentTimeMillis());
        }

        void update(StreamProcessingNode node, int recEvents, long recTime) {
            AbstractStreamOrchestrator.ReconstructionStats.NodeStats nodeStats = recStats.get(node);
            synchronized (nodeStats) {
                nodeStats.events += recEvents;
                nodeStats.totalTime += recTime;
            }
        }

        long totalEvents() {
            long sum = 0;
            for (Entry<StreamProcessingNode, AbstractStreamOrchestrator.ReconstructionStats.NodeStats> entry : recStats.entrySet()) {
                AbstractStreamOrchestrator.ReconstructionStats.NodeStats stat = entry.getValue();
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
            for (Entry<StreamProcessingNode, AbstractStreamOrchestrator.ReconstructionStats.NodeStats> entry : recStats.entrySet()) {
                AbstractStreamOrchestrator.ReconstructionStats.NodeStats stat = entry.getValue();
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
            for (Entry<StreamProcessingNode, AbstractStreamOrchestrator.ReconstructionStats.NodeStats> entry : recStats.entrySet()) {
                AbstractStreamOrchestrator.ReconstructionStats.NodeStats stat = entry.getValue();
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


    AbstractStreamOrchestrator(OrchestratorSetup setup,
                         OrchestratorPaths paths,
                         OrchestratorOptions options) {
        this.orchestrator = new CoreOrchestrator(setup, options.poolSize);

        this.setup = setup;
        this.paths = paths;
        this.options = options;

        this.freeNodes = new LinkedBlockingQueue<>();
        this.nodesExecutor = Executors.newCachedThreadPool();

        this.recSem = new Semaphore(1);
        this.stats = new AbstractStreamOrchestrator.ReconstructionStats();
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
//        try {
//            processAllFiles(); @todo ===============>
//        } catch (InterruptedException e) {
//            throw new RuntimeException("Interrupted");
//        }
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
    public void executeSetup(StreamProcessingNode node) {
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
    void setupNode(StreamProcessingNode node) {
        try {
            Logging.info("Start processing on %s...", node.name());
            if (!checkChain(node)) {
                deploy(node);
            }
            subscribe(node);

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
            System.exit(1);
            // TODO cleanup
            throw e;
        }
    }


    boolean checkChain(StreamProcessingNode node) {
        Logging.info("Searching services in %s...", node.name());
        if (node.checkServices()) {
            Logging.info("All services already deployed on %s", node.name());
            return true;
        }
        return false;
    }


    void deploy(StreamProcessingNode node) {
        Logging.info("Deploying services in %s...", node.name());
        node.deployServices();
        Logging.info("All services deployed on %s", node.name());
    }


    void subscribe(StreamProcessingNode node) {
        node.subscribeErrors(AbstractStreamOrchestrator.ErrorHandlerCB::new);
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



    void printAverage(StreamProcessingNode node) {
        long endTime = System.currentTimeMillis();
        long recTime = endTime - node.startTime.get();
        double timePerEvent = recTime / (double) node.totalEvents.get();
        stats.update(node, node.totalEvents.get(), recTime);
        Logging.info("Finished processing on %s. Average event time = %.2f ms",
            node.name(), timePerEvent);
    }



    private class ErrorHandlerCB implements EngineCallback {

        private final StreamProcessingNode node;

        private Timer timer;

        ErrorHandlerCB(StreamProcessingNode node) {
            this.node = node;
        }

        @Override
        public void callback(EngineData data) {
                 handleEngineError(data);
        }

        private synchronized void handleEngineError(EngineData data) {
            try {
                Logging.error("Error in %s (ID: %d):%n%s",
                    data.getEngineName(), data.getCommunicationId(), data.getDescription());
                node.requestEvent(data.getCommunicationId(), "next-rec");
            } catch (OrchestratorException e) {
                Logging.error(e.getMessage());
            }
        }


        private synchronized void stopTimer() {
            if (timer != null) {
                timer.cancel();
            }
        }
    }



}
