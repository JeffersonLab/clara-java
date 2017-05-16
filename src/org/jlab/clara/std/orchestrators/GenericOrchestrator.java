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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.jlab.clara.base.ClaraLang;
import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.base.DpeName;
import org.jlab.clara.base.EngineCallback;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.std.orchestrators.CoreOrchestrator.DpeCallBack;
import org.json.JSONObject;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

/**
 * A generic orchestrator that runs a simple application loop over a set of
 * input files.
 */
public final class GenericOrchestrator extends AbstractOrchestrator {

    private final DpeReportCB dpeCallback;
    private final Benchmark benchmark;

    private long orchTimeStart;
    private long orchTimeEnd;


    public static void main(String[] args) {
        try {
            CommandLineBuilder cl = new CommandLineBuilder(args);
            if (!cl.success()) {
                System.err.printf("Usage:%n%n  cloud-orchestrator %s%n%n%n", cl.usage());
                System.err.print(cl.help());
                System.exit(1);
            }
            GenericOrchestrator fo = cl.build();
            boolean status = fo.run();
            if (status) {
                System.exit(0);
            } else {
                System.exit(1);
            }
        } catch (OrchestratorConfigError e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        } catch (OrchestratorError e) {
            Logging.error(e.getMessage());
            Logging.error("Exiting...");
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }


    /**
     * Helps constructing a {@link GenericOrchestrator} with all default and
     * required parameters.
     */
    public static class Builder {

        private final Map<String, ServiceInfo> ioServices;
        private final List<ServiceInfo> recChain;

        private final Set<String> dataTypes;
        private final JSONObject config;

        private final OrchestratorPaths.Builder paths;
        private final OrchestratorOptions.Builder options = OrchestratorOptions.builder();

        private DpeName frontEnd = OrchestratorConfigParser.localDpeName();
        private String session = "";

        /**
         * Sets the required arguments to start a reconstruction.
         *
         * @param servicesFile the YAML file describing the reconstruction chain
         * @param inputFiles the list of files to be processed (only names).
         * @throws OrchestratorConfigError if the reconstruction chain could not be parsed
         */
        public Builder(String servicesFile, List<String> inputFiles) {
            Objects.requireNonNull(servicesFile, "servicesFile parameter is null");
            Objects.requireNonNull(inputFiles, "inputFiles parameter is null");
            if (inputFiles.isEmpty()) {
                throw new IllegalArgumentException("inputFiles list is empty");
            }
            OrchestratorConfigParser parser = new OrchestratorConfigParser(servicesFile);
            this.ioServices = parser.parseInputOutputServices();
            this.recChain = parser.parseReconstructionChain();
            this.paths = new OrchestratorPaths.Builder(inputFiles);
            this.dataTypes = parser.parseDataTypes();
            this.config = parser.parseReconstructionConfig();
        }

        /**
         * Sets the name of the front-end. Use this if the orchestrator is not
         * running in the same node as the front-end, or if the orchestrator is
         * not using the proper network interface or port for the front-end.
         *
         * @param frontEnd the name of the front-end DPE
         * @return this object, so methods can be chained
         */
        public Builder withFrontEnd(DpeName frontEnd) {
            Objects.requireNonNull(frontEnd, "frontEnd parameter is null");
            this.frontEnd = frontEnd;
            return this;
        }

        /**
         * Uses a cloud of worker DPEs to process the set of input files.
         * By default, the orchestrator runs on local mode, which only uses the
         * local front-end DPE.
         *
         * @return this object, so methods can be chained
         */
        public Builder cloudMode() {
            options.cloudMode();
            return this;
        }

        /**
         * Sets the session used by the DPEs of interest.
         * The orchestrator will connect an use only the DPEs registered with
         * the given session, ignoring all others.
         *
         * @param session the session of interest
         * @return this object, so methods can be chained
         */
        public Builder withSession(String session) {
            Objects.requireNonNull(session, "session parameter is null");
            this.session = session;
            return this;
        }

        /**
         * Uses the front-end for reconstruction. By default, the front-end is
         * only used for registration and discovery.
         *
         * @return this object, so methods can be chained
         */
        public Builder useFrontEnd() {
            options.useFrontEnd();
            return this;
        }

        /**
         * Stages the input file on the node for local access.
         * By default, all files are expected to be on the input directory.
         * <p>
         * When staging is used, the files will be copied on demand from the
         * input directory into the staging directory before using it.
         * The output file will also be saved in the stating directory. When the
         * reconstruction is finished, it will be moved back to the output
         * directory.
         *
         * @return this object, so methods can be chained
         * @see #withStageDirectory(String)
         */
        public Builder useStageDirectory() {
            options.stageFiles();
            return this;
        }

        /**
         * Sets the size of the thread-pool that will process reports from
         * services and nodes.
         *
         * @param poolSize the pool size for the orchestrator
         * @return this object, so methods can be chained
         */
        public Builder withPoolSize(int poolSize) {
            options.withPoolSize(poolSize);
            return this;
        }

        /**
         * Sets the maximum number of threads to be used for reconstruction on
         * every node.
         *
         * @param maxThreads how many parallel threads should be used on the DPEs
         * @return this object, so methods can be chained
         */
        public Builder withMaxThreads(int maxThreads) {
            options.withMaxThreads(maxThreads);
            return this;
        }

        /**
         * Sets the maximum number of nodes to be used for reconstruction.
         *
         * @param maxNodes how many worker nodes should be used to process input files
         * @return this object, so methods can be chained
         */
        public Builder withMaxNodes(int maxNodes) {
            options.withMaxNodes(maxNodes);
            return this;
        }

        /**
         * Sets the frequency of the "done" event reports.
         *
         * @param frequency the frequency of done reports
         * @return this object, so methods can be chained
         */
        public Builder withReportFrequency(int frequency) {
            options.withReportFrequency(frequency);
            return this;
        }

        /**
         * Sets the number of events to skip.
         *
         * @param skip how many events to skip
         * @return this object, so methods can be chained
         */
        public Builder withSkipEvents(int skip) {
            options.withSkipEvents(skip);
            return this;
        }

        /**
         * Sets the maximum number of events to read.
         *
         * @param max how many events to process
         * @return this object, so methods can be chained
         */
        public Builder withMaxEvents(int max) {
            options.withMaxEvents(max);
            return this;
        }

        /**
         * Changes the path of the shared input directory.
         * This directory should contain all input files.
         *
         * @param inputDir the input directory
         * @return this object, so methods can be chained
         */
        public Builder withInputDirectory(String inputDir) {
            Objects.requireNonNull(inputDir, "inputDir parameter is null");
            if (inputDir.isEmpty()) {
                throw new IllegalArgumentException("inputDir parameter is empty");
            }
            paths.withInputDir(inputDir);
            return this;
        }

        /**
         * Changes the path of the shared output directory.
         * This directory will contain all reconstructed output files.
         *
         * @param outputDir the output directory
         * @return this object, so methods can be chained
         */
        public Builder withOutputDirectory(String outputDir) {
            Objects.requireNonNull(outputDir, "outputDir parameter is null");
            if (outputDir.isEmpty()) {
                throw new IllegalArgumentException("outputDir parameter is empty");
            }
            paths.withOutputDir(outputDir);
            return this;
        }

        /**
         * Changes the path of the local staging directory.
         * Files will be staged in this directory of every worker node
         * for fast access.
         *
         * @param stageDir the local staging directory
         * @return this object, so methods can be chained
         */
        public Builder withStageDirectory(String stageDir) {
            Objects.requireNonNull(stageDir, "stageDir parameter is null");
            if (stageDir.isEmpty()) {
                throw new IllegalArgumentException("stageDir parameter is empty");
            }
            paths.withStageDir(stageDir);
            return this;
        }

        /**
         * Creates the orchestrator.
         *
         * @return a new orchestrator object configured as requested
         */
        public GenericOrchestrator build() {
            OrchestratorSetup setup = new OrchestratorSetup(
                    frontEnd, ioServices, recChain,
                    dataTypes, config, session);
            return new GenericOrchestrator(setup, paths.build(), options.build());
        }
    }


    private GenericOrchestrator(OrchestratorSetup setup,
                                OrchestratorPaths paths,
                                OrchestratorOptions options) {
        super(setup, paths, options);
        Logging.verbose(true);
        dpeCallback = new DpeReportCB(orchestrator, options, setup.application,
                                      this::executeSetup);
        benchmark = new Benchmark(setup.application);
    }


    @Override
    protected void start() {
        orchTimeStart = System.currentTimeMillis();
        printStartup();
        waitFrontEnd();

        if (options.orchMode == OrchestratorMode.LOCAL) {
            Logging.info("Waiting for local node...");
        } else {
            Logging.info("Waiting for worker nodes...");
        }
        orchestrator.subscribeDpes(dpeCallback, setup.session);
        tryLocalNode();
        waitFirstNode();

        if (options.orchMode == OrchestratorMode.LOCAL) {
            WorkerNode localNode = dpeCallback.getLocalNode();
            benchmark.initialize(localNode.getRuntimeData());
        }
    }


    @Override
    void subscribe(WorkerNode node) {
        super.subscribe(node);
        if (options.orchMode == OrchestratorMode.LOCAL) {
            node.subscribeDone(n -> new DataHandlerCB(node, options));
        }
    }


    @Override
    protected void end() {
        removeStageDirectories();
        if (options.orchMode == OrchestratorMode.LOCAL) {
            try {
                WorkerNode localNode = dpeCallback.getLocalNode();
                benchmark.update(localNode.getRuntimeData());
                BenchmarkPrinter printer = new BenchmarkPrinter(benchmark, stats.totalEvents());
                printer.printBenchmark(setup.application);
            } catch (OrchestratorError e) {
                Logging.error("Could not generate benchmark: %s", e.getMessage());
            }
            orchTimeEnd = System.currentTimeMillis();
            float recTimeMs = stats.totalTime() / 1000.0f;
            float totalTimeMs = (orchTimeEnd - orchTimeStart) / 1000.0f;
            Logging.info("Average processing time  = %7.2f ms", stats.localAverage());
            Logging.info("Total processing time    = %7.2f s", recTimeMs);
            Logging.info("Total orchestrator time  = %7.2f s", totalTimeMs);
        } else {
            Logging.info("Local  average event processing time = %7.2f ms", stats.localAverage());
            Logging.info("Global average event processing time = %7.2f ms", stats.globalAverage());
        }
    }


    /**
     * Prints a startup message when the orchestrator starts to run.
     */
    protected void printStartup() {
        System.out.println("****************************************");
        System.out.println("*          CLARA Orchestrator          *");
        System.out.println("****************************************");
        System.out.println("- Front-end    = " + setup.frontEnd);
        System.out.println("- Start time   = " + ClaraUtil.getCurrentTime());
        System.out.println("- Threads      = " + options.maxThreads);
        System.out.println();
        System.out.println("- Input directory  = " + paths.inputDir);
        System.out.println("- Output directory = " + paths.outputDir);
        if (options.stageFiles) {
            System.out.println("- Stage directory  = " + paths.stageDir);
        }
        System.out.println("- Number of files  = " + paths.numFiles());
        System.out.println("****************************************");
    }


    private void waitFrontEnd() {
        final int maxAttempts = 5;
        int count = 0;
        while (true) {
            try {
                int timeout = count == 0 ? 2 : 4;
                orchestrator.getLocalRegisteredDpes(timeout);
                break;
            } catch (OrchestratorError e) {
                if (++count == maxAttempts) {
                    throw e;
                }
                Logging.error("Could not connect with front-end. Retrying...");
            }
        }
    }


    private void tryLocalNode() {
        if (options.useFrontEnd) {
            int cores = Runtime.getRuntime().availableProcessors();
            // TODO: filter local DPEs with non-matching sessions
            Map<ClaraLang, DpeName> localDpes = orchestrator.getLocalRegisteredDpes(2).stream()
                    .collect(Collectors.toMap(DpeName::language, Function.identity()));

            Set<ClaraLang> appLangs = setup.application.getLanguages();
            Set<ClaraLang> dpeLangs = localDpes.keySet();

            if (dpeLangs.containsAll(appLangs)) {
                for (ClaraLang lang : appLangs) {
                    DpeName dpe = localDpes.get(lang);
                    dpeCallback.callback(new DpeInfo(dpe, cores, DpeInfo.DEFAULT_CLARA_HOME));
                }
            }
        }
    }


    private void waitFirstNode() {
        try {
            if (!dpeCallback.waitFirstNode()) {
                throw new OrchestratorError("could not find a reconstruction node");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }


    static class DpeReportCB implements DpeCallBack {

        private final CoreOrchestrator orchestrator;
        private final OrchestratorOptions options;
        private final ApplicationInfo application;
        private final DpeName frontEnd;

        private final Consumer<WorkerNode> nodeConsumer;

        private final Map<String, WorkerNode.Builder> waitingNodes = new HashMap<>();
        private final Map<String, WorkerNode> availableNodes = new HashMap<>();

        private final CountDownLatch latch = new CountDownLatch(1);

        DpeReportCB(CoreOrchestrator orchestrator,
                    OrchestratorOptions options,
                    ApplicationInfo application,
                    Consumer<WorkerNode> nodeConsumer) {
            this.orchestrator = orchestrator;
            this.options = options;
            this.application = application;
            this.frontEnd = orchestrator.getFrontEnd();
            this.nodeConsumer = nodeConsumer;
        }

        @Override
        public void callback(DpeInfo dpe) {
            synchronized (availableNodes) {
                if (availableNodes.size() == options.maxNodes || ignoreDpe(dpe)) {
                    return;
                }
                String nodeName = getHost(dpe.name);
                WorkerNode.Builder nodeBuilder = waitingNodes.get(nodeName);
                if (nodeBuilder == null) {
                    nodeBuilder = new WorkerNode.Builder(application);
                    waitingNodes.put(nodeName, nodeBuilder);
                } else if (nodeBuilder.isReady()) {
                    return;
                }
                nodeBuilder.addDpe(dpe);
                if (nodeBuilder.isReady()) {
                    WorkerNode node = nodeBuilder.build(orchestrator);
                    availableNodes.put(nodeName, node);
                    nodeConsumer.accept(node);
                    latch.countDown();
                }
            }
        }

        public boolean waitFirstNode() throws InterruptedException {
            return latch.await(1, TimeUnit.MINUTES);
        }

        public WorkerNode getLocalNode() {
            String nodeName = getHost(frontEnd);
            synchronized (availableNodes) {
                return availableNodes.get(nodeName);
            }
        }

        private String getHost(DpeName name) {
            return name.address().host();
        }

        private boolean ignoreDpe(DpeInfo dpe) {
            String dpeNode = getHost(dpe.name);
            String feNode = getHost(frontEnd);
            if (options.orchMode == OrchestratorMode.LOCAL) {
                return !dpeNode.equals(feNode);
            }
            return dpeNode.equals(feNode) && !options.useFrontEnd;
        }
    }


    static class DataHandlerCB implements EngineCallback {

        private final WorkerNode localNode;
        private final OrchestratorOptions options;

        DataHandlerCB(WorkerNode localNode,
                    OrchestratorOptions options) {
            this.localNode = localNode;
            this.options = options;
        }

        @Override
        public void callback(EngineData data) {
            int totalEvents = localNode.eventNumber.addAndGet(options.reportFreq);
            long endTime = System.currentTimeMillis();
            double totalTime = (endTime - localNode.startTime.get());
            double timePerEvent = totalTime /  totalEvents;
            Logging.info("Processed  %5d events    "
                         + "total time = %7.2f s    "
                         + "average event time = %6.2f ms",
                         totalEvents, totalTime / 1000L, timePerEvent);
        }
    }


    static class CommandLineBuilder {

        private static final String ARG_FRONTEND      = "frontEnd";
        private static final String ARG_CLOUD_MODE    = "cloudMode";
        private static final String ARG_SESSION       = "session";
        private static final String ARG_USE_FRONTEND  = "useFrontEnd";
        private static final String ARG_STAGE_FILES   = "stageFiles";
        private static final String ARG_BULK_STAGE    = "bulkStage";
        private static final String ARG_INPUT_DIR     = "inputDir";
        private static final String ARG_OUTPUT_DIR    = "outputDir";
        private static final String ARG_STAGE_DIR     = "stageDir";
        private static final String ARG_POOL_SIZE     = "poolSize";
        private static final String ARG_MAX_NODES     = "maxNodes";
        private static final String ARG_MAX_THREADS   = "maxThreads";
        private static final String ARG_FREQUENCY     = "frequency";
        private static final String ARG_SKIP_EVENTS   = "skipEv";
        private static final String ARG_MAX_EVENTS    = "maxEv";
        private static final String ARG_SERVICES_FILE = "servicesFile";
        private static final String ARG_INPUT_FILES   = "inputFiles";

        private final JSAP jsap;
        private final JSAPResult config;

        CommandLineBuilder(String[] args) {
            jsap = new JSAP();
            setArguments(jsap);
            config = jsap.parse(args);
        }

        public boolean success() {
            return config.success();
        }

        public String usage() {
            return jsap.getUsage();
        }

        public String help() {
            return jsap.getHelp();
        }

        public GenericOrchestrator build() {
            String services = config.getString(ARG_SERVICES_FILE);
            OrchestratorConfigParser parser = new OrchestratorConfigParser(services);
            List<String> inFiles = parser.readInputFiles(config.getString(ARG_INPUT_FILES));

            Builder builder = new Builder(services, inFiles);

            builder.withInputDirectory(config.getString(ARG_INPUT_DIR));
            builder.withOutputDirectory(config.getString(ARG_OUTPUT_DIR));
            builder.withStageDirectory(config.getString(ARG_STAGE_DIR));

            builder.withPoolSize(config.getInt(ARG_POOL_SIZE));
            builder.withMaxThreads(config.getInt(ARG_MAX_THREADS));
            builder.withMaxNodes(config.getInt(ARG_MAX_NODES));

            builder.withSession(parseSession());
            builder.withFrontEnd(parseFrontEnd());

            if (config.getBoolean(ARG_CLOUD_MODE)) {
                builder.cloudMode();
            }
            if (config.getBoolean(ARG_USE_FRONTEND)) {
                builder.useFrontEnd();
            }
            if (config.getBoolean(ARG_STAGE_FILES)) {
                builder.useStageDirectory();
            }

            if (config.contains(ARG_FREQUENCY)) {
                builder.withReportFrequency(config.getInt(ARG_FREQUENCY));
            }
            if (config.contains(ARG_SKIP_EVENTS)) {
                builder.withSkipEvents(config.getInt(ARG_SKIP_EVENTS));
            }
            if (config.contains(ARG_MAX_EVENTS)) {
                builder.withMaxEvents(config.getInt(ARG_MAX_EVENTS));
            }

            return builder.build();
        }

        private DpeName parseFrontEnd() {
            String frontEnd = config.getString(ARG_FRONTEND)
                                    .replaceFirst("localhost", ClaraUtil.localhost());
            try {
                return new DpeName(frontEnd);
            } catch (IllegalArgumentException e) {
                throw new OrchestratorConfigError("invalid front-end name: " + frontEnd);
            }
        }

        private String parseSession() {
            String session = config.getString(ARG_SESSION);
            if (session == null) {
                return "";
            }
            return session;
        }

        private void setArguments(JSAP jsap) {
            FlaggedOption frontEnd = new FlaggedOption(ARG_FRONTEND)
                    .setStringParser(JSAP.STRING_PARSER)
                    .setRequired(false)
                    .setShortFlag('f')
                    .setDefault(OrchestratorConfigParser.localDpeName().toString());
            frontEnd.setHelp("The name of the CLARA front-end DPE.");

            FlaggedOption session = new FlaggedOption(ARG_SESSION)
                    .setStringParser(JSAP.STRING_PARSER)
                    .setShortFlag('s')
                    .setRequired(false);
            session.setHelp("The session of the CLARA DPEs to be used for reconstruction.");

            Switch cloudMode = new Switch(ARG_CLOUD_MODE)
                    .setShortFlag('C');
            cloudMode.setHelp("Use cloud mode for reconstruction.");

            Switch useFrontEnd = new Switch(ARG_USE_FRONTEND)
                    .setShortFlag('F');
            useFrontEnd.setHelp("Use front-end for reconstruction.");

            Switch stageFiles = new Switch(ARG_STAGE_FILES)
                    .setShortFlag('L');
            stageFiles.setHelp("Stage files to the local file-system before using them.");

            Switch bulkStage = new Switch(ARG_BULK_STAGE)
                    .setShortFlag('B');
            bulkStage.setHelp("Stage all files at once to the local file-system.");

            FlaggedOption inputDir = new FlaggedOption(ARG_INPUT_DIR)
                    .setStringParser(JSAP.STRING_PARSER)
                    .setRequired(false)
                    .setShortFlag('i')
                    .setDefault(OrchestratorPaths.INPUT_DIR);
            inputDir.setHelp("The input directory where the files to be processed are located.");

            FlaggedOption outputDir = new FlaggedOption(ARG_OUTPUT_DIR)
                    .setStringParser(JSAP.STRING_PARSER)
                    .setRequired(false)
                    .setShortFlag('o')
                    .setDefault(OrchestratorPaths.OUTPUT_DIR);
            outputDir.setHelp("The output directory where reconstructed files will be saved.");

            FlaggedOption stageDir = new FlaggedOption(ARG_STAGE_DIR)
                    .setStringParser(JSAP.STRING_PARSER)
                    .setRequired(false)
                    .setShortFlag('l')
                    .setDefault(OrchestratorPaths.STAGE_DIR);
            stageDir.setHelp("The local stage directory where the temporary files will be stored.");

            FlaggedOption poolSize = new FlaggedOption(ARG_POOL_SIZE)
                    .setStringParser(JSAP.INTEGER_PARSER)
                    .setShortFlag('p')
                    .setDefault(String.valueOf(OrchestratorOptions.DEFAULT_POOLSIZE))
                    .setRequired(false);
            poolSize.setHelp("The size of the thread-pool processing service and node reports.");

            FlaggedOption maxNodes = new FlaggedOption(ARG_MAX_NODES)
                    .setStringParser(JSAP.INTEGER_PARSER)
                    .setShortFlag('n')
                    .setDefault(String.valueOf(OrchestratorOptions.MAX_NODES))
                    .setRequired(false);
            maxNodes.setHelp("The maximum number of reconstruction nodes to be used.");

            FlaggedOption maxThreads = new FlaggedOption(ARG_MAX_THREADS)
                    .setStringParser(JSAP.INTEGER_PARSER)
                    .setShortFlag('t')
                    .setDefault(String.valueOf(OrchestratorOptions.MAX_THREADS))
                    .setRequired(false);
            maxThreads.setHelp("The maximum number of reconstruction threads to be used per node.");

            FlaggedOption reportFreq = new FlaggedOption(ARG_FREQUENCY)
                    .setStringParser(JSAP.INTEGER_PARSER)
                    .setShortFlag('r')
                    .setDefault(String.valueOf(OrchestratorOptions.DEFAULT_REPORT_FREQ))
                    .setRequired(false);
            reportFreq.setHelp("The report frequency of processed events.");

            FlaggedOption skipEvents = new FlaggedOption(ARG_SKIP_EVENTS)
                    .setStringParser(JSAP.INTEGER_PARSER)
                    .setShortFlag('k')
                    .setRequired(false);
            skipEvents.setHelp("The amount of events to skip at the beginning.");

            FlaggedOption maxEvents = new FlaggedOption(ARG_MAX_EVENTS)
                    .setStringParser(JSAP.INTEGER_PARSER)
                    .setShortFlag('e')
                    .setRequired(false);
            maxEvents.setHelp("The maximum number of events to process.");

            UnflaggedOption servicesFile = new UnflaggedOption(ARG_SERVICES_FILE)
                    .setStringParser(JSAP.STRING_PARSER)
                    .setRequired(true);
            servicesFile.setHelp("The YAML file with the reconstruction chain description.");

            UnflaggedOption inputFiles = new UnflaggedOption(ARG_INPUT_FILES)
                    .setStringParser(JSAP.STRING_PARSER)
                    .setRequired(true);
            inputFiles.setHelp("The file with the list of input files to be reconstructed"
                              + " (one name per line).");

            try {
                jsap.registerParameter(frontEnd);
                jsap.registerParameter(session);
                jsap.registerParameter(cloudMode);
                jsap.registerParameter(useFrontEnd);
                jsap.registerParameter(stageFiles);
                jsap.registerParameter(bulkStage);
                jsap.registerParameter(inputDir);
                jsap.registerParameter(outputDir);
                jsap.registerParameter(stageDir);
                jsap.registerParameter(poolSize);
                jsap.registerParameter(maxNodes);
                jsap.registerParameter(maxThreads);
                jsap.registerParameter(reportFreq);
                jsap.registerParameter(skipEvents);
                jsap.registerParameter(maxEvents);
                jsap.registerParameter(servicesFile);
                jsap.registerParameter(inputFiles);
            } catch (JSAPException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
