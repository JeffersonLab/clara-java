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

import java.util.Arrays;
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

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;


import org.jlab.clara.base.ClaraLang;
import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.base.DpeName;
import org.jlab.clara.base.EngineCallback;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.std.orchestrators.CoreOrchestrator.DpeCallBack;
import org.jlab.clara.util.EnvUtils;
import org.jlab.clara.util.OptUtils;
import org.jlab.clara.util.VersionUtils;

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
        CommandLineBuilder cl = new CommandLineBuilder();
        try {
            cl.parse(args);
            if (cl.hasVersion()) {
                System.out.println(VersionUtils.getClaraVersionFull());
                System.exit(0);
            }
            if (cl.hasHelp()) {
                System.out.println(cl.usage());
                System.exit(0);
            }
            GenericOrchestrator fo = cl.build();
            boolean status = fo.run();
            if (status) {
                System.exit(0);
            } else {
                System.exit(1);
            }
        } catch (CommandLineException e) {
            System.err.println("error: " + e.getMessage());
            System.err.println(cl.usage());
            System.exit(1);
        } catch (OrchestratorConfigException e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        } catch (OrchestratorException e) {
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

        private final OrchestratorSetup.Builder setup;
        private final OrchestratorPaths.Builder paths;
        private final OrchestratorOptions.Builder options;

        /**
         * Sets the required arguments for the generic data processing orchestrator.
         *
         * @param servicesFile the YAML file describing the data processing application
         * @param inputFiles the list of files to be processed (only names).
         * @throws OrchestratorConfigException in case of any error in the application description
         */
        public Builder(String servicesFile, List<String> inputFiles) {
            Objects.requireNonNull(servicesFile, "servicesFile parameter is null");
            Objects.requireNonNull(inputFiles, "inputFiles parameter is null");
            if (inputFiles.isEmpty()) {
                throw new IllegalArgumentException("inputFiles list is empty");
            }

            this.setup = initialSetup(servicesFile);
            this.paths = new OrchestratorPaths.Builder(inputFiles);
            this.options = new OrchestratorOptions.Builder();
        }

        private Builder(String servicesFile, String inputFile, String outputFile) {
            Objects.requireNonNull(servicesFile, "servicesFile parameter is null");
            Objects.requireNonNull(inputFile, "inputFile parameter is null");
            Objects.requireNonNull(outputFile, "outputFile parameter is null");

            this.setup = initialSetup(servicesFile);
            this.paths = new OrchestratorPaths.Builder(inputFile, outputFile);
            this.options = new OrchestratorOptions.Builder();
        }

        private OrchestratorSetup.Builder initialSetup(String servicesFile) {
            OrchestratorConfigParser parser = new OrchestratorConfigParser(servicesFile);
            return new OrchestratorSetup
                    .Builder(parser.parseInputOutputServices(),
                             parser.parseDataProcessingServices(),
                             parser.parseMonitoringServices())
                    .withConfig(parser.parseConfiguration())
                    .withConfigMode(parser.parseConfigurationMode())
                    .withDataTypes(parser.parseDataTypes());
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
            setup.withFrontEnd(frontEnd);
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
            setup.withSession(session);
            return this;
        }

        /**
         * Uses the front-end for data processing. By default, the front-end is
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
         * data processing is finished, it will be moved back to the output
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
         * Sets the maximum number of threads to be used for data processing on
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
         * Sets the maximum number of nodes to be used for data processing.
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
         * This directory will contain all output files.
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
         * Changes output file prefix.
         *
         * @param outputFilePrefix the output file prefix
         * @return this object, so methods can be chained
         */
        public Builder withOutputFilePrefix(String outputFilePrefix) {
            Objects.requireNonNull(outputFilePrefix, "outputFilePrefix parameter is null");
            paths.withOutputFilePrefix(outputFilePrefix);
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
            return new GenericOrchestrator(setup.build(), paths.build(), options.build());
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

        if (options.orchMode != OrchestratorMode.CLOUD) {
            Logging.info("Waiting for local node...");
        } else {
            Logging.info("Waiting for worker nodes...");
        }
        orchestrator.subscribeDpes(dpeCallback, setup.session);
        tryLocalNode();
        waitFirstNode();

        if (options.orchMode != OrchestratorMode.CLOUD) {
            WorkerNode localNode = dpeCallback.getLocalNode();
            benchmark.initialize(localNode.getRuntimeData());
        }
    }


    @Override
    void subscribe(WorkerNode node) {
        super.subscribe(node);
        if (options.orchMode != OrchestratorMode.CLOUD) {
            node.subscribeDone(n -> new DataHandlerCB(node, options));
        }
    }


    @Override
    protected void end() {
        removeStageDirectories();
        if (options.orchMode != OrchestratorMode.CLOUD) {
            try {
                WorkerNode localNode = dpeCallback.getLocalNode();
                benchmark.update(localNode.getRuntimeData());
                BenchmarkPrinter printer = new BenchmarkPrinter(benchmark, stats.totalEvents());
                printer.printBenchmark(setup.application);
            } catch (OrchestratorException e) {
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
        System.out.println("==========================================");
        System.out.println("            CLARA Orchestrator            ");
        System.out.println("==========================================");
        System.out.println(" Front-end          = " + setup.frontEnd);
        System.out.println(" Start time         = " + ClaraUtil.getCurrentTime());
        System.out.println(" Threads            = " + options.maxThreads);
        System.out.println();
        System.out.println(" Input directory    = " + paths.inputDir);
        System.out.println(" Output directory   = " + paths.outputDir);
        System.out.println(" Output file prefix = " + paths.prefix);
        if (options.stageFiles) {
            System.out.println(" Stage directory  = " + paths.stageDir);
        }
        System.out.println(" Number of files  = " + paths.numFiles());
        System.out.println("==========================================");
    }


    private void waitFrontEnd() {
        final int maxAttempts = 5;
        int count = 0;
        while (true) {
            try {
                int timeout = count == 0 ? 2 : 4;
                orchestrator.getRegisteredDpes(timeout);
                break;
            } catch (OrchestratorException e) {
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
            Map<ClaraLang, DpeName> localDpes = orchestrator.getRegisteredDpes(2).stream()
                    .filter(this::isLocalDpe)
                    .collect(Collectors.toMap(DpeName::language, Function.identity()));

            Set<ClaraLang> appLangs = setup.application.getLanguages();
            Set<ClaraLang> dpeLangs = localDpes.keySet();

            if (dpeLangs.containsAll(appLangs)) {
                for (ClaraLang lang : appLangs) {
                    DpeName dpe = localDpes.get(lang);
                    dpeCallback.callback(new DpeInfo(dpe, cores, EnvUtils.claraHome()));
                }
            }
        }
    }


    private boolean isLocalDpe(DpeName dpe) {
        if (options.orchMode == OrchestratorMode.DOCKER) {
            return true;
        }
        String feHost = orchestrator.getFrontEnd().address().host();
        String dpeHost = dpe.address().host();
        return dpeHost.equals(feHost);
    }


    private void waitFirstNode() {
        try {
            if (!dpeCallback.waitFirstNode()) {
                throw new OrchestratorException("could not find a data processing node");
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
            if (options.orchMode == OrchestratorMode.DOCKER) {
                return "docker_container";
            }
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
            double sliceTime = (endTime - localNode.lastReportTime.getAndSet(endTime));
            double timePerEvent = sliceTime /  options.reportFreq;

            Logging.info("Processed %4d events in %6.2f s"
                         + "   average event time = "
                         + (options.maxThreads > 2 ? "%6.2f ms" : "%8.2f ms")
                         + "   [ total %5d events %8.2f s ]",
                         options.reportFreq, sliceTime / 1000L, timePerEvent,
                         totalEvents, totalTime / 1000L);
        }
    }


    static class CommandLineException extends RuntimeException {

        CommandLineException(String message) {
            super(message);
        }

        CommandLineException(Throwable cause) {
            super(cause);
        }

        @Override
        public String getMessage() {
            Throwable cause = getCause();
            if (cause != null) {
                return cause.getMessage();
            }
            return super.getMessage();
        }
    }


    static class CommandLineBuilder {

        private final OptionSpec<String> frontEnd;
        private final OptionSpec<String> session;
        private final OptionSpec<String> inputDir;
        private final OptionSpec<String> outputDir;
        private final OptionSpec<String> stageDir;
        private final OptionSpec<String> prefix;
        private final OptionSpec<Integer> poolSize;
        private final OptionSpec<Integer> maxNodes;
        private final OptionSpec<Integer> maxThreads;
        private final OptionSpec<Integer> reportFreq;
        private final OptionSpec<Integer> skipEvents;
        private final OptionSpec<Integer> maxEvents;

        private final OptionSpec<String> arguments;

        private OptionParser parser;
        private OptionSet options;

        CommandLineBuilder() {
            parser = new OptionParser();

            frontEnd = parser.accepts("f")
                    .withRequiredArg()
                    .defaultsTo(OrchestratorConfigParser.localDpeName().toString());

            session = parser.accepts("s")
                    .withRequiredArg()
                    .defaultsTo("");

            parser.accepts("C");
            parser.accepts("F");
            parser.accepts("L");

            inputDir = parser.accepts("i")
                    .withRequiredArg()
                    .defaultsTo(OrchestratorPaths.INPUT_DIR);

            outputDir = parser.accepts("o")
                    .withRequiredArg()
                    .defaultsTo(OrchestratorPaths.OUTPUT_DIR);

            stageDir = parser.accepts("l")
                    .withRequiredArg()
                    .defaultsTo(OrchestratorPaths.STAGE_DIR);

            poolSize = parser.accepts("p")
                    .withRequiredArg()
                    .ofType(Integer.class)
                    .defaultsTo(OrchestratorOptions.DEFAULT_POOLSIZE);

            maxNodes = parser.accepts("n")
                    .withRequiredArg()
                    .ofType(Integer.class)
                    .defaultsTo(OrchestratorOptions.MAX_NODES);

            maxThreads = parser.accepts("t")
                    .withRequiredArg()
                    .ofType(Integer.class)
                    .defaultsTo(OrchestratorOptions.MAX_THREADS);

            reportFreq = parser.accepts("r")
                    .withRequiredArg()
                    .ofType(Integer.class)
                    .defaultsTo(OrchestratorOptions.DEFAULT_REPORT_FREQ);

            skipEvents = parser.accepts("k")
                    .withRequiredArg()
                    .ofType(Integer.class);

            maxEvents = parser.accepts("e")
                    .withRequiredArg()
                    .ofType(Integer.class)
                    .defaultsTo(0);

            prefix = parser.accepts("z")
                .withRequiredArg()
                .defaultsTo(OrchestratorPaths.OUTPUT_FILE_PREFIX);


            arguments = parser.nonOptions();

            parser.acceptsAll(Arrays.asList("version"));
            parser.acceptsAll(Arrays.asList("h", "help")).forHelp();
        }

        public void parse(String[] args) {
            try {
                options = parser.parse(args);
                if (hasVersion() || hasHelp()) {
                    return;
                }
                int numArgs = options.nonOptionArguments().size();
                if (numArgs == 0) {
                    throw new CommandLineException("missing arguments");
                }
                if (numArgs < 2 || numArgs > 3) {
                    throw new CommandLineException("invalid number of arguments");
                }
            } catch (OptionException e) {
                throw new CommandLineException(e);
            }
        }

        public boolean hasVersion() {
            return options.has("version");
        }

        public boolean hasHelp() {
            return options.has("help");
        }

        public GenericOrchestrator build() {
            try {
                List<String> args = arguments.values(options);
                String services = args.get(0);
                List<String> files = args.subList(1, args.size());

                Builder builder;
                if (files.size() == 1) {
                    builder = new Builder(services, parseInputFiles(files.get(0)));
                    builder.withInputDirectory(options.valueOf(inputDir));
                    builder.withOutputDirectory(options.valueOf(outputDir));
                } else {
                    builder = new Builder(services, files.get(0), files.get(1));
                }
                builder.withStageDirectory(options.valueOf(stageDir));

                builder.withPoolSize(options.valueOf(poolSize));
                builder.withMaxThreads(options.valueOf(maxThreads));
                builder.withMaxNodes(options.valueOf(maxNodes));

                builder.withOutputFilePrefix(options.valueOf(prefix));

                builder.withFrontEnd(parseFrontEnd());
                builder.withSession(parseSession());

                if (options.has("C")) {
                    builder.cloudMode();
                }
                if (options.has("F")) {
                    builder.useFrontEnd();
                }
                if (options.has("L")) {
                    builder.useStageDirectory();
                }

                if (options.has(reportFreq)) {
                    builder.withReportFrequency(options.valueOf(reportFreq));
                }
                if (options.has(skipEvents)) {
                    builder.withSkipEvents(options.valueOf(skipEvents));
                }
                if (options.has(maxEvents)) {
                    builder.withMaxEvents(options.valueOf(maxEvents));
                }

                return builder.build();

            } catch (OptionException e) {
                throw new CommandLineException(e);
            }
        }

        private List<String> parseInputFiles(String filesList) {
            return OrchestratorConfigParser.readInputFiles(filesList);
        }

        private DpeName parseFrontEnd() {
            String frontEnd = options.valueOf(this.frontEnd)
                                     .replaceFirst("localhost", ClaraUtil.localhost());
            try {
                return new DpeName(frontEnd);
            } catch (IllegalArgumentException e) {
                throw new OrchestratorConfigException("invalid front-end name: " + frontEnd);
            }
        }

        private String parseSession() {
            String session = options.valueOf(this.session);
            if (session == null) {
                return "";
            }
            return session;
        }

        public String usage() {
            String wrapper = "clara-orchestrator";
            return String.format("usage: %s [options] <servicesFile> <datasetFile>", wrapper)
                + String.format("%n%n  Options:%n")
                + OptUtils.optionHelp("-C",
                        "Use the orchestrator on cloud mode.")
                + OptUtils.optionHelp("-F",
                        "Use the front-end for processing (on cloud mode).")
                + OptUtils.optionHelp("-L",
                        "Stage input files in the local file-system.")
                + OptUtils.optionHelp(frontEnd, "frontEnd",
                        "The name of the CLARA front-end DPE")
                + OptUtils.optionHelp(session, "session",
                        "The session name to filter worker DPEs")
                + OptUtils.optionHelp(inputDir, "inputDir",
                        "The directory with the set of input files")
                + OptUtils.optionHelp(outputDir, "outputDir",
                        "The directory where output files will be saved")
                + OptUtils.optionHelp(stageDir, "stageDir",
                        "The local directory where files will be staged")
                + OptUtils.optionHelp(poolSize, "poolSize",
                        "The size of the thread pool processing event reports")
                + OptUtils.optionHelp(maxNodes, "maxNodes",
                        "The maximum number of worker nodes to be used")
                + OptUtils.optionHelp(maxThreads, "maxThreads",
                        "The maximum number of threads to be used per node")
                + OptUtils.optionHelp(reportFreq, "frequency",
                        "The report frequency of processed events.")
                + OptUtils.optionHelp(skipEvents, "skipEv",
                        "The number of events to skip at the beginning")
                + OptUtils.optionHelp(maxEvents, "maxEv",
                        "The maximum number of events to process");
        }
    }
}
