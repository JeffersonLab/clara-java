package org.jlab.clara.std.orchestrators;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.base.DpeName;
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
        private final List<String> inputFiles;

        private final Set<String> dataTypes;
        private final JSONObject config;

        private final OrchestratorOptions.Builder options = OrchestratorOptions.builder();

        private DpeName frontEnd = OrchestratorConfigParser.localDpeName();
        private String session = "";

        private String inputDir = OrchestratorPaths.INPUT_DIR;
        private String outputDir = OrchestratorPaths.OUTPUT_DIR;
        private String stageDir = OrchestratorPaths.STAGE_DIR;

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
            this.inputFiles = inputFiles;
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
            this.inputDir = inputDir;
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
            this.outputDir = outputDir;
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
            this.stageDir = stageDir;
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
            OrchestratorPaths paths = new OrchestratorPaths(
                    inputFiles,
                    inputDir, outputDir, stageDir);
            return new GenericOrchestrator(setup, paths, options.build());
        }
    }


    private GenericOrchestrator(OrchestratorSetup setup,
                                OrchestratorPaths paths,
                                OrchestratorOptions options) {
        super(setup, paths, options);
        Logging.verbose(true);
    }


    @Override
    protected void start() {
        printStartup();
        waitFrontEnd();
        Logging.info("Waiting for reconstruction nodes...");
        DpeReportCB dpeCallback = new DpeReportCB(orchestrator, options, setup.application,
                                                  this::executeSetup);
        orchestrator.subscribeDpes(dpeCallback, setup.session);
    }


    @Override
    protected void end() {
        removeStageDirectories();
        Logging.info("Local  average event processing time = %.2f ms", stats.localAverage());
        Logging.info("Global average event processing time = %.2f ms", stats.globalAverage());
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



    static class DpeReportCB implements DpeCallBack {

        private final CoreOrchestrator orchestrator;
        private final OrchestratorOptions options;
        private final ApplicationInfo application;

        private final Consumer<WorkerNode> nodeConsumer;
        private final Map<String, WorkerNode.Builder> waitingNodes = new HashMap<>();
        private final Map<String, WorkerNode.Builder> availableNodes = new HashMap<>();

        DpeReportCB(CoreOrchestrator orchestrator,
                    OrchestratorOptions options,
                    ApplicationInfo application,
                    Consumer<WorkerNode> nodeConsumer) {
            this.orchestrator = orchestrator;
            this.options = options;
            this.application = application;
            this.nodeConsumer = nodeConsumer;
        }

        @Override
        public void callback(DpeInfo dpe) {
            synchronized (availableNodes) {
                if (availableNodes.size() == options.maxNodes || ignoreDpe(dpe)) {
                    return;
                }
                String nodeName = getHost(dpe);
                WorkerNode.Builder nodeBuilder = waitingNodes.get(nodeName);
                if (nodeBuilder == null) {
                    nodeBuilder = new WorkerNode.Builder(application);
                    waitingNodes.put(nodeName, nodeBuilder);
                } else if (nodeBuilder.isReady()) {
                    return;
                }
                nodeBuilder.addDpe(dpe);
                if (nodeBuilder.isReady()) {
                    availableNodes.put(nodeName, nodeBuilder);
                    nodeConsumer.accept(nodeBuilder.build(orchestrator));
                }
            }
        }

        private String getHost(DpeInfo dpe) {
            return dpe.name.address().host();
        }

        private boolean ignoreDpe(DpeInfo dpe) {
            DpeName name = dpe.name;
            DpeName fe = orchestrator.getFrontEnd();
            return name.equals(fe) && !options.useFrontEnd;
        }
    }


    static class CommandLineBuilder {

        private static final String ARG_FRONTEND      = "frontEnd";
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
            DpeName frontEnd = parseFrontEnd();
            String session = parseSession();

            String services = config.getString(ARG_SERVICES_FILE);
            String files = config.getString(ARG_INPUT_FILES);

            String inDir = config.getString(ARG_INPUT_DIR);
            String outDir = config.getString(ARG_OUTPUT_DIR);
            String tmpDir = config.getString(ARG_STAGE_DIR);

            OrchestratorOptions.Builder options = OrchestratorOptions.builder()
                    .withPoolSize(config.getInt(ARG_POOL_SIZE))
                    .withMaxThreads(config.getInt(ARG_MAX_THREADS))
                    .withMaxNodes(config.getInt(ARG_MAX_NODES));

            if (config.getBoolean(ARG_USE_FRONTEND)) {
                options.useFrontEnd();
            }
            if (config.getBoolean(ARG_STAGE_FILES)) {
                options.stageFiles();
            }
            if (config.contains(ARG_SKIP_EVENTS)) {
                options.withSkipEvents(config.getInt(ARG_SKIP_EVENTS));
            }
            if (config.contains(ARG_MAX_EVENTS)) {
                options.withMaxEvents(config.getInt(ARG_MAX_EVENTS));
            }

            OrchestratorConfigParser parser = new OrchestratorConfigParser(services);
            List<String> inFiles = parser.readInputFiles(files);

            OrchestratorSetup setup = new OrchestratorSetup(frontEnd,
                    parser.parseInputOutputServices(), parser.parseReconstructionChain(),
                    parser.parseDataTypes(), parser.parseReconstructionConfig(), session);
            OrchestratorPaths paths = new OrchestratorPaths(inFiles, inDir, outDir, tmpDir);

            return new GenericOrchestrator(setup, paths, options.build());
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
                jsap.registerParameter(useFrontEnd);
                jsap.registerParameter(stageFiles);
                jsap.registerParameter(bulkStage);
                jsap.registerParameter(inputDir);
                jsap.registerParameter(outputDir);
                jsap.registerParameter(stageDir);
                jsap.registerParameter(poolSize);
                jsap.registerParameter(maxNodes);
                jsap.registerParameter(maxThreads);
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
