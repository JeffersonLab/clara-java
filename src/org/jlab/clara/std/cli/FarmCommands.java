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

package org.jlab.clara.std.cli;

import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.clara.util.EnvUtils;
import org.jlab.clara.util.FileUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;

final class FarmCommands {

    private static final String FARM_STAGE = "farm.stage";
    private static final String FARM_MEMORY = "farm.memory";
    private static final String FARM_TRACK = "farm.track";
    private static final String FARM_OS = "farm.os";
    private static final String FARM_NODE = "farm.node";
    private static final String FARM_EXCLUSIVE = "farm.exclusive";
    private static final String FARM_CPU = "farm.cpu";
    private static final String FARM_DISK = "farm.disk";
    private static final String FARM_TIME = "farm.time";
    private static final String FARM_SYSTEM = "farm.system";
    private static final String FARM_H_SCALE = "farm.scaling";

    private static final int DEFAULT_FARM_H_SCALE = 0;
    private static final int DEFAULT_FARM_MEMORY = 0;
    private static final int DEFAULT_FARM_CORES = 0;
    private static final int DEFAULT_FARM_DISK_SPACE = 5;
    private static final int DEFAULT_FARM_TIME = 24 * 60;
    private static final String DEFAULT_FARM_OS = "centos7";
    private static final String DEFAULT_FARM_NODE = "";
    private static final String DEFAULT_FARM_EXCLUSIVE = "";
    private static final String DEFAULT_FARM_TRACK = "debug";

    private static final String JLAB_SYSTEM = "jlab";
    private static final String PBS_SYSTEM = "pbs";

    private static final String JLAB_SUB_EXT = ".jsub";
    private static final String PBS_SUB_EXT = ".qsub";

    private static final String JLAB_SUB_CMD = "jsub";
    private static final String PBS_SUB_CMD = "qsub";

    private static final String JLAB_STAT_CMD = "jobstat";
    private static final String PBS_STAT_CMD = "qstat";

    private static final Configuration FTL_CONFIG = new Configuration(Configuration.VERSION_2_3_25);

    static final Path PLUGIN = FileUtils.claraPath("plugins", "clas12");

    static final Path CLARA_USER = FileUtils.userPath();


    private FarmCommands() {
    }

    private static void configTemplates() {
        Path tplDir = getTemplatesDir();
        try {
            FTL_CONFIG.setDirectoryForTemplateLoading(tplDir.toFile());
            FTL_CONFIG.setDefaultEncoding("UTF-8");
            FTL_CONFIG.setNumberFormat("computer");
            FTL_CONFIG.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
            FTL_CONFIG.setLogTemplateExceptions(false);
        } catch (IOException e) {
            throw new IllegalStateException("Missing CLAS12 templates directory: " + tplDir);
        }
    }

    private static void clasVariables(Config.Builder builder) {
        builder.withConfigVariable(Config.SERVICES_FILE, defaultConfigFile());
        builder.withConfigVariable(Config.FILES_LIST, defaultFileList());

        builder.withEnvironmentVariable("CLAS12DIR", PLUGIN.toString());
    }

    private static void farmVariables(Config.Builder builder) {
        List<ConfigVariable.Builder> vl = new ArrayList<>();

        BiFunction<String, String, ConfigVariable.Builder> addBuilder = (n, d) -> {
            ConfigVariable.Builder b = ConfigVariable.newBuilder(n, d);
            vl.add(b);
            return b;
        };

        addBuilder.apply(FARM_CPU, "Farm resource core number request.")
            .withParser(ConfigParsers::toPositiveInteger)
            .withInitialValue(DEFAULT_FARM_CORES);

        addBuilder.apply(FARM_MEMORY, "Farm job memory request (in GB).")
            .withParser(ConfigParsers::toPositiveInteger)
            .withInitialValue(DEFAULT_FARM_MEMORY);

        addBuilder.apply(FARM_DISK, "Farm job disk space request (in GB).")
            .withParser(ConfigParsers::toPositiveInteger)
            .withInitialValue(DEFAULT_FARM_DISK_SPACE);

        addBuilder.apply(FARM_TIME, "Farm job wall time request (in min).")
            .withParser(ConfigParsers::toPositiveInteger)
            .withInitialValue(DEFAULT_FARM_TIME);

        addBuilder.apply(FARM_OS, "Farm resource OS.")
            .withInitialValue(DEFAULT_FARM_OS);

        addBuilder.apply(FARM_NODE,
            "Preferred farm node flavor (JLAB specific, e.g. farm16, farm18, etc.)")
            .withInitialValue(DEFAULT_FARM_NODE);

        addBuilder.apply(FARM_EXCLUSIVE,
            "Exclusive farm node request (JLAB specific, e.g. farm16, farm18, etc.)")
            .withInitialValue(DEFAULT_FARM_EXCLUSIVE);

        addBuilder.apply(FARM_STAGE, "Local directory to stage reconstruction files.")
            .withParser(ConfigParsers::toDirectory);

        addBuilder.apply(FARM_TRACK, "Farm job track.")
            .withInitialValue(DEFAULT_FARM_TRACK);

        addBuilder.apply(FARM_H_SCALE, "Farm horizontal scaling factor."
            + " Split the list of input files into chunks of the given size"
            + " to be processed in parallel within separate farm jobs.")
            .withParser(ConfigParsers::toNonNegativeInteger)
            .withInitialValue(DEFAULT_FARM_H_SCALE);

        addBuilder.apply(FARM_SYSTEM, "Farm batch system. Accepts pbs and jlab.")
            .withExpectedValues(JLAB_SYSTEM, PBS_SYSTEM)
            .withInitialValue(JLAB_SYSTEM);


        vl.forEach(builder::withConfigVariable);
    }

    private static String defaultConfigFile() {
        Path ymlPath = PLUGIN.resolve("config/services.yml");
        if (Files.exists(ymlPath)) {
            return ymlPath.toString();
        }
        Path compatibilityPath = PLUGIN.resolve("config/services.yaml");
        if (Files.exists(compatibilityPath)) {
            return compatibilityPath.toString();
        }
        return ymlPath.toString();
    }

    private static String defaultFileList() {
        Path filesPath = CLARA_USER.resolve("config/files.txt");
        if (Files.exists(filesPath)) {
            return filesPath.toString();
        }
        Path compatibilityPath = CLARA_USER.resolve("config/files.list");
        if (Files.exists(compatibilityPath)) {
            return compatibilityPath.toString();
        }
        return filesPath.toString();
    }

    static boolean hasPlugin() {
        return Files.isDirectory(PLUGIN);
    }

    static void register(ClaraShell.Builder builder) {
        configTemplates();
        builder.withConfiguration(FarmCommands::clasVariables);
        builder.withConfiguration(FarmCommands::farmVariables);
        builder.withRunSubCommand(RunFarm::new);
    }


    private abstract static class FarmCommand extends AbstractCommand {

        protected final RunUtils runUtils;

        protected FarmCommand(Context context, String name, String description) {
            super(context, name, description);
            runUtils = new RunUtils(config);
        }

        protected Path getJobScript(String ext) {
            String name = String.format("farm_%s", runUtils.getSession());
            return CLARA_USER.resolve("config/" + name + ext);
        }
    }


    static class RunFarm extends FarmCommand {

        RunFarm(Context context) {
            super(context, "farm", "Run CLARA data processing on the farm.");
        }

        @Override
        public int execute(String[] args) {
            String system = config.getString(FARM_SYSTEM);
            if (system.equals(JLAB_SYSTEM)) {
                if (CommandUtils.checkProgram(JLAB_SUB_CMD)) {
                    try {
                        // check to see if we have exclusive node request
                        switch (config.getString(FARM_EXCLUSIVE)) {
                            case "farm18":
                                config.setValue(FARM_NODE, "farm18");
                                config.setValue(FARM_CPU, 80); // removed for SLURM
                                config.setValue(FARM_MEMORY, 90); // removed for SLURM
//                                config.setValue(FARM_CPU, 0);
//                                config.setValue(FARM_MEMORY, 0);
                                config.setValue(Config.MAX_THREADS, 20);
                                break;
                            case "farm16":
                                config.setValue(FARM_NODE, "farm16"); //cores = 72
                                config.setValue(FARM_CPU, 0);
                                config.setValue(FARM_MEMORY, 0);
                                config.setValue(Config.MAX_THREADS, 32);
                                break;
                            case "farm14":
                                config.setValue(FARM_NODE, "farm14"); //cores = 48
                                config.setValue(FARM_CPU, 0);
                                config.setValue(FARM_MEMORY, 0);
                                config.setValue(Config.MAX_THREADS, 24);
                                break;
                            case "farm13":
                                config.setValue(FARM_NODE, "farm13"); //cores = 32
                                config.setValue(FARM_CPU, 0);
                                config.setValue(FARM_MEMORY, 0);
                                config.setValue(Config.MAX_THREADS, 16);
                                break;
                            case "qcd12s":
                                config.setValue(FARM_NODE, "qcd12s"); //cores = 32
                                config.setValue(FARM_CPU, 0);
                                config.setValue(FARM_MEMORY, 0);
                                config.setValue(Config.MAX_THREADS, 16);
                                break;
                            default:
                                break;
                        }

                        int horizScale = config.getInt(FARM_H_SCALE);
                        if (horizScale > 0) {
                            return splitIntoMultipleJobs(horizScale);
                        } else {
                            Path jobFile = createJLabScript();
                            return CommandUtils.runProcess(JLAB_SUB_CMD, jobFile.toString());
                        }
                    } catch (IOException e) {
                        writer.println("Error: could not set job:  " + e.getMessage());
                        return EXIT_ERROR;
                    } catch (TemplateException e) {
                        String error = e.getMessageWithoutStackTop();
                        writer.println("Error: could not set job: " + error);
                        return EXIT_ERROR;
                    }
                }
                writer.println("Error: can not run farm job from this node = " + getHost());
                return EXIT_ERROR;
            }

            if (system.equals(PBS_SYSTEM)) {
                if (CommandUtils.checkProgram(PBS_SUB_CMD)) {
                    try {
                        Path jobFile = createPbsScript();
                        return CommandUtils.runProcess(PBS_SUB_CMD, jobFile.toString());
                    } catch (IOException e) {
                        writer.println("Error: could not set job:  " + e.getMessage());
                        return EXIT_ERROR;
                    } catch (TemplateException e) {
                        String error = e.getMessageWithoutStackTop();
                        writer.println("Error: could not set job: " + error);
                        return EXIT_ERROR;
                    }
                }
                writer.println("Error: can not run farm job from this node = " + getHost());
                return EXIT_ERROR;
            }

            writer.println("Error: invalid farm system = " + system);
            return EXIT_ERROR;
        }

        private int splitIntoMultipleJobs(int filesPerJob) throws IOException, TemplateException {
            String description = config.getString(Config.DESCRIPTION);
            Path fileList = Paths.get(config.getString(Config.FILES_LIST));
            try {
                Path dotDir = Paths.get(CLARA_USER.toString(), "config",
                    "." + runUtils.getSession());
                FileUtils.deleteFileTree(dotDir);
                FileUtils.createDirectories(dotDir);

                List<List<String>> filePartitions = partitionFiles(fileList, filesPerJob);
                for (int i = 0; i < filePartitions.size(); i++) {
                    Path subFileList = dotDir.resolve(appendIndex(description, i));
                    try (BufferedWriter writer = Files.newBufferedWriter(subFileList)) {
                        for (String inputFile : filePartitions.get(i)) {
                            writer.write(inputFile);
                            writer.newLine();
                        }
                    }
                    config.setValue(Config.DESCRIPTION, appendIndex(description, i));
                    config.setValue(Config.FILES_LIST, subFileList.toString());

                    Path jobFile = createJLabScript();
                    int stat = CommandUtils.runProcess(JLAB_SUB_CMD, jobFile.toString());
                    if (stat != EXIT_SUCCESS) {
                        return stat;
                    }
                }
                return EXIT_SUCCESS;
            } finally {
                config.setValue(Config.DESCRIPTION, description);
                config.setValue(Config.FILES_LIST, fileList);
            }
        }

        private String getClaraCommand() {
            Path wrapper = FileUtils.claraPath("lib", "clara", "run-clara");
            SystemCommandBuilder cmd = new SystemCommandBuilder(wrapper);

            cmd.addOption("-i", config.getString(Config.INPUT_DIR));
            cmd.addOption("-o", config.getString(Config.OUTPUT_DIR));
            cmd.addOption("-z", config.getString(Config.OUT_FILE_PREFIX));
            if (config.hasValue(FARM_STAGE)) {
                cmd.addOption("-l", config.getString(FARM_STAGE));
            }
            if (config.hasValue(Config.MAX_THREADS)) {
                cmd.addOption("-t", config.getInt(Config.MAX_THREADS));
            } else {
                cmd.addOption("-t", config.getInt(FARM_CPU));
            }
            if (config.hasValue(Config.REPORT_EVENTS)) {
                cmd.addOption("-r", config.getInt(Config.REPORT_EVENTS));
            }
            if (config.hasValue(Config.SKIP_EVENTS)) {
                cmd.addOption("-k", config.getInt(Config.SKIP_EVENTS));
            }
            if (config.hasValue(Config.MAX_EVENTS)) {
                cmd.addOption("-e", config.getInt(Config.MAX_EVENTS));
            }
            cmd.addOption("-s", runUtils.getSession());
            if (config.hasValue(Config.FRONTEND_HOST)) {
                cmd.addOption("-H", config.getString(Config.FRONTEND_HOST));
            }
            if (config.hasValue(Config.FRONTEND_PORT)) {
                cmd.addOption("-P", config.getInt(Config.FRONTEND_PORT));
            }

            cmd.addArgument(config.getString(Config.SERVICES_FILE));
            cmd.addArgument(config.getString(Config.FILES_LIST));

            cmd.multiLine(true);

            return cmd.toString();
        }

        private String getClaraCommandAffinity(String affinity,
                                               String session,
                                               String filesList) {
            String exec = FileUtils.claraPathAffinity(
                affinity,
                File.separator + "lib",
                File.separator + "clara",
                File.separator + "/run-clara ");
            SystemCommandBuilder cmd = new SystemCommandBuilder();

            cmd.addOptionNoSplit("-i", config.getString(Config.INPUT_DIR));
            cmd.addOption("-o", config.getString(Config.OUTPUT_DIR));
            cmd.addOption("-z", config.getString(Config.OUT_FILE_PREFIX));
            if (config.hasValue(FARM_STAGE)) {
                cmd.addOption("-l", config.getString(FARM_STAGE)
                    + File.separator + session);
            }
            if (config.hasValue(Config.MAX_THREADS)) {
                cmd.addOption("-t", config.getInt(Config.MAX_THREADS));
            } else {
                cmd.addOption("-t", config.getInt(FARM_CPU));
            }
            if (config.hasValue(Config.REPORT_EVENTS)) {
                cmd.addOption("-r", config.getInt(Config.REPORT_EVENTS));
            }
            if (config.hasValue(Config.SKIP_EVENTS)) {
                cmd.addOption("-k", config.getInt(Config.SKIP_EVENTS));
            }
            if (config.hasValue(Config.MAX_EVENTS)) {
                cmd.addOption("-e", config.getInt(Config.MAX_EVENTS));
            }
            cmd.addOption("-s", session);
            if (config.hasValue(Config.FRONTEND_HOST)) {
                cmd.addOption("-H", config.getString(Config.FRONTEND_HOST));
            }
            if (config.hasValue(Config.FRONTEND_PORT)) {
                cmd.addOption("-P", config.getInt(Config.FRONTEND_PORT));
            }

            cmd.addArgument(config.getString(Config.SERVICES_FILE));
            cmd.addArgument(filesList);

            cmd.multiLine(true);

            return exec + " " + cmd.toString();
        }

        private String getClaraCommandAffinityList(List<String> affinities) {
            StringBuilder sb = new StringBuilder();
            String description = config.getString(Config.DESCRIPTION);
            Path fileList = Paths.get(config.getString(Config.FILES_LIST));
            Path dotDir = Paths.get(CLARA_USER.toString(), "config",
                ".aff." + runUtils.getSession());
            try {
                FileUtils.deleteFileTree(dotDir);
                FileUtils.createDirectories(dotDir);

                List<String> files = Files.lines(fileList)
                    .collect(Collectors.toList());

                int splitFactor = files.size() / affinities.size();

                if (splitFactor > 0) {
                    List<List<String>>
                        filePartitions = partitionFilesForAffinity(fileList, splitFactor);

                    for (int i = 0; i < filePartitions.size(); i++) {
                        Path subFileList = dotDir.resolve(appendIndex(description, i));
                        BufferedWriter writer = Files.newBufferedWriter(subFileList);
                        for (String inputFile : filePartitions.get(i)) {
                            writer.write(inputFile);
                            writer.newLine();
                        }
                        writer.close();

                        sb.append(getClaraCommandAffinity(affinities.get(i),
                            runUtils.getSession() + "_" + i,
                            subFileList.toString())).append("> /dev/null 2>&1 &\n");
                        sb.append("a" + i + "=$!\n");
                        sb.append("sleep 10 \n");
                    }
                    for (int i = 0; i < filePartitions.size(); i++) {
                        sb.append("echo waiting pid = $a" + i + " \n");
                        sb.append("wait ${a" + i + "}\n");
                    }
                } else {
                    System.err.println("Error: Data set is too small for "
                        + "farm.exclusive and/or farm.scaling settings.");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return sb.toString();
        }

        private Path createClaraScript(Model model) throws IOException, TemplateException {
            Path wrapper = getJobScript(".sh");
            try (PrintWriter printer = FileUtils.openOutputTextFile(wrapper, false)) {
                processTemplate("farm-script.ftl", model, printer);
                model.put("farm", "script", wrapper);
            }
            wrapper.toFile().setExecutable(true);

            return wrapper;
        }

        private Path createJLabScript() throws IOException, TemplateException {
            Model model = createDataModel();
            createClaraScript(model);

            Path jobFile = getJobScript(JLAB_SUB_EXT);
            try (PrintWriter printer = FileUtils.openOutputTextFile(jobFile, false)) {
                processTemplate("farm-jlab.ftl", model, printer);
            }
            return jobFile;
        }

        private Path createPbsScript() throws IOException, TemplateException {
            Model model = createDataModel();
            createClaraScript(model);

            int diskKb = config.getInt(FARM_DISK) * 1024 * 1024;
            int time = config.getInt(FARM_TIME);
            String walltime = String.format("%d:%02d:00", time / 60, time % 60);

            model.put("farm", "disk", diskKb);
            model.put("farm", "time", walltime);

            Path jobFile = getJobScript(PBS_SUB_EXT);
            try (PrintWriter printer = FileUtils.openOutputTextFile(jobFile, false)) {
                processTemplate("farm-pbs.ftl", model, printer);
            }

            return jobFile;
        }

        private Model createDataModel() {
            Model model = new Model();

            // set core variables
            model.put("user", EnvUtils.userName());
            model.put("clara", "dir", EnvUtils.claraHome());
            model.put("clas12", "dir", CLARA_USER);

            // set monitor FE
            String monitor = runUtils.getMonitorFrontEnd();
            if (monitor != null) {
                model.put("clara", "monitorFE", monitor);
            }

            // set shell variables
            config.getVariables().stream()
                .filter(v -> !v.getName().startsWith("farm."))
                .filter(v -> v.hasValue())
                .forEach(v -> model.put(v.getName(), v.getValue()));

            // set farm variables
            config.getVariables().stream()
                .filter(v -> v.getName().startsWith("farm."))
                .filter(v -> v.hasValue())
                .forEach(v -> model.put("farm", v.getName().replace("farm.", ""), v.getValue()));

            // set farm command
            model.put("farm", "javaOpts", getJVMOptions());

            String farmExclusive = config.getString(FARM_EXCLUSIVE);
            if (farmExclusive.equals("")) {
                model.put("farm", "command", getClaraCommand());
            } else {
                switch (farmExclusive) {
                    case "farm18":
                        model.put("farm", "command",
                            getClaraCommandAffinityList(ClaraConstants.FARM18_NUMAS));
                        break;
                    case "farm16":
                        model.put("farm", "command",
                            getClaraCommandAffinityList(ClaraConstants.FARM16_NUMAS));
                        break;
                    case "farm14":
                        model.put("farm", "command",
                            getClaraCommandAffinityList(ClaraConstants.FARM14_NUMAS));
                        break;
                    case "farm13":
                        model.put("farm", "command",
                            getClaraCommandAffinityList(ClaraConstants.FARM13_NUMAS));
                        break;
                    case "qcd12s":
                        model.put("farm", "command",
                            getClaraCommandAffinityList(ClaraConstants.QCD12S_NUMAS));
                        break;
                    default:
                        break;
                }
            }
            return model;
        }

        private void processTemplate(String name, Model model, PrintWriter printer)
                throws IOException, TemplateException {
            Template template = FTL_CONFIG.getTemplate(name);
            template.process(model.getRoot(), printer);
        }

        private String getJVMOptions() {
            if (config.hasValue(Config.JAVA_OPTIONS)) {
                return config.getString(Config.JAVA_OPTIONS);
            }
            String jvmOpts = "-XX:+UseNUMA -XX:+UseBiasedLocking";
            if (config.hasValue(Config.JAVA_MEMORY)) {
                int memSize = config.getInt(Config.JAVA_MEMORY);
                return String.format("-Xms%dg -Xmx%dg %s", memSize, memSize, jvmOpts);
            }
            return jvmOpts;
        }
    }

    static class ShowFarmStatus extends FarmCommand {

        ShowFarmStatus(Context context) {
            super(context, "farmStatus", "Show status of farm submitted jobs.");
        }

        @Override
        public int execute(String[] args) {
            String system = config.getString(FARM_SYSTEM);
            if (system.equals(JLAB_SYSTEM)) {
                if (CommandUtils.checkProgram(JLAB_STAT_CMD)) {
                    return CommandUtils.runProcess(JLAB_STAT_CMD, "-u", EnvUtils.userName());
                }
                writer.println("Error: can not run farm operations from this node = " + getHost());
                return EXIT_ERROR;
            }
            if (system.equals(PBS_SYSTEM)) {
                if (CommandUtils.checkProgram(PBS_STAT_CMD)) {
                    return CommandUtils.runProcess(PBS_STAT_CMD, "-u", EnvUtils.userName());
                }
                writer.println("Error: can not run farm operations from this node = " + getHost());
                return EXIT_ERROR;
            }
            writer.println("Error: invalid farm system = " + system);
            return EXIT_ERROR;
        }
    }

    static class ShowFarmSub extends FarmCommand {

        ShowFarmSub(Context context) {
            super(context, "farmSub", "Show farm job submission file.");
        }

        @Override
        public int execute(String[] args) {
            String system = config.getString(FARM_SYSTEM);
            if (system.equals(JLAB_SYSTEM)) {
                return showFile(getJobScript(JLAB_SUB_EXT));
            }
            if (system.equals(PBS_SYSTEM)) {
                return showFile(getJobScript(PBS_SUB_EXT));
            }
            writer.println("Error: invalid farm system = " + system);
            return EXIT_ERROR;
        }

        private int showFile(Path subFile) {
            return RunUtils.printFile(terminal, subFile);
        }
    }


    private static class Model {

        private static final Function<String, Object> FN = k -> new HashMap<String, Object>();

        private final Map<String, Object> model = new HashMap<>();

        void put(String key, Object value) {
            getRoot().put(key, value);
        }

        void put(String node, String key, Object value) {
            getNode(node).put(key, value);
        }

        Map<String, Object> getRoot() {
            return model;
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> getNode(String key) {
            return (Map<String, Object>) model.computeIfAbsent(key, FN);
        }
    }


    private static Path getTemplatesDir() {
        String devDir = System.getenv("CLARA_TEMPLATES_DIR");
        if (devDir != null) {
            return Paths.get(devDir);
        }
        return FileUtils.claraPath("lib", "clara", "templates");
    }


    private static String getHost() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new UncheckedIOException(e);
        }
    }


    private static List<List<String>> partitionFiles(Path fileList, int filesPerJob)
            throws IOException {
        List<String> files = Files.lines(fileList)
            .collect(Collectors.toList());
        List<List<String>> groupedFiles = new ArrayList<>();
        for (int i = 0; i < files.size(); i += filesPerJob) {
            int end = Math.min(files.size(), i + filesPerJob);
            groupedFiles.add(files.subList(i, end));
        }
        return groupedFiles;
    }

    private static List<List<String>> partitionFilesForAffinity(Path fileList, int filesPerJob)
            throws IOException {
        List<String> files = Files.lines(fileList)
            .collect(Collectors.toList());
        List<List<String>> groupedFiles = new ArrayList<>();
        for (int i = 0; i < files.size(); i += filesPerJob) {
            int end = Math.min(files.size(), i + filesPerJob);
            groupedFiles.add(files.subList(i, end));
        }
        if ((groupedFiles.size() % 2) != 0) {
            List<String> last = groupedFiles.get(groupedFiles.size() - 1);
            groupedFiles.remove(groupedFiles.size() - 1);
            List<String> trueLast = groupedFiles.get(groupedFiles.size() - 1);
            groupedFiles.remove(groupedFiles.size() - 1);

            List<String> fLast = new ArrayList<>();
            fLast.addAll(last);
            fLast.addAll(trueLast);
            groupedFiles.add(fLast);
        }

        return groupedFiles;
    }


    private static String appendIndex(String str, int index) {
        return String.format("%s_%03d", str, index);
    }
}
