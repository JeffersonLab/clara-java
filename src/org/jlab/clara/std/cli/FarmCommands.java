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

import org.jlab.clara.util.FileUtils;

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

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;

final class FarmCommands {

    private static final String FARM_STAGE = "farm.stage";
    private static final String FARM_MEMORY = "farm.memory";
    private static final String FARM_TRACK = "farm.track";
    private static final String FARM_OS = "farm.os";
    private static final String FARM_CPU = "farm.cpu";
    private static final String FARM_DISK = "farm.disk";
    private static final String FARM_TIME = "farm.time";
    private static final String FARM_SYSTEM = "farm.system";

    private static final int DEFAULT_FARM_MEMORY = 70;
    private static final int DEFAULT_FARM_CORES = 72;
    private static final int DEFAULT_FARM_DISK_SPACE = 3;
    private static final int DEFAULT_FARM_TIME = 24 * 60;
    private static final int DEFAULT_FARM_JVM_MEMORY = 40;
    private static final String DEFAULT_FARM_OS = "centos7";
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

    static final Path PLUGIN = Paths.get(Config.claraHome(), "plugins", "clas12");


    private FarmCommands() { }

    private  static void configTemplates() {
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
        builder.withConfigVariable(Config.SESSION, Config.user());

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

        addBuilder.apply(FARM_STAGE, "Local directory to stage reconstruction files.");

        addBuilder.apply(FARM_TRACK, "Farm job track.")
            .withInitialValue(DEFAULT_FARM_TRACK);

        addBuilder.apply(FARM_SYSTEM, "Farm batch system. Accepts pbs and jlab.")
            .withExpectedValues(JLAB_SYSTEM, PBS_SYSTEM)
            .withInitialValue(JLAB_SYSTEM);

        vl.forEach(builder::withConfigVariable);
    }

    private static String defaultConfigFile() {
        return PLUGIN.resolve("config/services.yaml").toString();
    }

    private static String defaultFileList() {
        return PLUGIN.resolve("config/files.list").toString();
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

        protected FarmCommand(Context context, String name, String description) {
            super(context, name, description);
        }

        protected Path getJobScript(String ext) {
            String keyword = config.getValue(Config.DESCRIPTION).toString();
            String name = String.format("farm-%s-%s", Config.user(), keyword);
            return PLUGIN.resolve("config/" + name + ext);
        }
    }


    static class RunFarm extends FarmCommand {

        RunFarm(Context context) {
            super(context, "farm", "Run CLARA data processing on the farm.");
        }

        @Override
        public int execute(String[] args) {
            String system = config.getValue(FARM_SYSTEM).toString();
            if (system.equals(JLAB_SYSTEM)) {
                if (CommandUtils.checkProgram(JLAB_SUB_CMD)) {
                    try {
                        Path jobFile = createJLabScript();
                        return CommandUtils.runProcess(JLAB_SUB_CMD, jobFile.toString());
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

        private String getClaraCommand() {
            StringBuilder cmd = new StringBuilder();
            cmd.append("\"");
            cmd.append(Config.claraHome()).append("/lib/clara/run-clara");
            cmd.append("\"");

            appendOpt(cmd, "-i", config.getValue(Config.INPUT_DIR));
            appendOpt(cmd, "-o", config.getValue(Config.OUTPUT_DIR));
            if (config.hasValue(FARM_STAGE)) {
                appendOpt(cmd, "-l ", config.getValue(FARM_STAGE));
            }
            if (config.hasValue(Config.MAX_THREADS)) {
                appendOpt(cmd, "-t", config.getValue(Config.MAX_THREADS));
            } else {
                appendOpt(cmd, "-t", config.getValue(FARM_CPU));
            }
            appendOpt(cmd, "-s", config.getValue(Config.SESSION));
            if (config.hasValue(Config.DESCRIPTION)) {
                appendOpt(cmd, "-d", config.getValue(Config.DESCRIPTION));
            }
            if (config.hasValue(Config.FRONTEND_HOST)) {
                appendOpt(cmd, "-H", config.getValue(Config.FRONTEND_HOST));
            }
            if (config.hasValue(Config.FRONTEND_PORT)) {
                appendOpt(cmd, "-P", config.getValue(Config.FRONTEND_PORT));
            }
            appendOpt(cmd, "-J", getJVMOptions());

            appendArg(cmd, config.getValue(Config.SERVICES_FILE));
            appendArg(cmd, config.getValue(Config.FILES_LIST));

            return cmd.toString();
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

            int diskKb = (int) config.getValue(FARM_DISK) * 1024 * 1024;
            int time = (int) config.getValue(FARM_TIME);
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
            model.put("user", Config.user());
            model.put("clara", "dir", Config.claraHome());
            model.put("clas12", "dir", PLUGIN);

            // set monitor FE
            String monitor = System.getenv("CLARA_MONITOR_FRONT_END");
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
            model.put("farm", "command", getClaraCommand());

            return model;
        }

        private void processTemplate(String name, Model model, PrintWriter printer)
                throws IOException, TemplateException  {
            Template template = FTL_CONFIG.getTemplate(name);
            template.process(model.getRoot(), printer);
        }

        private String getJVMOptions() {
            if (config.hasValue(Config.JAVA_OPTIONS)) {
                return config.getValue(Config.JAVA_OPTIONS).toString();
            }
            int memSize = DEFAULT_FARM_JVM_MEMORY;
            if (config.hasValue(Config.JAVA_MEMORY)) {
                memSize = (Integer) config.getValue(Config.JAVA_MEMORY);
            }
            return String.format("-Xms%dg -Xmx%dg -XX:+UseNUMA -XX:+UseBiasedLocking",
                                 memSize, memSize);
        }

        private void appendOpt(StringBuilder sb, String opt, Object value) {
            sb.append(" ").append(opt).append(" \"").append(value).append("\"");
        }

        private void appendArg(StringBuilder sb, Object value) {
            sb.append(" \"").append(value).append("\"");
        }
    }


    static class ShowFarmStatus extends FarmCommand {

        ShowFarmStatus(Context context) {
            super(context, "farmStatus", "Show status of farm submitted jobs.");
        }

        @Override
        public int execute(String[] args) {
            String system = config.getValue(FARM_SYSTEM).toString();
            if (system.equals(JLAB_SYSTEM)) {
                if (CommandUtils.checkProgram(JLAB_STAT_CMD)) {
                    return CommandUtils.runProcess(JLAB_STAT_CMD, "-u", Config.user());
                }
                writer.println("Error: can not run farm operations from this node = " + getHost());
                return EXIT_ERROR;
            }
            if (system.equals(PBS_SYSTEM)) {
                if (CommandUtils.checkProgram(PBS_STAT_CMD)) {
                    return CommandUtils.runProcess(PBS_STAT_CMD, "-u", Config.user());
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
            String system = config.getValue(FARM_SYSTEM).toString();
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
        return Paths.get(Config.claraHome(), "lib", "clara", "templates");
    }


    private static String getHost() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new UncheckedIOException(e);
        }
    }
}
