package org.jlab.clara.std.cli;

import org.jlab.clara.util.FileUtils;
import org.jline.terminal.Terminal;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

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

    static final Path PLUGIN = Paths.get(Config.claraHome(), "plugins", "clas12");


    private FarmCommands() { }

    private static void clasVariables(ClaraShell.Builder builder) {
        builder.withConfigVariable(Config.SERVICES_FILE, defaultConfigFile());
        builder.withConfigVariable(Config.FILES_LIST, defaultFileList());
        builder.withConfigVariable(Config.SESSION, Config.user());
    }

    private static void farmVariables(ClaraShell.Builder builder) {
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
        clasVariables(builder);
        farmVariables(builder);

        builder.withEnvironmentVariable("CLAS12DIR", PLUGIN.toString());
        builder.withRunSubCommand((t, c) -> new RunFarm(t, c));
    }


    private abstract static class FarmCommand extends AbstractCommand {

        protected final Config config;

        protected FarmCommand(Terminal terminal, Config config, String name, String description) {
            super(terminal, name, description);
            this.config = config;
        }

        protected Path getJobScript(String ext) {
            String keyword = config.getValue(Config.DESCRIPTION).toString();
            String name = String.format("farm-%s-%s", Config.user(), keyword);
            return PLUGIN.resolve("config/" + name + ext);
        }
    }


    static class RunFarm extends FarmCommand {

        RunFarm(Terminal terminal, Config config) {
            super(terminal, config, "farm", "Run CLARA data processing on the farm.");
        }

        @Override
        public int execute(String[] args) {
            PrintWriter writer = terminal.writer();
            String system = config.getValue(FARM_SYSTEM).toString();
            if (system.equals(JLAB_SYSTEM)) {
                if (CommandUtils.checkProgram(JLAB_SUB_CMD)) {
                    try {
                        Path jobFile = createJLabScript();
                        return CommandUtils.runProcess(JLAB_SUB_CMD, jobFile.toString());
                    } catch (IOException e) {
                        writer.println("Error: could not set job:  " + e.getMessage());
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

        private Path createClaraScript() throws IOException {
            Path wrapper = getJobScript(".sh");
            try (PrintWriter printer = FileUtils.openOutputTextFile(wrapper, false)) {
                printer.printf("#!/bin/bash%n");
                printer.println();
                printer.printf("export MALLOC_ARENA_MAX=2%n");
                printer.printf("export MALLOC_MMAP_THRESHOLD_=131072%n");
                printer.printf("export MALLOC_TRIM_THRESHOLD_=131072%n");
                printer.printf("export MALLOC_TOP_PAD_=131072%n");
                printer.printf("export MALLOC_MMAP_MAX_=65536%n");
                printer.printf("export MALLOC_MMAP_MAX_=65536%n");
                printer.println();
                printer.printf("export CLARA_HOME=\"%s\"%n", Config.claraHome());
                printer.printf("export CLAS12DIR=\"%s\"%n", PLUGIN);
                printer.println();
                printer.printf("\"%s%s\"%n", Config.claraHome(), "/bin/remove-dpe");
                printer.println();
                printer.println("sleep $[ ( $RANDOM % 20 )  + 1 ]s");
                printer.println();
                printer.println(getClaraCommand());
            }
            wrapper.toFile().setExecutable(true);
            return wrapper;
        }

        private Path createJLabScript() throws IOException {
            Path jobFile = getJobScript(JLAB_SUB_EXT);
            Path wrapper = createClaraScript();
            try (PrintWriter printer = FileUtils.openOutputTextFile(jobFile, false)) {
                printer.printf("PROJECT: clas12%n");
                printer.printf("JOBNAME: rec-%s-%s%n",
                        Config.user(), config.getValue(Config.DESCRIPTION));
                printer.printf("MEMORY: %s GB%n", config.getValue(FARM_MEMORY));
                printer.printf("TRACK: %s%n", config.getValue(FARM_TRACK));
                printer.printf("OS: %s%n", config.getValue(FARM_OS));
                printer.printf("CPU: %s%n", config.getValue(FARM_CPU));
                printer.printf("DISK_SPACE: %s GB%n", config.getValue(FARM_DISK));
                printer.printf("TIME: %s%n", config.getValue(FARM_TIME));
                printer.printf("COMMAND: %s%n", wrapper);
            }
            return jobFile;
        }

        private Path createPbsScript() throws IOException {
            Path jobFile = getJobScript(PBS_SUB_EXT);
            Path wrapper = createClaraScript();

            int diskKb = (int) config.getValue(FARM_DISK) * 1024 * 1024;
            int time = (int) config.getValue(FARM_TIME);
            String walltime = String.format("%d:%02d:00", time / 60, time % 60);

            try (PrintWriter printer = FileUtils.openOutputTextFile(jobFile, false)) {
                printer.printf("#!/bin/csh%n");
                printer.println();
                printer.printf("#PBS -N rec-%s-%s%n",
                        Config.user(), config.getValue(Config.DESCRIPTION));
                printer.printf("#PBS -A clas12%n");
                printer.printf("#PBS -S /bin/csh%n");
                printer.printf("#PBS -l nodes=1:ppn=%s%n", config.getValue(FARM_CPU));
                printer.printf("#PBS -l file=%dkb%n", diskKb);
                printer.printf("#PBS -l walltime=%s%n", walltime);
                printer.println();
                printer.printf("\"%s\"%n", wrapper);
            }

            return jobFile;
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

        ShowFarmStatus(Terminal terminal, Config config) {
            super(terminal, config, "farmStatus", "Show status of farm submitted jobs.");
        }

        @Override
        public int execute(String[] args) {
            PrintWriter writer = terminal.writer();
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

        ShowFarmSub(Terminal terminal, Config config) {
            super(terminal, config, "farmSub", "Show farm job submission file.");
        }

        @Override
        public int execute(String[] args) {
            PrintWriter writer = terminal.writer();
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


    private static String getHost() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new UncheckedIOException(e);
        }
    }
}
