package org.jlab.clara.std.cli;

import org.jline.terminal.Terminal;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
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
    private static final String FARM_FLAVOR = "farm.flavor";

    private static final int DEFAULT_FARM_MEMORY = 70;
    private static final int DEFAULT_FARM_CORES = 72;
    private static final int DEFAULT_FARM_DISK_SPACE = 3;
    private static final int DEFAULT_FARM_TIME = 24 * 60;
    private static final int DEFAULT_FARM_JVM_MEMORY = 40;
    private static final String DEFAULT_FARM_OS = "centos7";
    private static final String DEFAULT_FARM_TRACK = "debug";

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

        addBuilder.apply(FARM_FLAVOR, "")
            .withExpectedValues("jlab", "pbs")
            .withInitialValue("jlab");

        vl.forEach(builder::withConfigVariable);
    }

    private static String defaultConfigFile() {
        return PLUGIN.resolve("config/services.yaml").toString();
    }

    private static String defaultFileList() {
        return PLUGIN.resolve("config/files.list").toString();
    }

    private static String defaultFarmSubFile() {
        return PLUGIN.resolve("config/clara_p.jsub").toString();
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


    static class RunFarm extends AbstractCommand {

        private final Config config;

        RunFarm(Terminal terminal, Config config) {
            super(terminal, "farm", "Run CLARA data processing on the farm.");

            this.config = config;
        }

        @Override
        public int execute(String[] args) {
            PrintWriter writer = terminal.writer();
            String flavor = config.getValue(FARM_FLAVOR).toString();
            if (flavor.equals("jlab")) {
                if (CommandUtils.checkProgram("jsub")) {
                    String jsubFile = defaultFarmSubFile();
                    try {
                        setFarmScript(jsubFile);
                        return CommandUtils.runProcess("jsub", jsubFile);
                    } catch (IOException e) {
                        writer.println("Error: could not create file = " + jsubFile);
                    }
                }
                writer.println("Error: can not run farm job from this node = " + getHost());
                return EXIT_ERROR;
            }
            writer.println("Error: invalid farm flavor = " + flavor);
            return EXIT_ERROR;
        }

        private void setFarmScript(String path) throws IOException {
            StringBuilder cmd = new StringBuilder();
            cmd.append("setenv CLARA_HOME ").append(Config.claraHome()).append("; ");
            cmd.append("setenv CLAS12DIR ").append(PLUGIN).append("; ");
            cmd.append(Config.claraHome()).append("/bin/remove-dpe; ");

            cmd.append(Config.claraHome()).append("/lib/clara/run-farm");
            appendOpt(cmd, "-c", config.getValue(Config.SERVICES_FILE));
            appendOpt(cmd, "-f", config.getValue(Config.FILES_LIST));
            appendOpt(cmd, "-i", config.getValue(Config.INPUT_DIR));
            appendOpt(cmd, "-o", config.getValue(Config.OUTPUT_DIR));
            if (config.hasValue(FARM_STAGE)) {
                appendOpt(cmd, "-l ", config.getValue(FARM_STAGE));
            }
            appendOpt(cmd, "-t", config.getValue(FARM_CPU));
            appendOpt(cmd, "-s", config.getValue(Config.SESSION));
            appendOpt(cmd, "-J", getJVMOptions());
            appendOpt(cmd, "-W", 20);

            try (PrintStream printer = new PrintStream(new FileOutputStream(path, false))) {
                printer.printf("PROJECT: clas12%n");
                printer.printf("JOBNAME: rec-%s-%s%n",
                        Config.user(), config.getValue(Config.DESCRIPTION));
                printer.printf("MEMORY: %s GB%n", config.getValue(FARM_MEMORY));
                printer.printf("TRACK: %s%n", config.getValue(FARM_TRACK));
                printer.printf("OS: %s%n", config.getValue(FARM_OS));
                printer.printf("CPU: %s%n", config.getValue(FARM_CPU));
                printer.printf("DISK_SPACE: %s GB%n", config.getValue(FARM_DISK));
                printer.printf("TIME: %s%n", config.getValue(FARM_TIME));
                printer.printf("COMMAND: %s%n", cmd.toString());
            }
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
    }


    private static String getHost() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new UncheckedIOException(e);
        }
    }
}
