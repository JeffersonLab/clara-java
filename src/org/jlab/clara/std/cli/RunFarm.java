package org.jlab.clara.std.cli;

import org.jline.terminal.Terminal;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

class RunFarm extends AbstractCommand {

    private static final String FARM_STAGE = "farm.stage";
    private static final String FARM_MEMORY = "farm.memory";
    private static final String FARM_TRACK = "farm.track";
    private static final String FARM_OS = "farm.os";
    private static final String FARM_CPU = "farm.cpu";
    private static final String FARM_DISK = "farm.disk";
    private static final String FARM_TIME = "farm.time";
    private static final String FARM_FLAVOR = "farm.flavor";

    static final Path PLUGIN = Paths.get(Config.claraHome(), "plugins", "clas12");

    private final Config config;


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

        addBuilder.apply(FARM_CPU, "Farm resource core number request")
            .withParser(ConfigParsers::toPositiveInteger)
            .withInitialValue(72);

        addBuilder.apply(FARM_MEMORY, "Farm job memory request (in GB)")
            .withParser(ConfigParsers::toPositiveInteger)
            .withInitialValue(70);

        addBuilder.apply(FARM_DISK, "Farm job disk space request (in GB)")
            .withParser(ConfigParsers::toPositiveInteger)
            .withInitialValue(3);

        addBuilder.apply(FARM_TIME, "Farm job wall time request (in min)")
            .withParser(ConfigParsers::toPositiveInteger)
            .withInitialValue(1440);

        addBuilder.apply(FARM_OS, "Farm resource OS")
            .withInitialValue("centos7");

        addBuilder.apply(FARM_STAGE, "Local directory to stage reconstruction files")
            .withInitialValue("undefined");

        addBuilder.apply(FARM_TRACK, "Farm job track")
            .withInitialValue("debug");

        addBuilder.apply(FARM_FLAVOR, "")
            .withExpectedValues("jlab", "pbs")
            .withInitialValue("jlab");

        vl.stream().map(ConfigVariable.Builder::build)
                   .forEach(builder::withConfigVariable);
    }

    private static String defaultConfigFile() {
        return PLUGIN.resolve("config/services.yaml").toString();
    }

    private static String defaultFileList() {
        return PLUGIN.resolve("config/files.list").toString();
    }

    private static String defaultFarmSubFile() {
        return PLUGIN.resolve("config/p_clara.jsub").toString();
    }

    static boolean hasPlugin() {
        return Files.isDirectory(PLUGIN);
    }

    static void register(ClaraShell.Builder builder) {
        clasVariables(builder);
        farmVariables(builder);

        builder.withRunSubCommand((t, c) -> new RunFarm(t, c));
    }


    RunFarm(Terminal terminal, Config config) {
        super(terminal, "farm", "Run CLARA data processing on the farm.");

        this.config = config;
    }

    @Override
    public int execute(String[] args) {

        String flavor = config.getValue(FARM_FLAVOR).toString();
        if (flavor.equals("jlab")) {
            if (CommandUtils.checkProgram("jsub")) {
                String jsubFile = defaultFarmSubFile();
                try {
                    setFarmScript(jsubFile);
                    return CommandUtils.runProcess("jsub", jsubFile);
                } catch (IOException e) {
                    terminal.writer().println("Error: could not create file = " + jsubFile);
                }
            }
            terminal.writer().println("Error: can not run farm job from this node = " + getHost());
            return EXIT_ERROR;
        }
        terminal.writer().println("Error: invalid farm flavor = " + flavor);
        return EXIT_ERROR;
    }

    private void setFarmScript(String path) throws IOException {
        StringBuilder cmd = new StringBuilder();
        cmd.append("setenv CLARA_HOME ").append(Config.claraHome()).append("; ");
        cmd.append(Config.claraHome()).append("/bin/remove-dpe; ");

        cmd.append(Config.claraHome()).append("/bin/etc/f-clara");
        cmd.append(" ").append(config.getValue(Config.INPUT_DIR));
        cmd.append(" ").append(config.getValue(Config.OUTPUT_DIR));
        cmd.append(" ").append(config.getValue(Config.SERVICES_FILE));
        cmd.append(" ").append(config.getValue(Config.FILES_LIST));
        cmd.append(" ").append(PLUGIN);
        cmd.append(" ").append("clara");
        cmd.append(" ").append(config.getValue(FARM_CPU));
        cmd.append(" ").append(config.getValue(Config.SESSION));
        cmd.append(" ").append(config.getValue(FARM_STAGE));

        try (PrintStream printer = new PrintStream(new FileOutputStream(path, false))) {
            printer.printf("PROJECT: clas12%n");
            printer.printf("JOBNAME: rec-%s-clara%n", Config.user());
            printer.printf("MEMORY: %s GB%n", config.getValue(FARM_MEMORY));
            printer.printf("TRACK: %s%n", config.getValue(FARM_TRACK));
            printer.printf("OS: %s%n", config.getValue(FARM_OS));
            printer.printf("CPU: %s%n", config.getValue(FARM_CPU));
            printer.printf("DISK_SPACE: %s GB%n", config.getValue(FARM_DISK));
            printer.printf("TIME: %s%n", config.getValue(FARM_TIME));
            printer.printf("COMMAND: %s%n", cmd.toString());
        }
    }

    private String getHost() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new UncheckedIOException(e);
        }
    }
}
