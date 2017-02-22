package org.jlab.clara.cli;

import org.jline.terminal.Terminal;

public class ShowCommand extends Command {

    private final RunConfig runConfig;

    public ShowCommand(Terminal terminal, RunConfig runConfig) {
        super(terminal, "show", "Show values");
        this.runConfig = runConfig;
        setArguments();
    }

    private void setArguments() {
        arguments.put("config", new Argument("config", "Show parameter values", ""));
    }

    @Override
    public void execute(String[] args) {
        if (args.length == 1) {
            terminal.writer().println("Missing arguments.");
        } else if ("config".equals(args[1])) {
            showConfig();
        } else {
            terminal.writer().println("Invalid command: " + args[1]);
        }
    }

    public void showConfig() {
        terminal.writer().println();
        terminal.writer().println("orchestrator: " + runConfig.getOrchestrator());
        terminal.writer().println("localHost: " + runConfig.getLocalHost());
        terminal.writer().println("configFile: " + runConfig.getConfigFile());
        terminal.writer().println("filesList: " + runConfig.getFilesList());
        terminal.writer().println("inputDir: " + runConfig.getInputDir());
        terminal.writer().println("outputDir: " + runConfig.getOutputDir());
        terminal.writer().println("useFrontEnd: " + runConfig.isUseFrontEnd());
        terminal.writer().println("session: " + runConfig.getSession());
        terminal.writer().println("maxNodes: " + runConfig.getMaxNodes());
        terminal.writer().println("maxThreads: " + runConfig.getMaxThreads());
        terminal.writer().println("farmFlavor: " + runConfig.getFarmFlavor());
        terminal.writer().println("farmLoadingZone: " + runConfig.getFarmLoadingZone());
        terminal.writer().println("farmMemory: " + runConfig.getFarmMemory());
        terminal.writer().println("farmTrack: " + runConfig.getFarmTrack());
        terminal.writer().println("farmOS: " + runConfig.getFarmOS());
        terminal.writer().println("farmCPU: " + runConfig.getFarmCPU());
        terminal.writer().println("farmDisk: " + runConfig.getFarmDisk());
        terminal.writer().println("farmTime: " + runConfig.getFarmTime());
    }
}
