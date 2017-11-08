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

import org.jlab.clara.base.ClaraLang;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class ShowCommand extends BaseCommand {

    ShowCommand(Context context) {
        super(context, "show", "Show values");
        setArguments();
    }

    private void setArguments() {
        addSubCommand("config", args -> showConfig(), "Show configuration variables.");
        addSubCommand("services", args -> showServices(), "Show services YAML.");
        addSubCommand("files", args -> showFiles(), "Show input files list.");
        addSubCommand("inputDir", args -> showInputDir(), "List input files directory.");
        addSubCommand("outputDir", args -> showOutputDir(), "List output files directory.");
        addSubCommand("logDir", args -> showLogDir(), "List logs directory.");
        addSubCommand("logDpe", args -> showDpeLog(), "Show front-end DPE log.");
        addSubCommand("logOrchestrator", args -> showOrchestratorLog(), "Show orchestrator log.");
        if (FarmCommands.hasPlugin()) {
            addSubCommand(FarmCommands.ShowFarmStatus::new);
            addSubCommand(FarmCommands.ShowFarmSub::new);
        }
    }

    private int showConfig() {
        writer.println();
        config.getVariables().forEach(this::printFormat);
        return EXIT_SUCCESS;
    }

    private void printFormat(ConfigVariable variable) {
        System.out.printf("%-20s %s%n", variable.getName() + ":", getValue(variable));
    }

    private String getValue(ConfigVariable variable) {
        if (!variable.hasValue()) {
            return "NO VALUE";
        }
        Object value = variable.getValue();
        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }
        return "\"" + value + "\"";
    }

    private int showServices() {
        return printFile(Config.SERVICES_FILE);
    }

    private int showFiles() {
        return printFile(Config.FILES_LIST);
    }

    private int showInputDir() {
        String variable = Config.INPUT_DIR;
        if (!config.hasValue(variable)) {
            writer.printf("Error: variable %s is not set%n", variable);
            return EXIT_ERROR;
        }
        return RunUtils.listFiles(config.getValue(variable).toString(), "lh");
    }

    private int showOutputDir() {
        String variable = Config.OUTPUT_DIR;
        if (!config.hasValue(variable)) {
            writer.printf("Error: variable %s is not set%n", variable);
            return EXIT_ERROR;
        }
        return RunUtils.listFiles(config.getValue(variable).toString(), "lh");
    }

    private int showLogDir() {
        String logDir = Paths.get(Config.claraHome(), "log").toString();
        return RunUtils.listFiles(logDir, "lhtr");
    }

    private int showDpeLog() {
        return printLog("fe_dpe", "DPE");
    }

    private int showOrchestratorLog() {
        return printLog("orch", "orchestrator");
    }

    private int printFile(String variable) {
        if (!config.hasValue(variable)) {
            writer.printf("Error: variable %s is not set%n", variable);
            return EXIT_ERROR;
        }
        Path path = Paths.get(config.getValue(variable).toString());
        return RunUtils.printFile(terminal, path);
    }

    private int printLog(String component, String description) {
        String keyword = config.getValue(Config.DESCRIPTION).toString();
        List<Path> logs;
        try {
            RunUtils runUtils = new RunUtils(config);
            logs = runUtils.getLogFiles(keyword, component);
        } catch (IOException e) {
            writer.printf("Error: could not open log directory%n");
            return EXIT_ERROR;
        }
        if (logs.isEmpty()) {
            writer.printf("Error: no logs for %s%n", Config.user());
            return EXIT_ERROR;
        }
        Path log = logs.get(0);
        if (component.equals("fe_dpe")) {
            return RunUtils.paginateFile(terminal, description, getDpeLogs(log));
        }
        return RunUtils.paginateFile(terminal, description, log);
    }

    private Path[] getDpeLogs(Path fe) {
        List<Path> logs = new ArrayList<>();
        logs.add(fe);
        RunUtils runUtils = new RunUtils(config);
        for (ClaraLang lang : Arrays.asList(ClaraLang.CPP, ClaraLang.PYTHON)) {
            Path path = runUtils.getLogFile(fe, lang);
            if (Files.exists(path)) {
                logs.add(path);
            }
        }
        return logs.toArray(new Path[logs.size()]);
    }
}
