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

import org.jline.terminal.Terminal;

import java.nio.file.Path;
import java.nio.file.Paths;

class ShowCommand extends BaseCommand {

    private final Config config;

    ShowCommand(Terminal terminal, Config config) {
        super(terminal, "show", "Show values");
        this.config = config;
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
    }

    private int showConfig() {
        terminal.writer().println();
        config.getVariables().forEach(this::printFormat);
        return EXIT_SUCCESS;
    }

    private void printFormat(ConfigVariable variable) {
        System.out.printf("%-20s %s\n", variable.getName() + ":", getValue(variable));
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
            terminal.writer().printf("error: variable %s is not set%n", variable);
            return EXIT_ERROR;
        }
        return RunUtils.listFiles(config.getValue(variable).toString());
    }

    private int showOutputDir() {
        String variable = Config.OUTPUT_DIR;
        if (!config.hasValue(variable)) {
            terminal.writer().printf("error: variable %s is not set%n", variable);
            return EXIT_ERROR;
        }
        return RunUtils.listFiles(config.getValue(variable).toString());
    }

    private int showLogDir() {
        String logDir = Paths.get(Config.claraHome(), "log").toString();
        return RunUtils.listFiles(logDir);
    }

    private int showDpeLog() {
        return printLog("fe-dpe", "DPE");
    }

    private int showOrchestratorLog() {
        return printLog("orch", "orchestrator");
    }

    private int printFile(String variable) {
        if (!config.hasValue(variable)) {
            terminal.writer().printf("error: variable %s is not set%n", variable);
            return EXIT_ERROR;
        }
        Path path = Paths.get(config.getValue(variable).toString());
        return RunUtils.printFile(terminal, path);
    }

    private int printLog(String component, String description) {
        String host = config.getValue(Config.FRONTEND_HOST).toString();
        String keyword = config.getValue(Config.DESCRIPTION).toString();
        Path path = RunUtils.getLogFile(host, keyword, component);
        return RunUtils.paginateFile(terminal, path, description);
    }
}
