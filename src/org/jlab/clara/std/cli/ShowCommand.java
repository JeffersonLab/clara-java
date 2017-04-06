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

import org.jline.builtins.Commands;
import org.jline.terminal.Terminal;

import java.io.IOException;
import java.nio.file.Files;
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

    private int showDpeLog() {
        return showLog("fe-dpe", "DPE");
    }

    private int showOrchestratorLog() {
        return showLog("orch", "orchestrator");
    }

    private int showLog(String type, String description) {
        String host = config.getValue(Config.FRONTEND_HOST).toString();
        Path path = RunUtils.getLogFile(host, type);
        if (!Files.exists(path)) {
            terminal.writer().printf("error: no %s log: %s%n", description, path);
            return EXIT_ERROR;
        }
        try {
            String[] args = new String[] {path.toString()};
            Commands.less(terminal, System.out, System.err, Paths.get(""), args);
        } catch (IOException e) {
            terminal.writer().printf("error: could not open %s log: %s%n", description, e);
            return EXIT_ERROR;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return EXIT_ERROR;
        }
        return EXIT_SUCCESS;
    }
}
