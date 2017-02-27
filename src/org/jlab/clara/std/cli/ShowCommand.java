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

class ShowCommand extends Command {

    private final RunConfig runConfig;

    ShowCommand(Terminal terminal, RunConfig runConfig) {
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
