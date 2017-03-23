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

class ShowCommand extends BaseCommand {

    private final Config config;

    ShowCommand(Terminal terminal, Config config) {
        super(terminal, "show", "Show values");
        this.config = config;
        setArguments();
    }

    private void setArguments() {
        addSubCommand("config", args -> showConfig(), "Show parameter values");
    }

    private int showConfig() {
        terminal.writer().println();
        printFormat("localhost", config.getLocalHost());
        printFormat("configFile", config.getConfigFile());
        printFormat("filesList", config.getFilesList());
        printFormat("inputDir", config.getInputDir());
        printFormat("outputDir", config.getOutputDir());
        printFormat("useFrontEnd", Boolean.toString(config.isUseFrontEnd()));
        printFormat("session", config.getSession());
        printFormat("maxNodes", Integer.toString(config.getMaxNodes()));
        printFormat("maxThreads", Integer.toString(config.getMaxThreads()));
        return EXIT_SUCCESS;
    }

    private void printFormat(String param, String value) {
        System.out.printf("%-20s %s\n", param, value);
    }
}
