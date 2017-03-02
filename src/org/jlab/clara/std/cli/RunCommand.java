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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.PumpStreamHandler;
import org.jline.terminal.Terminal;

class RunCommand extends Command {

    private final RunConfig runConfig;
    private final Set<Process> backgroundProcesses;

    RunCommand(Terminal terminal, RunConfig runConfig) {
        super(terminal, "run", "Start data processing");
        this.runConfig = runConfig;
        this.backgroundProcesses = new HashSet<>();
        setArguments();
    }

    private void setArguments() {
        arguments.put("local", new Argument("local", "", ""));
        arguments.put("farm", new Argument("farm", "", ""));
    }

    @Override
    public void execute(String[] args) {

        if (args.length == 1) {
            terminal.writer().println("Missing arguments.");
        } else if ("local".equals(args[1])) {
            runLocal();
        } else if ("farm".equals(args[1])) {
            terminal.writer().println("running run farm.");
        } else {
            terminal.writer().println("Invalid command: " + args[1]);
        }
    }

    private void runLocal() {
        CommandLine cmdLine = new CommandLine("java");
        cmdLine.addArgument("-cp");
        cmdLine.addArgument(System.getProperty("java.class.path"));
        cmdLine.addArgument(runConfig.getOrchestrator());
        cmdLine.addArgument("-F");
        cmdLine.addArgument(runConfig.getConfigFile());
        cmdLine.addArgument(runConfig.getFilesList());

        DefaultExecutor executor = new DefaultExecutor();
        PumpStreamHandler streamHandler = new PumpStreamHandler(terminal.output());
        executor.setStreamHandler(streamHandler);

        try {
            addBackgroundProcess(CommandUtils.runDpe("j_dpe"));
            executor.execute(cmdLine);
        } catch (ExecuteException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void addBackgroundProcess(Process p) {
        if (p.isAlive()) {
            backgroundProcesses.add(p);
        }
    }

    @Override
    public void close() {
        for (Process process : backgroundProcesses) {
            try {
                process.destroy();
                process.waitFor(10, TimeUnit.SECONDS);
                if (process.isAlive()) {
                    process.destroyForcibly();
                    process.waitFor(5, TimeUnit.SECONDS);
                }
                process.getOutputStream().flush();
            } catch (InterruptedException e) {
                // ignore
            } catch (IOException e) {
                // ignore
            }
        }
    }
}
