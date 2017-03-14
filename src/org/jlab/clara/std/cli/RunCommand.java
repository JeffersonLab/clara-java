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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.jlab.clara.base.ClaraLang;
import org.jlab.clara.std.orchestrators.OrchestratorConfigParser;
import org.jline.terminal.Terminal;

class RunCommand extends Command {

    private final RunConfig runConfig;
    private final Map<ClaraLang, Process> backgroundDpes;

    RunCommand(Terminal terminal, RunConfig runConfig) {
        super(terminal, "run", "Start data processing");
        this.runConfig = runConfig;
        this.backgroundDpes = new HashMap<>();
        setArguments();
    }

    private void setArguments() {
        arguments.put("local", new Argument("local", "", args -> { }));
        arguments.put("farm", new Argument("farm", "", args -> { }));
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
        try {
            startLocalDpes();

            Path orchestrator = Paths.get(RunConfig.claraHome(), "bin", "clara-orchestrator");
            CommandUtils.runCommand(terminal,
                    orchestrator.toString(),
                    "-F",
                    "-t", Integer.toString(runConfig.getMaxThreads()),
                    "-i", runConfig.getInputDir(),
                    "-o", runConfig.getOutputDir(),
                    runConfig.getConfigFile(),
                    runConfig.getFilesList());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void startLocalDpes() throws IOException {
        OrchestratorConfigParser parser = new OrchestratorConfigParser(runConfig.getConfigFile());
        Set<ClaraLang> languages = parser.parseLanguages();

        String javaDpe = Paths.get(RunConfig.claraHome(), "bin", "j_dpe").toString();
        addBackgroundDpeProcess(ClaraLang.JAVA, javaDpe);

        if (languages.contains(ClaraLang.CPP)) {
            String cppDpe = Paths.get(RunConfig.claraHome(), "bin", "c_dpe").toString();
            addBackgroundDpeProcess(ClaraLang.CPP, cppDpe, "--fe-host", "localhost");
        }

        if (languages.contains(ClaraLang.PYTHON)) {
            String cppDpe = Paths.get(RunConfig.claraHome(), "bin", "p_dpe").toString();
            addBackgroundDpeProcess(ClaraLang.PYTHON, cppDpe, "--fe-host", "localhost");
        }
    }

    private void addBackgroundDpeProcess(ClaraLang lang, String... command) throws IOException {
        if (!backgroundDpes.containsKey(lang)) {
            backgroundDpes.put(lang, CommandUtils.runDpe(command));
        }
    }

    @Override
    public void close() {
        // kill the DPEs in reverse order (the front-end last)
        for (ClaraLang lang : Arrays.asList(ClaraLang.PYTHON, ClaraLang.CPP, ClaraLang.JAVA)) {
            try {
                Process process = backgroundDpes.get(lang);
                if (process == null) {
                    continue;
                }
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
