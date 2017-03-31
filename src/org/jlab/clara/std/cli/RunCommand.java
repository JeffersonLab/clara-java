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

import org.jlab.clara.base.ClaraLang;
import org.jlab.clara.std.orchestrators.OrchestratorConfigParser;
import org.jline.terminal.Terminal;

class RunCommand extends BaseCommand {

    RunCommand(Terminal terminal, Config config) {
        super(terminal, "run", "Start data processing");
        addSubCommand(new RunLocal(terminal, config));
    }

    private static class RunLocal extends AbstractCommand {

        private final Config config;
        private final Map<ClaraLang, Process> backgroundDpes;

        RunLocal(Terminal terminal, Config config) {
            super(terminal, "local", "");
            this.config = config;
            this.backgroundDpes = new HashMap<>();
        }

        @Override
        public int execute(String[] args) {
            try {
                startLocalDpes();

                Path orchestrator = Paths.get(Config.claraHome(), "bin", "clara-orchestrator");
                int exitStatus = CommandUtils.runProcess(orchestrator.toString(),
                        "-F",
                        "-t", config.getValue(Config.MAX_THREADS).toString(),
                        "-i", config.getValue(Config.INPUT_DIR).toString(),
                        "-o", config.getValue(Config.OUTPUT_DIR).toString(),
                        config.getValue(Config.SERVICES_FILE).toString(),
                        config.getValue(Config.FILES_LIST).toString());

                if (Thread.interrupted()) {
                    destroyDpes();
                    Thread.currentThread().interrupt();
                }
                return exitStatus;
            } catch (Exception e) {
                e.printStackTrace();
                return EXIT_ERROR;
            }
        }

        private void startLocalDpes() throws IOException {
            String configFile = config.getValue(Config.SERVICES_FILE).toString();
            OrchestratorConfigParser parser = new OrchestratorConfigParser(configFile);
            Set<ClaraLang> languages = parser.parseLanguages();

            if (checkDpes(languages)) {
                return;
            }
            destroyDpes();

            String javaDpe = Paths.get(Config.claraHome(), "bin", "j_dpe").toString();
            addBackgroundDpeProcess(ClaraLang.JAVA, javaDpe);

            if (languages.contains(ClaraLang.CPP)) {
                String cppDpe = Paths.get(Config.claraHome(), "bin", "c_dpe").toString();
                addBackgroundDpeProcess(ClaraLang.CPP, cppDpe, "--fe-host", "localhost");
            }

            if (languages.contains(ClaraLang.PYTHON)) {
                String pyDpe = Paths.get(Config.claraHome(), "bin", "p_dpe").toString();
                addBackgroundDpeProcess(ClaraLang.PYTHON, pyDpe, "--fe-host", "localhost");
            }
        }

        private boolean checkDpes(Set<ClaraLang> languages) {
            return languages.equals(backgroundDpes.keySet())
                && languages.stream().allMatch(this::isDpeAlive);
        }

        private boolean isDpeAlive(ClaraLang lang) {
            Process process = backgroundDpes.get(lang);
            if (process == null) {
                return false;
            }
            return process.isAlive();
        }

        private void addBackgroundDpeProcess(ClaraLang lang, String... command)
                throws IOException {
            if (!backgroundDpes.containsKey(lang)) {
                backgroundDpes.put(lang, CommandUtils.runDpe(command));
            }
        }

        private void destroyDpes() {
            // kill the DPEs in reverse order (the front-end last)
            for (ClaraLang lang : Arrays.asList(ClaraLang.PYTHON, ClaraLang.CPP, ClaraLang.JAVA)) {
                Process process = backgroundDpes.remove(lang);
                if (process == null) {
                    continue;
                }
                CommandUtils.destroyProcess(process);
            }
        }

        @Override
        public void close() {
            destroyDpes();
        }
    }
}
