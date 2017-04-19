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
import java.net.ServerSocket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.jlab.clara.base.ClaraLang;
import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.base.DpeName;
import org.jlab.clara.std.orchestrators.OrchestratorConfigError;
import org.jlab.clara.std.orchestrators.OrchestratorConfigParser;
import org.jline.terminal.Terminal;

class RunCommand extends BaseCommand {

    RunCommand(Terminal terminal, Config config) {
        super(terminal, "run", "Start CLARA data processing");
        addSubCommand(new RunLocal(terminal, config));
    }

    private static class RunLocal extends AbstractCommand {

        private static final int LOWER_PORT = 7000;
        private static final int UPPER_PORT = 8000;
        private static final int STEP_PORTS = 20;

        private final Config config;
        private final Map<ClaraLang, DpeProcess> backgroundDpes;

        RunLocal(Terminal terminal, Config config) {
            super(terminal, "local", "Run CLARA data processing on the local node.");
            this.config = config;
            this.backgroundDpes = new HashMap<>();
        }

        @Override
        public int execute(String[] args) {
            try {
                DpeName feDpe = startLocalDpes();

                Path orchestrator = Paths.get(Config.claraHome(), "bin", "clara-orchestrator");
                int exitStatus = runOrchestrator(feDpe,
                        orchestrator.toString(),
                        "-F",
                        "-f", feDpe.toString(),
                        "-t", getThreads().toString(),
                        "-i", config.getValue(Config.INPUT_DIR).toString(),
                        "-o", config.getValue(Config.OUTPUT_DIR).toString(),
                        config.getValue(Config.SERVICES_FILE).toString(),
                        config.getValue(Config.FILES_LIST).toString());

                if (Thread.interrupted()) {
                    destroyDpes();
                    Thread.currentThread().interrupt();
                }
                return exitStatus;
            } catch (OrchestratorConfigError e) {
                terminal.writer().println("Error: " + e.getMessage());
                return EXIT_ERROR;
            } catch (Exception e) {
                e.printStackTrace();
                return EXIT_ERROR;
            }
        }

        private int runOrchestrator(DpeName feName, String... cmd) {
            String logFile = RunUtils.getLogFile(getHost(feName), getKeyword(), "orch").toString();
            return CommandUtils.runProcess(buildProcess(cmd, logFile));
        }

        private DpeName startLocalDpes() throws IOException {
            String configFile = config.getValue(Config.SERVICES_FILE).toString();
            OrchestratorConfigParser parser = new OrchestratorConfigParser(configFile);
            Set<ClaraLang> languages = parser.parseLanguages();

            if (checkDpes(languages)) {
                return backgroundDpes.get(ClaraLang.JAVA).name;
            }
            destroyDpes();

            DpeName feName = new DpeName(findHost(), findPort(), ClaraLang.JAVA);
            String javaDpe = Paths.get(Config.claraHome(), "bin", "j_dpe").toString();
            addBackgroundDpeProcess(feName, javaDpe,
                    "--host", getHost(feName),
                    "--port", getPort(feName));

            if (languages.contains(ClaraLang.CPP)) {
                int cppPort = feName.address().pubPort() + 10;
                DpeName cppName = new DpeName(feName.address().host(), cppPort, ClaraLang.CPP);
                String cppDpe = Paths.get(Config.claraHome(), "bin", "c_dpe").toString();
                addBackgroundDpeProcess(cppName, cppDpe,
                        "--host", getHost(cppName),
                        "--port", getPort(cppName),
                        "--fe-host", getHost(feName),
                        "--fe-port", getPort(feName));
            }

            if (languages.contains(ClaraLang.PYTHON)) {
                int pyPort = feName.address().pubPort() + 5;
                DpeName pyName = new DpeName(feName.address().host(), pyPort, ClaraLang.PYTHON);
                String pyDpe = Paths.get(Config.claraHome(), "bin", "p_dpe").toString();
                addBackgroundDpeProcess(pyName, pyDpe,
                        "--host", getHost(pyName),
                        "--port", getPort(pyName),
                        "--fe-host", getHost(feName),
                        "--fe-port", getPort(feName));
            }

            return feName;
        }

        private String findHost() {
            if (config.hasValue(Config.FRONTEND_HOST)) {
                return config.getValue(Config.FRONTEND_HOST).toString();
            }
            return ClaraUtil.localhost();
        }

        private int findPort() {
            if (config.hasValue(Config.FRONTEND_PORT)) {
                return (Integer) config.getValue(Config.FRONTEND_PORT);
            }

            List<Integer> ports = IntStream.iterate(LOWER_PORT, n -> n + STEP_PORTS)
                                           .limit((UPPER_PORT - LOWER_PORT) / STEP_PORTS)
                                           .boxed()
                                           .collect(Collectors.toList());
            Collections.shuffle(ports);

            for (Integer port : ports) {
                int ctrlPort = port + 2;
                try (ServerSocket socket = new ServerSocket(ctrlPort)) {
                    socket.setReuseAddress(true);
                    return port;
                } catch (IOException e) {
                    continue;
                }
            }
            throw new IllegalStateException("Cannot find an available port");
        }

        private boolean checkDpes(Set<ClaraLang> languages) {
            return languages.equals(backgroundDpes.keySet())
                && languages.stream().allMatch(this::isDpeAlive);
        }

        private boolean isDpeAlive(ClaraLang lang) {
            DpeProcess dpe = backgroundDpes.get(lang);
            if (dpe == null) {
                return false;
            }
            return dpe.process.isAlive();
        }

        private void addBackgroundDpeProcess(DpeName name, String... command)
                throws IOException {
            if (!backgroundDpes.containsKey(name.language())) {
                String logFile = RunUtils.getLogFile(name, getKeyword()).toString();
                ProcessBuilder builder = buildProcess(command, logFile);
                if (name.language() == ClaraLang.JAVA) {
                    String javaOptions = getJVMOptions();
                    if (javaOptions != null) {
                        builder.environment().put("JAVA_OPTS", javaOptions);
                    }
                }
                DpeProcess dpe = new DpeProcess(name, builder);
                backgroundDpes.put(name.language(), dpe);
            }
        }

        private void destroyDpes() {
            // kill the DPEs in reverse order (the front-end last)
            for (ClaraLang lang : Arrays.asList(ClaraLang.PYTHON, ClaraLang.CPP, ClaraLang.JAVA)) {
                DpeProcess dpe = backgroundDpes.remove(lang);
                if (dpe == null) {
                    continue;
                }
                CommandUtils.destroyProcess(dpe.process);
            }
        }

        private ProcessBuilder buildProcess(String[] command, String logFile) {
            String[] wrapper = CommandUtils.uninterruptibleCommand(command, logFile);
            ProcessBuilder builder = new ProcessBuilder(wrapper);
            builder.environment().putAll(config.getenv());
            builder.inheritIO();
            return builder;
        }

        private Integer getThreads() {
            if (config.hasValue(Config.MAX_THREADS)) {
                return (Integer) config.getValue(Config.MAX_THREADS);
            }
            return Runtime.getRuntime().availableProcessors();
        }

        private String getKeyword() {
            return config.getValue(Config.DESCRIPTION).toString();
        }

        private String getJVMOptions() {
            if (config.hasValue(Config.JAVA_OPTIONS)) {
                return config.getValue(Config.JAVA_OPTIONS).toString();
            }
            if (config.hasValue(Config.JAVA_MEMORY)) {
                int memSize = (Integer) config.getValue(Config.JAVA_MEMORY);
                return String.format("-Xms%dg -Xmx%dg -XX:+UseNUMA -XX:+UseBiasedLocking",
                                     memSize, memSize);
            }
            return null;
        }

        @Override
        public void close() {
            destroyDpes();
        }
    }

    private static class DpeProcess {

        private final DpeName name;
        private final Process process;

        DpeProcess(DpeName name, ProcessBuilder builder) throws IOException {
            this.name = name;
            this.process = builder.start();
            ClaraUtil.sleep(2000);
        }
    }

    private static String getHost(DpeName name) {
        return name.address().host();
    }

    private static String getPort(DpeName name) {
        return Integer.toString(name.address().pubPort());
    }
}
