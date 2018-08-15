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
import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.clara.std.orchestrators.OrchestratorConfigException;
import org.jlab.clara.std.orchestrators.OrchestratorConfigParser;
import org.jlab.clara.util.FileUtils;

class RunCommand extends BaseCommand {

    RunCommand(Context context) {
        super(context, "run", "Start CLARA data processing");
        addSubCommand(RunLocal::new);
    }

    private static class RunLocal extends AbstractCommand {

        private static final int LOWER_PORT = 7000;
        private static final int UPPER_PORT = 8000;
        private static final int STEP_PORTS = 20;

        private final Map<ClaraLang, DpeProcess> backgroundDpes;
        private final RunUtils runUtils;

        RunLocal(Context context) {
            super(context, "local", "Run CLARA data processing on the local node.");
            this.backgroundDpes = new HashMap<>();
            this.runUtils = new RunUtils(config);
        }

        @Override
        public int execute(String[] args) {
            try {
                DpeName feDpe = startLocalDpes();
                int exitStatus = runOrchestrator(feDpe);
                if (Thread.interrupted()) {
                    destroyDpes();
                    Thread.currentThread().interrupt();
                }
                return exitStatus;
            } catch (OrchestratorConfigException e) {
                writer.println("Error: " + e.getMessage());
                return EXIT_ERROR;
            } catch (Exception e) {
                e.printStackTrace();
                return EXIT_ERROR;
            }
        }

        private int runOrchestrator(DpeName feName) {
            String[] cmd = orchestratorCmd(feName);
            Path logFile = runUtils.getLogFile(getHost(feName), "orch");
            return CommandUtils.runProcess(buildProcess(cmd, logFile));
        }

        private String[] orchestratorCmd(DpeName feName) {
            Path orchestrator = FileUtils.claraPath("bin", "clara-orchestrator");
            SystemCommandBuilder cmd = new SystemCommandBuilder(orchestrator);

            cmd.addOption("-F");
            cmd.addOption("-f", feName);

            cmd.addOption("-t", getThreads());
            if (config.hasValue(Config.REPORT_EVENTS)) {
                cmd.addOption("-r", config.getInt(Config.REPORT_EVENTS));
            }
            if (config.hasValue(Config.SKIP_EVENTS)) {
                cmd.addOption("-k", config.getInt(Config.SKIP_EVENTS));
            }
            if (config.hasValue(Config.MAX_EVENTS)) {
                cmd.addOption("-e", config.getInt(Config.MAX_EVENTS));
            }
            cmd.addOption("-i", config.getString(Config.INPUT_DIR));
            cmd.addOption("-o", config.getString(Config.OUTPUT_DIR));
            cmd.addOption("-z", config.getString(Config.OUT_FILE_PREFIX));

            cmd.addArgument(config.getString(Config.SERVICES_FILE));
            cmd.addArgument(config.getString(Config.FILES_LIST));

            return cmd.toArray();
        }

        private DpeName startLocalDpes() throws IOException {
            String configFile = config.getString(Config.SERVICES_FILE);
            OrchestratorConfigParser parser = new OrchestratorConfigParser(configFile);
            Set<ClaraLang> languages = parser.parseLanguages();

            if (checkDpes(languages)) {
                return backgroundDpes.get(ClaraLang.JAVA).name;
            }
            destroyDpes();

            DpeName feName = new DpeName(findHost(), findPort(), ClaraLang.JAVA);
            String javaDpe = FileUtils.claraPath("bin", "j_dpe").toString();
            addBackgroundDpeProcess(feName, javaDpe,
                    "--host", getHost(feName),
                    "--port", getPort(feName),
                    "--session", runUtils.getSession());

            if (languages.contains(ClaraLang.CPP)) {
                int cppPort = feName.address().pubPort() + 10;
                DpeName cppName = new DpeName(feName.address().host(), cppPort, ClaraLang.CPP);
                String cppDpe = FileUtils.claraPath("bin", "c_dpe").toString();
                addBackgroundDpeProcess(cppName, cppDpe,
                        "--host", getHost(cppName),
                        "--port", getPort(cppName),
                        "--fe-host", getHost(feName),
                        "--fe-port", getPort(feName));
            }

            if (languages.contains(ClaraLang.PYTHON)) {
                int pyPort = feName.address().pubPort() + 5;
                DpeName pyName = new DpeName(feName.address().host(), pyPort, ClaraLang.PYTHON);
                String pyDpe = FileUtils.claraPath("bin", "p_dpe").toString();
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
                return config.getString(Config.FRONTEND_HOST);
            }
            return ClaraUtil.localhost();
        }

        private int findPort() {
            if (config.hasValue(Config.FRONTEND_PORT)) {
                return config.getInt(Config.FRONTEND_PORT);
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
            return dpe != null && dpe.process.isAlive();
        }

        private void addBackgroundDpeProcess(DpeName name, String... command)
                throws IOException {
            if (!backgroundDpes.containsKey(name.language())) {
                Path logFile = runUtils.getLogFile(name);
                ProcessBuilder builder = buildProcess(command, logFile);
                if (name.language() == ClaraLang.JAVA) {
                    String javaOptions = getJVMOptions();
                    if (javaOptions != null) {
                        builder.environment().put("JAVA_OPTS", javaOptions);
                    }
                }
                String monitor = runUtils.getMonitorFrontEnd();
                if (monitor != null) {
                    builder.environment().put(ClaraConstants.ENV_MONITOR_FE, monitor);
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

        private ProcessBuilder buildProcess(String[] command, Path logFile) {
            String[] wrapper = CommandUtils.uninterruptibleCommand(command, logFile);
            ProcessBuilder builder = new ProcessBuilder(wrapper);
            builder.environment().putAll(config.getenv());
            builder.inheritIO();
            return builder;
        }

        private Integer getThreads() {
            if (config.hasValue(Config.MAX_THREADS)) {
                return config.getInt(Config.MAX_THREADS);
            }
            return Runtime.getRuntime().availableProcessors();
        }

        private String getJVMOptions() {
            if (config.hasValue(Config.JAVA_OPTIONS)) {
                return config.getString(Config.JAVA_OPTIONS);
            }
            if (config.hasValue(Config.JAVA_MEMORY)) {
                int memSize = config.getInt(Config.JAVA_MEMORY);
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
