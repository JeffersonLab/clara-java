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
import org.jlab.clara.base.DpeName;
import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.clara.util.FileUtils;
import org.jline.builtins.Commands;
import org.jline.terminal.Terminal;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

class RunUtils {

    private final Config config;

    RunUtils(Config config) {
        this.config = config;
    }

    String getMonitorFrontEnd() {
        if (config.hasValue(Config.MONITOR_HOST)) {
            DpeName monDpe = new DpeName(config.getString(Config.MONITOR_HOST),
                                         ClaraConstants.MONITOR_PORT,
                                         ClaraLang.JAVA);
            return monDpe.canonicalName();
        }
        return System.getenv(ClaraConstants.ENV_MONITOR_FE);
    }

    String getSession() {
        String sessionId = config.getString(Config.SESSION);
        String sessionDesc = config.getString(Config.DESCRIPTION);
        return sessionId + "_" + sessionDesc;
    }

    Path getLogDir() {
        return Paths.get(config.getString(Config.LOG_DIR));
    }

    Path getLogFile(DpeName name) {
        ClaraLang lang = name.language();
        String component = lang == ClaraLang.JAVA ? "fe_dpe" : lang + "_dpe";
        return getLogFile(name.address().host(), component);
    }

    Path getLogFile(String host, String component) {
        String logName = String.format("%s_%s_%s.log", host, getSession(), component);
        return getLogDir().resolve(logName);
    }

    Path getLogFile(Path feLog, ClaraLang dpeLang) {
        Path logDir = getLogDir();
        String name = FileUtils.getFileName(feLog).toString();
        return logDir.resolve(name.replaceAll("fe_dpe", dpeLang + "_dpe"));
    }

    List<Path> getLogFiles(String component) throws IOException {
        String glob = String.format("glob:*_%s_%s.log", getSession(), component);
        PathMatcher matcher = FileSystems.getDefault().getPathMatcher(glob);

        Function<Path, Long> modDate = path -> path.toFile().lastModified();

        return Files.list(getLogDir())
                    .filter(Files::isRegularFile)
                    .filter(p -> matcher.matches(p.getFileName()))
                    .sorted((a, b) -> Long.compare(modDate.apply(b), modDate.apply(a)))
                    .collect(Collectors.toList());
    }

    static int printFile(Terminal terminal, Path path) {
        if (!Files.exists(path)) {
            terminal.writer().printf("error: no file %s%n", path);
            return Command.EXIT_ERROR;
        }
        try {
            Files.lines(path).forEach(terminal.writer()::println);
            return Command.EXIT_SUCCESS;
        } catch (IOException e) {
            terminal.writer().printf("error: could not open file: %s%n", e);
            return Command.EXIT_ERROR;
        }
    }

    static int paginateFile(Terminal terminal, String description, Path... paths) {
        for (Path path : paths) {
            if (!Files.exists(path)) {
                terminal.writer().printf("error: no %s log: %s%n", description, path);
                return 1;
            }
        }
        try {
            String[] args = Arrays.stream(paths)
                                  .map(Path::toString)
                                  .toArray(String[]::new);
            Commands.less(terminal, System.out, System.err, Paths.get(""), args);
            return Command.EXIT_SUCCESS;
        } catch (IOException e) {
            terminal.writer().printf("error: could not open %s log: %s%n", description, e);
            return Command.EXIT_ERROR;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Command.EXIT_ERROR;
        }
    }

    static int listFiles(Path dir, String opts) {
        return CommandUtils.runProcess("ls", "-" + opts, dir.toString());
    }
}
