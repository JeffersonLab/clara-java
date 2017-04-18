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

final class RunUtils {

    private RunUtils() { }

    static Path getLogDir() {
        return Paths.get(Config.claraHome(), "log");
    }

    static Path getLogFile(DpeName name, String keyword) {
        ClaraLang lang = name.language();
        String component = lang == ClaraLang.JAVA ? "fe-dpe" : lang + "-dpe";
        return getLogFile(name.address().host(), keyword, component);
    }

    static Path getLogFile(String host, String keyword, String component) {
        String logName = String.format("%s-%s-%s-%s.log", host, Config.user(), keyword, component);
        return getLogDir().resolve(logName);
    }

    static Path getLogFile(Path feLog, ClaraLang dpeLang) {
        Path logDir = getLogDir();
        String name = FileUtils.getFileName(feLog).toString();
        return logDir.resolve(name.replaceAll("fe-dpe", dpeLang + "-dpe"));
    }

    static List<Path> getLogFiles(String keyword, String component) throws IOException {
        String glob = String.format("glob:*-%s-%s-%s.log", Config.user(), keyword, component);
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

    static int listFiles(String dir, String opts) {
        return CommandUtils.runProcess("ls", "-" + opts, dir);
    }
}
