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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.jlab.clara.util.FileUtils;
import org.jline.reader.Completer;
import org.jline.reader.impl.completer.ArgumentCompleter;
import org.jline.reader.impl.completer.FileNameCompleter;
import org.jline.reader.impl.completer.NullCompleter;
import org.jline.reader.impl.completer.StringsCompleter;
import org.jline.terminal.Terminal;

class SetCommand extends BaseCommand {

    private final Config config;

    SetCommand(Terminal terminal, Config config) {
        super(terminal, "set", "Parameter settings");
        this.config = config;
        setArguments();
    }

    private void setArguments() {
        subCmd("servicesFile",
                config::setConfigFile, ConfigParsers::toExistingFile,
                new FileNameCompleter(),
                "Full path to the file describing application service composition. "
                    + "(default: $CLARA_HOME/plugins/clas12/config/services.yaml)");

        subCmd("files",
                this::setFiles, Function.identity(),
                new FileNameCompleter(),
                "Set the input files to be processed (Example: /mnt/data/files/*.evio).");

        subCmd("fileList",
                config::setFilesList, ConfigParsers::toExistingFile,
                new FileNameCompleter(),
                "Full path to the file containing the names of data-files to be processed. "
                    + "Note: actual files are located in the inputDir. "
                    + "(Default: $CLARA_HOME/plugins/clas12/config/files.list)");

        subCmd("inputDir",
                config::setInputDir, ConfigParsers::toExistingDirectory,
                new FileNameCompleter(),
                "The input directory where the files to be processed are located. "
                    + "(Default: $CLARA_HOME/data/in)");

        subCmd("outputDir",
                config::setOutputDir, ConfigParsers::toExistingDirectory,
                new FileNameCompleter(),
                "The output directory where processed files will be saved. "
                    + "(Default: $CLARA_HOME/data/out)");

        subCmd("session",
                config::setSession, ConfigParsers::toString,
                new FileNameCompleter(),
                "The data processing session. (Default: $USER)");

        subCmd("threads",
                config::setMaxThreads, ConfigParsers::toInteger,
                NullCompleter.INSTANCE,
                "The maximum number of processing threads to be used per node. "
                    + "In case value = auto all system cores will be used. (Default: 2)");
    }

    private <T> void subCmd(String name,
                            Consumer<T> action,
                            Function<String, T> parser,
                            Completer completer,
                            String description) {
        Command subCmd = new AbstractCommand(terminal, name, description) {

            @Override
            public int execute(String[] args) {
                try {
                    action.accept(parser.apply(args[0]));
                    return EXIT_SUCCESS;
                } catch (Exception e) {
                    terminal.writer().printf("could not set variable: %s%n", e.getMessage());
                    return EXIT_ERROR;
                }
            }

            @Override
            public Completer getCompleter() {
                return new ArgumentCompleter(new StringsCompleter(name), completer);
            }
        };

        addSubCommand(subCmd);
    }

    private void setFiles(String files) {
        try {
            Path path = Paths.get(FileUtils.expandHome(files));
            File tempFile = File.createTempFile("temp", "");
            try (PrintStream printer = new PrintStream(new BufferedOutputStream(
                    new FileOutputStream(tempFile, false)))) {
                if (Files.isDirectory(path)) {
                    int numFiles = listDir(printer, path, f -> true);
                    if (numFiles > 0) {
                        config.setInputDir(path.toString());
                        config.setFilesList(tempFile.getAbsolutePath());
                    } else {
                        System.out.println("Error: empty directory");
                    }
                } else if (Files.isRegularFile(path)) {
                    printer.println(path.getFileName());
                    config.setInputDir(FileUtils.getParent(path).toString());
                    config.setFilesList(tempFile.getAbsolutePath());
                } else if (path.getFileName().toString().contains("*")
                        && Files.isDirectory(FileUtils.getParent(path))) {
                    String pattern = path.getFileName().toString();
                    String glob = "glob:" + pattern;
                    PathMatcher matcher = FileSystems.getDefault().getPathMatcher(glob);
                    int numFiles = listDir(printer, FileUtils.getParent(path), matcher::matches);
                    if (numFiles > 0) {
                        config.setInputDir(FileUtils.getParent(path).toString());
                        config.setFilesList(tempFile.getAbsolutePath());
                    } else {
                        System.out.println("Error: no files matched");
                    }
                } else {
                    System.out.println("Error: invalid path");
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private int listDir(PrintStream printer, Path directory, Predicate<Path> filter)
            throws IOException {
        List<Path> files = Files.list(directory)
                    .filter(Files::isRegularFile)
                    .map(Path::getFileName)
                    .filter(filter)
                    .sorted()
                    .collect(Collectors.toList());
        files.forEach(printer::println);
        return files.size();
    }
}
