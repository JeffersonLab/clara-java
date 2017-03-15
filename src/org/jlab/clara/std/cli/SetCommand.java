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

import org.jline.reader.Completer;
import org.jline.reader.impl.completer.FileNameCompleter;
import org.jline.terminal.Terminal;

class SetCommand extends Command {

    private final RunConfig runConfig;

    SetCommand(Terminal terminal, RunConfig runConfig) {
        super(terminal, "set", "Parameter settings");
        this.runConfig = runConfig;
        setArguments();
        setCompleters();
    }

    private void setArguments() {
        // CHECKSTYLE.OFF: LineLength
        newArg("plugin", "Plugin installation directory. (Default: $CLARA_HOME/plugins/clas12)", null);
        newArg("session", "The data processing session. (Default: $USER)", runConfig::setSession);
        newArg("inputDir", "The input directory where the files to be processed are located. (Default: $CLARA_HOME/data/in)", runConfig::setInputDir);
        newArg("outputDir", "The output directory where processed files will be saved. (Default: $CLARA_HOME/data/out)", runConfig::setOutputDir);
        newArg("threads", "The maximum number of processing threads to be used per node. In case value = auto all system cores will be used. (Default: 2)", runConfig::setMaxThreads, Integer::parseInt);
        newArg("fileList", "Full path to the file containing the names of data-files to be processed. Note: actual files are located in the inputDir. (Default: $CLARA_HOME/plugins/clas12/config/files.list)", runConfig::setFilesList);
        newArg("files", "Set the input files to be processed (Example: /mnt/data/files/*.evio).", this::setFiles);
        newArg("yaml", "Full path to the file describing application service composition. (Default: $CLARA_HOME/plugins/clas12/config/services.yaml)", runConfig::setConfigFile);
        // CHECKSTYLE.ON: LineLength
    }

    private void setCompleters() {
        Completer fileCompleter = new FileNameCompleter();
        arguments.get("fileList").setCompleter(fileCompleter);
        arguments.get("files").setCompleter(fileCompleter);
        arguments.get("yaml").setCompleter(fileCompleter);
        arguments.get("inputDir").setCompleter(fileCompleter);
        arguments.get("outputDir").setCompleter(fileCompleter);
    }

    private <T> void newArg(String name, String description, Consumer<String> action) {
        newArg(name, description, action, a -> a);
    }

    private <T> void newArg(String name,
                        String description,
                        Consumer<T> action,
                        Function<String, T> parser) {
        Function<String[], Integer> commandAction = args -> {
            T val;
            try {
                val = parser.apply(args[2]);
            } catch (Exception e) {
                throw new IllegalArgumentException("invalid argument", e);
            }
            action.accept(val);
            return EXIT_SUCCESS;
        };
        arguments.put(name, new Argument(name, description, commandAction));
    }

    @Override
    public int execute(String[] args) {
        return executeSubcommand(args);
    }

    private void setFiles(String files) {
        try {
            Path path = Paths.get(expandHome(files));
            File tempFile = File.createTempFile("temp", "");
            try (PrintStream printer = new PrintStream(new BufferedOutputStream(
                    new FileOutputStream(tempFile, false)))) {
                if (Files.isDirectory(path)) {
                    int numFiles = listDirectory(printer, path.getParent(), f -> true);
                    if (numFiles > 0) {
                        runConfig.setInputDir(path.toString());
                        runConfig.setFilesList(tempFile.getAbsolutePath());
                    } else {
                        System.out.println("Error: empty directory");
                    }
                } else if (Files.isRegularFile(path)) {
                    printer.println(path.getFileName());
                    runConfig.setInputDir(path.getParent().toString());
                    runConfig.setFilesList(tempFile.getAbsolutePath());
                } else if (path.getFileName().toString().contains("*")
                        && Files.isDirectory(path.getParent())) {
                    String pattern = path.getFileName().toString();
                    String glob = "glob:" + pattern;
                    PathMatcher pathMatcher = FileSystems.getDefault().getPathMatcher(glob);
                    int numFiles = listDirectory(printer, path.getParent(), pathMatcher::matches);
                    if (numFiles > 0) {
                        runConfig.setInputDir(path.getParent().toString());
                        runConfig.setFilesList(tempFile.getAbsolutePath());
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

    private String expandHome(String path) {
        if (path.startsWith("~")) {
            return newPath(path, "~");
        } else if (path.startsWith("$HOME")) {
            return newPath(path, "$HOME");
        }
        return path;
    }

    private String newPath(String path, String replace) {
        String home = System.getProperty("user.home");
        path = path.replace(replace, home);
        return path;
    }

    private int listDirectory(PrintStream printer, Path directory, Predicate<Path> filter)
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
