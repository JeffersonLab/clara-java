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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.jlab.clara.util.FileUtils;
import org.jline.reader.Completer;
import org.jline.reader.impl.completer.ArgumentCompleter;
import org.jline.reader.impl.completer.FileNameCompleter;
import org.jline.reader.impl.completer.StringsCompleter;

class SetCommand extends BaseCommand {

    SetCommand(Context context) {
        super(context, "set", "Parameter settings");
        setArguments();
    }

    private void setArguments() {
        List<CommandFactory> commands = new LinkedList<>();
        config.getVariables().stream().map(this::subCmd).forEach(commands::add);

        commands.add(1, subCmd("files", this::setFiles, new FileNameCompleter(),
                "Set the input files to be processed (example: /mnt/data/files/*.evio). "
                        + "This will set both fileList and inputDir variables."));

        commands.forEach(this::addSubCommand);
    }

    private CommandFactory subCmd(ConfigVariable v) {
        return subCmd(v.getName(), v::setValue, v.getCompleter(), v.getDescription());
    }

    private CommandFactory subCmd(String name,
                                  Consumer<String[]> action,
                                  Completer completer,
                                  String description) {
        return session -> new AbstractCommand(session, name, description) {

            @Override
            public int execute(String[] args) {
                try {
                    action.accept(args);
                    return EXIT_SUCCESS;
                } catch (Exception e) {
                    writer.printf("Error: %s%n", e.getMessage());
                    return EXIT_ERROR;
                }
            }

            @Override
            public Completer getCompleter() {
                return new ArgumentCompleter(new StringsCompleter(name), completer);
            }
        };
    }

    private void setFiles(String[] args) {
        if (args.length < 1) {
            throw new IllegalArgumentException("missing argument");
        }
        try {
            Path path = Paths.get(FileUtils.expandHome(args[0]));
            File output = getOutputFile();
            try (PrintWriter printer = FileUtils.openOutputTextFile(output.toPath(), false)) {
                if (Files.isDirectory(path)) {
                    listCommand(printer, args[0]);
                    int numFiles = listDir(printer, path, f -> true);
                    if (numFiles > 0) {
                        config.setValue(Config.INPUT_DIR, path.toString());
                        config.setValue(Config.FILES_LIST, output.getAbsolutePath());
                    } else {
                        throw new IllegalArgumentException("empty input directory");
                    }
                } else if (Files.isRegularFile(path)) {
                    listCommand(printer, args[0]);
                    printer.println(path.getFileName());
                    config.setValue(Config.INPUT_DIR, FileUtils.getParent(path).toString());
                    config.setValue(Config.FILES_LIST, output.getAbsolutePath());
                } else if (path.getFileName().toString().contains("*")
                        && Files.isDirectory(FileUtils.getParent(path))) {
                    listCommand(printer, args[0]);
                    String pattern = path.getFileName().toString();
                    String glob = "glob:" + pattern;
                    PathMatcher matcher = FileSystems.getDefault().getPathMatcher(glob);
                    int numFiles = listDir(printer, FileUtils.getParent(path), matcher::matches);
                    if (numFiles > 0) {
                        config.setValue(Config.INPUT_DIR, FileUtils.getParent(path).toString());
                        config.setValue(Config.FILES_LIST, output.getAbsolutePath());
                    } else {
                        printer.println("# no files matched");
                        throw new IllegalArgumentException("no files matched");
                    }
                } else {
                    throw new IllegalArgumentException("invalid path");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private File getOutputFile() {
        Path dir = FarmCommands.hasPlugin()
                ? FarmCommands.PLUGIN.resolve("config")
                : Paths.get("");
        String keyword = config.getValue(Config.DESCRIPTION).toString();
        String name = String.format("files-%s-%s.txt", Config.user(), keyword);
        return dir.resolve(name).toFile();
    }

    private void listCommand(PrintWriter printer, String arg) {
        printer.printf("# auto-generated by: set files %s%n", arg);
    }

    private int listDir(PrintWriter printer, Path directory, Predicate<Path> filter)
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
