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
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.util.FileUtils;
import org.jline.reader.Completer;
import org.jline.reader.impl.completer.ArgumentCompleter;
import org.jline.reader.impl.completer.FileNameCompleter;
import org.jline.reader.impl.completer.NullCompleter;
import org.jline.reader.impl.completer.StringsCompleter;

class SourceCommand extends AbstractCommand {

    private final CommandRunner commandRunner;

    SourceCommand(Context context, CommandRunner commandRunner) {
        super(context, "source", "Read and execute commands from file");
        this.commandRunner = commandRunner;
    }

    @Override
    public int execute(String[] args) {
        boolean verbose = true;
        String sourceFile;

        if (args.length == 1) {
            sourceFile = args[0];
        } else if (args.length == 2) {
            if (args[0].equals("-q")) {
                verbose = false;
            } else {
                writer.println("Error: invalid number of arguments");
                return EXIT_ERROR;
            }
            sourceFile = args[1];
        } else {
            writer.println("Error: missing filename argument");
            return EXIT_ERROR;
        }

        Path path = FileUtils.expandHome(sourceFile);
        try {
            for (String line : readLines(path)) {
// System.out.println("DDD "+line);
                if (verbose) {
                    writer.println(line);
                }
                commandRunner.execute(line);
            }
            return EXIT_SUCCESS;
        } catch (NoSuchFileException e) {
            writer.println("Error: no such file: " + path);
            return EXIT_ERROR;
        } catch (IOException e) {
            writer.println("Error: could not read source file: " + e.getMessage());
            return EXIT_ERROR;
        } catch (UncheckedIOException e) {
            writer.println("Error: could not read source file: " + e.getCause().getMessage());
            return EXIT_ERROR;
        }
    }

    private static List<String> readLines(Path sourceFile) throws IOException {
        Pattern pattern = Pattern.compile("^\\s*#.*$");
        return Files.lines(sourceFile)
                    .filter(line -> !line.isEmpty())
                    .filter(line -> !pattern.matcher(line).matches())
                    .collect(Collectors.toList());
    }

    @Override
    public Completer getCompleter() {
        Completer command = new StringsCompleter(getName());
        Completer fileCompleter = new FileNameCompleter();
        return new ArgumentCompleter(command, fileCompleter, NullCompleter.INSTANCE);
    }

    @Override
    public void printHelp(PrintWriter printer) {
        printer.printf("%n  %s <file_path>%n", name);
        printer.printf("%s.%n", ClaraUtil.splitIntoLines(description, "    ", 72));
    }
}
