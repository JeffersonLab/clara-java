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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.jlab.clara.base.ClaraUtil;
import org.jline.reader.Completer;
import org.jline.reader.impl.completer.ArgumentCompleter;
import org.jline.reader.impl.completer.FileNameCompleter;
import org.jline.reader.impl.completer.NullCompleter;
import org.jline.reader.impl.completer.StringsCompleter;
import org.jline.terminal.Terminal;

class SourceCommand extends AbstractCommand {

    private final CommandRunner commandRunner;

    SourceCommand(Terminal terminal, CommandRunner commandRunner) {
        super(terminal, "source", "Read and execute commands from file");
        this.commandRunner = commandRunner;
    }

    @Override
    public int execute(String[] args) {
        if (args.length < 1) {
            terminal.writer().println("Missing filename argument");
            return EXIT_ERROR;
        }
        Path path = Paths.get(args[0]);
        for (String line : readLines(path)) {
            terminal.writer().println(line);
            commandRunner.execute(line);
        }
        return EXIT_SUCCESS;
    }

    private static List<String> readLines(Path sourceFile) {
        try {
            Pattern pattern = Pattern.compile("^\\s*#.*$");
            return Files.lines(sourceFile)
                        .filter(line -> !line.isEmpty())
                        .filter(line -> !pattern.matcher(line).matches())
                        .collect(Collectors.toList());
        } catch (FileNotFoundException | NoSuchFileException e) {
            System.out.println("Error: invalid file " + e.getMessage());
        } catch (IOException e) {
            System.out.println("Error: could not read source file " + e.getMessage());
        }
        return new ArrayList<>();
    }

    @Override
    public Completer getCompleter() {
        Completer command = new StringsCompleter(getName());
        Completer fileCompleter = new FileNameCompleter();
        return new ArgumentCompleter(command, fileCompleter, NullCompleter.INSTANCE);
    }

    @Override
    public void printHelp(PrintWriter writer) {
        writer.printf("%n  source <file_path>%n");
        writer.printf("%s.%n", ClaraUtil.splitIntoLines(description, "    ", 72));
    }
}
