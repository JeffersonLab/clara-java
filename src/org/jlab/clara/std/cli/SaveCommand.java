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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;

import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.util.FileUtils;
import org.jline.reader.Completer;
import org.jline.reader.impl.completer.ArgumentCompleter;
import org.jline.reader.impl.completer.FileNameCompleter;
import org.jline.reader.impl.completer.NullCompleter;
import org.jline.reader.impl.completer.StringsCompleter;

class SaveCommand extends AbstractCommand {

    SaveCommand(Context context) {
        super(context, "save", "Export configuration to file");
    }

    @Override
    public int execute(String[] args) {
        if (args.length < 1) {
            writer.println("Error: missing filename argument");
            return EXIT_ERROR;
        }
        Path path = Paths.get(FileUtils.expandHome(args[0]));
        if (Files.exists(path)) {
            boolean overwrite = scanAnswer();
            if (!overwrite) {
                writer.println("The config was not saved");
                return EXIT_SUCCESS;
            }
        }
        return writeFile(path);
    }

    private boolean scanAnswer() {
        @SuppressWarnings("resource")
        Scanner scan = new Scanner(System.in);
        while (true) {
            String answer;
            System.out.print("The file already exists. Do you want to overwrite it? (y/N): ");
            answer = scan.nextLine();
            switch (answer) {
                case "y":
                case "Y":
                case "yes":
                case "Yes":
                    return true;
                case "n":
                case "N":
                case "no":
                case "No":
                case "":
                    return false;
                default:
                    System.out.println("Invalid answer.");
            }
        }
    }

    private int writeFile(Path path) {
        try (PrintWriter printer = FileUtils.openOutputTextFile(path, false)) {
            for (ConfigVariable variable : config.getVariables()) {
                if (variable.hasValue()) {
                    printer.printf("set %s %s%n", variable.getName(), variable.getValue());
                }
            }
        } catch (IOException e) {
            writer.printf("Error: could not write file: %s: %s%n", path, e.getMessage());
            return EXIT_ERROR;
        }
        writer.println("Config saved in " + path);
        return EXIT_SUCCESS;
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
