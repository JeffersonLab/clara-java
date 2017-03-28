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
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;

import org.jlab.clara.base.ClaraUtil;
import org.jline.reader.Completer;
import org.jline.reader.impl.completer.ArgumentCompleter;
import org.jline.reader.impl.completer.FileNameCompleter;
import org.jline.reader.impl.completer.StringsCompleter;
import org.jline.terminal.Terminal;

class SaveCommand extends Command {

    private final RunConfig runConfig;

    SaveCommand(Terminal terminal, RunConfig runConfig) {
        super(terminal, "save", "Export configuration to file");
        this.runConfig = runConfig;
    }

    @Override
    public int execute(String[] args) {
        if (args.length < 2) {
            terminal.writer().println("Missing filename argument");
            return EXIT_ERROR;
        }
        Path path = Paths.get(args[1]);
        if (Files.exists(path)) {
            boolean overwrite = scanAnswer();
            if (!overwrite) {
                terminal.writer().println("The config was not saved");
                return EXIT_SUCCESS;
            }
        }
        writeFile(path);
        terminal.writer().println("Config saved in " + path.toFile().getAbsolutePath());
        return EXIT_SUCCESS;
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

    private void writeFile(Path path) {
        try (PrintStream printer = new PrintStream(new FileOutputStream(path.toFile(), false))) {
            printer.println("set filesList " + runConfig.getFilesList());
            printer.println("set inputDir " + runConfig.getInputDir());
            printer.println("set outputDir " + runConfig.getOutputDir());
            printer.println("set session " + runConfig.getSession());
            printer.println("set threads " + runConfig.getMaxThreads());
        } catch (FileNotFoundException e) {
            terminal.writer().println("Could not create file: " + path);
        }
    }

    @Override
    public Completer getCompleter() {
        Completer command = new StringsCompleter(getName());
        Completer fileCompleter = new FileNameCompleter();
        return new ArgumentCompleter(command, fileCompleter);
    }

    @Override
    public void printHelp(PrintWriter writer) {
        writer.printf("%n  save <file_path>%n");
        writer.printf("%s%n", ClaraUtil.splitIntoLines(description, "    ", 72));
    }
}
