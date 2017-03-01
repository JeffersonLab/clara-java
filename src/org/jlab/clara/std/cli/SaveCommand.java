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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;

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
    public void execute(String[] args) {
        if (args.length == 2) {
            Path path = Paths.get(args[1]);
            if (Files.exists(path)) {
                boolean answer = scanAnswer();
                if (answer) {
                    writeFile(path);
                    terminal.writer().println("Config saved in " + path.toFile().getAbsolutePath());
                } else {
                    terminal.writer().println("The config was not saved");
                }
            } else {
                writeFile(path);
                terminal.writer().println("Config saved in " + path.toFile().getAbsolutePath());
            }
        } else {
            terminal.writer().println("Missing filename argument");
        }
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
                    continue;
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
            printer.println("set farmFlavor " + runConfig.getFarmFlavor());
            printer.println("set farmLoadingZone " + runConfig.getFarmLoadingZone());
            printer.println("set farmMemory " + runConfig.getFarmMemory());
            printer.println("set farmTrack " + runConfig.getFarmTrack());
            printer.println("set farmOS " + runConfig.getFarmOS());
            printer.println("set farmCPU " + runConfig.getFarmCPU());
            printer.println("set farmDisk " + runConfig.getFarmDisk());
            printer.println("set farmTime " + runConfig.getFarmTime());
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
    public void showFullHelp() {
        terminal.writer().printf("\n  save <file_path>\n");
        terminal.writer().printf("    %s\n", splitLine(description, 72));
    }
}
