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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.jline.builtins.Less;
import org.jline.builtins.Source;

class HelpCommand extends BaseCommand {

    private final Map<String, Command> commands;

    HelpCommand(Context context, Map<String, Command> commands) {
        super(context, "help", "Display help information about CLARA shell");
        this.commands = commands;
        addCommands();
    }

    @Override
    public int execute(String[] args) {
        if (args.length < 1) {
            writer.println("Commands:\n");
            subCommands.values().stream()
                       .map(Command::getName)
                       .forEach(this::printCommand);
            writer.println("\nUse help <command> for details about each command.");
            return EXIT_SUCCESS;
        }

        Command command = commands.get(args[0]);
        if (command == null) {
            writer.println("Invalid command name.");
            return EXIT_ERROR;
        }
        return showHelp(command);
    }

    private void printCommand(String name) {
        Command command = commands.get(name);
        writer.printf("   %-14s", command.getName());
        writer.printf("%s%n", command.getDescription());
    }

    private void addCommands() {
        commands.values().stream()
                .filter(c -> !c.getName().equals("help"))
                .forEach(c -> addSubCommand(c.getName(), args -> 0, c.getDescription()));
    }

    private int showHelp(Command command) {
        try {
            String help = getHelp(command);
            if (terminal.getHeight() - 2 > countLines(help)) {
                writer.print(help);
            } else {
                Less less = new Less(terminal);
                less.run(new Source() {

                    @Override
                    public String getName() {
                        return "help " + command.getName();
                    }

                    @Override
                    public InputStream read() throws IOException {
                        String text = String.format("help %s%n%s%n", command.getName(), help);
                        return new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8));
                    }
                });
            }
            return EXIT_SUCCESS;
        } catch (IOException e) {
            writer.print("Error: could not show help: " + e.getMessage());
            return EXIT_ERROR;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return EXIT_ERROR;
        }
    }

    private String getHelp(Command command) {
        StringWriter helpWriter = new StringWriter();
        PrintWriter printer = new PrintWriter(helpWriter);
        command.printHelp(printer);
        printer.close();
        return helpWriter.toString();
    }

    private int countLines(String str) {
        // TODO it could be faster
        String[] lines = str.split("\r\n|\r|\n");
        return lines.length;
    }
}
