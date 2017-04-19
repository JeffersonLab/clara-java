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

import java.util.Map;

import org.jline.terminal.Terminal;

class HelpCommand extends BaseCommand {

    private final Map<String, Command> commands;

    HelpCommand(Terminal terminal, Map<String, Command> commands) {
        super(terminal, "help", "Display help information about CLARA shell");
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
        command.printHelp(writer);
        return EXIT_SUCCESS;
    }

    private void printCommand(String name) {
        Command command = commands.get(name);
        writer.printf("   %-14s", command.getName());
        writer.printf("%s\n", command.getDescription());
    }

    private void addCommands() {
        commands.values().stream()
                .filter(c -> !c.getName().equals("help"))
                .forEach(c -> addSubCommand(c.getName(), args -> 0, c.getDescription()));
    }
}
