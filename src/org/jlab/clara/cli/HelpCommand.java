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

package org.jlab.clara.cli;

import java.util.Map;

import org.jline.terminal.Terminal;

public class HelpCommand extends Command {

    private final Map<String, Command> commands;

    public HelpCommand(Terminal terminal, Map<String, Command> commands) {
        super(terminal, "help", "Display help information about CLARA shell");
        this.commands = commands;
    }

    @Override
    public void execute(String[] args) {
        //String[] aux = Arrays.copyOfRange(args, 1, args.length);
        if (args.length == 1) {
            terminal.writer().println("Commands:\n");
            printCommand("set");
            printCommand("edit");
            printCommand("run");
            printCommand("monitor");
            printCommand("reset");
            terminal.writer().println("\nUse help <command> for details about each command.");
        } else {
            commands.get(args[1]).showFullHelp();
        }
    }

    private void printCommand(String name) {
        Command command = commands.get(name);
        terminal.writer().printf("   %-14s", command.getName());
        terminal.writer().printf("%s\n", command.getDescription());
    }
}
