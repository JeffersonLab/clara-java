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

import java.io.PrintWriter;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.jlab.clara.base.ClaraUtil;
import org.jline.reader.Completer;
import org.jline.reader.impl.completer.AggregateCompleter;
import org.jline.reader.impl.completer.ArgumentCompleter;
import org.jline.reader.impl.completer.StringsCompleter;
import org.jline.terminal.Terminal;

abstract class Command implements AutoCloseable {

    protected static final int EXIT_SUCCESS = 0;
    protected static final int EXIT_ERROR = 1;

    protected final String name;
    protected final String description;
    protected final Terminal terminal;
    protected Map<String, SubCommand> subCommands;

    Command(Terminal terminal, String name, String description) {
        this.name = name;
        this.description = description;
        this.terminal = terminal;
        this.subCommands = new LinkedHashMap<>();
    }

    public abstract int execute(String[] args);

    protected int executeSubcommand(String[] args) {
        if (args.length < 1) {
            terminal.writer().println("Error: missing argument.");
            return EXIT_ERROR;
        }
        String subCommandName = args[0];
        SubCommand subCommand = subCommands.get(subCommandName);
        if (subCommand == null) {
            terminal.writer().println("Error: invalid argument.");
            return EXIT_ERROR;
        }
        try {
            String[] cmdArgs = Arrays.copyOfRange(args, 1, args.length);
            return subCommand.getAction().apply(cmdArgs);
        } catch (IllegalArgumentException e) {
            terminal.writer().println("Error: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return EXIT_ERROR;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    protected Completer getCompleter() {
        List<Completer> completers = subCommands.values()
                .stream()
                .map(this::getCompleter)
                .collect(Collectors.toList());
        return new AggregateCompleter(completers);
    }

    private Completer getCompleter(SubCommand arg) {
        Completer command = new StringsCompleter(getName());
        Completer subCommand = new StringsCompleter(arg.getName());
        return new ArgumentCompleter(command, subCommand, arg.getCompleter());
    }

    protected void printHelp(PrintWriter writer) {
        writer.println("Commands:");
        for (SubCommand cmd: subCommands.values()) {
            writer.printf("%n  %s %s%n", name, cmd.getName());
            writer.printf("%s%n", ClaraUtil.splitIntoLines(cmd.getDescription(), "    ", 72));
        }
    }

    @Override
    public void close() throws Exception {
        // nothing
    }
}
