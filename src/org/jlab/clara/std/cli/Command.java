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

import java.util.LinkedHashMap;
import java.util.StringTokenizer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.jline.reader.Completer;
import org.jline.reader.impl.completer.AggregateCompleter;
import org.jline.reader.impl.completer.ArgumentCompleter;
import org.jline.reader.impl.completer.StringsCompleter;
import org.jline.terminal.Terminal;

abstract class Command implements AutoCloseable {

    protected final String name;
    protected final String description;
    protected final Terminal terminal;
    protected Map<String, Argument> arguments;

    Command(Terminal terminal, String name, String description) {
        this.name = name;
        this.description = description;
        this.terminal = terminal;
        this.arguments = new LinkedHashMap<>();
    }

    public abstract void execute(String[] args);

    protected void executeSubcommand(String[] args) {
        if (args.length < 2) {
            terminal.writer().println("Error: missing argument.");
            return;
        }
        String subCommandName = args[1];
        Argument subCommand = arguments.get(subCommandName);
        if (subCommand == null) {
            terminal.writer().println("Error: invalid argument.");
            return;
        }
        try {
            subCommand.getAction().accept(args);
        } catch (IllegalArgumentException e) {
            terminal.writer().println("Error: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    protected Completer getCompleter() {
        List<Completer> completers = arguments.values()
                .stream()
                .map(this::getCompleter)
                .collect(Collectors.toList());
        return new AggregateCompleter(completers);
    }

    private Completer getCompleter(Argument arg) {
        Completer command = new StringsCompleter(getName());
        Completer subCommand = new StringsCompleter(arg.getName());
        return new ArgumentCompleter(command, subCommand, arg.getCompleter());
    }

    protected void showFullHelp() {
        terminal.writer().println("Commands:\n");
        for (Argument aux: arguments.values()) {
            terminal.writer().printf("  %s %s\n", name, aux.getName());
            String description = aux.getDescription();
            if (description.length() > 72) {
                terminal.writer().printf("    %s\n", splitLine(description, 72));
            } else {
                terminal.writer().printf("    %s\n", aux.getDescription());
            }
            terminal.writer().println();

        }
    }

    protected String splitLine(String input, int maxLineLength) {
        StringTokenizer tok = new StringTokenizer(input, " ");
        StringBuilder output = new StringBuilder(input.length());
        int lineLen = 0;
        while (tok.hasMoreTokens()) {
            String word = tok.nextToken() + " ";

            if (lineLen + word.length() > maxLineLength) {
                output.append("\n    ");
                lineLen = 0;
            }
            output.append(word);
            lineLen += word.length();
        }
        return output.toString();
    }

    @Override
    public void close() throws Exception {
        // nothing
    }
}
