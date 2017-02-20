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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.jline.reader.Completer;
import org.jline.reader.impl.completer.StringsCompleter;
import org.jline.terminal.Terminal;

public abstract class Command {

    private final String name;
    private final String description;
    protected final Terminal terminal;
    protected Map<String, Argument> arguments;

    public Command(Terminal terminal, String name, String description) {
        this.name = name;
        this.description = description;
        this.terminal = terminal;
        this.arguments = new LinkedHashMap<>();
    }

    public abstract void execute(String[] args);

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    protected Completer getCompleter() {
        Completer command = new StringsCompleter(getName());
        Completer subCommands = argumentsCompleter();
        return new ArgumentCompleter(command, subCommands);
    }

    protected Completer argumentsCompleter() {
        List<String> names = arguments.values()
                .stream()
                .map(Argument::getName)
                .collect(Collectors.toList());
        return new StringsCompleter(names);
    }

    public void showFullHelp() {
        terminal.writer().println("Commands:\n");
        for (Argument aux: arguments.values()) {
            terminal.writer().printf("   %s %-15s", name, aux.getName());
            terminal.writer().printf("   %s\n", aux.getDescription());

        }
    }
}
