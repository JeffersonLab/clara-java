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

import java.util.Arrays;
import java.util.Map;

import org.jlab.clara.util.EnvUtils;
import org.jline.reader.EndOfFileException;
import org.jline.reader.Parser;
import org.jline.reader.impl.DefaultParser;
import org.jline.terminal.Terminal;
import org.jline.terminal.Terminal.Signal;
import org.jline.terminal.Terminal.SignalHandler;

class CommandRunner {

    private final Terminal terminal;
    private final Parser parser;
    private final Map<String, Command> commands;

    CommandRunner(Terminal terminal, Map<String, Command> commands) {
        this.terminal = terminal;
        this.parser = new DefaultParser();
        this.commands = commands;
    }

    public int execute(String line) {
        String[] shellArgs = parseLine(line);
        if (shellArgs == null) {
            return Command.EXIT_ERROR;
        }
        if (shellArgs.length == 0) {
            return Command.EXIT_SUCCESS;
        }
        String commandName = shellArgs[0];
        Command command = commands.get(commandName);
        if (command == null) {
            if ("exit".equals(commandName)) {
                throw new EndOfFileException();
            }
            terminal.writer().println("Invalid command");
            return Command.EXIT_ERROR;
        }
        Thread execThread = Thread.currentThread();
        SignalHandler prevIntHandler = terminal.handle(Signal.INT, s -> {
            execThread.interrupt();
        });
        try {
            String[] cmdArgs = Arrays.copyOfRange(shellArgs, 1, shellArgs.length);
            return command.execute(cmdArgs);
        } finally {
            terminal.handle(Signal.INT, prevIntHandler);
            terminal.writer().flush();
        }
    }

    private String[] parseLine(String line) {
        try {
            String cmd = EnvUtils.expandEnvironment(line, System.getenv()).trim();
            return parser.parse(cmd, cmd.length() + 1)
                         .words()
                         .stream()
                         .toArray(String[]::new);
        } catch (IllegalArgumentException e) {
            terminal.writer().println(e.getMessage());
            return null;
        }
    }

    Parser getParser() {
        return parser;
    }
}
