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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.jline.reader.Completer;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.completer.AggregateCompleter;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

public class ClaraShell {

    private final RunConfig runConfig;
    private final Map<String, Command> commands;
    private final Terminal terminal;
    private final LineReader reader;

    public static void main(String[] args) {
        ClaraShell shell = new ClaraShell();
        shell.run();
    }

    public ClaraShell() {
        try {
            runConfig = new RunConfig();
            terminal = TerminalBuilder.builder().system(true).build();
            commands = initCommands(terminal, runConfig);
            reader = LineReaderBuilder.builder()
                    .completer(initCompleter(commands))
                    .terminal(terminal)
                    .build();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Map<String, Command> initCommands(Terminal terminal, RunConfig runConfig) {
        Map<String, Command> commands = new LinkedHashMap<>();
        addCommand(commands, new SetCommand(terminal, runConfig));
        addCommand(commands, new EditCommand(terminal, runConfig));
        addCommand(commands, new RunCommand(terminal, runConfig));
        addCommand(commands, new MonitorCommand(terminal));
        addCommand(commands, new ResetCommand(terminal, runConfig));
        addCommand(commands, new ShowCommand(terminal, runConfig));
        addCommand(commands, new HelpCommand(terminal, commands));
        return commands;
    }

    private static void addCommand(Map<String, Command> commands, Command command) {
        commands.put(command.getName(), command);
    }

    private static Completer initCompleter(Map<String, Command> commands) {
        List<Completer> completers = commands.values()
                .stream()
                .map(Command::getCompleter)
                .collect(Collectors.toList());
        return new AggregateCompleter(completers);
    }

    public void run() {
        try {
            printWelcomeMessage();
            PrintWriter out = new PrintWriter(System.out);
            String line;
            while ((line = readLine("")) != null) {
                String[] splited = line.split(" ");
                String commandName = splited[0];
                Command command = commands.get(commandName);
                if (command == null) {
                    if ("exit".equals(commandName)) {
                        break;
                    } else if ("".equals(line)) {
                        continue;
                    }
                    terminal.writer().println("Invalid command");
                } else {
                    command.execute(splited);
                }
                out.flush();
            }
        } catch (EndOfFileException e) {
            // ignore
        } catch (UserInterruptException e) {
            // ignore
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            try {
                terminal.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void printWelcomeMessage() {
        System.out.println();
        System.out.println("   ██████╗██╗      █████╗ ██████╗  █████╗ ");
        System.out.println("  ██╔════╝██║     ██╔══██╗██╔══██╗██╔══██╗ 4.3.0");
        System.out.println("  ██║     ██║     ███████║██████╔╝███████║");
        System.out.println("  ██║     ██║     ██╔══██║██╔══██╗██╔══██║");
        System.out.println("  ╚██████╗███████╗██║  ██║██║  ██║██║  ██║");
        System.out.println("   ╚═════╝╚══════╝╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝");
        System.out.println();
    }

    private String readLine(String promtMessage) throws IOException {
        return reader.readLine(promtMessage + "\nclara> ");
    }
}
