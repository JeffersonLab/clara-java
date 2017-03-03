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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.jline.reader.Completer;
import org.jline.reader.EndOfFileException;
import org.jline.reader.History;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.completer.AggregateCompleter;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

/**
 * An interactive shell to run CLARA DPEs and orchestrators.
 */
public class ClaraShell implements AutoCloseable {

    private static final String HISTORY_NAME = ".clara_history";

    private final RunConfig runConfig;
    private final Terminal terminal;
    private final Map<String, Command> commands;
    private final CommandRunner commandRunner;
    private final LineReader reader;
    private final History history;

    public static void main(String[] args) {
        ClaraShell shell = new ClaraShell();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                shell.close();
            }
        });
        shell.run();
    }

    /**
     * Creates a new user-interactive shell.
     */
    public ClaraShell() {
        try {
            runConfig = new RunConfig();
            terminal = TerminalBuilder.builder().build();
            commands = new HashMap<>();
            commandRunner = new CommandRunner(terminal, commands);
            initCommands();
            reader = LineReaderBuilder.builder()
                    .completer(initCompleter(commands))
                    .terminal(terminal)
                    .build();
            history = new DefaultHistory(reader);
            loadHistory();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Map<String, Command> initCommands() {
        addCommand(commands, new SetCommand(terminal, runConfig));
        addCommand(commands, new EditCommand(terminal, runConfig));
        addCommand(commands, new RunCommand(terminal, runConfig));
        addCommand(commands, new MonitorCommand(terminal));
        addCommand(commands, new ResetCommand(terminal, runConfig));
        addCommand(commands, new ShowCommand(terminal, runConfig));
        addCommand(commands, new SaveCommand(terminal, runConfig));
        addCommand(commands, new SourceCommand(terminal, commandRunner));
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

    private void loadHistory() {
        Path histFile = Paths.get(RunConfig.claraHome(), HISTORY_NAME);
        reader.setVariable(LineReader.HISTORY_FILE, histFile);
        history.load();
    }

    @Override
    public void close() {
        for (Command command : commands.values()) {
            try {
                command.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        try {
            history.save();
            terminal.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Runs the shell accepting user commands.
     */
    public void run() {
        printWelcomeMessage();
        while (true) {
            try {
                String line = readLine("");
                if (line == null) {
                    continue;
                }
                commandRunner.execute(line);
            } catch (EndOfFileException e) {
                break;
            } catch (UserInterruptException e) {
                break;
            } finally {
                terminal.flush();
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

    private String readLine(String promtMessage) {
        return reader.readLine(promtMessage + "\nclara> ");
    }
}
