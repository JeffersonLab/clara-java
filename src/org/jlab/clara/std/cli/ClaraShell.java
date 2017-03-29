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

import org.jlab.clara.util.ArgUtils;
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
public final class ClaraShell implements AutoCloseable {

    private static final String HISTORY_NAME = ".clara_history";

    private final Config config;
    private final Terminal terminal;
    private final Map<String, Command> commands;
    private final CommandRunner commandRunner;
    private final LineReader reader;
    private final History history;

    private volatile boolean running;

    public static void main(String[] args) {
        ClaraShell shell = ClaraShell.newBuilder().build();

        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            shell.stop();
            try {
                mainThread.interrupt();
                mainThread.join(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            shell.close();
        }));

        shell.run();
    }


    /**
     * Creates a new builder of a CLARA shell instance.
     *
     * @return the builder
     */
    public static Builder newBuilder() {
        return new Builder(TerminalBuilder.builder());
    }


    /**
     * Helps configuring and creating a new {@link ClaraShell}.
     */
    public static class Builder {

        private final Terminal terminal;
        private final Config config;

        /**
         * Creates a new builder.
         *
         * @param termBuilder the builder of the virtual terminal
         */
        public Builder(TerminalBuilder termBuilder) {
            try {
                this.terminal = termBuilder.build();
                this.config = new Config();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        /**
         * Adds a configuration variable to the shell session with the given
         * default value.
         *
         * @param name the name of the variable
         * @param defaultValue the default value of the variable
         * @return this builder
         */
        public Builder withConfigVariable(String name, Object defaultValue) {
            ArgUtils.requireNonEmpty(name, "name");
            ArgUtils.requireNonNull(defaultValue, "value");

            config.setValue(name, defaultValue);
            return this;
        }

        /**
         * Adds a configuration variable to the shell session.
         * This new variable cannot have the same name as one of the default
         * configuration variables.
         *
         * @param variable the configuration variable
         * @return this builder
         */
        public Builder withConfigVariable(ConfigVariable variable) {
            ArgUtils.requireNonNull(variable, "variable");

            config.addVariable(variable);
            return this;
        }

        /**
         * Creates the user-interactive CLARA shell instance.
         *
         * @return the created shell
         */
        public ClaraShell build() {
            return new ClaraShell(this);
        }
    }


    private ClaraShell(Builder builder) {
        terminal = builder.terminal;
        config = builder.config;
        commands = new HashMap<>();
        commandRunner = new CommandRunner(terminal, commands);
        initCommands();
        reader = LineReaderBuilder.builder()
                .completer(initCompleter())
                .parser(commandRunner.getParser())
                .terminal(terminal)
                .build();
        history = new DefaultHistory(reader);
        loadHistory();
    }

    private void initCommands() {
        addCommand(new EditCommand(terminal, config));
        addCommand(new RunCommand(terminal, config));
        addCommand(new SetCommand(terminal, config));
        addCommand(new ShowCommand(terminal, config));
        addCommand(new SaveCommand(terminal, config));
        addCommand(new SourceCommand(terminal, commandRunner));
        addCommand(new HelpCommand(terminal, commands));
    }

    private void addCommand(Command command) {
        commands.put(command.getName(), command);
    }

    private Completer initCompleter() {
        List<Completer> completers = commands.values()
                .stream()
                .map(Command::getCompleter)
                .collect(Collectors.toList());
        return new AggregateCompleter(completers);
    }

    private void loadHistory() {
        Path histFile = Paths.get(Config.claraHome(), HISTORY_NAME);
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
        running = true;
        while (running) {
            try {
                Thread.interrupted();
                String line = readLine("");
                if (line == null) {
                    continue;
                }
                commandRunner.execute(line);
            } catch (EndOfFileException e) {
                break;
            } catch (UserInterruptException e) {
                continue;
            } finally {
                terminal.flush();
            }
        }
    }

    /**
     * Programmatically stops the shell main loop.
     */
    public void stop() {
        running = false;
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
