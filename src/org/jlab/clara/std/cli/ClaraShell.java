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
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
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
        ClaraShell.Builder builder = ClaraShell.newBuilder();
        if (FarmCommands.hasPlugin()) {
            FarmCommands.register(builder);
        }
        ClaraShell shell = builder.build();

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

        private final List<CommandFactory> runSubCommands = new ArrayList<>();
        private final List<CommandFactory> editSubCommands = new ArrayList<>();

        private final List<CommandFactory> userCommands = new ArrayList<>();

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
         * @param builder the builder of the configuration variable
         * @return this builder
         */
        public Builder withConfigVariable(ConfigVariable.Builder builder) {
            ArgUtils.requireNonNull(builder, "variable builder");

            config.addVariable(builder.build());
            return this;
        }

        /**
         * Sets an environment variable for CLARA processes.
         * The variable will be added to the environment of the DPEs started by
         * {@code run local}.
         *
         * @param name the name of the variable
         * @param value the value of the variable
         * @return this builder
         */
        public Builder withEnvironmentVariable(String name, String value) {
            ArgUtils.requireNonEmpty(name, "name");
            ArgUtils.requireNonNull(value, "value");
            config.setenv(name, value);
            return this;
        }

        /**
         * Adds a new subcommand to the {@code run} builtin command.
         * This new subcommand cannot have the same name as one of the default
         * subcommands.
         *
         * @param factory the factory to create the subcommand
         * @return this builder
         */
        public Builder withRunSubCommand(CommandFactory factory) {
            ArgUtils.requireNonNull(factory, "command factory");

            runSubCommands.add(factory);
            return this;
        }

        /**
         * Adds a new subcommand to the {@code edit} builtin command.
         *
         * @param name the name of the subcommand
         * @param description the help description for the command
         * @param fileArg the path to the text file to be edited
         * @return this builder
         */
        public Builder withEditSubCommand(String name,
                                          String description,
                                          Function<Config, String> fileArg) {
            ArgUtils.requireNonEmpty(name, "name");
            ArgUtils.requireNonEmpty(description, "description");
            ArgUtils.requireNonNull(fileArg, "edit subcommand argument");

            editSubCommands.add(EditCommand.newArgument(name, description, fileArg));
            return this;
        }

        /**
         * Adds a new builtin command to the CLARA shell session.
         * This new command cannot have the same name as one of the default
         * builtin commands.
         *
         * @param name the name of the command
         * @param description the help description for the command
         * @param command the list containing the program and its arguments
         * @return this builder
         */
        public Builder withBuiltinCommand(String name, String description, List<String> command) {
            ArgUtils.requireNonEmpty(name, "name");
            ArgUtils.requireNonEmpty(description, "description");
            ArgUtils.requireNonNull(description, "command");

            String[] arrCommand = command.toArray(new String[command.size()]);
            userCommands.add(AbstractCommand.wrap(name, description, arrCommand));
            return this;
        }

        /**
         * Adds a new builtin command to the CLARA shell session.
         * This new command cannot have the same name as one of the default
         * builtin commands.
         *
         * @param name the name of the command
         * @param description the help description for the command
         * @param command a string array containing the program and its arguments
         * @return this builder
         */
        public Builder withBuiltinCommand(String name, String description, String... command) {
            ArgUtils.requireNonEmpty(name, "name");
            ArgUtils.requireNonEmpty(description, "description");
            ArgUtils.requireNonNull(description, "command");

            userCommands.add(AbstractCommand.wrap(name, description, command));
            return this;
        }

        /**
         * Adds a new builtin command to the CLARA shell session.
         * This new command cannot have the same name as one of the default
         * builtin commands.
         *
         * @param factory the factory to create the command
         * @return this builder
         */
        public Builder withBuiltinCommand(CommandFactory factory) {
            ArgUtils.requireNonNull(factory, "command factory");
            userCommands.add(factory);
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
        commands = new LinkedHashMap<>();
        commandRunner = new CommandRunner(terminal, commands);
        initCommands(builder);
        reader = LineReaderBuilder.builder()
                .completer(initCompleter())
                .parser(commandRunner.getParser())
                .terminal(terminal)
                .build();
        history = new DefaultHistory(reader);
        loadHistory();
    }

    private void initCommands(Builder builder) {
        addCommand(initCommand(RunCommand::new, builder.runSubCommands));
        addCommand(initCommand(EditCommand::new, builder.editSubCommands));

        builder.userCommands.forEach(c -> addCommand(c.create(terminal, config)));

        addCommand(new SetCommand(terminal, config));
        addCommand(new ShowCommand(terminal, config));
        addCommand(new SaveCommand(terminal, config));
        addCommand(new SourceCommand(terminal, commandRunner));
        addCommand(new HelpCommand(terminal, commands));
    }

    private Command initCommand(CommandFactory baseCommand,
                                List<CommandFactory> userSubCommands) {
        BaseCommand cmd = (BaseCommand) baseCommand.create(terminal, config);
        userSubCommands.forEach(c -> cmd.addSubCommand(c.create(terminal, config)));
        return cmd;
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
        printWelcomeMessage(terminal.writer());
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

    private void printWelcomeMessage(PrintWriter writer) {
        writer.println();
        writer.println("   ██████╗██╗      █████╗ ██████╗  █████╗ ");
        writer.println("  ██╔════╝██║     ██╔══██╗██╔══██╗██╔══██╗ 4.3.0");
        writer.println("  ██║     ██║     ███████║██████╔╝███████║");
        writer.println("  ██║     ██║     ██╔══██║██╔══██╗██╔══██║");
        writer.println("  ╚██████╗███████╗██║  ██║██║  ██║██║  ██║");
        writer.println("   ╚═════╝╚══════╝╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝");
        writer.println();
        writer.println();
        writer.println(" Run 'help' to show available commands.");
    }

    private String readLine(String promtMessage) {
        return reader.readLine(promtMessage + "\nclara> ");
    }
}
