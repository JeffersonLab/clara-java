package org.jlab.clara.std.cli;

import org.jline.reader.Completer;

import java.io.PrintWriter;

/**
 * A command that can be executed by the CLARA shell.
 * <p>
 * The {@link CommandUtils} class provides helper methods to wrap existing CLI
 * programs as subprocesses that will be started when executing the command.
 */
public interface Command extends AutoCloseable {

    /**
     * Exit status for successful execution.
     */
    int EXIT_SUCCESS = 0;

    /**
     * Exit status for failed execution.
     */
    int EXIT_ERROR = 1;

    /**
     * Executes the command with the give arguments.
     * The command must support being interrupted by the user
     * (by interrupting the caller thread).
     *
     * @param args the command arguments.
     *             The command name is not part of the arguments.
     * @return the exit status of the execution
     */
    int execute(String[] args);

    /**
     * Gets the name of the command.
     *
     * @return the name
     */
    String getName();

    /**
     * Gets a brief explanation of what the command does.
     * More detailed information should be provided with {@link #printHelp}.
     *
     * @return a short description of the command
     */
    String getDescription();

    /**
     * Gets a completer to generate tab-completion candidates of this command
     * and its arguments.
     *
     * @return the completer for the command
     */
    Completer getCompleter();

    /**
     * Prints a detailed description of what the command does.
     *
     * @param writer where to print the help text
     */
    void printHelp(PrintWriter writer);
}
