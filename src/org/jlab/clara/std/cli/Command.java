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
     * @param printer where to print the help text
     */
    void printHelp(PrintWriter printer);
}
