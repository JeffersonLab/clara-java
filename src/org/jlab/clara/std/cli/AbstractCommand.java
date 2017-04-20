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
import org.jline.reader.impl.completer.ArgumentCompleter;
import org.jline.reader.impl.completer.NullCompleter;
import org.jline.reader.impl.completer.StringsCompleter;
import org.jline.terminal.Terminal;

import java.io.PrintWriter;

/**
 * Default class for new shell commands.
 * The name and a brief description of the command are required.
 * The command can access to the virtual terminal used by the shell, if needed.
 */
public abstract class AbstractCommand implements Command {

    /**
     *  The name of the command.
     */
    protected final String name;

    /**
     * The description of the command.
     */
    protected final String description;

    /**
     * The virtual terminal used by the shell.
     */
    protected final Terminal terminal;

    /**
     * The configuration of the shell session.
     */
    protected final Config config;

    /**
     * The text-output stream of the terminal.
     */
    protected final PrintWriter writer;


    static CommandFactory wrap(String name, String description, String... command) {
        return session -> {
            return new AbstractCommand(session, name, description) {
                @Override
                public int execute(String[] args) {
                    return CommandUtils.runProcess(command);
                }
            };
        };
    }


    /**
     * Creates a new command.
     *
     * @param context the context of the shell session
     * @param name the name of the command
     * @param description the description of the command
     */
    protected AbstractCommand(Context context, String name, String description) {
        this.name = name;
        this.description = description;
        this.terminal = context.terminal();
        this.config = context.config();
        this.writer = terminal.writer();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public Completer getCompleter() {
        return new ArgumentCompleter(new StringsCompleter(name), NullCompleter.INSTANCE);
    }

    @Override
    public void printHelp(PrintWriter printer) {
        printer.println(getDescription());
    }

    @Override
    public void close() throws Exception {
        // nothing
    }
}
