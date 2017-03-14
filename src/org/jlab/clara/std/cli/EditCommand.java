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
import org.jline.reader.impl.completer.StringsCompleter;
import org.jline.terminal.Terminal;

class EditCommand extends Command {

    private final RunConfig runConfig;
    private final String editor;

    EditCommand(Terminal terminal, RunConfig runConfig) {
        super(terminal, "edit", "Edit data processing conditions");
        this.runConfig = runConfig;
        this.editor = CommandUtils.getEditor();
        setArguments();
    }

    private void setArguments() {
        subCommands.put("composition", new SubCommand("composition",
                "Edit application service-based composition.", this::editConfigFile));
        subCommands.put("files", new SubCommand("files",
                "Edit input file list.", this::editFilesList));
    }

    @Override
    public int execute(String[] args) {
        return executeSubcommand(args);
    }

    private int editConfigFile(String[] args) {
        return CommandUtils.runProcess(editor, runConfig.getConfigFile());
    }

    private int editFilesList(String[] args) {
        return CommandUtils.runProcess(editor, runConfig.getFilesList());
    }

    @Override
    public Completer getCompleter() {
        StringsCompleter command = new StringsCompleter(getName());
        StringsCompleter subCommands = new StringsCompleter("composition", "files");
        return new ArgumentCompleter(command, subCommands);
    }
}
