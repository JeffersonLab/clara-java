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

import org.jline.terminal.Terminal;

public class EditCommand extends Command {

    private final RunConfig runConfig;
    private final String editor;

    public EditCommand(Terminal terminal, RunConfig runConfig) {
        super(terminal, "edit", "Edit data processing conditions");
        arguments.put("composition", new Argument("composition",
                "Edit application service-based composition.", this::editConfigFile));
        arguments.put("files", new Argument("files",
                "Edit input file list.", this::editFilesList));
        this.runConfig = runConfig;
        this.editor = CommandUtils.getEditor();
    }

    @Override
    public void execute(String[] args) {
        if (args.length >= 2) {
            String subCommandName = args[1];
            Argument subCommand = arguments.get(subCommandName);
            if (subCommand != null) {
                subCommand.getAction().accept(null);
            } else {
                terminal.writer().println("Invalid argument.");
            }
        } else {
            terminal.writer().println("Missing argument.");
        }
    }

    private void editConfigFile(String[] args) {
        CommandUtils.runProcess(editor, runConfig.getConfigFile());
    }

    private void editFilesList(String[] args) {
        CommandUtils.runProcess(editor, runConfig.getFilesList());
    }
}
