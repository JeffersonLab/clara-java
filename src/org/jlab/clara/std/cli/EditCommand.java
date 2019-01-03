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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Function;

class EditCommand extends BaseCommand {

    EditCommand(Context context) {
        super(context, "edit", "Edit data processing conditions");

        addArgument("services", "Edit services composition.",
            c -> Paths.get(c.getString(Config.SERVICES_FILE)));
        addArgument("files", "Edit input file list.",
            c -> Paths.get(c.getString(Config.FILES_LIST)));
    }

    void addArgument(String name, String description, Function<Config, Path> fileArg) {
        addSubCommand(newArgument(name, description, fileArg));
    }

    static CommandFactory newArgument(String name,
                                      String description,
                                      Function<Config, Path> fileArg) {
        return session -> new AbstractCommand(session, name, description) {
            @Override
            public int execute(String[] args) {
                return CommandUtils.editFile(fileArg.apply(session.config()));
            }
        };
    }
}
