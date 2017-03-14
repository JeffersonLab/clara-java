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

import org.jline.terminal.Terminal;

class MonitorCommand extends Command {

    MonitorCommand(Terminal terminal) {
        super(terminal, "monitor", "Monitor data processing");
        setArguments();
    }

    private void setArguments() {
        subCommands.put("composition", newArg("composition"));
        subCommands.put("files", newArg("files"));
        subCommands.put("idir", newArg("idir"));
        subCommands.put("odir", newArg("odir"));
        subCommands.put("params", newArg("params"));
        subCommands.put("logdir", newArg("logdir"));
        subCommands.put("logdpe", newArg("logdpe"));
        subCommands.put("logco", newArg("logco"));
        subCommands.put("jjobstat", newArg("jjobstat"));
        subCommands.put("pjobstat", newArg("pjobstat"));
    }

    private SubCommand newArg(String name) {
        return new SubCommand(name, "", args -> 0);
    }

    @Override
    public int execute(String[] args) {
        terminal.writer().println("Running command " + getName());
        return EXIT_SUCCESS;
    }
}
