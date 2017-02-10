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

public class MonitorCommand extends Command {

    public MonitorCommand(Terminal terminal) {
        super(terminal, "monitor", "Monitor data processing");
        setArguments();
    }

    private void setArguments() {
        arguments.put("composition", new Argument("composition",
                "Show application service-based composition.", "file"));
        arguments.put("files", new Argument("files",
                "Show input file list.", "file"));
        arguments.put("idir", new Argument("idir", "", ""));
        arguments.put("odir", new Argument("odir", "", ""));
        arguments.put("params", new Argument("params", "", ""));
        arguments.put("logdir", new Argument("logdir", "", ""));
        arguments.put("logdpe", new Argument("logdpe", "", ""));
        arguments.put("logco", new Argument("logco", "", ""));
        arguments.put("jjobstat", new Argument("jjobstat", "", ""));
        arguments.put("pjobstat", new Argument("pjobstat", "", ""));
    }

    @Override
    public void execute(String[] args) {
        terminal.writer().println("Running command " + getName());
    }

}
