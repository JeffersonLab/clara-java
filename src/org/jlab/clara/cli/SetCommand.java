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

import org.jline.reader.Completer;
import org.jline.reader.impl.completer.ArgumentCompleter;
import org.jline.reader.impl.completer.StringsCompleter;
import org.jline.terminal.Terminal;

public class SetCommand extends Command {

    public SetCommand(Terminal terminal) {
        super(terminal, "set", "Parameter settings");
        setArguments();
    }

    private void setArguments() {
        arguments.put("description", new Argument("description", "", ""));
        arguments.put("plugin", new Argument("plugin", "", ""));
        arguments.put("session", new Argument("session", "", ""));
        arguments.put("inputDir", new Argument("inputDir", "", ""));
        arguments.put("outputDir", new Argument("outputDir", "", ""));
        arguments.put("threads", new Argument("threads", "", ""));
        arguments.put("fileList", new Argument("fileList", "", ""));
        arguments.put("yaml", new Argument("yaml", "", ""));
        arguments.put("farmFlavor", new Argument("farmFlavor", "", ""));
        arguments.put("farmLoadingZone", new Argument("farmLoadingZone", "", ""));
        arguments.put("farmMemory", new Argument("farmMemory", "", ""));
        arguments.put("farmTrack", new Argument("farmTrack", "", ""));
        arguments.put("farmOS", new Argument("farmOS", "", ""));
        arguments.put("farmCPU", new Argument("farmCPU", "", ""));
        arguments.put("farmDisk", new Argument("farmDisk", "", ""));
        arguments.put("farmTime", new Argument("farmTime", "", ""));
    }

    @Override
    public void execute(String[] args) {
        terminal.writer().println("Running command " + getName());
    }

    @Override
    public Completer getCompleter() {
        Completer command = new StringsCompleter(getName());
        Completer subCommands = argumentsCompleter();
        return new ArgumentCompleter(command, subCommands);
    }
}
