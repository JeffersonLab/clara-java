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

import java.util.function.Consumer;
import java.util.function.Function;

import org.jline.reader.Completer;
import org.jline.reader.impl.completer.FileNameCompleter;
import org.jline.reader.impl.completer.StringsCompleter;
import org.jline.terminal.Terminal;

public class SetCommand extends Command {

    private final RunConfig runConfig;

    public SetCommand(Terminal terminal, RunConfig runConfig) {
        super(terminal, "set", "Parameter settings");
        this.runConfig = runConfig;
        setArguments();
        setCompleters();
    }

    private void setArguments() {
        newArg("description", "", null);
        newArg("plugin", "", null);
        newArg("session", "", runConfig::setSession);
        newArg("inputDir", "", runConfig::setInputDir);
        newArg("outputDir", "", runConfig::setOutputDir);
        newArg("threads", "", runConfig::setMaxThreads, Integer::parseInt);
        newArg("fileList", "", runConfig::setFilesList);
        newArg("yaml", "", runConfig::setConfigFile);
        newArg("farmFlavor", "", runConfig::setFarmFlavor);
        newArg("farmLoadingZone", "", runConfig::setFarmLoadingZone);
        newArg("farmMemory", "", runConfig::setFarmMemory, Integer::parseInt);
        newArg("farmTrack", "", runConfig::setFarmTrack);
        newArg("farmOS", "", runConfig::setFarmOS);
        newArg("farmCPU", "", runConfig::setFarmCPU, Integer::parseInt);
        newArg("farmDisk", "", runConfig::setFarmDisk, Integer::parseInt);
        newArg("farmTime", "", runConfig::setFarmTime, Integer::parseInt);
    }

    private void setCompleters() {
        Completer fileCompleter = new FileNameCompleter();
        arguments.get("fileList").setCompleter(fileCompleter);
        arguments.get("yaml").setCompleter(fileCompleter);
        arguments.get("inputDir").setCompleter(fileCompleter);
        arguments.get("outputDir").setCompleter(fileCompleter);
        arguments.get("farmFlavor").setCompleter(new StringsCompleter("dps", "jlab"));
    }

    private <T> void newArg(String name, String description, Consumer<String> action) {
        newArg(name, description, action, a -> a);
    }

    private <T> void newArg(String name,
                        String description,
                        Consumer<T> action,
                        Function<String, T> parser) {
        Consumer<String[]> commandAction = args -> {
            T val;
            try {
                val = parser.apply(args[2]);
            } catch (Exception e) {
                throw new IllegalArgumentException("invalid argument", e);
            }
            action.accept(val);
        };
        arguments.put(name, new Argument(name, description, commandAction));
    }

    @Override
    public void execute(String[] args) {
        if (args.length >= 3) {
            String subCommandName = args[1];
            Argument subCommand = arguments.get(subCommandName);
            if (subCommand != null) {
                try {
                    subCommand.getAction().accept(args);
                } catch (IllegalArgumentException e) {
                    terminal.writer().println("Error: " + e.getMessage());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                terminal.writer().println("Invalid argument.");
            }
        } else {
            terminal.writer().println("Missing argument.");
        }
    }
}
