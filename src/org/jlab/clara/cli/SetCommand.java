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
        // CHECKSTYLE.OFF: LineLength
        newArg("description", "A single string (no spaces) describing a data processing", null);
        newArg("plugin", "Plugin installation directory. (Default: $CLARA_HOME/plugins/clas12)", null);
        newArg("session", "The data processing session. (Default: $USER)", runConfig::setSession);
        newArg("inputDir", "The input directory where the files to be processed are located. (Default: $CLARA_HOME/data/in)", runConfig::setInputDir);
        newArg("outputDir", "The output directory where processed files will be saved. (Default: $CLARA_HOME/data/out)", runConfig::setOutputDir);
        newArg("threads", "The maximum number of processing threads to be used per node. In case value = auto all system cores will be used. (Default: 2)", runConfig::setMaxThreads, Integer::parseInt);
        newArg("fileList", "Full path to the file containing the names of data-files to be processed. Note: actual files are located in the inputDir. (Default: $CLARA_HOME/plugins/clas12/config/files.list)", runConfig::setFilesList);
        newArg("yaml", "Full path to the file describing application service composition. (Default: $CLARA_HOME/plugins/clas12/config/services.yaml)", runConfig::setConfigFile);
        newArg("farmFlavor", "Farm batch system. Accepts pbs and jlab. (Default jlab)", runConfig::setFarmFlavor);
        newArg("farmLoadingZone", "Will stage input data set into the farm local directory. (Default /scratch/pbs)", runConfig::setFarmLoadingZone);
        newArg("farmMemory", "Farm job memory request (in GB). (Default: 70)", runConfig::setFarmMemory, Integer::parseInt);
        newArg("farmTrack", "Farm job track. (Default: debug)", runConfig::setFarmTrack);
        newArg("farmOS", "Farm resource OS. (Default: centos7)", runConfig::setFarmOS);
        newArg("farmCPU", "Farm resource core number request. (Ddefault: 72)", runConfig::setFarmCPU, Integer::parseInt);
        newArg("farmDisk", "Farm job disk space request (in GB). (Default: 3)", runConfig::setFarmDisk, Integer::parseInt);
        newArg("farmTime", "Farm job wall time request (in min). (Default: 1440)", runConfig::setFarmTime, Integer::parseInt);
        // CHECKSTYLE.ON: LineLength
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
