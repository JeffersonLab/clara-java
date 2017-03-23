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

import org.jlab.clara.base.ClaraUtil;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Helpers to run CLI commands.
 */
public final class CommandUtils {

    private CommandUtils() { }

    /**
     * Gets the default text editor of the user.
     * This is the program defined by $EDITOR environment variable.
     * If the variable is not set, use nano.
     *
     * @return the default editor defined by the user
     */
    public static String getEditor() {
        return Optional.ofNullable(System.getenv("EDITOR")).orElse("nano");
    }

    /**
     * Opens the given file in the default text editor of the user.
     *
     * @param filePath the file to be edited
     * @return the exit status of the editor program
     */
    public static int editFile(String filePath) {
        return runProcess(getEditor(), filePath);
    }

    /**
     * Runs the given CLI program as a subprocess, and waits until it is
     * finished. The subprocess will be destroyed if the caller thread is
     * interrupted.
     *
     * @param command a string array containing the program and its arguments
     * @return the exit value of the subprocess
     */
    public static int runProcess(String... command) {
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.redirectInput(ProcessBuilder.Redirect.INHERIT);
        builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        return runProcess(builder);
    }

    /**
     * Starts the subprocess defined by the given subprocess builder, and waits
     * until it is finished. The subprocess will be destroyed if the caller
     * thread is interrupted.
     *
     * @param builder the builder of the subprocess to be run
     * @return the exit value of the subprocess
     */
    public static int runProcess(ProcessBuilder builder) {
        try {
            Process process = builder.start();
            try {
                return process.waitFor();
            } catch (InterruptedException e) {
                destroyProcess(process);
                Thread.currentThread().interrupt();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 1;
    }

    /**
     * Destroys the given subprocess.
     *
     * @param process the subprocess to be destroyed
     */
    public static void destroyProcess(Process process) {
        process.destroy();
        try {
            process.waitFor(10, TimeUnit.SECONDS);
            if (process.isAlive()) {
                process.destroyForcibly();
                process.waitFor(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            // ignore
        }
        try {
            process.getOutputStream().flush();
        } catch (IOException e) {
            // ignore
        }
    }

    /**
     * Starts the given DPE as a background subprocess.
     * The DPE process will continue running even if the user presses CTRL-C on
     * the CLARA shell.
     *
     * @param command a string array containing the DPE program and its arguments
     * @return the subprocess that is running the DPE
     * @throws IOException if the subprocess could not be started
     */
    public static Process runDpe(String... command) throws IOException {
        ProcessBuilder builder = new ProcessBuilder(wrapCommand(command));
        builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        builder.redirectError(ProcessBuilder.Redirect.INHERIT);
        Process process = builder.start();
        ClaraUtil.sleep(2000);
        return process;
    }

    /**
     * Wraps the given CLI program into a script that ignores when the user
     * presses CTRL-C.
     *
     * @param command a string array containing the DPE program and its arguments
     * @return the wrapper program that runs the given command
     */
    public static List<String> wrapCommand(String... command) {
        List<String> wrapperCmd = new ArrayList<>();
        wrapperCmd.add(commandWrapper());
        wrapperCmd.addAll(Arrays.asList(command));
        return wrapperCmd;
    }

    /**
     * Gets the path to the wrapper script that prevents interrupting a program
     * with CTRL-C.
     *
     * @return the path to the script
     */
    public static String commandWrapper() {
        return Optional.ofNullable(System.getenv("CLARA_COMMAND_WRAPPER"))
                .orElse(Paths.get(Config.claraHome(), "lib", "clara", "cmd-wrapper").toString());
    }
}
