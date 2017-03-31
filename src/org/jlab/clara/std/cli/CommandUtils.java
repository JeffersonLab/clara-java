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
import java.net.ServerSocket;
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
        builder.inheritIO();
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
    public static Process runDpe(String[] command) throws IOException {
        return runDpeInternal(uninterruptibleCommand(command));
    }

    /**
     * Starts the given DPE as a background subprocess.
     * The DPE output will also be logged into the given file.
     * The DPE process will continue running even if the user presses CTRL-C on
     * the CLARA shell.
     *
     * @param command a string array containing the DPE program and its arguments
     * @param logFile the path to the DPE log file
     * @return the subprocess that is running the DPE
     * @throws IOException if the subprocess could not be started
     */
    public static Process runDpe(String[] command, String logFile) throws IOException {
        return runDpeInternal(uninterruptibleCommand(command, logFile));
    }

    private static Process runDpeInternal(String[] command) throws IOException {
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.inheritIO();
        Process process = builder.start();
        ClaraUtil.sleep(2000);
        return process;
    }

    /**
     * Finds a free port in the given range to be used by a DPE.
     *
     * @param begin the lower port in the range, inclusive
     * @param end the upper port in the range, exclusive
     * @return the first available port in the range
     * @throws IllegalStateException if there are no free ports in the given range
     */
    public static Integer getAvailableDpePort(int begin, int end) {
        for (int port = begin; port < end; port += 10) {
            try (ServerSocket socket = new ServerSocket(port)) {
                socket.setReuseAddress(true);
                return socket.getLocalPort();
            } catch (IOException e) {
                continue;
            }
        }
        throw new IllegalStateException("Cannot find an available port");
    }

    /**
     * Wraps the given CLI program into a script that ignores when the user
     * presses CTRL-C.
     * The output of the command will be printed to the standard output.
     *
     * @param command a string array containing the DPE program and its arguments
     * @return the wrapper program that runs the given command
     */
    public static String[] uninterruptibleCommand(String... command) {
        List<String> wrapperCmd = new ArrayList<>();
        wrapperCmd.add(commandWrapper());
        wrapperCmd.addAll(Arrays.asList(command));
        return wrapperCmd.toArray(new String[wrapperCmd.size()]);
    }

    /**
     * Wraps the given CLI program into a script that ignores when the user
     * presses CTRL-C.
     * The output of the command will be printed to the standard output
     * and also logged into the given log file.
     *
     * @param command a string array containing the DPE program and its arguments
     * @param logFile the path to the log file
     * @return the wrapper program that runs the given command
     */
    public static String[] uninterruptibleCommand(String[] command, String logFile) {
        List<String> logCmd = new ArrayList<>();
        logCmd.add(commandLogger());
        logCmd.add(logFile);
        logCmd.addAll(Arrays.asList(command));
        return logCmd.toArray(new String[logCmd.size()]);
    }

    private static String commandWrapper() {
        return Optional.ofNullable(System.getenv("CLARA_COMMAND_WRAPPER"))
                .orElse(Paths.get(Config.claraHome(), "lib", "clara", "cmd-wrapper").toString());
    }

    private static String commandLogger() {
        return Optional.ofNullable(System.getenv("CLARA_COMMAND_LOGGER"))
                .orElse(Paths.get(Config.claraHome(), "lib", "clara", "cmd-logger").toString());
    }
}
