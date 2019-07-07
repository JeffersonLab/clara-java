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

import org.jlab.clara.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Stream;

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
     * @param file the file to be edited
     * @return the exit status of the editor program
     */
    public static int editFile(Path file) {
        return runProcess(getEditor(), file.toString());
    }

    /**
     * Checks if the given program exists in {@code $PATH}.
     *
     * @param name the name of the program
     * @return true if the program was found in {@code $PATH}
     */
    public static boolean checkProgram(String name) {
        return Stream.of(System.getenv("PATH")
                .split(Pattern.quote(File.pathSeparator)))
                .map(Paths::get)
                .anyMatch(path -> Files.exists(path.resolve(name)));
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
        return wrapperCmd.toArray(new String[0]);
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
    public static String[] uninterruptibleCommand(String[] command, Path logFile) {
        SystemCommandBuilder b = new SystemCommandBuilder(commandLogger());
        b.addArgument(logFile);
        Arrays.asList(command).forEach(b::addArgument);
        return b.toArray();
    }

    private static String commandWrapper() {
        return Optional.ofNullable(System.getenv("CLARA_COMMAND_WRAPPER"))
                .orElse(FileUtils.claraPath("lib", "clara", "cmd-wrapper").toString());
    }

    private static String commandLogger() {
        return Optional.ofNullable(System.getenv("CLARA_COMMAND_LOGGER"))
                .orElse(FileUtils.claraPath("lib", "clara", "cmd-logger").toString());
    }
}
