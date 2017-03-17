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

final class CommandUtils {

    private CommandUtils() { }

    public static String getEditor() {
        return Optional.ofNullable(System.getenv("EDITOR")).orElse("nano");
    }

    public static int runProcess(String... command) {
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.redirectInput(ProcessBuilder.Redirect.INHERIT);
        builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
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

    public static Process runDpe(String... command) throws IOException {
        ProcessBuilder builder = new ProcessBuilder(wrapCommand(command));
        builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        builder.redirectError(ProcessBuilder.Redirect.INHERIT);
        Process process = builder.start();
        ClaraUtil.sleep(2000);
        return process;
    }

    public static List<String> wrapCommand(String... command) {
        List<String> wrapperCmd = new ArrayList<>();
        wrapperCmd.add(commandWrapper());
        wrapperCmd.addAll(Arrays.asList(command));
        return wrapperCmd;
    }

    public static String commandWrapper() {
        return Optional.ofNullable(System.getenv("CLARA_COMMAND_WRAPPER"))
                .orElse(Paths.get(RunConfig.claraHome(), "lib", "clara", "cmd-wrapper").toString());
    }
}
