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

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.Executor;
import org.jlab.clara.base.ClaraUtil;
import org.jline.terminal.Terminal;
import org.jline.terminal.Terminal.Signal;
import org.jline.terminal.Terminal.SignalHandler;

import java.io.IOException;
import java.util.Optional;

final class CommandUtils {

    private CommandUtils() { }

    public static String getEditor() {
        return Optional.ofNullable(System.getenv("EDITOR")).orElse("nano");
    }

    public static void runProcess(String... command) {
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.redirectInput(ProcessBuilder.Redirect.INHERIT);
        builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        try {
            Process process = builder.start();
            try {
                process.waitFor();
            } catch (InterruptedException e) {
                process.destroy();
                Thread.currentThread().interrupt();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void runCommand(Terminal terminal, String command, String... args) {
        CommandLine cmdLine = new CommandLine(command);
        for (String arg : args) {
            cmdLine.addArgument(arg);
        }
        DefaultExecuteResultHandler resultHandler = new DefaultExecuteResultHandler();
        ExecuteWatchdog watchdog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
        Executor executor = new DefaultExecutor();
        executor.setWatchdog(watchdog);
        SignalHandler prevIntHandler = terminal.handle(Signal.INT, s -> {
            Thread.currentThread().interrupt();
        });
        try {
            executor.execute(cmdLine, resultHandler);
            resultHandler.waitFor();
        } catch (ExecuteException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            watchdog.destroyProcess();
        } finally {
            terminal.handle(Signal.INT, prevIntHandler);
        }
    }

    public static Process runDpe(String... command) throws IOException {
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        Process process = builder.start();
        ClaraUtil.sleep(2000);
        return process;
    }
}
