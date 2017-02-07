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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;

import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

public class ClaraShell {

    private final Terminal terminal;
    private final LineReader reader;

    public static void main(String[] args) {
        ClaraShell shell = new ClaraShell();
        shell.run();
    }

    public ClaraShell() {
        try {
            terminal = TerminalBuilder.builder().build();
            reader = LineReaderBuilder.builder()
                    .terminal(terminal)
                    .build();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void run() {
        try {
            printWelcomeMessage();
            PrintWriter out = new PrintWriter(System.out);
            String line;
            while ((line = readLine("")) != null) {
                System.out.println(line);
                out.flush();
            }
        } catch (EndOfFileException e) {
            // ignore
        } catch (UserInterruptException e) {
            // ignore
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            try {
                terminal.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void printWelcomeMessage() {
        System.out.println();
        System.out.println(" ██████╗██╗      █████╗ ██████╗  █████╗ ");
        System.out.println("██╔════╝██║     ██╔══██╗██╔══██╗██╔══██╗ 4.3.0");
        System.out.println("██║     ██║     ███████║██████╔╝███████║");
        System.out.println("██║     ██║     ██╔══██║██╔══██╗██╔══██║");
        System.out.println("╚██████╗███████╗██║  ██║██║  ██║██║  ██║");
        System.out.println(" ╚═════╝╚══════╝╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝");
        System.out.println();
    }

    private String readLine(String promtMessage) throws IOException {
        return reader.readLine(promtMessage + "\nclara> ");
    }
}
