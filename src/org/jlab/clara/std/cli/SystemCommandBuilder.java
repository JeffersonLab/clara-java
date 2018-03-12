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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

class SystemCommandBuilder {

    private static final Pattern NEED_QUOTES = Pattern.compile("[^A-Za-z0-9/._-]");

    private final List<String> cmd = new ArrayList<>();

    private boolean quoteAll;

    SystemCommandBuilder(Path program) {
        this(program.toString());
    }

    SystemCommandBuilder(String program) {
        cmd.add(program);
    }

    public void addOption(String option) {
        cmd.add(option);
    }

    public void addOption(String option, Object value) {
        cmd.add(option);
        cmd.add(value.toString());
    }

    public void addArgument(Object argument) {
        cmd.add(argument.toString());
    }

    public void quoteAll(boolean quote) {
        quoteAll = quote;
    }

    private String mayQuote(String value) {
        Matcher m = NEED_QUOTES.matcher(value);
        if (m.find() || quoteAll) {
            return "\"" + value + "\"";
        }
        return value;
    }

    public String[] toArray() {
        return cmd.stream().toArray(String[]::new);
    }

    @Override
    public String toString() {
        return cmd.stream()
                .map(this::mayQuote)
                .collect(Collectors.joining(" "));
    }
}
