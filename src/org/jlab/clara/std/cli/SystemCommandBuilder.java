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

    private final List<Token> cmd = new ArrayList<>();

    private boolean quoteAll = false;
    private boolean multiLine = false;

    SystemCommandBuilder() { }

    SystemCommandBuilder(Path program) {
        this(program.toString());
    }

    SystemCommandBuilder(String program) {
        cmd.add(new Token(program, false));
    }

    public void addOption(String option) {
        cmd.add(new Token(option, true));
    }

    public void addOption(String option, Object value) {
        cmd.add(new Token(option, true));
        cmd.add(new Token(value.toString(), false));
    }

    public void addOptionNoSplit(String option, Object value) {
        cmd.add(new Token(option, false));
        cmd.add(new Token(value.toString(), false));
    }

    public void addArgument(Object argument) {
        cmd.add(new Token(argument.toString(), true));
    }

    public void quoteAll(boolean quote) {
        quoteAll = quote;
    }

    public void multiLine(boolean split) {
        multiLine = split;
    }

    private Token mayQuote(Token token) {
        Matcher m = NEED_QUOTES.matcher(token.value);
        if (m.find() || quoteAll) {
            String quoted = "\"" + token.value + "\"";
            return new Token(quoted, token.split);
        }
        return token;
    }

    private String maySplit(Token token) {
        if (multiLine && token.split) {
            return "\\\n        " + token.value;
        }
        return token.value;
    }

    public String[] toArray() {
        return cmd.stream()
                .map(Token::toString)
                .toArray(String[]::new);
    }

    @Override
    public String toString() {
        return cmd.stream()
                .map(this::mayQuote)
                .map(this::maySplit)
                .collect(Collectors.joining(" "));
    }


    private static final class Token {

        private final String value;
        private final boolean split;

        private Token(String value, boolean split) {
            this.value = value;
            this.split = split;
        }

        @Override
        public String toString() {
            return value;
        }
    }
}
