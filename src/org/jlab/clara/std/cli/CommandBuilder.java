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

class CommandBuilder {

    private final boolean quote;
    private final List<String> cmd = new ArrayList<>();

    CommandBuilder(Path program, boolean useQuotes) {
        this(program.toString(), useQuotes);
    }

    CommandBuilder(String program, boolean useQuotes) {
        quote = useQuotes;
        cmd.add(mayQuote(program));
    }

    public void addOption(String option) {
        cmd.add(option);
    }

    public void addOption(String option, Object value) {
        cmd.add(option);
        cmd.add(mayQuote(value));
    }

    public void addArgument(Object argument) {
        cmd.add(mayQuote(argument));
    }

    private String mayQuote(Object value) {
        if (quote) {
            return "\"" + value + "\"";
        }
        return value.toString();
    }

    public String[] toArray() {
        return cmd.toArray(new String[0]);
    }

    @Override
    public String toString() {
        return String.join(" ", cmd);
    }
}
