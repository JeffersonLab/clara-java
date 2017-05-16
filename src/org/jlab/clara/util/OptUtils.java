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

package org.jlab.clara.util;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionSpec;

public final class OptUtils {

    private OptUtils() { }

    public static <V> String optionHelp(String name, String... help) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < help.length; i++) {
            sb.append(String.format("  %-25s  %s%n", i == 0 ? name : "", help[i]));
        }
        return sb.toString();
    }

    public static <V> String optionHelp(OptionSpec<V> spec, String arg, String... help) {
        String lhs = optionName(spec, arg);
        return optionHelp(lhs, help);
    }

    public static <V> String optionName(OptionSpec<V> spec, String arg) {
        StringBuilder sb = new StringBuilder();
        String name = spec.options().get(0);
        sb.append("-");
        if (name.length() > 1) {
            sb.append("-");
        }
        sb.append(name);
        if (arg != null) {
            sb.append(" <").append(arg).append(">");
        }
        return sb.toString();
    }

    public static <V> String getDefault(OptionSpec<V> stageDir) {
        ArgumentAcceptingOptionSpec<V> spec = (ArgumentAcceptingOptionSpec<V>) stageDir;
        StringBuilder sb = new StringBuilder();
        sb.append("(default: ");
        sb.append(spec.defaultValues().get(0));
        sb.append(")");
        return sb.toString();
    }
}
