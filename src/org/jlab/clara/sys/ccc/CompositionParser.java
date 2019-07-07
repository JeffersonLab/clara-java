/*
 *   Copyright (c) 2016.  Jefferson Lab (JLab). All rights reserved. Permission
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

package org.jlab.clara.sys.ccc;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

final class CompositionParser {

    private CompositionParser() { }

    public static String removeFirst(String s) {
        if (s == null || s.length() == 0) {
            return s;
        }
        return s.substring(1);
    }

    public static String removeFirst(String input, String firstCharacter) {
        input = input.startsWith(firstCharacter) ? input.substring(1) : input;
        return input;
    }

    public static String removeLast(String s) {
        if (s == null || s.length() == 0) {
            return s;
        }
        return s.substring(0, s.length() - 1);
    }

    public static String getFirstService(String composition) {
        StringTokenizer st = new StringTokenizer(composition, ";");
        String a = st.nextToken();

        if (a.contains(",")) {
            StringTokenizer stk = new StringTokenizer(a, ",");
            return stk.nextToken();
        } else {
            return a;
        }
    }

    public static String getJSetElementAt(List<String> set, int index) {
        int ind = -1;
        for (String s : set) {
            ind++;
            if (index == ind) {
                return s;
            }
        }
        throw new NoSuchElementException();
    }
}
