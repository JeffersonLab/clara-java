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

package org.jlab.clara.std.orchestrators;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;

final class Logging {

    private static final Object LOCK = new Object();
    private static boolean debug = false;

    private Logging() { }


    static String getCurrentTime() {
        Format formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return formatter.format(new Date());
    }


    static void info(String msg) {
        String currentTime = getCurrentTime();
        synchronized (LOCK) {
            if (debug) {
                System.out.printf("%s: ", currentTime);
            }
            System.out.println(msg);
        }
    }


    static void info(String format, Object... args) {
        String currentTime = getCurrentTime();
        synchronized (LOCK) {
            if (debug) {
                System.out.printf("%s: ", currentTime);
            }
            System.out.printf(format, args);
            System.out.println();
        }
    }


    static void error(String msg) {
        String currentTime = getCurrentTime();
        synchronized (LOCK) {
            if (debug) {
                System.err.printf("%s: ", currentTime);
            }
            System.err.println(msg);
        }
    }


    static void error(String format, Object... args) {
        String currentTime = getCurrentTime();
        synchronized (LOCK) {
            if (debug) {
                System.err.printf("%s: ", currentTime);
            }
            System.err.printf(format, args);
            System.err.println();
        }
    }


    static void verbose(boolean enable) {
        synchronized (LOCK) {
            debug = enable;
        }
    }
}
