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
