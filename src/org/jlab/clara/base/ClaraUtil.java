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

package org.jlab.clara.base;

import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgUtil;

import javax.annotation.ParametersAreNonnullByDefault;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extra helper methods for CLARA orchestrator and services.
 *
 * @author gurjyan
 * @version 4.x
 */
@ParametersAreNonnullByDefault
public final class ClaraUtil {

    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern(ClaraConstants.DATE_FORMAT);

    /**
     * Regex to validate a full canonical name.
     * Groups are used to separate each component.
     * <p>
     * A canonical name should have any of the following structures:
     * <pre>
     * {@literal <host>%<port>_<language>}
     * {@literal <host>%<port>_<language>:<container>}
     * {@literal <host>%<port>_<language>:<container>:<engine>}
     * </pre>
     */
    public static final Pattern CANONICAL_NAME_PATTERN =
            Pattern.compile("^(([^:%_ ]+)(%\\d+)?_(java|python|cpp))(:(\\w+)(:(\\w+))?)?$");

    private ClaraUtil() {
    }

    /**
     * Checks if the given name is a proper CLARA canonical name.
     * <p>
     * A canonical name should have any of the following structures:
     * <pre>
     * {@literal <host>%<port>_<language>}
     * {@literal <host>%<port>_<language>:<container>}
     * {@literal <host>%<port>_<language>:<container>:<engine>}
     * </pre>
     *
     * @param name the name to be checked
     * @return true if the string is a CLARA canonical name, false if not
     */
    public static boolean isCanonicalName(String name) {
        Matcher matcher = CANONICAL_NAME_PATTERN.matcher(name);
        return matcher.matches();
    }

    /**
     * Checks if the given name is a proper DPE canonical name.
     * <p>
     * A DPE canonical name should have the following structure:
     * <pre>
     * {@literal <host>_<language>}
     * </pre>
     *
     * @param name the name to be checked
     * @return true if the string is a DPE canonical name, false if not
     */
    public static boolean isDpeName(String name) {
        Matcher matcher = CANONICAL_NAME_PATTERN.matcher(name);
        return matcher.matches() && matcher.group(5) == null;
    }

    /**
     * Checks if the given name is a proper container canonical name.
     * <p>
     * A container canonical name should have the following structure:
     * <pre>
     * {@literal <host>_<language>:<container>}
     * </pre>
     *
     * @param name the name to be checked
     * @return true if the string is a container canonical name, false if not
     */
    public static boolean isContainerName(String name) {
        Matcher matcher = CANONICAL_NAME_PATTERN.matcher(name);
        return matcher.matches() && matcher.group(6) != null && matcher.group(7) == null;
    }

    /**
     * Checks if the given name is a proper service canonical name.
     * <p>
     * A service canonical name should have the following structure:
     * <pre>
     * {@literal <host>_<language>:<container>:<engine>}
     * </pre>
     *
     * @param name the name to be checked
     * @return true if the string is a service canonical name, false if not
     */
    public static boolean isServiceName(String name) {
        Matcher matcher = CANONICAL_NAME_PATTERN.matcher(name);
        return matcher.matches() && matcher.group(8) != null;
    }


    /**
     * Gets the DPE name from the given CLARA canonical name.
     *
     * @param canonicalName a CLARA canonical name
     * @return the DPE name
     */
    public static String getDpeName(String canonicalName) {
        if (!isCanonicalName(canonicalName)) {
            throw new IllegalArgumentException("Not a canonical name: " + canonicalName);
        }
        xMsgTopic topic = xMsgTopic.wrap(canonicalName);
        return topic.domain();
    }

    /**
     * Gets the container name from the given CLARA canonical name.
     * This returns just the container name part, without the DPE name
     * (i.e. not a container canonical name).
     *
     * @param canonicalName a CLARA canonical name with a container part
     * @return the container name
     */
    public static String getContainerName(String canonicalName) {
        if (!isCanonicalName(canonicalName)) {
            throw new IllegalArgumentException("Not a canonical name: " + canonicalName);
        }
        xMsgTopic topic = xMsgTopic.wrap(canonicalName);
        return topic.subject();
    }

    /**
     * Gets the container canonical name from the given CLARA canonical name.
     * This returns the full canonical name, including the DPE name.
     *
     * @param canonicalName a CLARA canonical name with a container part
     * @return the container canonical name
     */
    public static String getContainerCanonicalName(String canonicalName) {
        if (!isCanonicalName(canonicalName)) {
            throw new IllegalArgumentException("Not a canonical name: " + canonicalName);
        }
        int firstSep = canonicalName.indexOf(xMsgConstants.TOPIC_SEP);
        if (firstSep < 0) {
            throw new IllegalArgumentException("Not a container name: " + canonicalName);
        }
        int secondSep = canonicalName.indexOf(xMsgConstants.TOPIC_SEP, firstSep + 1);
        if (secondSep < 0) {
            return canonicalName;
        }
        return canonicalName.substring(0, secondSep);
    }

    /**
     * Gets the service engine name from the given CLARA canonical name.
     * This returns just the engine name part, without the container and DPE
     * names (i.e. not a service canonical name).
     *
     * @param canonicalName a service canonical name
     * @return the container name
     */
    public static String getEngineName(String canonicalName) {
        if (!isCanonicalName(canonicalName)) {
            throw new IllegalArgumentException("Not a canonical name: " + canonicalName);
        }
        xMsgTopic topic = xMsgTopic.wrap(canonicalName);
        return topic.type();
    }

    /**
     * Gets the DPE host address from the given CLARA canonical name.
     *
     * @param canonicalName a CLARA canonical name
     * @return the DPE host address
     */
    public static String getDpeHost(String canonicalName) {
        if (!isCanonicalName(canonicalName)) {
            throw new IllegalArgumentException("Not a canonical name: " + canonicalName);
        }
        int portSep = canonicalName.indexOf(ClaraConstants.PORT_SEP);
        if (portSep > 0) {
            return canonicalName.substring(0, portSep);
        } else {
            int langSep = canonicalName.indexOf(ClaraConstants.LANG_SEP);
            return canonicalName.substring(0, langSep);
        }
    }

    /**
     * Gets the DPE port from the given CLARA canonical name.
     *
     * @param canonicalName a CLARA canonical name
     * @return the DPE port or the default port if not set in the name
     */
    public static int getDpePort(String canonicalName) {
        if (!isCanonicalName(canonicalName)) {
            throw new IllegalArgumentException("Not a canonical name: " + canonicalName);
        }
        int portSep = canonicalName.indexOf(ClaraConstants.PORT_SEP);
        int langSep = canonicalName.indexOf(ClaraConstants.LANG_SEP);
        if (portSep > 0) {
            String port = canonicalName.substring(portSep + 1, langSep);
            return Integer.parseInt(port);
        } else {
            return getPort(canonicalName, langSep + 1);
        }
    }

    /**
     * Gets the DPE language from the given CLARA canonical name.
     *
     * @param canonicalName a CLARA canonical name
     * @return the DPE language
     */
    public static String getDpeLang(String canonicalName) {
        if (!isCanonicalName(canonicalName)) {
            throw new IllegalArgumentException("Not a canonical name: " + canonicalName);
        }
        String dpeName = getDpeName(canonicalName);
        return dpeName.substring(dpeName.indexOf(ClaraConstants.LANG_SEP) + 1);
    }

    /**
     * Gets the default DPE port for the given language.
     *
     * @param lang a supported CLARA language
     * @return the default port for a DPE of the given language
     */
    public static int getDefaultPort(String lang) {
        return getPort(lang, 0);
    }

    private static int getPort(String fullName, int index) {
        switch (fullName.charAt(index)) {
            case 'j': case 'J':
                return ClaraConstants.JAVA_PORT;
            case 'c': case 'C':
                return ClaraConstants.CPP_PORT;
            case 'p': case 'P':
                return ClaraConstants.PYTHON_PORT;
            default:
                throw new IllegalArgumentException("Invalid language:" + fullName);
        }
    }

    /**
     * Helps creating a set of engine data types.
     *
     * @param dataTypes all the data types
     * @return a set with the data types
     */
    public static Set<EngineDataType> buildDataTypes(EngineDataType... dataTypes) {
        Set<EngineDataType> set = new HashSet<>();
        Collections.addAll(set, dataTypes);
        return set;
    }


    /**
     * Returns the stack trace of a exception as a string.
     * @param e an exception
     * @return a string with the stack trace of the exception
     */
    public static String reportException(Throwable e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }

    /**
     * Returns the list of <code>Throwable</code> objects in the
     * exception chain.
     * <p>
     * A throwable without cause will return a list containing
     * one element: the input throwable.
     * A throwable with one cause will return a list containing
     * two elements: the input throwable and the cause throwable.
     * A <code>null</code> throwable will return an empty list.</p>
     *
     * @param throwable  the throwable to inspect, may be null
     * @return the list of throwables
     */
    public static List<Throwable> getThrowableList(Throwable throwable) {
        List<Throwable> list = new ArrayList<Throwable>();
        while (throwable != null && !list.contains(throwable)) {
            list.add(throwable);
            throwable = throwable.getCause();
        }
        return list;
    }

    /**
     * Obtains the root cause of the given the <code>Throwable</code>, if any.
     *
     * @param throwable the throwable to get the root cause for, may be null
     * @return the root cause of the <code>Throwable</code>,
     *         <code>null</code> if none found or null throwable input
     */
    public static Throwable getRootCause(Throwable throwable) {
        List<Throwable> list = getThrowableList(throwable);
        return list.size() < 2 ? null : list.get(list.size() - 1);
    }

    /**
     * Converts exception stack trace to a string.
     *
     * @param e exception
     * @return String of the stack trace
     */
    public static String stack2str(Exception e) {
        try {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            return sw.toString();
        } catch (Exception e2) {
            return "bad stack";
        }
    }


    /**
     * Checks to see if the service is locally deployed.
     *
     * @param serviceName service canonical name (dpe-ip:container:engine)
     * @return true/false
     */
    public static Boolean isRemoteService(String serviceName) {
        xMsgTopic topic = xMsgTopic.wrap(serviceName);
        for (String s : xMsgUtil.getLocalHostIps()) {
            if (s.equals(topic.domain())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Gets the IPv4 value for the localhost address.
     *
     * @return the localhost IP
     */
    public static String localhost() {
        return xMsgUtil.localhost();
    }

    /**
     * Gets the current time and returns string representation of it.
     * @return string representing the current time.
     */
    public static String getCurrentTime() {
        return LocalDateTime.now().format(FORMATTER);
    }

    /**
     * Causes the currently executing thread to sleep for the given number of
     * milliseconds.
     * <p>
     * If any thread interrupts the current thread, this method will return
     * and the interrupt status will be set.
     *
     * @param millis the length of time to sleep in milliseconds
     */
    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Causes the currently executing thread to sleep for the given duration of time.
     * <p>
     * If any thread interrupts the current thread, this method will return
     * and the interrupt status will be set.
     *
     * @param duration the length of time to sleep
     * @param unit the time unit for the duration of the sleep
     */
    public static void sleep(long duration, TimeUnit unit) {
        try {
            unit.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
