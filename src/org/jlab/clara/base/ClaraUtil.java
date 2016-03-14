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

import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.engine.EngineStatus;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.excp.xMsgAddressException;

import javax.annotation.ParametersAreNonnullByDefault;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.SocketException;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extra helper methods for Clara orchestrator and services.
 *
 * @author gurjyan
 * @version 4.x
 */
@ParametersAreNonnullByDefault
public final class ClaraUtil {

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
     * Checks if the given name is a proper Clara canonical name.
     * <p>
     * A canonical name should have any of the following structures:
     * <pre>
     * {@literal <host>%<port>_<language>}
     * {@literal <host>%<port>_<language>:<container>}
     * {@literal <host>%<port>_<language>:<container>:<engine>}
     * </pre>
     *
     * @param name the name to be checked
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
     */
    public static boolean isServiceName(String name) {
        Matcher matcher = CANONICAL_NAME_PATTERN.matcher(name);
        return matcher.matches() && matcher.group(8) != null;
    }


    public static String getDpeName(String canonicalName) throws ClaraException {
        if (!isCanonicalName(canonicalName)) {
            throw new ClaraException("Clara-Error: not a canonical name");
        }
        xMsgTopic topic = xMsgTopic.wrap(canonicalName);
        return topic.domain();
    }

    public static String getContainerName(String canonicalName) throws ClaraException {
        if (!isCanonicalName(canonicalName)) {
            throw new ClaraException("Clara-Error: not a canonical name");
        }
        xMsgTopic topic = xMsgTopic.wrap(canonicalName);
        return topic.subject();
    }

    public static String getContainerCanonicalName(String canonicalName) throws ClaraException {
        if (!isCanonicalName(canonicalName)) {
            throw new ClaraException("Clara-Error: not a canonical name");
        }
        int firstSep = canonicalName.indexOf(xMsgConstants.TOPIC_SEP);
        if (firstSep < 0) {
            throw new ClaraException("Clara-Error: not a container or service name");
        }
        int secondSep = canonicalName.indexOf(xMsgConstants.TOPIC_SEP, firstSep + 1);
        if (secondSep < 0) {
            return canonicalName;
        }
        return canonicalName.substring(0, secondSep);
    }

    public static String getEngineName(String canonicalName) throws ClaraException {
        if (!isCanonicalName(canonicalName)) {
            throw new ClaraException("Clara-Error: not a canonical name");
        }
        xMsgTopic topic = xMsgTopic.wrap(canonicalName);
        return topic.type();
    }

    public static String getDpeHost(String canonicalName) throws ClaraException {
        if (!isCanonicalName(canonicalName)) {
            throw new ClaraException("Clara-Error: not a canonical name");
        }
        int portSep = canonicalName.indexOf(xMsgConstants.PRXHOSTPORT_SEP);
        if (portSep > 0) {
            return canonicalName.substring(0, portSep);
        } else {
            int langSep = canonicalName.indexOf(xMsgConstants.LANG_SEP);
            return canonicalName.substring(0, langSep);
        }
    }

    public static int getDpePort(String canonicalName) throws ClaraException {
        if (!isCanonicalName(canonicalName)) {
            throw new ClaraException("Clara-Error: not a canonical name");
        }
        int portSep = canonicalName.indexOf(xMsgConstants.PRXHOSTPORT_SEP);
        int langSep = canonicalName.indexOf(xMsgConstants.LANG_SEP);
        if (portSep > 0) {
            String port = canonicalName.substring(portSep + 1, langSep);
            return Integer.parseInt(port);
        } else {
            return xMsgConstants.DEFAULT_PORT;
        }
    }

    public static String getDpeLang(String canonicalName) throws ClaraException {
        if (!isCanonicalName(canonicalName)) {
            throw new ClaraException("Clara-Error: not a canonical name");
        }
        String dpeName = getDpeName(canonicalName);
        return dpeName.substring(dpeName.indexOf(xMsgConstants.LANG_SEP) + 1);
    }

    /**
     * Helps creating a set of engine data types.
     *
     * @param dataTypes all the data types
     * @return a set with the data types
     */
    public static Set<EngineDataType> buildDataTypes(EngineDataType... dataTypes) {
        Set<EngineDataType> set = new HashSet<EngineDataType>();
        for (EngineDataType dt : dataTypes) {
            set.add(dt);
        }
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


    public static Boolean isHostLocal(String hostName)
            throws IOException {
        for (String s : xMsgUtil.getLocalHostIps()) {
            if (s.equals(hostName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks to see if the service is locally deployed.
     *
     * @param serviceName service canonical name (dpe-ip:container:engine)
     * @return true/false
     * @throws org.jlab.clara.base.error.ClaraException
     */
    public static Boolean isRemoteService(String serviceName)
            throws ClaraException, IOException {

        try {
            xMsgTopic topic = xMsgTopic.wrap(serviceName);
            for (String s : xMsgUtil.getLocalHostIps()) {
                if (s.equals(topic.domain())) {
                    return false;
                }
            }
        } catch (SocketException e) {
            throw new ClaraException(e.getMessage());
        }

        return true;
    }

    public static String localhost() {
        try {
            // Workaround to convert IOException into a runtime exception,
            // until Java 8 is used
            return xMsgUtil.localhost();
        } catch (IOException e) {
            throw new xMsgAddressException(e);
        }
    }

    public static String removeFirst(String s) {
        if (s == null || s.length() == 0) {
            return s;
        }
        return s.substring(1, s.length());
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

    /**
     * Gets the current time and returns string representation of it.
     * @return string representing the current time.
     */
    public static String getCurrentTimeInH() {
        Format formatter = new SimpleDateFormat("HH:mm:ss MM/dd");
        return formatter.format(new Date());
    }

    /**
     * Gets the current time and returns string representation of it.
     * @return string representing the current time.
     */
    public static String getCurrentTime() {
        Format formatter = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        return formatter.format(new Date());
    }

    /**
     * Gets the current time and returns string representation of it.
     * @return string representing the current time.
     */
    public static String getCurrentTime(String format) {
        Format formatter = new SimpleDateFormat(format);
        return formatter.format(new Date());
    }

    /**
     * Current time in milli-seconds.
     * @return current time in ms.
     */
    public static long getCurrentTimeInMs() {
        return new GregorianCalendar().getTimeInMillis();
    }

    public static void sleep(int s) {
        try {
            Thread.sleep(s);
        } catch (InterruptedException e) {
            e.printStackTrace();
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
        return null;
    }

    public static  xMsgTopic buildTopic(Object... args) {
        StringBuilder topic  = new StringBuilder();
        topic.append(args[0]);
        for (int i = 1; i < args.length; i++) {
            topic.append(xMsgConstants.TOPIC_SEP);
            topic.append(args[i]);
        }
        return xMsgTopic.wrap(topic.toString());
    }


    public static String buildData(Object... args) {
        StringBuilder topic  = new StringBuilder();
        topic.append(args[0]);
        for (int i = 1; i < args.length; i++) {
            topic.append(xMsgConstants.DATA_SEP);
            topic.append(args[i]);
        }
        return topic.toString();
    }

    public static String getUniqueName() {
        return UUID.randomUUID().toString();
    }

    public static void validateTimeout(int timeout) {
        if (timeout <= 0) {
            throw new IllegalArgumentException("Invalid timeout: " + timeout);
        }
    }

    public static String getStatusText(EngineStatus status) {
        switch (status) {
            case INFO:
                return xMsgConstants.INFO;
            case WARNING:
                return xMsgConstants.WARNING;
            case ERROR:
                return xMsgConstants.ERROR;
            default:
                throw new IllegalStateException("Clara-Error: Unknown status " + status);
        }
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
}
