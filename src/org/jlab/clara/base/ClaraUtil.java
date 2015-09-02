/*
 * Copyright (C) 2015. Jefferson Lab, CLARA framework (JLAB). All Rights Reserved.
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for educational, research, and not-for-profit purposes,
 * without fee and without a signed licensing agreement.
 *
 * Author Vardan Gyurjyan
 * Department of Experimental Nuclear Physics, Jefferson Lab.
 *
 * IN NO EVENT SHALL JLAB BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL,
 * INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF
 * THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF JLAB HAS BEEN ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * JLAB SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE. THE CLARA SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY, PROVIDED
 * HEREUNDER IS PROVIDED "AS IS". JLAB HAS NO OBLIGATION TO PROVIDE MAINTENANCE,
 * SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 */

package org.jlab.clara.base;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.ParametersAreNonnullByDefault;

import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.util.CConstants;

/**
 * Extra helper methods for Clara orchestrator and services.
 */
@ParametersAreNonnullByDefault
public final class ClaraUtil {

    private ClaraUtil() { }


    /**
     * Regex to validate a full canonical name.
     * Groups are used to separate each component.
     * <p>
     * A canonical name should have any of the following structures:
     * <pre>
     * {@literal <host>_<language>}
     * {@literal <host>_<language>:<container>}
     * {@literal <host>_<language>:<container>:<engine>}
     * </pre>
     */
    public static final Pattern CANONICAL_NAME_PATTERN =
            Pattern.compile("^([^:_ ]+_(java|python|cpp))(:(\\w+)(:(\\w+))?)?$");


    /**
     * Checks if the given name is a proper DPE canonical name.
     * <p>
     * A DPE canonical name should have the following structure:
     * <pre>
     * {@literal <host>_<language>}
     * </pre>
     *
     * @param name the name to be checked
     * @see ClaraUtil#formDpeName
     */
    public static boolean isDpeName(String name) {
        Matcher matcher = CANONICAL_NAME_PATTERN.matcher(name);
        return matcher.matches() && matcher.group(3) == null;
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
     * @see ClaraUtil#formContainerName
     */
    public static boolean isContainerName(String name) {
        Matcher matcher = CANONICAL_NAME_PATTERN.matcher(name);
        return matcher.matches() && matcher.group(4) != null && matcher.group(5) == null;
    }


    /**
     * Checks if the given name is a proper Clara canonical name.
     * <p>
     * A canonical name should have any of the following structures:
     * <pre>
     * {@literal <host>_<language>}
     * {@literal <host>_<language>:<container>}
     * {@literal <host>_<language>:<container>:<engine>}
     * </pre>
     *
     * @param name the name to be checked
     */
    public static boolean isCanonicalName(String name) {
        Matcher matcher = CANONICAL_NAME_PATTERN.matcher(name);
        return matcher.matches();
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
     * @see ClaraUtil#formServiceName
     */
    public static boolean isServiceName(String name) {
        Matcher matcher = CANONICAL_NAME_PATTERN.matcher(name);
        return matcher.matches() && matcher.group(6) != null;
    }


    /**
     * Returns the host part of a canonical name.
     *
     * @param canonicalName a proper canonical name
     * @return the hostname
     */
    public static String getHostName(String canonicalName) {
        return canonicalName.substring(0, canonicalName.indexOf(CConstants.LANG_SEP));
    }


    /**
     * Returns the DPE part of a canonical name.
     *
     * @param canonicalName a proper canonical name
     * @return the DPE name
     */
    public static String getDpeName(String canonicalName) {
        int firstSep = canonicalName.indexOf(CConstants.TOPIC_SEP);
        if (firstSep < 0) {
            return canonicalName;
        }
        return canonicalName.substring(0, firstSep);
    }


    /**
     * Returns the canonical container part of a container or service name.
     *
     * @param canonicalName a proper container or service name
     * @return the canonical container name
     */
    public static String getContainerCanonicalName(String canonicalName) {
        int firstSep = canonicalName.indexOf(CConstants.TOPIC_SEP);
        int secondSep = canonicalName.indexOf(CConstants.TOPIC_SEP, firstSep + 1);
        if (secondSep < 0) {
            return canonicalName;
        }
        return canonicalName.substring(0, secondSep);
    }


    /**
     * Returns the container part of a container or service name.
     *
     * @param canonicalName a proper container or service name
     * @return the container name
     */
    public static String getContainerName(String canonicalName) {
        int firstSep = canonicalName.indexOf(CConstants.TOPIC_SEP);
        int secondSep = canonicalName.indexOf(CConstants.TOPIC_SEP, firstSep + 1);
        if (secondSep < 0) {
            return canonicalName.substring(firstSep + 1);
        }
        return canonicalName.substring(firstSep + 1, secondSep);
    }


    /**
     * Returns the engine part of a service name.
     *
     * @param serviceName a proper service name
     * @return the engine of the name
     */
    public static String getEngineName(String serviceName) {
        int firstSep = serviceName.indexOf(CConstants.TOPIC_SEP);
        int secondSep = serviceName.indexOf(CConstants.TOPIC_SEP, firstSep + 1);
        return serviceName.substring(secondSep + 1);
    }


    /**
     * Constructs a proper DPE canonical name with the given host and language.
     *
     * @param host the hostname of the DPE
     * @param lang the language of the DPE
     * @return the DPE canonical name
     */
    public static String formDpeName(String host, ClaraLang lang) {
        return host + CConstants.LANG_SEP + lang;
    }


    /**
     * Constructs a proper container canonical name with the given DPE and name.
     *
     * @param dpeName the DPE hosting the container
     * @param container the name of the container
     * @return the container canonical name
     * @see #formDpeName
     */
    public static String formContainerName(String dpeName, String container) {
        if (!isDpeName(dpeName)) {
            throw new IllegalArgumentException("Not a DPE name: " + dpeName);
        }
        return dpeName + CConstants.TOPIC_SEP + container;
    }


    /**
     * Constructs a proper container canonical name with the given host,
     * language and name.
     *
     * @param host the hostname of the DPE
     * @param lang the language of the DPE
     * @param container the name of the container
     * @return the container canonical name
     */
    public static String formContainerName(String host, ClaraLang lang, String container) {
        return formDpeName(host, lang) + CConstants.TOPIC_SEP + container;
    }


    /**
     * Constructs a proper service canonical name with the given container and engine.
     *
     * @param containerName the container running the service
     * @param engine the name of the service engine
     * @return the service canonical name
     * @see #formContainerName
     */
    public static String formServiceName(String containerName, String engine) {
        if (!isContainerName(containerName)) {
            throw new IllegalArgumentException("Not a container name: " + containerName);
        }
        return containerName + CConstants.TOPIC_SEP + engine;
    }


    /**
     * Constructs a proper service canonical name with the given host,
     * language, container and engine.
     *
     * @param host the hostname of the DPE
     * @param lang the language of the DPE
     * @param container the name of the container
     * @param engine the name of the service engine
     * @return the service canonical name
     */
    public static String formServiceName(String host, ClaraLang lang,
                                         String container, String engine) {
        return formContainerName(host, lang, container) + CConstants.TOPIC_SEP + engine;
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
}
