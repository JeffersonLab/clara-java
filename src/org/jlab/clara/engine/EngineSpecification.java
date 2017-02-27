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

package org.jlab.clara.engine;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.error.YAMLException;


/**
 * Loads the service specification from a YAML file.
 */
public class EngineSpecification {

    /**
     * Reports any problem parsing the service specification file.
     */
    public static class ParseException extends RuntimeException {

        // CHECKSTYLE.OFF: Javadoc
        public ParseException() {
        }

        public ParseException(String message) {
            super(message);
        }

        public ParseException(Throwable cause) {
            super(cause);
        }

        public ParseException(String message, Throwable cause) {
            super(message, cause);
        }
        // CHECKSTYLE.ON: Javadoc
    }

    private String name;
    private String engine;
    private String type;

    private String author;
    private String email;

    private String version;
    private String description;


    /**
     * Uses the service class to detect the YAML file.
     *
     * Example:
     * <pre>
     * package std.services.Simple;
     *
     * class Simple extends JService {
     *     private CServiceSpecification info;
     *
     *     public Simple() {
     *         this.info = new CServiceSpecification(this.getClass());
     *     }
     *
     *     ...
     *
     *     public getName() {
     *         return info.name();
     *     }
     *
     *     ...
     * }
     * </pre>
     * will search for the file
     * <code>std/services/Simple.yaml</code> in the CLASSPATH.
     *
     * @param c the engine class
     */
    public EngineSpecification(Class<?> c) {
        this(c.getName());
    }


    /**
     * Uses the full service engine class name to detect the YAML file.
     * <p>
     * Example:
     * <pre>
     *     new CServiceSpecification("std.services.convertors.EvioToEvioReader")
     * </pre>
     * will search for the file
     * <code>std/services/convertors/EvioToEvioReader.yaml</code> in the CLASSPATH.
     *
     * @param engine the engine classpath
     */
    @SuppressWarnings("unchecked")
    public EngineSpecification(String engine) {
        InputStream input = getSpecStream(engine);
        if (input != null) {
            Yaml yaml = new Yaml();
            try {
                Object content = yaml.load(input);
                if (content instanceof Map) {
                    parseContent((Map<String, Object>) content);
                } else {
                    throw new ParseException("Unexpected YAML content");
                }
            } catch (YAMLException e) {
                throw new ParseException(e);
            } finally {
                try {
                    input.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        } else {
            throw new ParseException("Service specification file not found for " + engine);
        }
    }


    private InputStream getSpecStream(String engine) {
        ClassLoader cl = getClass().getClassLoader();
        Path resourcePath = Paths.get(engine.replaceAll("\\.", File.separator) + ".yaml");
        InputStream resource = cl.getResourceAsStream(resourcePath.getFileName().toString());
        if (resource == null) {
            resource = cl.getResourceAsStream(resourcePath.toString());
        }
        return resource;
    }


    private void parseContent(Map<String, Object> content) {
        name = parseString(content, "name");
        engine = parseString(content, "engine");
        author = parseString(content, "author");
        email = parseString(content, "email");
        type = parseString(content, "type");
        version = parseString(content, "version");
        description = parseString(content, "description");
    }


    private String parseString(Map<String, Object> content, String key) {
        Object value = content.get(key);
        if (value == null) {
            throw new ParseException("Missing key: " + key);
        }
        if (value instanceof String) {
            return (String) value;
        } else if (value instanceof Integer || value instanceof Double) {
            return value.toString();
        } else {
            throw new ParseException("Bad type for: " + key);
        }
    }


    /**
     * Returns the name of the engine.
     *
     * @return the engine name
     */
    public String name() {
        return name;
    }


    /**
     * Returns the classpath of the engine.
     *
     * @return the engine classpath
     */
    public String engine() {
        return engine;
    }


    /**
     * Returns the type of the engine (java, cpp, python).
     *
     * @return the engine language
     */
    public String type() {
        return type;
    }


    /**
     * Returns the name of the author of the engine.
     *
     * @return the engine author name
     */
    public String author() {
        return author;
    }


    /**
     * Returns the email of the author of the engine.
     *
     * @return the engine author email
     */
    public String email() {
        return email;
    }


    /**
     * Returns the version of the engine.
     *
     * @return the engine version
     */
    public String version() {
        return version;
    }


    /**
     * Returns the description of the engine.
     *
     * @return the engine description
     */
    public String description() {
        return description;
    }


    @Override
    public String toString() {
        return "NAME:"          + "\n    " + name + "\n\n"
             + "ENGINE:"        + "\n    " + engine + "\n\n"
             + "TYPE:"          + "\n    " + type + "\n\n"
             + "VERSION:"       + "\n    " + version + "\n\n"
             + "AUTHOR:"        + "\n    " + author + " <" + email + ">\n\n"
             + "DESCRIPTION:"   + "\n    " + description;
    }
}
