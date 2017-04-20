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

import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;

import org.jlab.clara.util.ArgUtils;
import org.jline.reader.Completer;
import org.jline.reader.impl.completer.FileNameCompleter;

/**
 * Environment configuration for a CLARA shell session.
 */
public class Config {

    /**
     * The variable for the orchestrator configuration file.
     */
    public static final String SERVICES_FILE = "servicesFile";

    /**
     * The variable for the list of input files.
     */
    public static final String FILES_LIST = "fileList";

    /**
     * The variable for the input directory.
     */
    public static final String INPUT_DIR = "inputDir";

    /**
     * The variable for the output directory.
     */
    public static final String OUTPUT_DIR = "outputDir";

    /**
     * The variable for the session of the CLARA DPE.
     */
    public static final String SESSION = "session";

    /**
     * The variable for the data processing description keyword.
     */
    public static final String DESCRIPTION = "description";

    /**
     * The variable for the address of the front-end DPE.
     */
    public static final String FRONTEND_HOST = "feHost";

    /**
     * The variable for the port of the front-end DPE.
     */
    public static final String FRONTEND_PORT = "fePort";

    /**
     * The variable for the switch to use the front-end DPE or not.
     */
    public static final String USE_FRONTEND = "useFE";

    /**
     * The variable for the number of reconstruction threads.
     */
    public static final String MAX_THREADS = "threads";

    /**
     * The variable for the the JVM heap size used by the DPE.
     */
    public static final String JAVA_MEMORY = "javaMemory";

    /**
     * The variable for the JVM options of the Java DPE (overrides {@link JAVA_MEMORY}).
     */
    public static final String JAVA_OPTIONS = "javaOptions";

    private final Map<String, ConfigVariable> variables;
    private final Map<String, String> environment;

    /**
     * Helps creating the configuration for a shell session.
     */
    public static class Builder {

        private final Config config;

        /**
         * Creates a new builder.
         */
        public Builder() {
            this.config = new Config();
        }

        /**
         * Sets a configuration variable with the given initial value.
         *
         * @param name the name of the variable
         * @param initialValue the initial value of the variable
         * @return this builder
         */
        public Builder withConfigVariable(String name, Object initialValue) {
            ArgUtils.requireNonEmpty(name, "name");
            ArgUtils.requireNonNull(initialValue, "value");

            config.setValue(name, initialValue);
            return this;
        }

        /**
         * Adds a configuration variable.
         * This new variable cannot have the same name as one of the default
         * configuration variables.
         *
         * @param builder the builder of the configuration variable
         * @return this builder
         */
        public Builder withConfigVariable(ConfigVariable.Builder builder) {
            ArgUtils.requireNonNull(builder, "variable builder");

            config.addVariable(builder.build());
            return this;
        }

        /**
         * Sets an environment variable for commands.
         * The variable will be added to the environment of the DPEs started by
         * the {@code run} command.
         *
         * @param name the name of the variable
         * @param value the value of the variable
         * @return this builder
         */
        public Builder withEnvironmentVariable(String name, String value) {
            ArgUtils.requireNonEmpty(name, "name");
            ArgUtils.requireNonNull(value, "value");
            config.setenv(name, value);
            return this;
        }

        /**
         * Creates the shell configuration.
         *
         * @return the configuration
         */
        public Config build() {
            return config;
        }
    }


    Config() {
        variables = initVariables();
        environment = new HashMap<>();
    }

    static Map<String, ConfigVariable> initVariables() {
        Map<String, ConfigVariable> m = new LinkedHashMap<>();
        defaultVariables().forEach((n, b) -> m.put(n, b.build()));
        return m;
    }

    static Map<String, ConfigVariable.Builder> defaultVariables() {
        Map<String, ConfigVariable.Builder> defaultVariables = new LinkedHashMap<>();

        BiFunction<String, String, ConfigVariable.Builder> addBuilder = (n, d) -> {
            ConfigVariable.Builder b = ConfigVariable.newBuilder(n, d);
            defaultVariables.put(n, b);
            return b;
        };
        String claraHome = claraHome();

        addBuilder.apply(SERVICES_FILE,
                "Path to the file describing application service composition.")
                .withParser(ConfigParsers::toExistingFile)
                .withCompleter(fileCompleter());

        addBuilder.apply(FILES_LIST,
                "Path to the file containing the names of data-files to be processed.")
                .withParser(ConfigParsers::toExistingFile)
                .withCompleter(fileCompleter());

        addBuilder.apply(INPUT_DIR,
                "The input directory where the files to be processed are located.")
                .withInitialValue(Paths.get(claraHome, "data", "input").toString())
                .withParser(ConfigParsers::toExistingDirectory)
                .withCompleter(fileCompleter());

        addBuilder.apply(OUTPUT_DIR,
                "The output directory where processed files will be saved.")
                .withInitialValue(Paths.get(claraHome, "data", "output").toString())
                .withParser(ConfigParsers::toDirectory)
                .withCompleter(fileCompleter());

        addBuilder.apply(MAX_THREADS,
                "The maximum number of processing threads to be used per node.")
                .withParser(ConfigParsers::toPositiveInteger);

        addBuilder.apply(FRONTEND_HOST,
                "The IP address to be used by the front-end DPE.")
                .withParser(ConfigParsers::toHostAddress);

        addBuilder.apply(FRONTEND_PORT,
                "The port to be used by the front-end DPE.")
                .withParser(ConfigParsers::toPositiveInteger);

        addBuilder.apply(SESSION,
                "The data processing session.")
                .withInitialValue("")
                .withParser(ConfigParsers::toAlphaNumWordOrEmpty);

        addBuilder.apply(DESCRIPTION,
                "A single word (no spaces) describing the data processing.")
                .withInitialValue("clara")
                .withParser(ConfigParsers::toAlphaNumWord);

        addBuilder.apply(USE_FRONTEND,
                "Use the front-end DPE for reconstruction.")
                .withInitialValue(true)
                .withParser(ConfigParsers::toBoolean);

        addBuilder.apply(JAVA_MEMORY, "DPE JVM memory size (in GB)")
            .withParser(ConfigParsers::toPositiveInteger);

        addBuilder.apply(JAVA_OPTIONS,
                        "DPE JVM options (overrides " + JAVA_MEMORY + ")");

        return defaultVariables;
    }

    /**
     * Gets the value of the CLARA_HOME environment variable.
     *
     * @return the value of the environment variable, if set
     */
    public static String claraHome() {
        String claraHome = System.getenv("CLARA_HOME");
        if (claraHome == null) {
            throw new RuntimeException("Missing CLARA_HOME variable");
        }
        return claraHome;
    }

    /**
     * Gets the user name.
     *
     * @return the name of the user running the shell.
     */
    public static String user() {
        return System.getProperty("user.name");
    }

    /**
     * Checks if a variable of the given name exists.
     *
     * @param name the variable to check
     * @return true if there is a variable with the given name, false otherwise
     */
    public boolean hasVariable(String name) {
        return variables.containsKey(name);
    }

    /**
     * Checks if the given variable has a value.
     *
     * @param variable the variable to check
     * @return true if there is a variable has a value, false otherwise
     * @throws IllegalArgumentException if the variable does not exist
     */
    public boolean hasValue(String variable) {
        return getVariable(variable).hasValue();
    }

    /**
     * Gets the value of the specified variable.
     *
     * @param variable the name of the variable
     * @return the current value of the variable, if set
     * @throws IllegalArgumentException if the variable does not exist
     * @throws IllegalStateException if the variable has no value
     */
    public Object getValue(String variable) {
        return getVariable(variable).getValue();
    }

    void setValue(String variable, Object value) {
        getVariable(variable).setValue(value);
    }

    void addVariable(ConfigVariable variable) {
        ConfigVariable prev = variables.putIfAbsent(variable.getName(), variable);
        if (prev != null) {
            String msg = String.format("a variable named %s already exists", variable.getName());
            throw new IllegalArgumentException(msg);
        }
    }

    ConfigVariable getVariable(String name) {
        ConfigVariable v = variables.get(name);
        if (v == null) {
            throw new IllegalArgumentException("no variable named " + name);
        }
        return v;
    }

    Collection<ConfigVariable> getVariables() {
        return variables.values();
    }

    void setenv(String name, String value) {
        ArgUtils.requireNonEmpty(name, "name");
        ArgUtils.requireNonEmpty(value, "value");
        if (name.equals("CLARA_HOME")) {
            throw new IllegalArgumentException("cannot override $CLARA_HOME variable");
        }
        environment.put(name, value);
    }

    /**
     * Returns an unmodifiable string map view of the custom environment
     * variables set for the shell.
     * These are extra variables to the system environment, defined when the
     * shell was created.
     * <p>
     * The variables should be added to the inherited environment when starting
     * a subprocess:
     * <pre>
     * ProcessBuilder b = new ProcessBuilder(command);
     * b.environment().putAll(config.getenv());
     * b.start()
     * </pre>
     *
     * @return the custom extra environment as a map of variable names to values
     */
    public Map<String, String> getenv() {
        return Collections.unmodifiableMap(environment);
    }

    private static Completer fileCompleter() {
        return new FileNameCompleter();
    }
}
