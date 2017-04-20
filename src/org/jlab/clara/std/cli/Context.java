package org.jlab.clara.std.cli;

import org.jline.terminal.Terminal;

/**
 * The context for a shell session.
*/
public class Context {

    private final Terminal terminal;
    private final Config config;

    /**
     * Creates ththe context for a shell session.
     *
     * @param terminal the terminal used by the shell
     * @param config the configuration for the shell session
     */
    public Context(Terminal terminal, Config config) {
        this.terminal = terminal;
        this.config = config;
    }

    /**
     * Gets the virtual terminal used by the shell.
     *
     * @return the virtual terminal
     */
    public Terminal terminal() {
        return terminal;
    }

    /**
     * Gets the configuration variables for the shell session.
     *
     * @return the configuration
     */
    public Config config() {
        return config;
    }
}
