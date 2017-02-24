package org.jlab.clara.std.orchestrators;

/**
 * An error configuring the orchestrator.
 */
public class OrchestratorConfigError extends RuntimeException {

    private static final long serialVersionUID = 7169655555225259425L;

    /**
     * Constructs a new exception.
     *
     * @param message the detail message
     */
    public OrchestratorConfigError(String message) {
        super(message);
    }

    /**
     * Constructs a new exception.
     *
     * @param cause the cause of the exception
     */
    public OrchestratorConfigError(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new exception.
     *
     * @param message the detail message
     * @param cause the cause of the exception
     */
    public OrchestratorConfigError(String message, Throwable cause) {
        super(message, cause);
    }

}
