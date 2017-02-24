package org.jlab.clara.std.orchestrators;

import org.jlab.clara.base.ClaraUtil;

/**
 * An error in the orchestrator.
 */
public class OrchestratorError extends RuntimeException {

    private static final long serialVersionUID = -5459481851420223735L;

    /**
     * Constructs a new exception.
     *
     * @param message the detail message
     */
    public OrchestratorError(String message) {
        super(message);
    }

    /**
     * Constructs a new exception.
     *
     * @param cause the cause of the exception
     */
    public OrchestratorError(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new exception.
     *
     *
     * @param message the detail message
     * @param cause the cause of the exception
     */
    public OrchestratorError(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public String getMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append(super.getMessage());
        for (Throwable e: ClaraUtil.getThrowableList(getCause())) {
            sb.append(": ").append(e.getMessage());
        }
        return sb.toString();
    }
}
