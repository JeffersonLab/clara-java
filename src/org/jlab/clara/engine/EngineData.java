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

import org.jlab.clara.base.core.DataUtil.EngineDataAccessor;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;

/**
 * Engine data passed in/out to the service engine.
 */
public class EngineData {

    private Object data;
    private xMsgMeta.Builder metadata = xMsgMeta.newBuilder();

    /**
     * Creates an empty engine data object.
     * The user-data must be set with {@link #setData}.
     */
    public EngineData() {
        this.metadata = xMsgMeta.newBuilder();
    }

    private EngineData(Object data, xMsgMeta.Builder metadata) {
        this.data = data;
        this.metadata = metadata;
    }

    private xMsgMeta.Builder getMetadata() {
        return metadata;
    }

    /**
     * Gets the user-data.
     * The value must be cast to its proper Java class.
     * Use {@link #getMimeType} to get information about the type of the data.
     *
     * @return the user-data or null if not set
     */
    public Object getData() {
        return data;
    }

    /**
     * Gets the mime-type string for the user-data.
     * The mime-type acts as a clue for which Java class must be used when
     * casting the user-data.
     *
     * @return a string with the mime-type or empty if not set
     */
    public String getMimeType() {
        return metadata.getDataType();
    }

    /**
     * Sets a new user-data for this object.
     * <p>
     * The mime-type string and the Java class of the user-data
     * must correspond to an existing {@link EngineDataType} supported by
     * the orchestrator or engine, to serialize the data if necessary.
     *
     * @param mimeType the mime-type for the user-data
     * @param data the object with the user-data
     */
    public void setData(String mimeType, Object data) {
        this.data = data;
        this.metadata.setDataType(mimeType);
    }

    /**
     * Gets the description of the data and/or status.
     * Each engine can set a description to provide extra information about the
     * result of a request.
     *
     * @return a string with the description or empty if not set
     */
    public String getDescription() {
        return metadata.getDescription();
    }

    /**
     * Sets a description for the data.
     * It can provide further details about the user-data,
     * or the cause for the error status, etc.
     *
     * @param description a description for the data
     */
    public void setDescription(String description) {
        metadata.setDescription(description);
    }

    /**
     * Gets the status for the data.
     * Useful to check if the result of an engine execution
     * was a warning or an error.
     * <p>
     * The default status is always {@link EngineStatus#INFO info}.
     *
     * @return the status of the data
     */
    public EngineStatus getStatus() {
        xMsgMeta.Status status = metadata.getStatus();
        switch (status) {
            case INFO:
                return EngineStatus.INFO;
            case WARNING:
                return EngineStatus.WARNING;
            case ERROR:
                return EngineStatus.ERROR;
            default:
                throw new IllegalStateException("Unknown status " + status);
        }
    }

    /**
     * Gets the optional severity for the status of the data.
     * <p>
     * The default severity for a new status is 1.
     *
     * @return the value of the severity (engine-specific)
     */
    public int getStatusSeverity() {
        return metadata.getSeverityId();
    }

    /**
     * Sets a new status for this data.
     * Useful to set the result of an engine execution request
     * as a warning or error.
     *
     * @param status the new status
     */
    public void setStatus(EngineStatus status) {
        setStatus(status, 1);
    }

    /**
     * Sets a new status for this data, with custom severity.
     * Useful to set the result of an engine execution request
     * as a warning or error.
     * <p>
     * Each engine defines the interpretation of the severity values.
     *
     * @param status the new status
     * @param severity the custom severity as a positive integer
     */
    public void setStatus(EngineStatus status, int severity) {
        if (severity <= 0) {
            throw new IllegalArgumentException("severity must be positive: " + severity);
        }
        switch (status) {
            case INFO:
                metadata.setStatus(xMsgMeta.Status.INFO);
                break;
            case WARNING:
                metadata.setStatus(xMsgMeta.Status.WARNING);
                break;
            case ERROR:
                metadata.setStatus(xMsgMeta.Status.ERROR);
                break;
            default:
                throw new IllegalStateException("Unknown status " + status);
        }
        metadata.setSeverityId(severity);
    }

    /**
     * Gets the state of the execution result set by the engine, if any.
     * <p>
     * This value will be set only when the data is the result of a engine,
     * and it is used in composition requests to route the data to the next
     * service in the composition.
     *
     * @return the state set by the engine or empty
     */
    public String getEngineState() {
        return metadata.getSenderState();
    }

    /**
     * Sets a state for this data.
     * Engines should set a state for the result of processing a specific
     * request, and it should be one of the defined states by the engine.
     * <p>
     * States define the flow the output data in a composition request,
     * where results are routed to the next services based on its state.
     *
     * @param state the state for the data
     */
    public void setEngineState(String state) {
        metadata.setSenderState(state);
    }

    /**
     * Gets the canonical name of the engine that returned this data, if any.
     * <p>
     * This value will be set only when the data is the result of a engine
     * request, and it can be obtained by the next service in a composition or
     * by the monitoring orchestrators.
     *
     * @return the canonical name of the engine or empty
     *         if not created by an engine
     */
    public String getEngineName() {
        return metadata.getAuthor();
    }

    /**
     * Gets the version of the engine that returned this data, if any.
     * <p>
     * This value will be set only when the data is the result of a engine
     * request, and it can be obtained by the next service in a composition or
     * by the monitoring orchestrators.
     *
     * @return the version of the engine or empty if not created by an engine
     */
    public String getEngineVersion() {
        return metadata.getVersion();
    }

    /**
     * Gets the ID for the request this data is part of.
     * <p>
     * This value should be set by the orchestrator or data-source engine
     * sending a new request (a single request to an engine, or the first
     * request of a new application composition request).
     *
     * @return the ID of the request
     */
    public int getCommunicationId() {
        return metadata.getCommunicationId();
    }

    /**
     * Sets an ID for the request this data is part of.
     * <p>
     * Orchestrators and data-source engines should set a proper communication
     * ID to identify unique requests.
     * Engines should avoid changing the communication ID in the middle of an
     * application composition request.
     *
     * @param value the communication ID
     */
    public void setCommunicationId(int value) {
        metadata.setCommunicationId(value);
    }

    /**
     * Gets the composition for the request this data is part of.
     * <p>
     * The composition is set when an orchestrator publishes the data, i.e.,
     * only the engines receiving the data (or the orchestrators receiving the
     * results) can observe its value.
     *
     * @return a string with the composition
     */
    public String getComposition() {
        return metadata.getComposition();
    }

    /**
     * Gets the time that took the engine to process a request and return this data.
     * If set, this is the time spent by a successful request to the
     * {@link Engine#configure configure} or {@link Engine#execute execute}
     * method of the engine that created this data.
     * <p>
     * This value will be set only when the data is the result of a engine
     * request, and it can be obtained by the next service in a composition or
     * by the monitoring orchestrators.
     *
     * @return the execution time of the request or 0 if not a result of a request
     */
    public long getExecutionTime() {
        return metadata.getExecutionTime();
    }


    @Override
    public String toString() {
        return "EngineData: " + metadata.getDataType() + " " + data;
    }


    static {
        EngineDataAccessor.setDefault(new Accessor());
    }

    private static class Accessor extends EngineDataAccessor {

        @Override
        protected xMsgMeta.Builder getMetadata(EngineData data) {
            return data.getMetadata();
        }

        @Override
        protected EngineData build(Object data, xMsgMeta.Builder metadata) {
            return new EngineData(data, metadata);
        }
    }
}
