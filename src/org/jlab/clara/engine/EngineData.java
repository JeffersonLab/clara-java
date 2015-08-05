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

package org.jlab.clara.engine;

import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;

/**
 * Engine data passed in/out to the service engine.
 */
public class EngineData {

    private Object data;
    private xMsgMeta.Builder metaData = xMsgMeta.newBuilder();

    public Object getData() {
        return data;
    }

    public String getMimeType() {
        return metaData.getDataType();
    }

    public void setData(String mimeType, Object data) {
        this.data = data;
        this.metaData.setDataType(mimeType);
    }

    public String getDescription() {
        return metaData.getDescription();
    }

    public void setDescription(String description) {
        metaData.setDescription(description);
    }

    public EngineStatus getStatus() {
        xMsgMeta.Status status = metaData.getStatus();
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

    public int getStatusSeverity() {
        return metaData.getSeverityId();
    }

    public void setStatus(EngineStatus status) {
        setStatus(status, 1);
    }

    public void setStatus(EngineStatus status, int severity) {
        if (severity <= 0) {
            throw new IllegalArgumentException("severity must be positive: " + severity);
        }
        switch (status) {
            case INFO:
                metaData.setStatus(xMsgMeta.Status.INFO);
                break;
            case WARNING:
                metaData.setStatus(xMsgMeta.Status.WARNING);
                break;
            case ERROR:
                metaData.setStatus(xMsgMeta.Status.ERROR);
                break;
            default:
                throw new IllegalStateException("Unknown status " + status);
        }
        metaData.setSeverityId(severity);
    }

    public String getState() {
        return metaData.getSenderState();
    }

    public void setState(String state) {
        metaData.setSenderState(state);
    }

    public int getRequestId() {
        return metaData.getCommunicationId();
    }

    public long getExecutionTime() {
        return metaData.getExecutionTime();
    }

    public String getAuthor() {
        return metaData.getAuthor();
    }

    public String getVersion() {
        return metaData.getVersion();
    }
}
