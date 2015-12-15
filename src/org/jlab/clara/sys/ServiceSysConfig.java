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

package org.jlab.clara.sys;

import org.jlab.clara.sys.ccc.ServiceState;

/**
 *  Service system configuration.
 */
class ServiceSysConfig {

    private final ServiceState state;

    private boolean isDataRequest;
    private boolean isDoneRequest;
    private int doneReportThreshold;
    private int dataReportThreshold;

    private int dataRequestCount;
    private int doneRequestCount;

    public ServiceSysConfig(String name, String initialState) {
        state = new ServiceState(name, initialState);
    }

    public void addRequest() {
        dataRequestCount++;
        doneRequestCount++;
    }

    public void resetDoneRequestCount() {
        doneRequestCount = 0;
    }

    public void resetDataRequestCount() {
        dataRequestCount = 0;
    }

    public boolean isDataRequest() {
        return isDataRequest && dataRequestCount >= dataReportThreshold;
    }

    public void setDataRequest(boolean isDataRequest) {
        this.isDataRequest = isDataRequest;
    }

    public boolean isDoneRequest() {
        return isDoneRequest && doneRequestCount >= doneReportThreshold;
    }

    public void setDoneRequest(boolean isDoneRequest) {
        this.isDoneRequest = isDoneRequest;
    }

    public int getDoneReportThreshold() {
        return doneReportThreshold;
    }

    public void setDoneReportThreshold(int doneReportThreshold) {
        this.doneReportThreshold = doneReportThreshold;
    }

    public int getDataReportThreshold() {
        return dataReportThreshold;
    }

    public void setDataReportThreshold(int dataReportThreshold) {
        this.dataReportThreshold = dataReportThreshold;
    }

    public int getDataRequestCount() {
        return dataRequestCount;
    }

    public int getDoneRequestCount() {
        return doneRequestCount;
    }

    public void updateState(String newState) {
        state.setState(newState);
    }
}
