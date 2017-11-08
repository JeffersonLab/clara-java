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

package org.jlab.clara.sys;

import org.jlab.clara.sys.ccc.ServiceState;

/**
 *  Service system configuration.
 */
class ServiceSysConfig {

    private final ServiceState state;

    private boolean isDataRequest;
    private boolean isDoneRequest;
    private boolean isRingRequest;

    private int doneReportThreshold;
    private int dataReportThreshold;

    private int dataRequestCount;
    private int doneRequestCount;

    ServiceSysConfig(String name, String initialState) {
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

    public boolean isRingRequest() {
        return isRingRequest;
    }

    public void setRingRequest(boolean isRingRequest) {
        this.isRingRequest = isRingRequest;
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
