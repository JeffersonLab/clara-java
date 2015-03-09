package org.jlab.clara.util;

/**
 * <p>
 *     Service system configuration class
 * </p>
 *
 * @author gurjyan
 * @version 1.x
 * @since 3/9/15
 */
public class CServiceSysConfig {
    private boolean isDataRequest;
    private boolean isDoneRequest;
    private int doneReportThreshold;
    private int dataReportThreshold;

    private int dataRequestCount;
    private int doneRequestCount;

    public void addRequest(){
        dataRequestCount++;
        doneRequestCount++;
    }

    public void resetDoneRequestCount(){
        doneRequestCount = 0;
    }

    public void resetDataRequestCount(){
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
}
