package org.jlab.clara.util;

import org.jlab.coda.xmsg.data.xMsgD;

/**
 * Engine data passed in/out to the service engine.
 *
 * @author gurjyan
 * @version 1.x
 * @since 2/27/15
 */
public class EngineData {
    private Object data;
    private CDataType dataType;
    private String dataDescription;
    private String dataVersion;

    private xMsgD.Data.Severity status;
    private int statusSeverityId;
    private String statusText;

    private String state;
    private int id;

    public EngineData(Object dataObject, xMsgD.Data.Builder xData){
        this.data = dataObject;
        this.dataDescription = xData.getDataDescription();
        this.dataVersion = xData.getDataVersion();
        this.id = xData.getId();

        switch (xData.getDataType()) {
            case T_VLSINT32:
                dataType = CDataType.T_VLSINT32;
                break;
            case T_VLSINT64:
                dataType = CDataType.T_VLSINT64;
                break;
            case T_FLSINT32:
                dataType = CDataType.T_FLSINT32;
                break;
            case T_FLSINT64:
                dataType = CDataType.T_FLSINT64;
                break;
            case T_FLOAT:
                dataType = CDataType.T_FLOAT;
                break;
            case T_DOUBLE:
                dataType = CDataType.T_DOUBLE;
                break;
            case T_STRING:
                dataType = CDataType.T_STRING;
                break;
            case T_BYTES:
                switch (xData.getByteArrayType()) {
                    case JOBJECT:
                        dataType = CDataType.JOBJECT;
                        break;
                    case NETCDF:
                        dataType = CDataType.NETCDF;
                        break;
                    case HDF:
                        dataType = CDataType.HDF;
                        break;
                    case EVIO:
                        dataType = CDataType.EVIO;
                        break;
                }
                break;
            case T_VLSINT32A:
                dataType = CDataType.T_VLSINT32A;
                break;
            case T_VLSINT64A:
                dataType = CDataType.T_VLSINT64A;
                break;
            case T_FLSINT32A:
                dataType = CDataType.T_FLSINT32A;
                break;
            case T_FLSINT64A:
                dataType = CDataType.T_FLSINT64A;
                break;
            case T_FLOATA:
                dataType = CDataType.T_FLOATA;
                break;
            case T_DOUBLEA:
                dataType = CDataType.T_DOUBLEA;
                break;
            case T_STRINGA:
                dataType = CDataType.T_STRINGA;
                break;
            case T_EXTERNAL_OBJECT:
                switch (xData.getByteArrayType()) {
                    case JOBJECT:
                        dataType = CDataType.JOBJECT;
                        break;
                    case NETCDF:
                        dataType = CDataType.NETCDF;
                        break;
                    case HDF:
                        dataType = CDataType.HDF;
                        break;
                    case EVIO:
                        dataType = CDataType.EVIO;
                        break;
                }
                break;
        }
    }

    public int getId() {
        return id;
    }

    public xMsgD.Data.Severity getStatus() {
        return status;
    }

    public int getStatusSeverityId() {
        return statusSeverityId;
    }

    public String getStatusText() {
        return statusText;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public CDataType getDataType() {
        return dataType;
    }

    public void setDataType(CDataType dataType) {
        this.dataType = dataType;
    }

    public String getDataDescription() {
        return dataDescription;
    }

    public void setDataDescription(String dataDescription) {
        this.dataDescription = dataDescription;
    }

    public String getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(String dataVersion) {
        this.dataVersion = dataVersion;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public void reportStatus(xMsgD.Data.Severity status,
                             int severityId,
                             String text){
        this.status = status;
        this.statusSeverityId = severityId;
        this.statusText = text;
    }
}
