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

import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.data.xMsgD.xMsgData;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;

/**
 * Engine data passed in/out to the service engine.
 *
 * @author gurjyan
 * @version 1.x
 * @since 2/27/15
 */
public class EngineData {

    private xMsgMeta.Builder metaData;

    private xMsgData.Builder xData;
    private Object data;
    private EDataType dataType;
    private String dataDescription = xMsgConstants.UNDEFINED.toString();
    private String dataVersion = xMsgConstants.UNDEFINED.toString();

    private int statusSeverityId;
    private EStatus status;

    private String state = xMsgConstants.UNDEFINED.toString();
    private int id;

    public EngineData(xMsgMeta.Builder xMeta, Object data){
        this.metaData = xMeta;
        dataDescription = metaData.getDescription();
        dataVersion = metaData.getVersion();
        statusSeverityId = metaData.getSeverityId();
        status = Enum.valueOf(EStatus.class, metaData.getStatus().name());
        state = metaData.getSenderState();
        id = metaData.getCommunicationId();

        if(metaData.getDataType().equals("binary/native")) {
            xData = (xMsgData.Builder) data;

            dataType = Enum.valueOf(EDataType.class, xData.getType().name());
        } else {
            this.data = data;
            dataType = Enum.valueOf(EDataType.class, metaData.getDataType());
        }
    }

    public void newData(EDataType dataType, Object data) {
        this.data = data;
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

    public int getStatusSeverityId() {
        return statusSeverityId;
    }

    public void setStatusSeverityId(int statusSeverityId) {
        this.statusSeverityId = statusSeverityId;
    }

    public EStatus getStatus() {
        return status;
    }

    public void setStatus(EStatus status) {
        this.status = status;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public int getId() {
        return id;
    }

    public EDataType getDataType() {
        return dataType;
     }

    public Object getData(){
        return data;
     }

    public xMsgMeta.Builder getMetaData() {
        return metaData;
    }

    public xMsgData.Builder getxData() {
        return xData;
    }
}
