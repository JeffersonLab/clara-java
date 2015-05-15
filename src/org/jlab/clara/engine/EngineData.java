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
    private String dataType;
    private String dataDescription = xMsgConstants.UNDEFINED.getStringValue();
    private String dataVersion = xMsgConstants.UNDEFINED.getStringValue();

    private int statusSeverityId;
    private String statusText = xMsgConstants.UNDEFINED.getStringValue();

    private String state = xMsgConstants.UNDEFINED.getStringValue();
    private int id;

    public EngineData(xMsgMeta.Builder xMeta, Object data){
        this.metaData = xMeta;
        if(metaData.getDataType().equals(xMsgMeta.DataType.X_Object)) {
            xData = (xMsgData.Builder) data;
            dataType = xData.getType().name();
        } else if(metaData.getDataType().equals(xMsgMeta.DataType.J_Object)){
            dataType = data.getClass().getSimpleName();
        } else {
            this.data = data;
            dataType = xMsgMeta.DataType.getDescriptor().getName();
        }
    }

    public String getDataType(){
        return dataType;
     }

    public Object getData(){
        return data;
     }


}
