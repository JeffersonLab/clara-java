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

import com.google.protobuf.ByteString;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.data.xMsgD.xMsgData;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;

import java.util.Arrays;
import java.util.List;

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
    private String dataDescription = xMsgConstants.UNDEFINED.getStringValue();
    private String dataVersion = xMsgConstants.UNDEFINED.getStringValue();

    private int statusSeverityId;
    private EStatus status;

    private String state = xMsgConstants.UNDEFINED.getStringValue();
    private int id;

    public EngineData(xMsgMeta.Builder xMeta, Object data){
        this.metaData = xMeta;
        dataDescription = metaData.getDescription();
        dataVersion = metaData.getVersion();
        statusSeverityId = metaData.getSeverityId();
        status = Enum.valueOf(EStatus.class, metaData.getStatus().name());
        state = metaData.getSenderState();
        id = metaData.getCommunicationId();

        if(metaData.getDataType().equals(xMsgMeta.DataType.X_Object)) {
            xData = (xMsgData.Builder) data;

            dataType = Enum.valueOf(EDataType.class, xData.getType().name());
        } else {
            this.data = data;
            dataType = Enum.valueOf(EDataType.class, metaData.getDataType().name());
        }
    }

    public void newData(EDataType dataType, Object data) {
        switch (dataType) {
            case UNDEFINED:
                metaData.setDataType(xMsgMeta.DataType.UNDEFINED);
                this.data = data;
                break;

            case J_Object:
                metaData.setDataType(xMsgMeta.DataType.J_Object);
                this.data = data;
                break;

            case C_Object:
                metaData.setDataType(xMsgMeta.DataType.C_Object);
                this.data = data;
                break;

            case P_Object:
                metaData.setDataType(xMsgMeta.DataType.P_Object);
                this.data = data;
                break;

            case NCDFS_Object:
                metaData.setDataType(xMsgMeta.DataType.NCDFS_Object);
                this.data = data;
                break;

            case T_VLSINT32:
                metaData.setDataType(xMsgMeta.DataType.X_Object);
                xData.setType(xMsgData.Type.T_VLSINT32);
                xData.setVLSINT32((Integer) data);
                break;

            case T_VLSINT64:
                metaData.setDataType(xMsgMeta.DataType.X_Object);
                xData.setType(xMsgData.Type.T_VLSINT64);
                xData.setVLSINT64((Long) data);
                break;

            case T_FLSINT32:
                metaData.setDataType(xMsgMeta.DataType.X_Object);
                xData.setType(xMsgData.Type.T_FLSINT32);
                xData.setFLSINT32((Integer) data);
                break;

            case T_FLSINT64:
                metaData.setDataType(xMsgMeta.DataType.X_Object);
                xData.setType(xMsgData.Type.T_FLSINT64);
                xData.setFLSINT64((Long) data);
                break;

            case T_FLOAT:
                metaData.setDataType(xMsgMeta.DataType.X_Object);
                xData.setType(xMsgData.Type.T_FLOAT);
                xData.setFLOAT((Float) data);
                break;

            case T_DOUBLE:
                metaData.setDataType(xMsgMeta.DataType.X_Object);
                xData.setType(xMsgData.Type.T_DOUBLE);
                xData.setDOUBLE((Double) data);
                break;

            case T_STRING:
                metaData.setDataType(xMsgMeta.DataType.X_Object);
                xData.setType(xMsgData.Type.T_STRING);
                xData.setSTRING((String) data);
                break;

            case T_BYTES:
                metaData.setDataType(xMsgMeta.DataType.X_Object);
                xData.setType(xMsgData.Type.T_BYTES);
                if (data instanceof ByteString) {
                    xData.setBYTES((ByteString) data);
                } else {
                    xData.setBYTES(ByteString.copyFrom((byte[]) data));
                }
                break;

            case T_VLSINT32A:
                metaData.setDataType(xMsgMeta.DataType.X_Object);
                xData.setType(xMsgData.Type.T_VLSINT32A);
                if (data instanceof List) {
                    xData.addAllVLSINT32A((List<Integer>) data);
                } else {
                    xData.addAllVLSINT32A(Arrays.asList((Integer[]) data));
                }
                break;

            case T_VLSINT64A:
                metaData.setDataType(xMsgMeta.DataType.X_Object);
                xData.setType(xMsgData.Type.T_VLSINT64A);
                if (data instanceof List) {
                    xData.addAllVLSINT64A((List<Long>) data);
                } else {
                    xData.addAllVLSINT64A(Arrays.asList((Long[]) data));
                }
                break;

            case T_FLSINT32A:
                metaData.setDataType(xMsgMeta.DataType.X_Object);
                xData.setType(xMsgData.Type.T_FLSINT32A);
                if (data instanceof List) {
                    xData.addAllFLSINT32A((List<Integer>) data);
                } else {
                    xData.addAllFLSINT32A(Arrays.asList((Integer[]) data));
                }
                break;

            case T_FLSINT64A:
                metaData.setDataType(xMsgMeta.DataType.X_Object);
                xData.setType(xMsgData.Type.T_FLSINT64A);
                if (data instanceof List) {
                    xData.addAllFLSINT64A((List<Long>) data);
                } else {
                    xData.addAllFLSINT64A(Arrays.asList((Long[]) data));
                }
                break;

            case T_FLOATA:
                metaData.setDataType(xMsgMeta.DataType.X_Object);
                xData.setType(xMsgData.Type.T_FLOATA);
                if (data instanceof List) {
                    xData.addAllFLOATA((List<Float>) data);
                } else {
                    xData.addAllFLOATA(Arrays.asList((Float[]) data));
                }
                break;

            case T_DOUBLEA:
                metaData.setDataType(xMsgMeta.DataType.X_Object);
                xData.setType(xMsgData.Type.T_DOUBLEA);
                if (data instanceof List) {
                    xData.addAllDOUBLEA((List<Double>) data);
                } else {
                    xData.addAllDOUBLEA(Arrays.asList((Double[]) data));
                }
                break;

            case T_STRINGA:
                metaData.setDataType(xMsgMeta.DataType.X_Object);
                xData.setType(xMsgData.Type.T_STRINGA);
                if (data instanceof List) {
                    xData.addAllSTRINGA((List<String>) data);
                } else {
                    xData.addAllSTRINGA(Arrays.asList((String[]) data));
                }
                break;

        }
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

    public Integer getInt() {
        if (metaData.getDataType().equals(xMsgMeta.DataType.X_Object) && xData != null) {
            switch (dataType) {
                case T_VLSINT32:
                    return xData.getVLSINT32();
                case T_FLSINT32:
                    return xData.getFLSINT32();
            }
        }
        return null;
    }

    public Long getLong() {
        if (metaData.getDataType().equals(xMsgMeta.DataType.X_Object) && xData != null) {
            switch (dataType) {
                case T_VLSINT64:
                    return xData.getVLSINT64();
                case T_FLSINT64:
                    return xData.getFLSINT64();
            }
        }
        return null;
    }

    public Float getFloat() {
        if (metaData.getDataType().equals(xMsgMeta.DataType.X_Object) && xData != null) {
            switch (dataType) {
                case T_FLOAT:
                    return xData.getFLOAT();
            }
        }
        return null;
    }

    public Double getDouble() {
        if (metaData.getDataType().equals(xMsgMeta.DataType.X_Object) && xData != null) {
            switch (dataType) {
                case T_DOUBLE:
                    return xData.getDOUBLE();
            }
        }
        return null;
    }

    public String getString() {
        if (metaData.getDataType().equals(xMsgMeta.DataType.X_Object) && xData != null) {
            switch (dataType) {
                case T_STRING:
                    return xData.getSTRING();
            }
        }
        return null;
    }

    public Integer[] getIntArray() {
        if (metaData.getDataType().equals(xMsgMeta.DataType.X_Object) && xData != null) {
            switch (dataType) {
                case T_VLSINT32A:
                    return xData.getVLSINT32AList().toArray(new Integer[xData.getVLSINT32AList().size()]);
                case T_FLSINT32A:
                    return xData.getFLSINT32AList().toArray(new Integer[xData.getFLSINT32AList().size()]);
            }
        }
        return null;
    }

    public Long[] getLongArray() {
        if (metaData.getDataType().equals(xMsgMeta.DataType.X_Object) && xData != null) {
            switch (dataType) {
                case T_VLSINT64A:
                    return xData.getVLSINT64AList().toArray(new Long[xData.getVLSINT64AList().size()]);
                case T_FLSINT64A:
                    return xData.getFLSINT64AList().toArray(new Long[xData.getFLSINT64AList().size()]);
            }
        }
        return null;
    }

    public Float[] getFloatArray() {
        if (metaData.getDataType().equals(xMsgMeta.DataType.X_Object) && xData != null) {
            switch (dataType) {
                case T_FLOATA:
                    return xData.getFLOATAList().toArray(new Float[xData.getFLOATAList().size()]);
            }
        }
        return null;
    }

    public Double[] getDoubleArray() {
        if (metaData.getDataType().equals(xMsgMeta.DataType.X_Object) && xData != null) {
            switch (dataType) {
                case T_DOUBLEA:
                    return xData.getDOUBLEAList().toArray(new Double[xData.getDOUBLEAList().size()]);
            }
        }
        return null;
    }

    public byte[] getByteArray() {
        if (metaData.getDataType().equals(xMsgMeta.DataType.X_Object) && xData != null) {
            switch (dataType) {
                case T_BYTES:
                    return xData.getBYTES().toByteArray();
            }
        }
        return null;
    }


    /**
     * The following methods do not copy
     */

    public ByteString getByteString() {
        if (metaData.getDataType().equals(xMsgMeta.DataType.X_Object) && xData != null) {
            switch (dataType) {
                case T_BYTES:
                    return xData.getBYTES();
            }
        }
        return null;
    }

    public List<ByteString> getByteStringList() {
        if (metaData.getDataType().equals(xMsgMeta.DataType.X_Object) && xData != null) {
            switch (dataType) {
                case T_BYTES:
                    return xData.getBYTESAList();
            }
        }
        return null;
    }

    public List<Integer> getIntList() {
        if (metaData.getDataType().equals(xMsgMeta.DataType.X_Object) && xData != null) {
            switch (dataType) {
                case T_VLSINT32A:
                    return xData.getVLSINT32AList();
                case T_FLSINT32A:
                    return xData.getFLSINT32AList();
            }
        }
        return null;
    }

    public List<Long> getLongList() {
        if (metaData.getDataType().equals(xMsgMeta.DataType.X_Object) && xData != null) {
            switch (dataType) {
                case T_VLSINT64A:
                    return xData.getVLSINT64AList();
                case T_FLSINT64A:
                    return xData.getFLSINT64AList();
            }
        }
        return null;
    }

    public List<Float> getFloatList() {
        if (metaData.getDataType().equals(xMsgMeta.DataType.X_Object) && xData != null) {
            switch (dataType) {
                case T_FLOATA:
                    return xData.getFLOATAList();
            }
        }
        return null;
    }

    public List<Double> getDoubleList() {
        if (metaData.getDataType().equals(xMsgMeta.DataType.X_Object) && xData != null) {
            switch (dataType) {
                case T_DOUBLEA:
                    return xData.getDOUBLEAList();
            }
        }
        return null;
    }

    public xMsgMeta.Builder getMetaData() {
        return metaData;
    }

    public xMsgData.Builder getxData() {
        return xData;
    }
}
