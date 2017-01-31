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

package org.jlab.clara.base.core;

import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.engine.EngineStatus;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Set;

public final class DataUtil {

    private static final EngineDataAccessor DATA_ACCESSOR = EngineDataAccessor.getDefault();

    private DataUtil() { }

    public static EngineData buildErrorData(String msg, int severity, Throwable exception) {
        EngineData outData = new EngineData();
        outData.setData(EngineDataType.STRING.mimeType(), msg);
        outData.setDescription(ClaraUtil.reportException(exception));
        outData.setStatus(EngineStatus.ERROR, severity);
        return outData;
    }

    /**
     * Convoluted way to access the internal EngineData metadata,
     * which is hidden to users.
     *
     * @param data {@link org.jlab.clara.engine.EngineData} object
     * @return {@link org.jlab.coda.xmsg.data.xMsgM.xMsgMeta.Builder} object
     */
    public static xMsgMeta.Builder getMetadata(EngineData data) {
        return DATA_ACCESSOR.getMetadata(data);
    }

    /**
     * Builds a message by serializing passed data object using serialization
     * routine defined in one of the data types objects.
     *
     * @param topic     the topic where the data will be published
     * @param data      the data to be serialized
     * @param dataTypes the set of registered data types
     * @throws ClaraException if the data could not be serialized
     */
    public static xMsgMessage serialize(xMsgTopic topic,
                                        EngineData data,
                                        Set<EngineDataType> dataTypes)
            throws ClaraException {

        xMsgMeta.Builder metadata = DATA_ACCESSOR.getMetadata(data);
        String mimeType = metadata.getDataType();
        for (EngineDataType dt : dataTypes) {
            if (dt.mimeType().equals(mimeType)) {
                try {
                    ByteBuffer bb = dt.serializer().write(data.getData());
                    if (bb.order() == ByteOrder.BIG_ENDIAN) {
                        metadata.setByteOrder(xMsgMeta.Endian.Big);
                    } else {
                        metadata.setByteOrder(xMsgMeta.Endian.Little);
                    }
                    return new xMsgMessage(topic, metadata, bb.array());
                } catch (ClaraException e) {
                    throw new ClaraException("Could not serialize " + mimeType, e);
                }
            }
        }
        if (mimeType.equals(EngineDataType.STRING.mimeType())) {
            ByteBuffer bb = EngineDataType.STRING.serializer().write(data.getData());
            return new xMsgMessage(topic, metadata, bb.array());
        }
        throw new ClaraException("Unsupported mime-type = " + mimeType);
    }

    /**
     * De-serializes data of the message {@link org.jlab.coda.xmsg.core.xMsgMessage},
     * represented as a byte[] into an object of az type defined using the mimeType/dataType
     * of the meta-data (also as a part of the xMsgMessage). Second argument is used to
     * pass the serialization routine as a method of the
     * {@link org.jlab.clara.engine.EngineDataType} object.
     *
     * @param msg {@link org.jlab.coda.xmsg.core.xMsgMessage} object
     * @param dataTypes set of {@link org.jlab.clara.engine.EngineDataType} objects
     * @return {@link org.jlab.clara.engine.EngineData} object containing de-serialized data object
     *          and metadata
     * @throws ClaraException
     */
    public static EngineData deserialize(xMsgMessage msg, Set<EngineDataType> dataTypes)
            throws ClaraException {
        xMsgMeta.Builder metadata = msg.getMetaData();
        String mimeType = metadata.getDataType();
        for (EngineDataType dt : dataTypes) {
            if (dt.mimeType().equals(mimeType)) {
                try {
                    ByteBuffer bb = ByteBuffer.wrap(msg.getData());
                    if (metadata.getByteOrder() == xMsgMeta.Endian.Little) {
                        bb.order(ByteOrder.LITTLE_ENDIAN);
                    }
                    Object userData = dt.serializer().read(bb);
                    return DATA_ACCESSOR.build(userData, metadata);
                } catch (ClaraException e) {
                    throw new ClaraException("CLARA-Error: Could not deserialize " + mimeType, e);
                }
            }
        }
        throw new ClaraException("CLARA-Error: Unsupported mime-type = " + mimeType);
    }


    public abstract static class EngineDataAccessor {

        private static volatile EngineDataAccessor defaultAccessor;

        public static EngineDataAccessor getDefault() {
            new EngineData(); // Load the accessor
            EngineDataAccessor a = defaultAccessor;
            if (a == null) {
                throw new IllegalStateException("EngineDataAccessor should not be null");
            }
            return a;
        }

        public static void setDefault(EngineDataAccessor accessor) {
            if (defaultAccessor != null) {
                throw new IllegalStateException("EngineDataAccessor should be null");
            }
            defaultAccessor = accessor;
        }

        protected abstract xMsgMeta.Builder getMetadata(EngineData data);

        protected abstract EngineData build(Object data, xMsgMeta.Builder metadata);
    }
}
