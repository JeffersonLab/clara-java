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

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.coda.xmsg.data.xMsgD.xMsgData;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * Defines a data type used by a {@link Engine service engine}.
 * Data type can be a predefined type, or a custom-defined type.
 * When declaring a custom type, its serialization routine must be provided.
 */
public class EngineDataType {

    /**
     * Signed int of 32 bits.
     *
     * @see <a href="https://developers.google.com/protocol-buffers/docs/encoding">Wire types</a>
     */
    public static final EngineDataType SINT32 = buildPrimitive(MimeType.SINT32);
    /**
     * Signed int of 64 bits.
     *
     * @see <a href="https://developers.google.com/protocol-buffers/docs/encoding">Wire types</a>
     */
    public static final EngineDataType SINT64 = buildPrimitive(MimeType.SINT64);
    /**
     * Signed fixed integer of 32 bits.
     *
     * @see <a href="https://developers.google.com/protocol-buffers/docs/encoding">Wire types</a>
     */
    public static final EngineDataType SFIXED32 = buildPrimitive(MimeType.SFIXED32);
    /**
     * Signed fixed integer of 64 bits.
     *
     * @see <a href="https://developers.google.com/protocol-buffers/docs/encoding">Wire types</a>
     */
    public static final EngineDataType SFIXED64 = buildPrimitive(MimeType.SFIXED64);
    /**
     * A float (32 bits floating-point number).
     */
    public static final EngineDataType FLOAT = buildPrimitive(MimeType.FLOAT);
    /**
     * A double (64 bits floating-point number).
     */
    public static final EngineDataType DOUBLE = buildPrimitive(MimeType.DOUBLE);
    /**
     * A string.
     */
    public static final EngineDataType STRING = buildPrimitive(MimeType.STRING);
    /**
     * Raw bytes.
     * On Java a {@link ByteBuffer} is used to wrap the byte array and its endianess.
     */
    public static final EngineDataType BYTES = buildRawBytes();
    /**
     * An array of signed varints of 32 bits.
     *
     * @see <a href="https://developers.google.com/protocol-buffers/docs/encoding">Wire types</a>
     */
    public static final EngineDataType ARRAY_SINT32 = buildPrimitive(MimeType.ARRAY_SINT32);
    /**
     * An array of signed varints of 64 bits.
     *
     * @see <a href="https://developers.google.com/protocol-buffers/docs/encoding">Wire types</a>
     */
    public static final EngineDataType ARRAY_SINT64 = buildPrimitive(MimeType.ARRAY_SINT64);
    /**
     * An array of signed fixed integers of 32 bits.
     *
     * @see <a href="https://developers.google.com/protocol-buffers/docs/encoding">Wire types</a>
     */
    public static final EngineDataType ARRAY_SFIXED32 = buildPrimitive(MimeType.ARRAY_SFIXED32);
    /**
     * An array of signed fixed integers of 64 bits.
     *
     * @see <a href="https://developers.google.com/protocol-buffers/docs/encoding">Wire types</a>
     */
    public static final EngineDataType ARRAY_SFIXED64 = buildPrimitive(MimeType.ARRAY_SFIXED64);
    /**
     * An array of floats (32 bits floating-point numbers).
     */
    public static final EngineDataType ARRAY_FLOAT = buildPrimitive(MimeType.ARRAY_FLOAT);
    /**
     * An array of doubles (64 bits floating-point numbers).
     */
    public static final EngineDataType ARRAY_DOUBLE = buildPrimitive(MimeType.ARRAY_DOUBLE);
    /**
     * An array of strings.
     */
    public static final EngineDataType ARRAY_STRING = buildPrimitive(MimeType.ARRAY_STRING);
    /**
     * JSON text.
     */
    public static final EngineDataType JSON = buildJson();
    /**
     * An xMsg native data object.
     */
    public static final EngineDataType NATIVE = buildNative();
    private final String mimeType;
    private final ClaraSerializer serializer;

    /**
     * Creates a new user data type.
     * The data type is identified by its mime-type string.
     * The serializer will be used in order to send data through the network,
     * or to a different language DPE.
     *
     * @param mimeType the name of this data-type
     * @param serializer the custom serializer for this data-type
     */
    public EngineDataType(String mimeType, ClaraSerializer serializer) {
        Objects.requireNonNull(mimeType, "null mime-type");
        Objects.requireNonNull(serializer, "null serializer");
        if (mimeType.isEmpty()) {
            throw new IllegalArgumentException("empty mime-type");
        }
        this.mimeType = mimeType;
        this.serializer = serializer;
    }

    private static EngineDataType buildPrimitive(MimeType mimeType) {
        return new EngineDataType(mimeType.toString(), new PrimitiveSerializer(mimeType));
    }

    private static EngineDataType buildRawBytes() {
        return new EngineDataType(MimeType.BYTES.toString(), new RawBytesSerializer());
    }

    private static EngineDataType buildJson() {
        return new EngineDataType(MimeType.JSON.toString(), new StringSerializer());
    }

    private static EngineDataType buildNative() {
        return new EngineDataType(MimeType.NATIVE.toString(), new NativeSerializer());
    }

    /**
     * Returns the name of this data type.
     */
    public String mimeType() {
        return mimeType;
    }

    /**
     * Returns the serializer of this data type.
     */
    public ClaraSerializer serializer() {
        return serializer;
    }

    @Override
    public String toString() {
        return mimeType;
    }


    // CHECKSTYLE.OFF: MethodParamPad
    private enum MimeType {
        SINT32          ("binary/sint32"),
        SINT64          ("binary/sint64"),
        SFIXED32        ("binary/sfixed32"),
        SFIXED64        ("binary/sfixed64"),
        FLOAT           ("binary/float"),
        DOUBLE          ("binary/double"),
        STRING          ("text/string"),
        BYTES           ("binary/bytes"),

        ARRAY_SINT32    ("binary/array-sint32"),
        ARRAY_SINT64    ("binary/array-sint64"),
        ARRAY_SFIXED32  ("binary/array-sfixed32"),
        ARRAY_SFIXED64  ("binary/array-sfixed32"),
        ARRAY_FLOAT     ("binary/array-float"),
        ARRAY_DOUBLE    ("binary/array-double"),
        ARRAY_STRING    ("binary/array-string"),
        ARRAY_BYTES     ("binary/array-string"),

        JSON            ("application/json"),
        NATIVE          ("native");

        private final String name;

        MimeType(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }
    // CHECKSTYLE.ON: MethodParamPad


    private static class NativeSerializer implements ClaraSerializer {

        @Override
        public ByteBuffer write(Object data) throws ClaraException {
            xMsgData xData = (xMsgData) data;
            return ByteBuffer.wrap(xData.toByteArray());
        }

        @Override
        public Object read(ByteBuffer data) throws ClaraException {
            try {
                return xMsgData.parseFrom(data.array());
            } catch (InvalidProtocolBufferException e) {
                throw new ClaraException(e.getMessage());
            }
        }
    }


    private static class StringSerializer implements ClaraSerializer {

        @Override
        public ByteBuffer write(Object data) throws ClaraException {
            String text = (String) data;
            return ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public Object read(ByteBuffer data) throws ClaraException {
            return new String(data.array(), StandardCharsets.UTF_8);
        }
    }


    private static class RawBytesSerializer implements ClaraSerializer {

        @Override
        public ByteBuffer write(Object data) throws ClaraException {
            return (ByteBuffer) data;
        }

        @Override
        public Object read(ByteBuffer data) throws ClaraException {
            return data;
        }
    }


    private static class PrimitiveSerializer implements ClaraSerializer {

        private final MimeType mimeType;
        private final NativeSerializer nativeSerializer = new NativeSerializer();

        PrimitiveSerializer(MimeType mimeType) {
            this.mimeType = mimeType;
        }

        @Override
        public ByteBuffer write(Object data) throws ClaraException {
            xMsgData.Builder proto = xMsgData.newBuilder();
            switch (mimeType) {
                case SINT32:
                    proto.setVLSINT32((Integer) data);
                    break;
                case SINT64:
                    proto.setVLSINT64((Long) data);
                    break;
                case SFIXED32:
                    proto.setFLSINT32((Integer) data);
                    break;
                case SFIXED64:
                    proto.setFLSINT64((Long) data);
                    break;
                case DOUBLE:
                    proto.setDOUBLE((Double) data);
                    break;
                case FLOAT:
                    proto.setFLOAT((Float) data);
                    break;
                case STRING:
                    proto.setSTRING((String) data);
                    break;
                case BYTES:
                    proto.setBYTES((ByteString) data);
                    break;

                case ARRAY_SINT32: {
                    Integer[] a = (Integer[]) data;
                    proto.addAllVLSINT32A(Arrays.asList(a));
                    break;
                }
                case ARRAY_SINT64: {
                    Long[] a = (Long[]) data;
                    proto.addAllVLSINT64A(Arrays.asList(a));
                    break;
                }
                case ARRAY_SFIXED32: {
                    Integer[] a = (Integer[]) data;
                    proto.addAllFLSINT32A(Arrays.asList(a));
                    break;
                }
                case ARRAY_SFIXED64: {
                    Long[] a = (Long[]) data;
                    proto.addAllFLSINT64A(Arrays.asList(a));
                    break;
                }
                case ARRAY_DOUBLE: {
                    Double[] a = (Double[]) data;
                    proto.addAllDOUBLEA(Arrays.asList(a));
                    break;
                }
                case ARRAY_FLOAT: {
                    Float[] a = (Float[]) data;
                    proto.addAllFLOATA(Arrays.asList(a));
                    break;
                }
                case ARRAY_STRING: {
                    String[] a = (String[]) data;
                    proto.addAllSTRINGA(Arrays.asList(a));
                    break;
                }
                default:
                    throw new IllegalStateException("Invalid mime-type: " + mimeType.toString());
            }
            return nativeSerializer.write(proto.build());
        }

        @Override
        public Object read(ByteBuffer data) throws ClaraException {
            xMsgData proto = (xMsgData) nativeSerializer.read(data);
            switch (mimeType) {
                case SINT32:
                    return proto.getVLSINT32();
                case SINT64:
                    return proto.getVLSINT64();
                case SFIXED32:
                    return proto.getFLSINT32();
                case SFIXED64:
                    return proto.getFLSINT64();
                case DOUBLE:
                    return proto.getDOUBLE();
                case FLOAT:
                    return proto.getFLOAT();
                case STRING:
                    return proto.getSTRING();
                case BYTES:
                    return proto.getBYTES();

                case ARRAY_SINT32: {
                    Integer[] a = new Integer[proto.getVLSINT32ACount()];
                    for (int i = 0; i < a.length; i++) {
                        a[i] = proto.getVLSINT32A(i);
                    }
                    return a;
                }
                case ARRAY_SINT64: {
                    Long[] a = new Long[proto.getVLSINT64ACount()];
                    for (int i = 0; i < a.length; i++) {
                        a[i] = proto.getVLSINT64A(i);
                    }
                    return a;
                }
                case ARRAY_SFIXED32: {
                    Integer[] a = new Integer[proto.getFLSINT32ACount()];
                    for (int i = 0; i < a.length; i++) {
                        a[i] = proto.getFLSINT32A(i);
                    }
                    return a;
                }
                case ARRAY_SFIXED64: {
                    Long[] a = new Long[proto.getFLSINT64ACount()];
                    for (int i = 0; i < a.length; i++) {
                        a[i] = proto.getFLSINT64A(i);
                    }
                    return a;
                }
                case ARRAY_DOUBLE: {
                    Double[] a = new Double[proto.getDOUBLEACount()];
                    for (int i = 0; i < a.length; i++) {
                        a[i] = proto.getDOUBLEA(i);
                    }
                    return a;
                }
                case ARRAY_FLOAT: {
                    Float[] a = new Float[proto.getFLOATACount()];
                    for (int i = 0; i < a.length; i++) {
                        a[i] = proto.getFLOATA(i);
                    }
                    return a;
                }
                case ARRAY_STRING: {
                    String[] a = new String[proto.getSTRINGACount()];
                    for (int i = 0; i < a.length; i++) {
                        a[i] = proto.getSTRINGA(i);
                    }
                    return a;
                }
                default:
                    throw new IllegalStateException("Invalid mime-type: " + mimeType.toString());
            }
        }
    }
}
