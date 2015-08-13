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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.array;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

import org.jlab.coda.xmsg.data.xMsgD.xMsgData;
import org.junit.Test;


public class EngineDataTypeTest {

    @Test
    public void testIntegerSerializer() throws Exception {
        EngineDataType dt = EngineDataType.SINT32;
        ClaraSerializer s = dt.serializer();

        ByteBuffer b = s.write(18);
        Integer d = (Integer) s.read(b);

        assertThat(d, is(18));
    }

    @Test
    public void testFloatingPointSerializer() throws Exception {
        EngineDataType dt = EngineDataType.DOUBLE;
        ClaraSerializer s = dt.serializer();

        ByteBuffer b = s.write(78.98);
        Double d = (Double) s.read(b);

        assertThat(d, is(closeTo(78.99, 0.01)));
    }

    @Test
    public void testStringSerializer() throws Exception {
        EngineDataType dt = EngineDataType.STRING;
        ClaraSerializer s = dt.serializer();

        ByteBuffer b = s.write("master of puppets");
        String d = (String) s.read(b);

        assertThat(d, is("master of puppets"));
    }

    @Test
    public void testIntegerArraySerializer() throws Exception {
        EngineDataType dt = EngineDataType.ARRAY_SINT32;
        ClaraSerializer s = dt.serializer();

        Integer[] v = new Integer[] { 4, 5, 6 };
        ByteBuffer b = s.write(v);
        Integer[] d = (Integer[]) s.read(b);

        assertThat(d, is(v));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testFloatingPointArraySerializer() throws Exception {
        EngineDataType dt = EngineDataType.ARRAY_DOUBLE;
        ClaraSerializer s = dt.serializer();

        Double[] v = new Double[] { 4.1, 5.6 };
        ByteBuffer b = s.write(v);
        Double[] d = (Double[]) s.read(b);

        assertThat(d, is(array(closeTo(4.1, 0.01), closeTo(5.6, 0.1))));
    }

    @Test
    public void testStringArraySerializer() throws Exception {
        EngineDataType dt = EngineDataType.ARRAY_STRING;
        ClaraSerializer s = dt.serializer();

        String[] v = new String[] { "Ride the Lightning",
                                    "Master of Puppets", "...And Justice for All"};
        ByteBuffer b = s.write(v);
        String[] d = (String[]) s.read(b);

        assertThat(d, is(v));
    }

    @Test
    public void testNativeSerializer() throws Exception {
        xMsgData.Builder builder = xMsgData.newBuilder();

        builder.setFLSINT32(56);
        builder.setDOUBLE(5.6);
        builder.addSTRINGA("Ride the Lightning");
        builder.addSTRINGA("Master of Puppets");
        builder.addSTRINGA("...And Justice for All");

        EngineDataType dt = EngineDataType.NATIVE;
        ClaraSerializer s = dt.serializer();

        ByteBuffer b = s.write(builder.build());
        xMsgData d = (xMsgData) s.read(b);

        assertThat(d, is(builder.build()));
    }


    @Test
    public void testRawBytesSerializer() throws Exception {

        byte[] r = new byte[100];
        new Random().nextBytes(r);

        ByteBuffer bb = ByteBuffer.wrap(r);
        bb.order(ByteOrder.LITTLE_ENDIAN);

        EngineDataType dt = EngineDataType.BYTES;
        ClaraSerializer s = dt.serializer();

        ByteBuffer b = s.write(bb);
        ByteBuffer d = (ByteBuffer) s.read(b);

        assertThat(d, is(sameInstance(bb)));
        assertThat(d.order(), is(ByteOrder.LITTLE_ENDIAN));
    }
}
