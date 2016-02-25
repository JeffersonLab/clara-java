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

package org.jlab.clara.base;

import org.jlab.clara.base.ClaraRequests.BaseRequest;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class ClaraRequestsTest {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();
    private ClaraBase baseMock;
    private ClaraComponent frontEnd;
    private String topic;
    private TestRequest request;

    @Before
    public void setUp() throws Exception {
        baseMock = mock(ClaraBase.class);
        frontEnd = ClaraComponent.dpe("10.2.9.1_java");
        topic = "dpe:10.2.9.6_java";
        request = spy(new TestRequest(baseMock, frontEnd, topic));
    }


    @Test
    public void requestIsSentToFrontEnd() throws Exception {
        request.run();

        ArgumentCaptor<ClaraComponent> compArg = ArgumentCaptor.forClass(ClaraComponent.class);
        verify(baseMock).send(compArg.capture(), any(xMsgMessage.class));

        assertThat(compArg.getValue().getDpeCanonicalName(), is(frontEnd.getDpeCanonicalName()));
    }


    @Test
    public void requestIsSentWithMessage() throws Exception {
        request.run();

        verify(request).msg();
    }


    @Test
    public void requestThrowsOnSendFailure() throws Exception {
        doThrow(new xMsgException("")).when(baseMock)
                .send(any(ClaraComponent.class), any(xMsgMessage.class));
        expectedEx.expect(ClaraException.class);

        request.run();
    }


    @Test
    public void requestThrowsOnMessageFailure() throws Exception {
        doThrow(new ClaraException("msg")).when(request).msg();
        expectedEx.expect(ClaraException.class);

        request.run();
    }


    @Test
    public void syncRequestIsSentToFrontEnd() throws Exception {
        request.syncRun(10, TimeUnit.SECONDS);

        ArgumentCaptor<ClaraComponent> compArg = ArgumentCaptor.forClass(ClaraComponent.class);
        verify(baseMock).syncSend(compArg.capture(), any(xMsgMessage.class), anyInt());

        assertThat(compArg.getValue().getDpeCanonicalName(), is(frontEnd.getDpeCanonicalName()));
    }


    @Test
    public void syncRequestIsSentWithMessage() throws Exception {
        request.syncRun(10, TimeUnit.SECONDS);

        verify(request).msg();
    }


    @Test
    public void syncRequestIsSentWithTimeoutInMillis() throws Exception {
        request.syncRun(20, TimeUnit.MILLISECONDS);
        verify(baseMock).syncSend(any(ClaraComponent.class), any(xMsgMessage.class), eq(20));
    }


    @Test
    public void syncRequestIsSentWithTimeoutInOtherUnit() throws Exception {
        request.syncRun(10, TimeUnit.SECONDS);
        verify(baseMock).syncSend(any(ClaraComponent.class), any(xMsgMessage.class), eq(10000));
    }


    @Test
    public void syncRequestParsesResponse() throws Exception {
        xMsgMessage response = mock(xMsgMessage.class);
        when(baseMock.syncSend(any(ClaraComponent.class), any(xMsgMessage.class), anyInt()))
              .thenReturn(response);

        request.syncRun(10, TimeUnit.SECONDS);

        verify(request).parseData(eq(response));
    }


    @Test
    public void syncRequestReturnsResponse() throws Exception {
        when(request.parseData(any(xMsgMessage.class))).thenReturn("test_response");

        assertThat(request.syncRun(10, TimeUnit.SECONDS), is("test_response"));
    }


    @Test
    public void syncRequestThrowsOnSendFailure() throws Exception {
        doThrow(new xMsgException("")).when(baseMock)
                .syncSend(any(ClaraComponent.class), any(xMsgMessage.class), anyInt());
        expectedEx.expect(ClaraException.class);

        request.syncRun(10, TimeUnit.SECONDS);
    }


    @Test
    public void syncRequestThrowsOnMessageFailure() throws Exception {
        doThrow(new ClaraException("msg")).when(request).msg();
        expectedEx.expect(ClaraException.class);

        request.syncRun(10, TimeUnit.SECONDS);
    }


    @Test
    public void syncRequestThrowsOnResponseFailure() throws Exception {
        doThrow(new ClaraException("msg")).when(request).parseData(any(xMsgMessage.class));
        expectedEx.expect(ClaraException.class);

        request.syncRun(10, TimeUnit.SECONDS);
    }


    @Test
    public void syncRequestThrowsOnTimeout() throws Exception {
        doThrow(new TimeoutException("")).when(baseMock)
                .syncSend(any(ClaraComponent.class), any(xMsgMessage.class), anyInt());
        expectedEx.expect(TimeoutException.class);

        request.syncRun(10, TimeUnit.SECONDS);
    }



    public static class TestRequest extends BaseRequest<TestRequest, String> {

        TestRequest(ClaraBase base, ClaraComponent frontEnd, String topic) {
            super(base, frontEnd, topic);
        }

        @Override
        protected xMsgMessage msg() throws ClaraException {
            return mock(xMsgMessage.class);
        }

        @Override
        protected String parseData(xMsgMessage msg) throws ClaraException {
            return "";
        }
    }
}
