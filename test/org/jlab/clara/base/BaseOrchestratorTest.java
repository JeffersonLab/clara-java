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

package org.jlab.clara.base;

import java.net.SocketException;
import java.util.concurrent.TimeoutException;

import javax.annotation.ParametersAreNonnullByDefault;

import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.sys.CBase;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.data.xMsgD.xMsgData;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class BaseOrchestratorTest {

    private CBase baseMock;
    private BaseOrchestrator orchestrator;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        baseMock = mock(CBase.class);
        orchestrator = new OrchestratorMock();
    }


    @Test
    public void exitDpeSendsRequest() throws Exception {
        orchestrator.exitDpe("10.2.9.96_java");
        assertSendCall("10.2.9.96", "dpe:10.2.9.96_java", "dpeExit");
    }


    @Test
    public void exitDpeThrowsOnFailure() throws Exception {
        expectClaraExceptionOnSend();
        orchestrator.exitDpe("10.2.9.96_java");
    }


    @Test
    public void exitDpeThrowsOnBadDpeName() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        orchestrator.exitDpe("10.2.9.96_java:master");
    }



    @Test
    public void deployContainerSendsRequest() throws Exception {
        orchestrator.deployContainer("10.2.9.96_java:master");

        assertSendCall("10.2.9.96", "dpe:10.2.9.96_java", "startContainer?master");
    }


    @Test
    public void deployContainerThrowsOnFailure() throws Exception {
        expectClaraExceptionOnSend();
        orchestrator.deployContainer("10.2.9.96_java:master");
    }


    @Test
    public void deployContainerThrowsOnBadContainerName() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        orchestrator.deployContainer("10.2.9.96_java");
    }



    @Test
    public void deployContainerSyncSendsRequest() throws Exception {
        orchestrator.deployContainerSync("10.2.9.96_java:master", 10);

        assertSyncSendCall("10.2.9.96", "dpe:10.2.9.96_java", "startContainer?master", 10);
    }


    @Test
    public void deployContainerSyncThrowsOnFailure() throws Exception {
        expectClaraExceptionOnSyncSend();
        orchestrator.deployContainerSync("10.2.9.96_java:master", 10);
    }


    @Test
    public void deployContainerSyncThrowsOnTimeout() throws Exception {
        expectTimeoutExceptionOnSyncSend();
        orchestrator.deployContainerSync("10.2.9.96_java:master", 10);
    }


    @Test
    public void deployContainerSyncThrowsOnBadContainerName() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        orchestrator.deployContainerSync("10.2.9.96_java", 10);
    }



    @Test
    public void removeContainerSendsRequest() throws Exception {
        orchestrator.removeContainer("10.2.9.96_java:master");

        assertSendCall("10.2.9.96", "dpe:10.2.9.96_java", "removeContainer?master");
    }


    @Test
    public void removeContainerThrowsOnFailure() throws Exception {
        expectClaraExceptionOnSend();
        orchestrator.removeContainer("10.2.9.96_java:master");
    }


    @Test
    public void removeContainerThrowsOnBadContainerName() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        orchestrator.removeContainer("10.2.9.96_java");
    }



    @Test
    public void removeContainerSyncSendsRequest() throws Exception {
        orchestrator.removeContainerSync("10.2.9.96_java:master", 10);

        assertSyncSendCall("10.2.9.96", "dpe:10.2.9.96_java", "removeContainer?master", 10);
    }


    @Test
    public void removeContainerSyncThrowsOnFailure() throws Exception {
        expectClaraExceptionOnSyncSend();
        orchestrator.removeContainerSync("10.2.9.96_java:master", 10);
    }


    @Test
    public void removeContainerSyncThrowsOnTimeout() throws Exception {
        expectTimeoutExceptionOnSyncSend();
        orchestrator.removeContainerSync("10.2.9.96_java:master", 10);
    }


    @Test
    public void removeContainerSyncThrowsOnBadContainerName() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        orchestrator.removeContainer("10.2.9.96_java");
    }



    @Test
    public void deployServiceSendsRequest() throws Exception {
        orchestrator.deployService("10.2.9.96_java:master:E1", 10);

        assertSendCall("10.2.9.96", "container:10.2.9.96_java:master",
                "deployService?E1?10");
    }


    @Test
    public void deployServiceThrowsOnFailure() throws Exception {
        expectClaraExceptionOnSend();
        orchestrator.deployService("10.2.9.96_java:master:E1", 10);
    }


    @Test
    public void deployServiceThrowsOnBadServiceName() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        orchestrator.deployService("10.2.9.96_java::E1", 10);
    }



    @Test
    public void deployServiceSyncSendsRequest() throws Exception {
        orchestrator.deployServiceSync("10.2.9.96_java:master:E1", 8, 30);

        assertSyncSendCall("10.2.9.96", "container:10.2.9.96_java:master",
                "deployService?E1?8", 30);
    }


    @Test
    public void deployServiceSyncThrowsOnFailure() throws Exception {
        expectClaraExceptionOnSyncSend();
        orchestrator.deployServiceSync("10.2.9.96_java:master:E1", 8, 30);
    }


    @Test
    public void deployServiceSyncThrowsOnTimeout() throws Exception {
        expectTimeoutExceptionOnSyncSend();
        orchestrator.deployServiceSync("10.2.9.96_java:master:E1", 8, 30);
    }


    @Test
    public void deployServiceSyncThrowsOnBadServiceName() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        orchestrator.deployServiceSync("10.2.9.96_java::E1", 10, 30);
    }



    @Test
    public void removeServiceSendsRequest() throws Exception {
        orchestrator.removeService("10.2.9.96_java:master:E1");

        assertSendCall("10.2.9.96", "container:10.2.9.96_java:master", "removeService?E1");
    }


    @Test
    public void removeServiceThrowsOnFailure() throws Exception {
        expectClaraExceptionOnSend();
        orchestrator.removeService("10.2.9.96_java:master:E1");
    }


    @Test
    public void removeServiceThrowsOnBadServiceName() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        orchestrator.removeService("10.2.9.96_java::E1");
    }



    @Test
    public void removeServiceSyncSendsRequest() throws Exception {
        orchestrator.removeServiceSync("10.2.9.96_java:master:E1", 30);

        assertSyncSendCall("10.2.9.96", "container:10.2.9.96_java:master", "removeService?E1", 30);
    }


    @Test
    public void removeServiceSyncThrowsOnFailure() throws Exception {
        expectClaraExceptionOnSyncSend();
        orchestrator.removeServiceSync("10.2.9.96_java:master:E1", 30);
    }


    @Test
    public void removeServiceSyncThrowsOnTimeout() throws Exception {
        expectTimeoutExceptionOnSyncSend();
        orchestrator.removeServiceSync("10.2.9.96_java:master:E1", 30);
    }


    @Test
    public void removeServiceSyncThrowsOnBadServiceName() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        orchestrator.removeServiceSync("10.2.9.96_java::E1", 30);
    }




    private void assertSendCall(String host, String topic, String data) throws Exception {
        ArgumentCaptor<xMsgMessage> msgArg = ArgumentCaptor.forClass(xMsgMessage.class);
        verify(baseMock).genericSend(eq(host), msgArg.capture());
        assertMessage(msgArg.getValue(), topic, data);
    }


    private void assertSyncSendCall(String host, String topic, String data, int timeout)
            throws Exception {
        ArgumentCaptor<xMsgMessage> msgArg = ArgumentCaptor.forClass(xMsgMessage.class);
        verify(baseMock).genericSyncSend(eq(host), msgArg.capture(), eq(timeout));
        assertMessage(msgArg.getValue(), topic, data);
    }


    private void assertMessage(xMsgMessage msg, String topic, String data) {
        xMsgData.Builder msgData = (xMsgData.Builder) msg.getData();
        assertThat(msg.getTopic(), is(topic));
        assertThat(msgData.getSTRING(), is(data));
    }


    private void expectClaraExceptionOnSend() throws Exception {
        doThrow(new xMsgException("")).when(baseMock)
                .genericSend(anyString(), any(xMsgMessage.class));
        expectedEx.expect(ClaraException.class);
    }


    private void expectClaraExceptionOnSyncSend() throws Exception {
        doThrow(new xMsgException("")).when(baseMock)
                .genericSyncSend(anyString(), any(xMsgMessage.class), anyInt());
        expectedEx.expect(ClaraException.class);
    }


    private void expectTimeoutExceptionOnSyncSend() throws Exception {
        doThrow(new TimeoutException()).when(baseMock)
                .genericSyncSend(anyString(), any(xMsgMessage.class), anyInt());
        expectedEx.expect(TimeoutException.class);
    }


    @ParametersAreNonnullByDefault
    private class OrchestratorMock extends BaseOrchestrator {
        public OrchestratorMock() throws ClaraException {
            super();
        }

        @Override
        CBase getClaraBase(String frontEndHost) throws SocketException, xMsgException {
            return baseMock;
        }
    }
}
