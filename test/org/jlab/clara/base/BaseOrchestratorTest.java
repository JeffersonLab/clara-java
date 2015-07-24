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
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.sys.CBase;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.data.xMsgD.xMsgData;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;
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

    private Composition composition =
            new Composition("10.2.9.96_java:master:E1+10.2.9.96_java:master:E2");

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



    @Test
    public void configureServiceSendsRequest() throws Exception {
        orchestrator.configureService("10.2.9.96_java:master:E1", mock(EngineData.class));

        assertSendCall("10.2.9.96",
                       "10.2.9.96_java:master:E1",
                       "10.2.9.96_java:master:E1",
                       xMsgMeta.ControlAction.CONFIGURE);
    }


    @Test
    public void configureServiceSendsData() throws Exception {
        // TODO check that data is sent
    }


    @Test
    public void configureServiceThrowsOnFailure() throws Exception {
        expectClaraExceptionOnSend();
        orchestrator.configureService("10.2.9.96_java:master:E1", mock(EngineData.class));
    }


    @Test
    public void configureServiceThrowsOnBadServiceName() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        orchestrator.configureService("10.2.9.96_java::E1", mock(EngineData.class));
    }



    @Test
    public void configureServiceSyncSendsRequest() throws Exception {
        orchestrator.configureServiceSync("10.2.9.96_java:master:E1", mock(EngineData.class), 20);

        assertSyncSendCall("10.2.9.96",
                           "10.2.9.96_java:master:E1",
                           "10.2.9.96_java:master:E1",
                           xMsgMeta.ControlAction.CONFIGURE,
                           20);
    }


    @Test
    public void configureServiceSyncSendsData() throws Exception {
        // TODO check that data is sent
    }


    @Test
    public void configureServiceSyncThrowsOnFailure() throws Exception {
        expectClaraExceptionOnSyncSend();
        orchestrator.configureServiceSync("10.2.9.96_java:master:E1", mock(EngineData.class), 10);
    }


    @Test
    public void configureServiceSyncThrowsOnTimeout() throws Exception {
        expectTimeoutExceptionOnSyncSend();
        orchestrator.configureServiceSync("10.2.9.96_java:master:E1", mock(EngineData.class), 10);
    }


    @Test
    public void configureServiceSyncThrowsOnBadServiceName() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        orchestrator.configureServiceSync("10.2.9.96_java::E1", mock(EngineData.class), 10);
    }



    @Test
    public void executeServiceSendsRequest() throws Exception {
        orchestrator.executeService("10.2.9.96_java:master:E1", mock(EngineData.class));

        assertSendCall("10.2.9.96",
                       "10.2.9.96_java:master:E1",
                       "10.2.9.96_java:master:E1",
                       xMsgMeta.ControlAction.EXECUTE);
    }


    @Test
    public void executeServiceSendsData() throws Exception {
        // TODO check that data is sent
    }


    @Test
    public void executeServiceThrowsOnFailure() throws Exception {
        expectClaraExceptionOnSend();
        orchestrator.executeService("10.2.9.96_java:master:E1", mock(EngineData.class));
    }


    @Test
    public void executeServiceThrowsOnBadServiceName() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        orchestrator.executeService("10.2.9.96_java::E1", mock(EngineData.class));
    }



    @Test
    public void executeServiceSyncSendsRequest() throws Exception {
        orchestrator.executeServiceSync("10.2.9.96_java:master:E1", mock(EngineData.class), 20);

        assertSyncSendCall("10.2.9.96",
                           "10.2.9.96_java:master:E1",
                           "10.2.9.96_java:master:E1",
                           xMsgMeta.ControlAction.EXECUTE,
                           20);
    }


    @Test
    public void executeServiceSyncSendsData() throws Exception {
        // TODO check that data is sent
    }


    @Test
    public void executeServiceSyncReturnsData() throws Exception {
        // TODO check that data is received
    }


    @Test
    public void executeServiceSyncThrowsOnFailure() throws Exception {
        expectClaraExceptionOnSyncSend();
        orchestrator.executeServiceSync("10.2.9.96_java:master:E1", mock(EngineData.class), 10);
    }


    @Test
    public void executeServiceSyncThrowsOnTimeout() throws Exception {
        expectTimeoutExceptionOnSyncSend();
        orchestrator.executeServiceSync("10.2.9.96_java:master:E1", mock(EngineData.class), 10);
    }


    @Test
    public void executeServiceSyncThrowsOnBadServiceName() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        orchestrator.executeServiceSync("10.2.9.96_java::E1", mock(EngineData.class), 10);
    }



    @Test
    public void executeCompositionSendsRequest() throws Exception {
        orchestrator.executeComposition(composition, mock(EngineData.class));

        assertSendCall("10.2.9.96",
                       "10.2.9.96_java:master:E1",
                       "10.2.9.96_java:master:E1+10.2.9.96_java:master:E2",
                       xMsgMeta.ControlAction.EXECUTE);
    }


    @Test
    public void executeCompositionSendsData() throws Exception {
        // TODO check that data is sent
    }


    @Test
    public void executeCompositionThrowsOnFailure() throws Exception {
        expectClaraExceptionOnSend();
        orchestrator.executeComposition(composition, mock(EngineData.class));
    }



    @Test
    public void executeCompositionSyncSendsRequest() throws Exception {
        orchestrator.executeCompositionSync(composition, mock(EngineData.class), 20);

        assertSyncSendCall("10.2.9.96",
                           "10.2.9.96_java:master:E1",
                           "10.2.9.96_java:master:E1+10.2.9.96_java:master:E2",
                           xMsgMeta.ControlAction.EXECUTE,
                           20);
    }


    @Test
    public void executeCompositionSyncSendsData() throws Exception {
        // TODO check that data is sent
    }


    @Test
    public void executeCompositionSyncReturnsData() throws Exception {
        // TODO check that data is received
    }


    @Test
    public void executeCompositionSyncThrowsOnFailure() throws Exception {
        expectClaraExceptionOnSyncSend();
        orchestrator.executeCompositionSync(composition, mock(EngineData.class), 10);
    }


    @Test
    public void executeCompositionSyncThrowsOnTimeout() throws Exception {
        expectTimeoutExceptionOnSyncSend();
        orchestrator.executeCompositionSync(composition, mock(EngineData.class), 10);
    }



    @Test
    public void startReportingDoneSendsRequest() throws Exception {
        orchestrator.startReportingDone("10.2.9.96_java:master:E1", 1000);
        assertSendCall("10.2.9.96", "10.2.9.96_java:master:E1", "serviceReportDone?1000");
    }


    @Test
    public void startReportingDoneThrowsOnFailure() throws Exception {
        expectClaraExceptionOnSend();
        orchestrator.startReportingDone("10.2.9.96_java:master:E1", 1000);
    }


    @Test
    public void startReportingDoneThrowsOnBadServiceName() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        orchestrator.startReportingDone("10.2.9.96_java:master", 1000);
    }



    @Test
    public void stopReportingDoneSendsRequest() throws Exception {
        orchestrator.stopReportingDone("10.2.9.96_java:master:E1");
        assertSendCall("10.2.9.96", "10.2.9.96_java:master:E1", "serviceReportDone?0");
    }


    @Test
    public void stopReportingDoneThrowsOnFailure() throws Exception {
        expectClaraExceptionOnSend();
        orchestrator.stopReportingDone("10.2.9.96_java:master:E1");
    }


    @Test
    public void stopReportingDoneThrowsOnBadServiceName() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        orchestrator.stopReportingDone("10.2.9.96_java:master");
    }



    @Test
    public void startReportingDataSendsRequest() throws Exception {
        orchestrator.startReportingData("10.2.9.96_java:master:E1", 1000);
        assertSendCall("10.2.9.96", "10.2.9.96_java:master:E1", "serviceReportData?1000");
    }


    @Test
    public void startReportingDataThrowsOnFailure() throws Exception {
        expectClaraExceptionOnSend();
        orchestrator.startReportingData("10.2.9.96_java:master:E1", 1000);
    }


    @Test
    public void startReportingDataThrowsOnBadServiceName() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        orchestrator.startReportingData("10.2.9.96_java:master", 1000);
    }



    @Test
    public void stopReportingDataSendsRequest() throws Exception {
        orchestrator.stopReportingData("10.2.9.96_java:master:E1");
        assertSendCall("10.2.9.96", "10.2.9.96_java:master:E1", "serviceReportData?0");
    }


    @Test
    public void stopReportingDataThrowsOnFailure() throws Exception {
        expectClaraExceptionOnSend();
        orchestrator.stopReportingData("10.2.9.96_java:master:E1");
    }


    @Test
    public void stopReportingDataThrowsOnBadServiceName() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        orchestrator.stopReportingData("10.2.9.96_java:master");
    }



    private void assertSendCall(String host, String topic, String data) throws Exception {
        ArgumentCaptor<xMsgMessage> msgArg = ArgumentCaptor.forClass(xMsgMessage.class);
        verify(baseMock).genericSend(eq(host), msgArg.capture());
        assertMessage(msgArg.getValue(), topic, data);
    }


    private void assertSendCall(String host, String topic,
                                String composition, xMsgMeta.ControlAction action)
            throws Exception {
        ArgumentCaptor<xMsgMessage> msgArg = ArgumentCaptor.forClass(xMsgMessage.class);
        verify(baseMock).genericSend(eq(host), msgArg.capture());

        assertMessage(msgArg.getValue(), topic, composition, action);
    }


    private void assertSyncSendCall(String host, String topic, String data, int timeout)
            throws Exception {
        ArgumentCaptor<xMsgMessage> msgArg = ArgumentCaptor.forClass(xMsgMessage.class);
        verify(baseMock).genericSyncSend(eq(host), msgArg.capture(), eq(timeout));
        assertMessage(msgArg.getValue(), topic, data);
    }


    private void assertSyncSendCall(String host, String topic,
                                    String composition, xMsgMeta.ControlAction action,
                                    int timeout)
            throws Exception {
        ArgumentCaptor<xMsgMessage> msgArg = ArgumentCaptor.forClass(xMsgMessage.class);
        verify(baseMock).genericSyncSend(eq(host), msgArg.capture(), eq(timeout));

        assertMessage(msgArg.getValue(), topic, composition, action);
    }


    private void assertMessage(xMsgMessage msg, String topic, String data)
        throws Exception {
        xMsgData msgData = xMsgData.parseFrom(msg.getData());
        assertThat(msg.getTopic().toString(), is(topic));
        assertThat(msgData.getSTRING(), is(data));
    }


    private void assertMessage(xMsgMessage msg, String topic,
                               String composition, xMsgMeta.ControlAction action) {
        xMsgMeta.Builder msgMeta = msg.getMetaData();

        assertThat(msg.getTopic().toString(), is(topic));
        assertThat(msgMeta.getComposition(), is(composition));
        assertThat(msgMeta.getAction(), is(action));
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
