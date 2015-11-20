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

import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineStatus;
import org.jlab.coda.xmsg.core.xMsgCallBack;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgSubscription;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;

import javax.annotation.ParametersAreNonnullByDefault;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.*;

public class BaseOrchestratorTest {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();
    private ClaraBase baseMock;
    private BaseOrchestrator orchestrator;
    private String feHost = "10.2.9.1";
    private Composition composition =
            new Composition("10.2.9.96_java:master:E1+10.2.9.96_java:master:E2");

    @Before
    public void setUp() throws Exception {
        baseMock = mock(ClaraBase.class);
        orchestrator = new OrchestratorMock();

        when(baseMock.getFrontEnd()).thenReturn(ClaraComponent.dpe(feHost));
    }


    @Test
    public void exitDpeSendsRequest() throws Exception {
        orchestrator.exitRemoteDpe("10.2.9.96_java");
        assertSendCall("10.2.9.96", "dpe:10.2.9.96_java", "dpeExit");
    }


    @Test
    public void exitDpeThrowsOnFailure() throws Exception {
        expectClaraExceptionOnSend();
        orchestrator.exitRemoteDpe("10.2.9.96_java");
    }


    @Test
    public void exitDpeThrowsOnBadDpeName() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        orchestrator.exitRemoteDpe("10.2.9.96_java:master");
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
        orchestrator.syncDeployContainer("10.2.9.96_java:master", 10);

        assertSyncSendCall("10.2.9.96", "dpe:10.2.9.96_java", "startContainer?master", 10);
    }


    @Test
    public void deployContainerSyncThrowsOnFailure() throws Exception {
        expectClaraExceptionOnSyncSend();
        orchestrator.syncDeployContainer("10.2.9.96_java:master", 10);
    }


    @Test
    public void deployContainerSyncThrowsOnTimeout() throws Exception {
        expectTimeoutExceptionOnSyncSend();
        orchestrator.syncDeployContainer("10.2.9.96_java:master", 10);
    }


    @Test
    public void deployContainerSyncThrowsOnBadContainerName() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        orchestrator.syncDeployContainer("10.2.9.96_java", 10);
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
        orchestrator.deployService("10.2.9.96_java:master:E1", "org.example.service.E1", 10);

        assertSendCall("10.2.9.96", "container:10.2.9.96_java:master",
                "deployService?E1?org.example.service.E1?10");
    }


    @Test
    public void deployServiceThrowsOnFailure() throws Exception {
        expectClaraExceptionOnSend();
        orchestrator.deployService("10.2.9.96_java:master:E1", "org.example.service.E1", 10);
    }


    @Test
    public void deployServiceThrowsOnBadServiceName() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        orchestrator.deployService("10.2.9.96_java::E1", "org.example.service.E1", 10);
    }



    @Test
    public void deployServiceSyncSendsRequest() throws Exception {
        orchestrator.deployServiceSync("10.2.9.96_java:master:E1", "org.example.service.E1", 8, 30);

        assertSyncSendCall("10.2.9.96", "container:10.2.9.96_java:master",
                "deployService?E1?org.example.service.E1?8", 30);
    }


    @Test
    public void deployServiceSyncThrowsOnFailure() throws Exception {
        expectClaraExceptionOnSyncSend();
        orchestrator.deployServiceSync("10.2.9.96_java:master:E1", "org.example.service.E1", 8, 30);
    }


    @Test
    public void deployServiceSyncThrowsOnTimeout() throws Exception {
        expectTimeoutExceptionOnSyncSend();
        orchestrator.deployServiceSync("10.2.9.96_java:master:E1", "org.example.service.E1", 8, 30);
    }


    @Test
    public void deployServiceSyncThrowsOnBadServiceName() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        orchestrator.deployServiceSync("10.2.9.96_java::E1", "org.example.service.E1", 10, 30);
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
//        orchestrator.executeCompositionSync(composition, mock(EngineData.class), 20);
//
//        assertSyncSendCall("10.2.9.96",
//                           "10.2.9.96_java:master:E1",
//                           "10.2.9.96_java:master:E1+10.2.9.96_java:master:E2",
//                           xMsgMeta.ControlAction.EXECUTE,
//                           20);
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
//        expectClaraExceptionOnSyncSend();
//        orchestrator.executeCompositionSync(composition, mock(EngineData.class), 10);
    }


    @Test
    public void executeCompositionSyncThrowsOnTimeout() throws Exception {
//        expectTimeoutExceptionOnSyncSend();
//        orchestrator.executeCompositionSync(composition, mock(EngineData.class), 10);
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



    @Test
    public void listenServiceStatusSendsRequest() throws Exception {
        EngineStatus status = EngineStatus.ERROR;
        EngineCallback callback = mock(EngineCallback.class);

        orchestrator.listenServiceStatus("10.2.9.96_java:master:SimpleEngine", status, callback);

        assertSubscriptionStarted("ERROR:10.2.9.96_java:master:SimpleEngine", status, callback);
    }


    @Test
    public void listenServiceStatusThrowsOnFailure() throws Exception {
        EngineStatus status = EngineStatus.ERROR;
        EngineCallback callback = mock(EngineCallback.class);
        expectClaraExceptionOnReceive();

        orchestrator.listenServiceStatus("10.2.9.96_java:master:SimpleEngine", status, callback);
    }


    @Test
    public void listenServiceStatusThrowsOnBadCanonicalName() throws Exception {
        EngineStatus status = EngineStatus.ERROR;
        EngineCallback callback = mock(EngineCallback.class);

        orchestrator.listenServiceStatus("10.2.9.96_java", status, callback);
        orchestrator.listenServiceStatus("10.2.9.96_java:master", status, callback);
        orchestrator.listenServiceStatus("10.2.9.96_java:master:SimpleEngine", status, callback);

        expectedEx.expect(IllegalArgumentException.class);

        orchestrator.listenServiceStatus("10.2.9.96_java#master", status, callback);
    }


    @Test
    public void listenServiceStatusStoresSubscriptionHandler() throws Exception {
        EngineStatus status = EngineStatus.ERROR;
        EngineCallback callback = mock(EngineCallback.class);
        xMsgSubscription handler = mockSubscriptionHandler();
        String key = "10.2.9.1#ERROR:10.2.9.96_java:master:SimpleEngine";

        orchestrator.listenServiceStatus("10.2.9.96_java:master:SimpleEngine", status, callback);

        assertSubscriptionRegistered(key, handler);
    }


    @Test
    public void listenServiceStatusThrowsOnDuplicatedSubscription() throws Exception {
        EngineStatus status = EngineStatus.ERROR;
        EngineCallback callback = mock(EngineCallback.class);
        orchestrator.listenServiceStatus("10.2.9.96_java:master:SimpleEngine", status, callback);

        expectedEx.expect(IllegalStateException.class);
        orchestrator.listenServiceStatus("10.2.9.96_java:master:SimpleEngine", status, callback);
    }



    @Test
    public void unlistenServiceStatusStopsSubscription() throws Exception {
        EngineStatus status = EngineStatus.ERROR;
        String key = "10.2.9.1#ERROR:10.2.9.96_java:master:SimpleEngine";
        xMsgSubscription handler = mock(xMsgSubscription.class);
        orchestrator.getSubscriptions().put(key, handler);

        orchestrator.unListenServiceStatus("10.2.9.96_java:master:SimpleEngine", status);

        verify(baseMock).unsubscribe(handler);
    }


    @Test
    public void unlistenServiceStatusThrowsOnBadCanonicalName() throws Exception {
        EngineStatus status = EngineStatus.ERROR;

        orchestrator.unListenServiceStatus("10.2.9.96_java", status);
        orchestrator.unListenServiceStatus("10.2.9.96_java:master", status);
        orchestrator.unListenServiceStatus("10.2.9.96_java:master:SimpleEngine", status);

        expectedEx.expect(IllegalArgumentException.class);

        orchestrator.unListenServiceStatus("10.2.9.96_java#master", status);
    }


    @Test
    public void unlistenServiceStatusRemovesSubscriptionHandler() throws Exception {
        EngineStatus status = EngineStatus.ERROR;
        String key = "10.2.9.1#ERROR:10.2.9.96_java:master:SimpleEngine";
        orchestrator.getSubscriptions().put(key, mock(xMsgSubscription.class));

        orchestrator.unListenServiceStatus("10.2.9.96_java:master:SimpleEngine", status);

        assertSubscriptionRemoved(key);
    }



    @Test
    public void listenServiceDataSendsRequest() throws Exception {
        EngineCallback callback = mock(EngineCallback.class);

        orchestrator.listenServiceData("10.2.9.96_java:master:SimpleEngine", callback);

        assertSubscriptionStarted("data:10.2.9.96_java:master:SimpleEngine", null, callback);
    }


    @Test
    public void listenServiceDataThrowsOnFailure() throws Exception {
        EngineCallback callback = mock(EngineCallback.class);
        expectClaraExceptionOnReceive();

        orchestrator.listenServiceData("10.2.9.96_java:master:SimpleEngine", callback);
    }


    @Test
    public void listenServiceDataThrowsOnBadCanonicalName() throws Exception {
        EngineCallback callback = mock(EngineCallback.class);

        orchestrator.listenServiceData("10.2.9.96_java", callback);
        orchestrator.listenServiceData("10.2.9.96_java:master", callback);
        orchestrator.listenServiceData("10.2.9.96_java:master:SimpleEngine", callback);

        expectedEx.expect(IllegalArgumentException.class);

        orchestrator.listenServiceData("10.2.9.96_java#master", callback);
    }


    @Test
    public void listenServiceDataStoresSubscriptionHandler() throws Exception {
        EngineCallback callback = mock(EngineCallback.class);
        xMsgSubscription handler = mockSubscriptionHandler();
        String key = "10.2.9.1#data:10.2.9.96_java:master:SimpleEngine";

        orchestrator.listenServiceData("10.2.9.96_java:master:SimpleEngine", callback);

        assertSubscriptionRegistered(key, handler);
    }


    @Test
    public void listenServiceDataThrowsOnDuplicatedSubscription() throws Exception {
        EngineCallback callback = mock(EngineCallback.class);
        orchestrator.listenServiceData("10.2.9.96_java:master:SimpleEngine", callback);

        expectedEx.expect(IllegalStateException.class);
        orchestrator.listenServiceData("10.2.9.96_java:master:SimpleEngine", callback);
    }



    @Test
    public void unlistenServiceDataStopsSubscription() throws Exception {
        String key = "10.2.9.1#data:10.2.9.96_java:master:SimpleEngine";
        xMsgSubscription handler = mock(xMsgSubscription.class);
        orchestrator.getSubscriptions().put(key, handler);

        orchestrator.unListenServiceData("10.2.9.96_java:master:SimpleEngine");

        verify(baseMock).unsubscribe(handler);
    }


    @Test
    public void unlistenServiceDataThrowsOnBadCanonicalName() throws Exception {
        orchestrator.unListenServiceData("10.2.9.96_java");
        orchestrator.unListenServiceData("10.2.9.96_java:master");
        orchestrator.unListenServiceData("10.2.9.96_java:master:SimpleEngine");

        expectedEx.expect(IllegalArgumentException.class);

        orchestrator.unListenServiceData("10.2.9.96#java:master");
    }


    @Test
    public void unlistenServiceDataRemovesSubscriptionHandler() throws Exception {
        String key = "10.2.9.1#data:10.2.9.96_java:master:SimpleEngine";
        orchestrator.getSubscriptions().put(key, mock(xMsgSubscription.class));

        orchestrator.unListenServiceData("10.2.9.96_java:master:SimpleEngine");

        assertSubscriptionRemoved(key);
    }



    @Test
    public void listenServiceDoneSendsRequest() throws Exception {
        EngineCallback callback = mock(EngineCallback.class);

        orchestrator.listenServiceDone("10.2.9.96_java:master:SimpleEngine", callback);

        assertSubscriptionStarted("done:10.2.9.96_java:master:SimpleEngine", null, callback);
    }


    @Test
    public void listenServiceDoneThrowsOnFailure() throws Exception {
        EngineCallback callback = mock(EngineCallback.class);
        expectClaraExceptionOnReceive();

        orchestrator.listenServiceDone("10.2.9.96_java:master:SimpleEngine", callback);
    }


    @Test
    public void listenServiceDoneThrowsOnBadCanonicalName() throws Exception {
        EngineCallback callback = mock(EngineCallback.class);

        orchestrator.listenServiceDone("10.2.9.96_java", callback);
        orchestrator.listenServiceDone("10.2.9.96_java:master", callback);
        orchestrator.listenServiceDone("10.2.9.96_java:master:SimpleEngine", callback);

        expectedEx.expect(IllegalArgumentException.class);

        orchestrator.listenServiceDone("10.2.9.96_java#master", callback);
    }


    @Test
    public void listenServiceDoneStoresSubscriptionHandler() throws Exception {
        EngineCallback callback = mock(EngineCallback.class);
        xMsgSubscription handler = mockSubscriptionHandler();
        String key = "10.2.9.1#done:10.2.9.96_java:master:SimpleEngine";

        orchestrator.listenServiceDone("10.2.9.96_java:master:SimpleEngine", callback);

        assertSubscriptionRegistered(key, handler);
    }


    @Test
    public void listenServiceDoneThrowsOnDuplicatedSubscription() throws Exception {
        EngineCallback callback = mock(EngineCallback.class);
        orchestrator.listenServiceDone("10.2.9.96_java:master:SimpleEngine", callback);

        expectedEx.expect(IllegalStateException.class);
        orchestrator.listenServiceDone("10.2.9.96_java:master:SimpleEngine", callback);
    }



    @Test
    public void unlistenServiceDoneStopsSubscription() throws Exception {
        String key = "10.2.9.1#done:10.2.9.96_java:master:SimpleEngine";
        xMsgSubscription handler = mock(xMsgSubscription.class);
        orchestrator.getSubscriptions().put(key, handler);

        orchestrator.unListenServiceDone("10.2.9.96_java:master:SimpleEngine");

        verify(baseMock).unsubscribe(handler);
    }


    @Test
    public void unlistenServiceDoneThrowsOnBadCanonicalName() throws Exception {
        orchestrator.unListenServiceDone("10.2.9.96_java");
        orchestrator.unListenServiceDone("10.2.9.96_java:master");
        orchestrator.unListenServiceDone("10.2.9.96_java:master:SimpleEngine");

        expectedEx.expect(IllegalArgumentException.class);

        orchestrator.unListenServiceDone("10.2.9.96_java#master");
    }


    @Test
    public void unlistenServiceDoneRemovesSubscriptionHandler() throws Exception {
        String key = "10.2.9.1#done:10.2.9.96_java:master:SimpleEngine";
        orchestrator.getSubscriptions().put(key, mock(xMsgSubscription.class));

        orchestrator.unListenServiceDone("10.2.9.96_java:master:SimpleEngine");

        assertSubscriptionRemoved(key);
    }


    @Test
    public void listenDpesSendsRequest() throws Exception {
        GenericCallback callback = mock(GenericCallback.class);

        orchestrator.listenDpes(callback);

        assertSubscriptionStarted("dpeAlive", callback);
    }


    @Test
    public void listenDpesThrowsOnFailure() throws Exception {
        GenericCallback callback = mock(GenericCallback.class);
        expectClaraExceptionOnReceive();

        orchestrator.listenDpes(callback);
    }


    @Test
    public void listenDpesStoresSubscriptionHandler() throws Exception {
        GenericCallback callback = mock(GenericCallback.class);
        xMsgSubscription handler = mockSubscriptionHandler();
        String key = "10.2.9.1#dpeAlive";

        orchestrator.listenDpes(callback);

        assertSubscriptionRegistered(key, handler);
    }


    @Test
    public void listenDpesThrowsOnDuplicatedSubscription() throws Exception {
        GenericCallback callback = mock(GenericCallback.class);
        orchestrator.listenDpes(callback);

        expectedEx.expect(IllegalStateException.class);
        orchestrator.listenDpes(callback);
    }



    @Test
    public void unlistenDpesStopsSubscription() throws Exception {
        String key = "10.2.9.1#dpeAlive";
        xMsgSubscription handler = mock(xMsgSubscription.class);
        orchestrator.getSubscriptions().put(key, handler);

        orchestrator.unListenDpes();

        verify(baseMock).unsubscribe(handler);
    }


    @Test
    public void unlistenDpesRemovesSubscriptionHandler() throws Exception {
        String key = "10.2.9.1#dpeAlive";
        orchestrator.getSubscriptions().put(key, mock(xMsgSubscription.class));

        orchestrator.unListenDpes();

        assertSubscriptionRemoved(key);
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
        String msgData = new String(msg.getData());
        assertThat(msg.getTopic().toString(), is(topic));
        assertThat(msgData, is(data));
    }


    private void assertMessage(xMsgMessage msg, String topic,
                               String composition, xMsgMeta.ControlAction action) {
        xMsgMeta.Builder msgMeta = msg.getMetaData();

        assertThat(msg.getTopic().toString(), is(topic));
        assertThat(msgMeta.getComposition(), is(composition));
        assertThat(msgMeta.getAction(), is(action));
    }


    private void assertSubscriptionStarted(String topic,
                                           EngineStatus status, EngineCallback callback)
            throws Exception {
        OrchestratorMock orchMock = (OrchestratorMock) orchestrator;
        verify(baseMock).genericReceive(feHost,
                                        xMsgTopic.wrap(topic),
                                        orchMock.userWrapperCallback);
        assertThat(orchMock.userEngineStatus, is(sameInstance(status)));
        assertThat(orchMock.userEngineCallback, is(sameInstance(callback)));
    }


    private void assertSubscriptionStarted(String topic, GenericCallback callback)
            throws Exception {
        OrchestratorMock orchMock = (OrchestratorMock) orchestrator;
        verify(baseMock).genericReceive(feHost,
                                        xMsgTopic.wrap(topic),
                                        orchMock.userWrapperCallback);
        assertThat(orchMock.userGenericCallback, is(sameInstance(callback)));
    }


    private void assertSubscriptionRegistered(String key, xMsgSubscription handler) {
        assertThat(orchestrator.getSubscriptions(), hasEntry(key, handler));
    }


    private void assertSubscriptionRemoved(String key) {
        assertThat(orchestrator.getSubscriptions(), not(hasKey(key)));
    }


    private xMsgSubscription mockSubscriptionHandler() throws Exception {
        xMsgSubscription handler = mock(xMsgSubscription.class);
        when(baseMock.genericReceive(anyString(), any(xMsgTopic.class), any(xMsgCallBack.class)))
                .thenReturn(handler);
        return handler;
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


    private void expectClaraExceptionOnReceive() throws Exception {
        doThrow(new xMsgException("")).when(baseMock)
                .genericReceive(anyString(), any(xMsgTopic.class), any(xMsgCallBack.class));
        expectedEx.expect(ClaraException.class);
    }


    @ParametersAreNonnullByDefault
    private class OrchestratorMock extends BaseOrchestrator {
        public EngineStatus userEngineStatus;
        public EngineCallback userEngineCallback;
        public GenericCallback userGenericCallback;
        public xMsgCallBack userWrapperCallback;

        public OrchestratorMock() throws ClaraException, IOException {
            super();
        }

        @Override
        ClaraBase getClaraBase(String name, DpeName frontEnd, int poolSize) {
            return baseMock;
        }

        @Override
        xMsgCallBack wrapEngineCallback(final EngineCallback userCallback,
                                        final EngineStatus userStatus) {
            userEngineStatus = userStatus;
            userEngineCallback = userCallback;
            userWrapperCallback = super.wrapEngineCallback(userCallback, userStatus);
            return userWrapperCallback;
        }

        @Override
        xMsgCallBack wrapGenericCallback(final GenericCallback userCallback) {
            userGenericCallback = userCallback;
            userWrapperCallback = super.wrapGenericCallback(userCallback);
            return userWrapperCallback;
        }
    }
}
