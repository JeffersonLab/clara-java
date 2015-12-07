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

import org.jlab.clara.base.ClaraRequests.BaseRequest;
import org.jlab.clara.base.ClaraSubscriptions.BaseSubscription;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineStatus;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.ParametersAreNonnullByDefault;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.*;

public class BaseOrchestratorTest {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();
    private ClaraBase baseMock;
    private BaseOrchestrator orchestrator;
    private String feHost = "10.2.9.1_java";
    private Composition composition =
            new Composition("10.2.9.96_java:master:E1+10.2.9.96_java:master:E2");

    private BaseRequest<?, ?> request;
    private BaseSubscription<?, ?> subscription;

    @Before
    public void setUp() throws Exception {
        baseMock = mock(ClaraBase.class);
        orchestrator = new OrchestratorMock();

        when(baseMock.getFrontEnd()).thenReturn(ClaraComponent.dpe(feHost));
    }


    @Test
    public void exitDpe() throws Exception {
        DpeName dpe = new DpeName("10.2.9.96_java");
        request = orchestrator.exit(dpe);

        assertRequest("10.2.9.96", "dpe:10.2.9.96_java", "stopDpe");
    }



    @Test
    public void deployContainer() throws Exception {
        ContainerName container = new ContainerName("10.2.9.96_java:master");
        request = orchestrator.deploy(container).withPoolsize(5);

        assertRequest("10.2.9.96", "dpe:10.2.9.96_java", "startContainer?master?5?undefined");
    }


    @Test
    public void exitContainer() throws Exception {
        ContainerName container = new ContainerName("10.2.9.96_java:master");
        request = orchestrator.exit(container);

        assertRequest("10.2.9.96", "dpe:10.2.9.96_java", "stopContainer?master");
    }



    @Test
    public void deployService() throws Exception {
        ServiceName service = new ServiceName("10.2.9.96_java:master:E1");
        request = orchestrator.deploy(service, "org.example.service.E1").withPoolsize(10);

        assertRequest("10.2.9.96", "dpe:10.2.9.96_java",
                "startService?master?E1?org.example.service.E1?10?undefined?undefined");
    }


    @Test
    public void exitService() throws Exception {
        ServiceName service = new ServiceName("10.2.9.96_java:master:E1");
        request = orchestrator.exit(service);

        assertRequest("10.2.9.96", "dpe:10.2.9.96_java", "stopService?master?E1");
    }



    @Test
    public void configureService() throws Exception {
        ServiceName service = new ServiceName("10.2.9.96_java:master:E1");
        request = orchestrator.configure(service).withData(mock(EngineData.class));

        assertRequest("10.2.9.96",
                       "10.2.9.96_java:master:E1",
                       "10.2.9.96_java:master:E1",
                       xMsgMeta.ControlAction.CONFIGURE);
    }



    @Test
    public void executeService() throws Exception {
        ServiceName service = new ServiceName("10.2.9.96_java:master:E1");
        request = orchestrator.execute(service).withData(mock(EngineData.class));

        assertRequest("10.2.9.96",
                       "10.2.9.96_java:master:E1",
                       "10.2.9.96_java:master:E1",
                       xMsgMeta.ControlAction.EXECUTE);
    }


    @Test
    public void executeComposition() throws Exception {
        request = orchestrator.execute(composition).withData(mock(EngineData.class));

        assertRequest("10.2.9.96",
                       "10.2.9.96_java:master:E1",
                       "10.2.9.96_java:master:E1+10.2.9.96_java:master:E2",
                       xMsgMeta.ControlAction.EXECUTE);
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



    private void assertRequest(String host, String topic, String data) throws Exception {
        assertThat(request.frontEnd.getDpeHost(), is(host));
        assertMessage(request.msg(), topic, data);
    }


    private void assertRequest(String host, String topic,
                                String composition, xMsgMeta.ControlAction action)
            throws Exception {
        assertThat(request.frontEnd.getDpeHost(), is(host));
        assertMessage(request.msg(), topic, composition, action);
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


    private void assertSubscription(String topic) throws Exception {
        assertThat(subscription.frontEnd.getDpeCanonicalName(), is(feHost));
        assertThat(subscription.topic, is(xMsgTopic.wrap(topic)));
    }



    @ParametersAreNonnullByDefault
    private class OrchestratorMock extends BaseOrchestrator {

        public OrchestratorMock() throws ClaraException, IOException {
            super();
        }

        @Override
        ClaraBase getClaraBase(String name, DpeName frontEnd, int poolSize) {
            return baseMock;
        }
    }
}
