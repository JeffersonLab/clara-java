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
import org.jlab.clara.base.ClaraSubscriptions.BaseSubscription;
import org.jlab.clara.base.core.ClaraBase;
import org.jlab.clara.base.core.ClaraComponent;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;
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
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BaseOrchestratorTest {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();
    private ClaraBase baseMock;
    private BaseOrchestrator orchestrator;
    private String feHost = "10.2.9.1_java";
    private Composition composition =
            new Composition("10.2.9.96_java:master:E1+10.2.9.96_java:master:E2;");

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
        EngineData data = new EngineData();
        data.setData(EngineDataType.STRING.mimeType(), "example");

        request = orchestrator.configure(service)
                              .withData(data)
                              .withDataTypes(EngineDataType.STRING);

        assertRequest("10.2.9.96",
                       "10.2.9.96_java:master:E1",
                       "10.2.9.96_java:master:E1;",
                       xMsgMeta.ControlAction.CONFIGURE);
    }



    @Test
    public void executeService() throws Exception {
        ServiceName service = new ServiceName("10.2.9.96_java:master:E1");
        EngineData data = new EngineData();
        data.setData(EngineDataType.STRING.mimeType(), "example");

        request = orchestrator.execute(service)
                              .withData(data)
                              .withDataTypes(EngineDataType.STRING);

        assertRequest("10.2.9.96",
                       "10.2.9.96_java:master:E1",
                       "10.2.9.96_java:master:E1;",
                       xMsgMeta.ControlAction.EXECUTE);
    }


    @Test
    public void executeComposition() throws Exception {
        EngineData data = new EngineData();
        data.setData(EngineDataType.STRING.mimeType(), "example");

        request = orchestrator.execute(composition)
                              .withData(data)
                              .withDataTypes(EngineDataType.STRING);

        assertRequest("10.2.9.96",
                       "10.2.9.96_java:master:E1",
                       "10.2.9.96_java:master:E1+10.2.9.96_java:master:E2;",
                       xMsgMeta.ControlAction.EXECUTE);
    }



    @Test
    public void startReportingDone() throws Exception {
        ServiceName service = new ServiceName("10.2.9.96_java:master:E1");
        request = orchestrator.configure(service).startDoneReporting(1000);

        assertRequest("10.2.9.96", "10.2.9.96_java:master:E1", "serviceReportDone?1000");
    }


    @Test
    public void stopReportingDone() throws Exception {
        ServiceName service = new ServiceName("10.2.9.96_java:master:E1");
        request = orchestrator.configure(service).stopDoneReporting();

        assertRequest("10.2.9.96", "10.2.9.96_java:master:E1", "serviceReportDone?0");
    }



    @Test
    public void startReportingData() throws Exception {
        ServiceName service = new ServiceName("10.2.9.96_java:master:E1");
        request = orchestrator.configure(service).startDataReporting(1000);

        assertRequest("10.2.9.96", "10.2.9.96_java:master:E1", "serviceReportData?1000");
    }


    @Test
    public void stopReportingData() throws Exception {
        ServiceName service = new ServiceName("10.2.9.96_java:master:E1");
        request = orchestrator.configure(service).stopDataReporting();

        assertRequest("10.2.9.96", "10.2.9.96_java:master:E1", "serviceReportData?0");
    }



    @Test
    public void listenServiceStatus() throws Exception {
        ServiceName service = new ServiceName("10.2.9.96_java:master:SimpleEngine");
        subscription = orchestrator.listen(service).status(EngineStatus.ERROR);

        assertSubscription("ERROR:10.2.9.96_java:master:SimpleEngine");
    }


    @Test
    public void listenServiceData() throws Exception {
        ServiceName service = new ServiceName("10.2.9.96_java:master:SimpleEngine");
        subscription = orchestrator.listen(service).data();

        assertSubscription("data:10.2.9.96_java:master:SimpleEngine");
    }


    @Test
    public void listenServiceDone() throws Exception {
        ServiceName service = new ServiceName("10.2.9.96_java:master:SimpleEngine");
        subscription = orchestrator.listen(service).done();

        assertSubscription("done:10.2.9.96_java:master:SimpleEngine");
    }



    @Test
    public void listenDpesAlive() throws Exception {
        subscription = orchestrator.listen().aliveDpes();

        assertSubscription("dpeAlive");
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

        OrchestratorMock() throws ClaraException, IOException {
            super();
        }

        @Override
        ClaraBase getClaraBase(String name, DpeName frontEnd, int poolSize) {
            return baseMock;
        }
    }
}
