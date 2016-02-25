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

import org.jlab.clara.base.ClaraSubscriptions.BaseSubscription;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.coda.xmsg.core.xMsgCallBack;
import org.jlab.coda.xmsg.core.xMsgSubscription;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class ClaraSubscriptionsTest {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();
    private ClaraBase baseMock;
    private ClaraComponent frontEnd;
    private EngineCallback callback;
    private Map<String, xMsgSubscription> subscriptions;

    @Before
    public void setUp() throws Exception {
        baseMock = mock(ClaraBase.class);
        frontEnd = ClaraComponent.dpe("10.2.9.1_java");
        callback = mock(EngineCallback.class);
        subscriptions = new HashMap<>();

        when(baseMock.listen(any(ClaraComponent.class),
                             any(xMsgTopic.class),
                             any(xMsgCallBack.class))).thenReturn(mock(xMsgSubscription.class));
    }


    @Test
    public void startSubscriptionUsesFrontEnd() throws Exception {
        build("data:10.2.9.96_java:master:Simple").start(callback);

        ArgumentCaptor<ClaraComponent> compArg = ArgumentCaptor.forClass(ClaraComponent.class);
        verify(baseMock).listen(compArg.capture(), any(xMsgTopic.class), any(xMsgCallBack.class));
        assertThat(compArg.getValue().getCanonicalName(), is(frontEnd.getCanonicalName()));
    }


    @Test
    public void startSubscriptionMatchesTopic() throws Exception {
        build("data:10.2.9.96_java:master:Simple").start(callback);

        verify(baseMock).listen(any(ClaraComponent.class),
                                eq(xMsgTopic.wrap("data:10.2.9.96_java:master:Simple")),
                                any(xMsgCallBack.class));
    }


    @Test
    public void startSubscriptionWrapsUserCallback() throws Exception {
        TestSubscription sub = build("data:10.2.9.96_java:master:Simple");

        xMsgCallBack xcb = mock(xMsgCallBack.class);
        when(sub.wrap(eq(callback))).thenReturn(xcb);

        sub.start(callback);

        verify(baseMock).listen(any(ClaraComponent.class), any(xMsgTopic.class), eq(xcb));
    }


    @Test
    public void startSubscriptionThrowsOnFailure() throws Exception {
        doThrow(new xMsgException("")).when(baseMock)
                .listen(any(ClaraComponent.class), any(xMsgTopic.class), any(xMsgCallBack.class));
        expectedEx.expect(ClaraException.class);

        build("data:10.2.9.96_java:master:Simple").start(callback);
    }


    @Test
    public void startSubscriptionStoresSubscriptionHandler() throws Exception {
        String key = "10.2.9.1#ERROR:10.2.9.96_java:master:Simple";
        xMsgSubscription handler = mock(xMsgSubscription.class);
        when(baseMock.listen(any(ClaraComponent.class),
                             any(xMsgTopic.class),
                             any(xMsgCallBack.class))).thenReturn(handler);

        build("ERROR:10.2.9.96_java:master:Simple").start(callback);

        assertThat(subscriptions, hasEntry(key, handler));
    }


    @Test
    public void startSubscriptionThrowsOnDuplicatedSubscription() throws Exception {
        build("data:10.2.9.96_java:master:Simple").start(callback);

        expectedEx.expect(IllegalStateException.class);
        build("data:10.2.9.96_java:master:Simple").start(callback);
    }


    @Test
    public void stopSubscriptionUsesHandler() throws Exception {
        xMsgSubscription handler = mock(xMsgSubscription.class);
        when(baseMock.listen(eq(frontEnd),
                             eq(xMsgTopic.wrap("ERROR:10.2.9.96_java:master:Simple")),
                             any(xMsgCallBack.class))).thenReturn(handler);
        build("ERROR:10.2.9.96_java:master:Simple").start(callback);
        build("WARNING:10.2.9.96_java:master:Simple").start(callback);

        build("ERROR:10.2.9.96_java:master:Simple").stop();

        verify(baseMock).unsubscribe(eq(handler));
    }


    @Test
    public void stopSubscriptionThrowsOnFailure() throws Exception {
        build("ERROR:10.2.9.96_java:master:Simple").start(callback);

        doThrow(new xMsgException("")).when(baseMock).unsubscribe(any(xMsgSubscription.class));
        expectedEx.expect(ClaraException.class);

        build("ERROR:10.2.9.96_java:master:Simple").stop();
    }


    @Test
    public void stopSubscriptionRemovesSubscriptionHandler() throws Exception {
        build("ERROR:10.2.9.96_java:master:Simple").start(callback);
        build("WARNING:10.2.9.96_java:master:Simple").start(callback);
        build("INFO:10.2.9.96_java:master:Simple").start(callback);

        build("ERROR:10.2.9.96_java:master:Simple").stop();

        assertThat(subscriptions, not(hasKey("10.2.9.1#ERROR:10.2.9.96_java:master:Simple")));
    }


    private TestSubscription build(String topic) {
        return spy(new TestSubscription(baseMock, subscriptions, frontEnd, xMsgTopic.wrap(topic)));
    }


    public static class TestSubscription
            extends BaseSubscription<TestSubscription, EngineCallback> {

        TestSubscription(ClaraBase base,
                         Map<String, xMsgSubscription> subscriptions,
                         ClaraComponent frontEnd,
                         xMsgTopic topic) {
            super(base, subscriptions, frontEnd, topic);
        }

        @Override
        protected xMsgCallBack wrap(EngineCallback callback) {
            return mock(xMsgCallBack.class);
        }
    }
}
