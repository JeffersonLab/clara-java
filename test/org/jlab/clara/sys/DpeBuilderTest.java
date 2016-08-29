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

package org.jlab.clara.sys;

import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.sys.Dpe.Builder;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DpeBuilderTest {

    private final String defaultHost;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    public DpeBuilderTest() throws Exception {
        defaultHost = ClaraUtil.localhost();
    }

    @Test
    public void dpeIsFrontEndByDefault() throws Exception {
        Builder builder = new Builder();

        assertTrue(builder.isFrontEnd);
    }

    @Test
    public void dpeIsWorkerIfReceivesFrontEndHost() throws Exception {
        Builder builder = new Builder("10.2.9.100");

        assertFalse(builder.isFrontEnd);
    }

    @Test
    public void workerUsesDefaultLocalAddress() throws Exception {
        Builder builder = new Builder("10.2.9.100");

        assertThat(builder.localAddress, is(proxy(defaultHost)));
    }

    @Test
    public void workerReceivesOptionalLocalHost() throws Exception {
        Builder builder = new Builder("10.2.9.100").withHost("10.2.9.4");

        assertThat(builder.localAddress, is(proxy("10.2.9.4")));
    }

    @Test
    public void workerReceivesOptionalLocalPort() throws Exception {
        Builder builder = new Builder("10.2.9.100").withPort(8500);

        assertThat(builder.localAddress, is(proxy(defaultHost, 8500)));
    }

    @Test
    public void workerReceivesOptionalLocalHostAndPort() throws Exception {
        Builder builder = new Builder("10.2.9.100").withHost("10.2.9.4").withPort(8500);

        assertThat(builder.localAddress, is(proxy("10.2.9.4", 8500)));
        assertThat(builder.frontEndAddress, is(proxy("10.2.9.100")));
    }

    @Test
    public void workerReceivesRemoteFrontEndAddress() throws Exception {
        Builder builder = new Builder("10.2.9.100");

        assertThat(builder.frontEndAddress, is(proxy("10.2.9.100")));
    }

    @Test
    public void workerReceivesRemoteFrontEndAddressAndPort() throws Exception {
        Builder builder = new Builder("10.2.9.100", 9000);

        assertThat(builder.frontEndAddress, is(proxy("10.2.9.100", 9000)));
    }

    @Test
    public void frontEndUsesDefaultLocalAddress() throws Exception {
        Builder builder = new Builder();

        assertThat(builder.localAddress, is(proxy(defaultHost)));
        assertThat(builder.frontEndAddress, is(proxy(defaultHost)));
    }

    @Test
    public void frontEndReceivesOptionalLocalHost() throws Exception {
        Builder builder = new Builder().withHost("10.2.9.100");

        assertThat(builder.localAddress, is(proxy("10.2.9.100")));
        assertThat(builder.frontEndAddress, is(proxy("10.2.9.100")));
    }

    @Test
    public void frontEndReceivesOptionalLocalPort() throws Exception {
        Builder builder = new Builder().withPort(8500);

        assertThat(builder.localAddress, is(proxy(defaultHost, 8500)));
        assertThat(builder.frontEndAddress, is(proxy(defaultHost, 8500)));
    }

    @Test
    public void frontEndReceivesOptionalLocalHostAndPort() throws Exception {
        Builder builder = new Builder().withHost("10.2.9.4").withPort(8500);

        assertThat(builder.localAddress, is(proxy("10.2.9.4", 8500)));
        assertThat(builder.frontEndAddress, is(proxy("10.2.9.4", 8500)));
    }

    @Test
    public void dpeUsesDefaultPoolSize() throws Exception {
        Builder builder = new Builder();

        assertThat(builder.poolSize, is(Dpe.DEFAULT_POOL_SIZE));
    }

    @Test
    public void dpeReceivesOptionalPoolSize() throws Exception {
        Builder builder = new Builder().withPoolSize(10);

        assertThat(builder.poolSize, is(10));
    }

    @Test
    public void dpeUsesDefaultEmptyDescription() throws Exception {
        Builder builder = new Builder();

        assertThat(builder.description, is(""));
    }

    @Test
    public void dpeReceivesOptionalDescription() throws Exception {
        Builder builder = new Builder().withDescription("A processing DPE");

        assertThat(builder.description, is("A processing DPE"));
    }

    @Test
    public void dpeUsesDefaultReportInterval() throws Exception {
        Builder builder = new Builder();

        assertThat(builder.reportPeriod, is(Dpe.DEFAULT_REPORT_PERIOD));
    }

    @Test
    public void dpeReceivesOptionalReportInterval() throws Exception {
        Builder builder = new Builder().withReportPeriod(20, TimeUnit.SECONDS);

        assertThat(builder.reportPeriod, is(20_000L));
    }


    private xMsgProxyAddress proxy(String host) throws Exception {
        return new xMsgProxyAddress(host, Dpe.DEFAULT_PROXY_PORT);
    }

    private xMsgProxyAddress proxy(String host, int port) throws Exception {
        return new xMsgProxyAddress(host, port);
    }
}
