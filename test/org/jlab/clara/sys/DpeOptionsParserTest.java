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
import org.jlab.clara.sys.DpeOptionsParser.DpeOptionsException;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DpeOptionsParserTest {

    private static final String DPE_HOST_OPT = "-dpe_host";
    private static final String DPE_PORT_OPT = "-dpe_port";

    private static final String FE_HOST_OPT = "-fe_host";
    private static final String FE_PORT_OPT = "-fe_port";

    private static final String POOL_OPT = "-poolsize";
    private static final String DESC_OPT = "-description";
    private static final String REPORT_OPT = "-report";
    private final String defaultHost;
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();
    private DpeOptionsParser parser;


    public DpeOptionsParserTest() throws Exception {
        defaultHost = ClaraUtil.localhost();
    }

    @Before
    public void setUp() throws Exception {
        parser = new DpeOptionsParser();
    }


    @Test
    public void dpeIsFrontEndByDefault() throws Exception {
        parse();

        assertTrue(parser.isFrontEnd());
    }

    @Test
    public void dpeIsWorkerIfReceivesFrontEndHost() throws Exception {
        parse(FE_HOST_OPT, "10.2.9.100");

        assertFalse(parser.isFrontEnd());
    }

    @Test
    public void workerUsesDefaultLocalAddress() throws Exception {
        parse(FE_HOST_OPT, "10.2.9.100");

        assertThat(parser.localAddress(), is(proxy(defaultHost)));
    }

    @Test
    public void workerReceivesOptionalLocalHost() throws Exception {
        parse(DPE_HOST_OPT, "10.2.9.4", FE_HOST_OPT, "10.2.9.100");

        assertThat(parser.localAddress(), is(proxy("10.2.9.4")));
    }

    @Test
    public void workerReceivesOptionalLocalPort() throws Exception {
        parse(DPE_PORT_OPT, "8500", FE_HOST_OPT, "10.2.9.100");

        assertThat(parser.localAddress(), is(proxy(defaultHost, 8500)));
    }

    @Test
    public void workerReceivesOptionalLocalHostAndPort() throws Exception {
        parse(DPE_HOST_OPT, "10.2.9.4", DPE_PORT_OPT, "8500", FE_HOST_OPT, "10.2.9.100");

        assertThat(parser.localAddress(), is(proxy("10.2.9.4", 8500)));
        assertThat(parser.frontEnd(), is(proxy("10.2.9.100")));
    }

    @Test
    public void workerReceivesRemoteFrontEndAddress() throws Exception {
        parse(FE_HOST_OPT, "10.2.9.100");

        assertThat(parser.frontEnd(), is(proxy("10.2.9.100")));
    }

    @Test
    public void workerReceivesRemoteFrontEndAddressAndPort() throws Exception {
        parse(FE_HOST_OPT, "10.2.9.100", FE_PORT_OPT, "9000");

        assertThat(parser.frontEnd(), is(proxy("10.2.9.100", 9000)));
    }

    @Test
    public void workerRequiresRemoteFrontEndHostWhenPortIsGiven() throws Exception {
        expectedEx.expect(DpeOptionsException.class);
        expectedEx.expectMessage("remote front-end host is required");

        parse(FE_PORT_OPT, "9000");
    }

    @Test
    public void frontEndUsesDefaultLocalAddress() throws Exception {
        parse();

        assertThat(parser.localAddress(), is(proxy(defaultHost)));
        assertThat(parser.frontEnd(), is(proxy(defaultHost)));
    }

    @Test
    public void frontEndReceivesOptionalLocalHost() throws Exception {
        parse(DPE_HOST_OPT, "10.2.9.100");

        assertThat(parser.localAddress(), is(proxy("10.2.9.100")));
        assertThat(parser.frontEnd(), is(proxy("10.2.9.100")));
    }

    @Test
    public void frontEndReceivesOptionalLocalPort() throws Exception {
        parse(DPE_PORT_OPT, "8500");

        assertThat(parser.localAddress(), is(proxy(defaultHost, 8500)));
        assertThat(parser.frontEnd(), is(proxy(defaultHost, 8500)));
    }

    @Test
    public void frontEndReceivesOptionalLocalHostAndPort() throws Exception {
        parse(DPE_HOST_OPT, "10.2.9.100", DPE_PORT_OPT, "8500");

        assertThat(parser.localAddress(), is(proxy("10.2.9.100", 8500)));
        assertThat(parser.frontEnd(), is(proxy("10.2.9.100", 8500)));
    }

    @Test
    public void dpeReceivesOptionalPoolSize() throws Exception {
        parse(POOL_OPT, "10");

        assertThat(parser.poolSize(), is(10));
    }

    @Test
    public void dpeUsesDefaultEmptyDescription() throws Exception {
        parse();

        assertThat(parser.description(), is(""));
    }

    @Test
    public void dpeReceivesOptionalDescription() throws Exception {
        parse(DESC_OPT, "A processing DPE");

        assertThat(parser.description(), is("A processing DPE"));
    }

    @Test
    public void dpeUsesDefaultReportInterval() throws Exception {
        parse();

        assertThat(parser.reportInterval(), is(10_000L));
    }

    @Test
    public void dpeReceivesOptionalReportInterval() throws Exception {
        parse(REPORT_OPT, "20");

        assertThat(parser.reportInterval(), is(20_000L));
    }


    private void parse(String... args) throws Exception {
        parser.parse(args);
    }

    private xMsgProxyAddress proxy(String host) throws Exception {
        return new xMsgProxyAddress(host, DpeOptionsParser.PROXY_PORT);
    }

    private xMsgProxyAddress proxy(String host, int port) throws Exception {
        return new xMsgProxyAddress(host, port);
    }
}
