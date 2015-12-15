package org.jlab.clara.sys;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.jlab.clara.sys.DpeOptionsParser.DpeOptionsException;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class DpeOptionsParserTest {

    private static final String isFeOpt = "-fe";

    private static final String dpeHostOpt = "-dpe_host";
    private static final String dpePortOpt = "-dpe_port";

    private static final String feHostOpt = "-fe_host";
    private static final String fePortOpt = "-fe_port";

    private static final String poolOpt = "-poolsize";
    private static final String descOpt = "-description";

    private DpeOptionsParser parser;

    private final String defaultHost;

    public DpeOptionsParserTest() throws Exception {
        defaultHost = xMsgUtil.localhost();
    }


    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

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
        parse(feHostOpt, "10.2.9.100");

        assertFalse(parser.isFrontEnd());
    }

    @Test
    public void workerUsesDefaultLocalAddress() throws Exception {
        parse(feHostOpt, "10.2.9.100");

        assertThat(parser.localAddress(), is(proxy(defaultHost)));
    }

    @Test
    public void workerReceivesOptionalLocalHost() throws Exception {
        parse(dpeHostOpt, "10.2.9.4", feHostOpt, "10.2.9.100");

        assertThat(parser.localAddress(), is(proxy("10.2.9.4")));
    }

    @Test
    public void workerReceivesOptionalLocalPort() throws Exception {
        parse(dpePortOpt, "8500", feHostOpt, "10.2.9.100");

        assertThat(parser.localAddress(), is(proxy(defaultHost, 8500)));
    }

    @Test
    public void workerReceivesRemoteFrontEndAddress() throws Exception {
        parse(feHostOpt, "10.2.9.100");

        assertThat(parser.frontEnd(), is(proxy("10.2.9.100")));
    }

    @Test
    public void workerReceivesRemoteFrontEndAddressAndPort() throws Exception {
        parse(feHostOpt, "10.2.9.100", fePortOpt, "9000");

        assertThat(parser.frontEnd(), is(proxy("10.2.9.100", 9000)));
    }

    @Test
    public void workerRequiresRemoteFrontEndHostWhenPortIsGiven() throws Exception {
        expectedEx.expect(DpeOptionsException.class);
        expectedEx.expectMessage("remote front-end host is required");

        parse(fePortOpt, "9000");
    }

    @Test
    public void frontEndUsesDefaultLocalAddress() throws Exception {
        parse();

        assertThat(parser.localAddress(), is(proxy(defaultHost)));
        assertThat(parser.frontEnd(), is(proxy(defaultHost)));
    }

    @Test
    public void frontEndReceivesOptionalLocalHost() throws Exception {
        parse(dpeHostOpt, "10.2.9.100");

        assertThat(parser.localAddress(), is(proxy("10.2.9.100")));
        assertThat(parser.frontEnd(), is(proxy("10.2.9.100")));
    }

    @Test
    public void frontEndReceivesOptionalHost() throws Exception {
        parse(isFeOpt, feHostOpt, "10.2.9.100");

        assertThat(parser.localAddress(), is(proxy(defaultHost)));
        assertThat(parser.frontEnd(), is(proxy("10.2.9.100")));
    }

    @Test
    public void frontEndReceivesOptionalPort() throws Exception {
        parse(isFeOpt, fePortOpt, "9500");

        assertThat(parser.localAddress(), is(proxy(defaultHost)));
        assertThat(parser.frontEnd(), is(proxy(defaultHost, 9500)));
    }

    @Test
    public void dpeReceivesOptionalPoolSize() throws Exception {
        parse(poolOpt, "10");

        assertThat(parser.poolSize(), is(10));
    }

    @Test
    public void dpeUsesDefaultEmptyDescription() throws Exception {
        parse();

        assertThat(parser.description(), is(""));
    }

    @Test
    public void dpeReceivesOptionalDescription() throws Exception {
        parse(descOpt, "A processing DPE");

        assertThat(parser.description(), is("A processing DPE"));
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
