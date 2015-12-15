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

package org.jlab.clara.sys;

import static java.util.Arrays.asList;

import java.io.IOException;

import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * Parses the DPE settings from the command line.
 */
class DpeOptionsParser {

    public static final int PROXY_PORT = xMsgConstants.DEFAULT_PORT;
    public static final int REG_PORT = xMsgConstants.REGISTRAR_PORT;

    private OptionParser parser;
    private OptionSet options;

    private final OptionSpec<String> dpeHost;
    private final OptionSpec<Integer> dpePort;

    private final OptionSpec<Void> isFrontEnd;

    private final OptionSpec<String> feHost;
    private final OptionSpec<Integer> fePort;

    private final OptionSpec<Integer> poolSize;
    private final OptionSpec<String> description;

    private boolean fe;
    private xMsgProxyAddress localAddress;
    private xMsgProxyAddress frontEndAddress;


    DpeOptionsParser() {
        parser = new OptionParser();

        isFrontEnd = parser.acceptsAll(asList("fe", "frontend"));

        dpeHost = parser.accepts("dpe_host").withRequiredArg();
        dpePort = parser.accepts("dpe_port").withRequiredArg().ofType(Integer.class);

        feHost = parser.accepts("fe_host").withRequiredArg();
        fePort = parser.accepts("fe_port").withRequiredArg().ofType(Integer.class);

        poolSize = parser.accepts("poolsize").withRequiredArg().ofType(Integer.class);
        description = parser.accepts("description").withRequiredArg();

        parser.acceptsAll(asList("h", "help")).forHelp();
    }

    public void parse(String[] args) {
        try {
            options = parser.parse(args);

            // Get local DPE address
            String localHost = valueOf(dpeHost, xMsgUtil.localhost());
            int localPort = valueOf(dpePort, PROXY_PORT);
            localAddress = new xMsgProxyAddress(localHost, localPort);

            // Act as front-end by default (no need to pass isFrontEnd),
            // but if feHost or fePort are passed and not isFrontEnd,
            // act as a worker DPE with remote front-end
            fe = true;
            if (!options.has(isFrontEnd) && (options.has(feHost) || options.has(fePort))) {
                fe = false;
            }

            if (fe) {
                // Get local FE address (use local DPE by default)
                String host = valueOf(feHost, localAddress.host());
                int port = valueOf(fePort, localAddress.port());
                frontEndAddress = new xMsgProxyAddress(host, port);
            } else {
                // Get remote FE address
                if (!options.has(feHost)) {
                    error("The remote front-end host is required");
                }
                String host = options.valueOf(feHost);
                int port = valueOf(fePort, PROXY_PORT);
                frontEndAddress = new xMsgProxyAddress(host, port);
            }

        } catch (OptionException e) {
            throw new DpeOptionsException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void error(String msg) {
        throw new DpeOptionsException(msg);
    }

    private <V> V valueOf(OptionSpec<V> spec, V defaultValue) {
        try {
            if (options.has(spec)) {
                return options.valueOf(spec);
            }
            return defaultValue;
        } catch (OptionException e) {
            throw new DpeOptionsException(e);
        }
    }

    public xMsgProxyAddress localAddress() {
        return localAddress;
    }

    public xMsgProxyAddress  frontEnd() {
        return frontEndAddress;
    }

    public int poolSize() {
        return valueOf(poolSize, xMsgConstants.DEFAULT_POOL_SIZE);
    }

    public String description() {
        return valueOf(description, "");
    }

    public boolean isFrontEnd() {
        return fe;
    }

    public boolean hasHelp() {
        return options.has("help");
    }

    public String usage() {
        return String.format("usage: j_dpe [options]%n%n  Options:%n")
             + optionHelp(dpeHost, "hostname", "use given host for this DPE")
             + optionHelp(dpePort, "port", "use given port for this DPE")
             + optionHelp(isFrontEnd, null, "use this DPE as front-end")
             + optionHelp(feHost, "hostname", "the host used by the front-end")
             + optionHelp(fePort, "port", "the port used by the front-end")
             + optionHelp(poolSize, "size", "the subscriptions poolsize for this DPE")
             + optionHelp(description, "string", "a short description of this DPE");
    }

    private static <V> String optionHelp(OptionSpec<V> spec, String arg, String... help) {
        StringBuilder sb = new StringBuilder();
        String[] lhs = new String[help.length];
        lhs[0] = optionName(spec, arg);
        for (int i = 0; i < help.length; i++) {
            sb.append(String.format("  %-22s  %s%n", lhs[i] == null ? "" : lhs[i], help[i]));
        }
        return sb.toString();
    }

    private static <V> String optionName(OptionSpec<V> spec, String arg) {
        StringBuilder sb = new StringBuilder();
        sb.append("-").append(spec.options().get(0));
        if (arg != null) {
            sb.append(" <").append(arg).append(">");
        }
        return sb.toString();
    }

    static class DpeOptionsException extends RuntimeException {

        DpeOptionsException(String message) {
            super(message);
        }

        DpeOptionsException(String message, Throwable cause) {
            super(message, cause);
        }

        DpeOptionsException(Throwable cause) {
            super(cause);
        }
    }
}
