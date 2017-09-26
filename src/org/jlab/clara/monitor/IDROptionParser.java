/*
 *   Copyright (c) 2017.  Jefferson Lab (JLab). All rights reserved. Permission
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

package org.jlab.clara.monitor;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.jlab.clara.util.OptUtils;

import static java.util.Arrays.asList;

/**
 * Parses the DPE settings from the command line.
 */
class IDROptionParser {

    private static final String DEFAULT_DB_HOST = "claraweb.jlab.org";
    private static final String DEFAULT_FE_HOST = "129.57.70.24";
    private static final int DEFAULT_FE_PORT = 9000;

    private final OptionSpec<String> proxyHost;
    private final OptionSpec<Integer> proxyPort;
    private final OptionSpec<String> dbHost;

    private OptionParser parser;
    private OptionSet options;

    IDROptionParser() {
        parser = new OptionParser();

        proxyHost = parser.acceptsAll(asList("fe-host", "m-host"))
                .withRequiredArg()
                .defaultsTo(DEFAULT_FE_HOST);

        proxyPort = parser.acceptsAll(asList("fe-port", "m-port"))
                .withRequiredArg()
                .ofType(Integer.class)
                .defaultsTo(DEFAULT_FE_PORT);

        dbHost = parser.acceptsAll(asList("db-host"))
                .withRequiredArg()
                .defaultsTo(DEFAULT_DB_HOST);

        parser.acceptsAll(asList("h", "help")).forHelp();
    }

    public boolean parse(String[] args) {
        try {
            options = parser.parse(args);

            if (!options.has(proxyHost)) {
                System.out.println("Using the default proxy host: clara1601.jlab.org");
            }
            if (!options.has(proxyPort)) {
                System.out.println("Using the default proxy port: 9000");
            }
            if (!options.has(dbHost)) {
                System.out.println("Using the default db host: claraweb.jlab.org");
            }

            return true;
        } catch (OptionException e) {
            System.err.println("error: " + e.getMessage());
            return false;
        }
    }

    public String getProxyHost() {
        return options.valueOf(proxyHost);
    }

    public int getProxyPort() {
        return options.valueOf(proxyPort);
    }

    public String getDataBaseHost() {
        return options.valueOf(dbHost);
    }

    public boolean hasHelp() {
        return options.has("help");
    }

    public String usage() {
        return String.format("usage: idr [options]%n%n  Options:%n")
            + OptUtils.optionHelp(proxyHost, "hostname", "the host used by the monitoring proxy")
            + OptUtils.optionHelp(proxyPort, "port", "the port used by the monitoring proxy")
            + OptUtils.optionHelp(dbHost, "hostname", "the host where the database is running");
    }

}
