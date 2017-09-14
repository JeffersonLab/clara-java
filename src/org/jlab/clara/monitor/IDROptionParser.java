package org.jlab.clara.monitor;

/**
 * Created by gurjyan on 9/13/17.
 */
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

    private final OptionSpec<String> proxyHost;
    private final OptionSpec<Integer> proxyPort;
    private final OptionSpec<String> dbHost;

    private OptionParser parser;
    private OptionSet options;

    private String m_host = "129.57.70.24";
    private int m_port = 9000;
    private String db_host ="claraweb.jlab.org";

    IDROptionParser() {
        parser = new OptionParser();

        proxyHost = parser.acceptsAll(asList("m-host")).withRequiredArg();
        proxyPort = parser.acceptsAll(asList("m-port"))
            .withRequiredArg().ofType(Integer.class);

        dbHost = parser.acceptsAll(asList("db-host")).withRequiredArg();

        parser.acceptsAll(asList("h", "help")).forHelp();
    }

    public void parse(String[] args) {
        try {
            options = parser.parse(args);

            if (!options.has(proxyHost)) {
                System.out.println("The monitoring xMsg proxy host is not specified, using the default host: clara1601.jlab.org");
            } else {
                m_host = options.valueOf(proxyHost);
            }

            if (!options.has(proxyPort)) {
                System.out.println("The monitoring xMsg proxy is not specified, using the default port = 9000");
            } else {
                m_port = options.valueOf(proxyPort);
            }

            if (!options.has(dbHost)) {
                System.out.println("The InfluxDB host is not specified, using the default DB = claraweb.jlab.org");
            } else {
                db_host = options.valueOf(dbHost);
            }

        } catch (OptionException e) {
            System.out.println(e.getMessage());
        }
    }

    public String getM_host() {
        return m_host;
    }

    public int getM_port() {
        return m_port;
    }

    public String getDb_host() {
        return db_host;
    }

    public boolean hasHelp() {
        return options.has("help");
    }

    public String usage() {
        return String.format("usage: idr [options]%n%n  Options:%n")
            + OptUtils.optionHelp(proxyHost, "hostname", "use given host for the monitor xMsg-proxy")
            + OptUtils.optionHelp(proxyPort, "port", "use given port for the monitor xMsg-proxy")
            + OptUtils.optionHelp(dbHost, "hostname", "the host where InfluxDB is running");
    }

}
