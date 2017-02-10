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

package org.jlab.clara.cli;

import java.nio.file.Paths;

import org.jlab.coda.xmsg.core.xMsgUtil;

public class RunConfig {

    private String orchestrator;
    private String localHost;
    private String configFile;
    private String filesList;
    private boolean useFrontEnd;
    private String session;
    private int maxNodes;
    private int maxThreads;

    public RunConfig() {
        String claraHome = claraHome();
        this.orchestrator = "org.jlab.clas.std.orchestrators.CloudOrchestrator";
        this.localHost = xMsgUtil.localhost();
        this.configFile = defaultConfigFile(claraHome);
        this.filesList = defaultFileList(claraHome);
        this.useFrontEnd = true;
        this.session = "";
        this.maxNodes = 1;
        this.maxThreads = Runtime.getRuntime().availableProcessors();
    }

    private static String claraHome() {
        String claraHome = System.getenv("CLARA_HOME");
        if (claraHome == null) {
            throw new RuntimeException("Missing CLARA_HOME variable");
        }
        return claraHome;
    }

    private static String defaultConfigFile(String claraHome) {
        return Paths.get(claraHome, "plugins", "clas12", "config", "services.yaml").toString();
    }

    private static String defaultFileList(String claraHome) {
        return Paths.get(claraHome, "plugins", "clas12", "config", "files.list").toString();
    }

    public String getOrchestrator() {
        return orchestrator;
    }

    public String getLocalHost() {
        return localHost;
    }

    public String getConfigFile() {
        return configFile;
    }

    public String getFilesList() {
        return filesList;
    }

    public boolean isUseFrontEnd() {
        return useFrontEnd;
    }

    public String getSession() {
        return session;
    }

    public int getMaxNodes() {
        return maxNodes;
    }

    public int getMaxThreads() {
        return maxThreads;
    }
}
