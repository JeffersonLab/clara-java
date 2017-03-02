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

package org.jlab.clara.std.cli;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.jlab.coda.xmsg.core.xMsgUtil;

class RunConfig {

    private String orchestrator;
    private String localHost;
    private String configFile;
    private String filesList;
    private String inputDir;
    private String outputDir;
    private boolean useFrontEnd;
    private String session;
    private int maxNodes;
    private int maxThreads;
    private String farmFlavor;
    private String farmLoadingZone;
    private int farmMemory;
    private String farmTrack;
    private String farmOS;
    private int farmCPU;
    private int farmDisk;
    private int farmTime;

    RunConfig() {
        setDefaults();
    }

    public void setDefaults() {
        String claraHome = claraHome();
        this.orchestrator = "org.jlab.clas.std.orchestrators.CloudOrchestrator";
        this.localHost = xMsgUtil.localhost();
        this.configFile = defaultConfigFile(claraHome);
        this.filesList = defaultFileList(claraHome);
        this.inputDir = Paths.get(claraHome, "data", "input").toString();
        this.outputDir = Paths.get(claraHome, "data", "output").toString();
        this.useFrontEnd = true;
        this.session = "";
        this.maxNodes = 1;
        this.maxThreads = Runtime.getRuntime().availableProcessors();
        this.farmFlavor = "jlab";
        this.farmLoadingZone = "undefined";
        this.farmMemory = 70;
        this.farmTrack = "debug";
        this.farmOS = "centos7";
        this.farmCPU = 72;
        this.farmDisk = 3;
        this.farmTime = 1440;
    }

    public static String claraHome() {
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

    public void setLocalHost(String localHost) {
        this.localHost = localHost;
    }

    public String getConfigFile() {
        return configFile;
    }

    public void setConfigFile(String configFile) {
        validateFile(configFile);
        this.configFile = configFile;
    }

    public String getFilesList() {
        return filesList;
    }

    public void setFilesList(String filesList) {
        validateFile(filesList);
        this.filesList = filesList;
    }

    public String getInputDir() {
        return inputDir;
    }

    public void setInputDir(String inputDir) {
        validateDirectory(inputDir);
        this.inputDir = inputDir;
    }

    public String getOutputDir() {
        return outputDir;
    }

    public void setOutputDir(String outputDir) {
        validateDirectory(outputDir);
        this.outputDir = outputDir;
    }

    public boolean isUseFrontEnd() {
        return useFrontEnd;
    }

    public String getSession() {
        return session;
    }

    public void setSession(String session) {
        this.session = session;
    }

    public int getMaxNodes() {
        return maxNodes;
    }

    public void setMaxNodes(int maxNodes) {
        this.maxNodes = maxNodes;
    }

    public int getMaxThreads() {
        return maxThreads;
    }

    public void setMaxThreads(int maxThreads) {
        if (maxThreads <= 0) {
            throw new IllegalArgumentException("invalid number of threads");
        }
        this.maxThreads = maxThreads;
    }

    public String getFarmFlavor() {
        return farmFlavor;
    }

    public void setFarmFlavor(String farmFlavor) {
        if ("".equals(farmFlavor)) {
            throw new IllegalArgumentException("empty argument");
        }
        if (!"dps".equals(farmFlavor) && !"jlab".equals(farmFlavor)) {
            throw new IllegalArgumentException("invalid argument");
        }
        this.farmFlavor = farmFlavor;
    }

    public String getFarmLoadingZone() {
        return farmLoadingZone;
    }

    public void setFarmLoadingZone(String farmLoadingZone) {
        validateDirectory(farmLoadingZone);
        this.farmLoadingZone = farmLoadingZone;
    }

    public int getFarmMemory() {
        return farmMemory;
    }

    public void setFarmMemory(int farmMemory) {
        if (farmMemory <= 0) {
            throw new IllegalArgumentException("invalid number");
        }
        this.farmMemory = farmMemory;
    }

    public String getFarmTrack() {
        return farmTrack;
    }

    public void setFarmTrack(String farmTrack) {
        if ("".equals(farmTrack)) {
            throw new IllegalArgumentException("empty argument");
        }
        this.farmTrack = farmTrack;
    }

    public String getFarmOS() {
        return farmOS;
    }

    public void setFarmOS(String farmOS) {
        if ("".equals(farmOS)) {
            throw new IllegalArgumentException("empty argument");
        }
        this.farmOS = farmOS;
    }

    public int getFarmCPU() {
        return farmCPU;
    }

    public void setFarmCPU(int farmCPU) {
        if (farmCPU <= 0) {
            throw new IllegalArgumentException("invalid number");
        }
        this.farmCPU = farmCPU;
    }

    public int getFarmDisk() {
        return farmDisk;
    }

    public void setFarmDisk(int farmDisk) {
        if (farmDisk <= 0) {
            throw new IllegalArgumentException("invalid number");
        }
        this.farmDisk = farmDisk;
    }

    public int getFarmTime() {
        return farmTime;
    }

    public void setFarmTime(int farmTime) {
        if (farmTime <= 0) {
            throw new IllegalArgumentException("invalid number");
        }
        this.farmTime = farmTime;
    }

    private static void validateFile(String file) {
        if (file.isEmpty()) {
            throw new IllegalArgumentException("empty argument");
        }
        Path path = Paths.get(file);
        if (!Files.exists(path)) {
            throw new IllegalArgumentException("file does not exist");
        }
        if (!Files.isRegularFile(path)) {
            throw new IllegalArgumentException("file is not a regular file");
        }
    }

    private static void validateDirectory(String dir) {
        if (dir.isEmpty()) {
            throw new IllegalArgumentException("empty argument");
        }
        Path path = Paths.get(dir);
        if (!Files.exists(path)) {
            throw new IllegalArgumentException("directory does not exist");
        }
        if (!Files.isDirectory(path)) {
            throw new IllegalArgumentException("directory should be a directory");
        }
    }
}
