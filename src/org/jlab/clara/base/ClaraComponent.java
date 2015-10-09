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

package org.jlab.clara.base;

import org.jlab.clara.util.CConstants;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.net.xMsgPrxAddress;

import java.io.IOException;

/**
 *  Clara addressee. This is used to define
 *  service, container or DPE addresses.
 *
 * @author gurjyan
 * @since 4.x
 */
public class ClaraComponent {

    private xMsgTopic topic;

    private String dpeLang;
    private String dpeHost;
    private int dpePort;
    private String dpeName;

    private String containerName;
    private String engineName;
    private String name;


    private boolean isOrchestrator = false;
    private boolean isDpe = false;
    private boolean isContainer = false;
    private boolean isService = false;

    private ClaraComponent(String dpeLang, String dpeHost, int dpePort, String container, String engine){
        this.dpeLang = dpeLang;
        this.dpeHost = dpeHost;
        this.dpePort = dpePort;
        this.dpeName = dpeHost + CConstants.PRXHOSTPORT_SEP +
                Integer.toString(dpePort) + CConstants.LANG_SEP + dpeLang;
        this.containerName = container;
        this.engineName = engine;
        topic = xMsgTopic.build(dpeName,containerName,engineName);
        name = topic.toString();
    }

    public static ClaraComponent orchestrator(String name, String dpeHost, int dpePort, String dpeLang){
        ClaraComponent a = new ClaraComponent(dpeLang,
                dpeHost,
                dpePort,
                xMsgTopic.ANY,
                xMsgTopic.ANY);
        a.setName(name);
        a.isOrchestrator = true;
        return a;
    }

    public static ClaraComponent orchestrator(String name, String dpeHost, String dpeLang){
        ClaraComponent a = new ClaraComponent(dpeLang,
                dpeHost,
                xMsgConstants.DEFAULT_PORT.getIntValue(),
                xMsgTopic.ANY,
                xMsgTopic.ANY);
        a.setName(name);
        a.isOrchestrator = true;
        return a;
    }

    public static ClaraComponent orchestrator(String name, String dpeHost){
        ClaraComponent a = new ClaraComponent(CConstants.JAVA_LANG,
                dpeHost,
                xMsgConstants.DEFAULT_PORT.getIntValue(),
                xMsgTopic.ANY,
                xMsgTopic.ANY);
        a.setName(name);
        a.isOrchestrator = true;
        return a;
    }

    public static ClaraComponent orchestrator(String name) throws IOException {
        ClaraComponent a = new ClaraComponent(CConstants.JAVA_LANG,
                xMsgUtil.localhost(),
                xMsgConstants.DEFAULT_PORT.getIntValue(),
                xMsgTopic.ANY,
                xMsgTopic.ANY);
        a.setName(name);
        a.isOrchestrator = true;
        return a;
    }


    public static ClaraComponent dpe(String dpeHost, int dpePort, String dpeLang){
        ClaraComponent a = new ClaraComponent(dpeLang,
                dpeHost,
                dpePort,
                xMsgTopic.ANY,
                xMsgTopic.ANY);
        a.isDpe = true;
        return a;
    }

    public static ClaraComponent dpe(String dpeHost, String dpeLang){
        ClaraComponent a = new ClaraComponent(dpeLang,
                dpeHost,
                xMsgConstants.DEFAULT_PORT.getIntValue(),
                xMsgTopic.ANY,
                xMsgTopic.ANY);
        a.isDpe = true;
        return a;
    }

    public static ClaraComponent dpe(String dpeHost){
        ClaraComponent a = new ClaraComponent(CConstants.JAVA_LANG,
                dpeHost,
                xMsgConstants.DEFAULT_PORT.getIntValue(),
                xMsgTopic.ANY,
                xMsgTopic.ANY);
        a.isDpe = true;
        return a;
    }

    public static ClaraComponent dpe() throws IOException {
        ClaraComponent a = new ClaraComponent(CConstants.JAVA_LANG,
                xMsgUtil.localhost(),
                xMsgConstants.DEFAULT_PORT.getIntValue(),
                xMsgTopic.ANY,
                xMsgTopic.ANY);
        a.isDpe = true;
        return a;
    }

    public static ClaraComponent container(String dpeHost, int dpePort, String dpeLang, String container){
        ClaraComponent a = new ClaraComponent(dpeLang,
                dpeHost,
                dpePort,
                container,
                xMsgTopic.ANY);
        a.isContainer = true;
        return a;
    }

    public static ClaraComponent container(String dpeHost, String dpeLang, String container){
        ClaraComponent a = new ClaraComponent(dpeLang,
                dpeHost,
                xMsgConstants.DEFAULT_PORT.getIntValue(),
                container,
                xMsgTopic.ANY);
        a.isContainer = true;
        return a;
    }

    public static ClaraComponent container(String dpeHost, String container){
        ClaraComponent a = new ClaraComponent(CConstants.JAVA_LANG,
                dpeHost,
                xMsgConstants.DEFAULT_PORT.getIntValue(),
                container,
                xMsgTopic.ANY);
        a.isContainer = true;
        return a;
    }

    public static ClaraComponent container(String container) throws IOException {
        ClaraComponent a = new ClaraComponent(CConstants.JAVA_LANG,
                xMsgUtil.localhost(),
                xMsgConstants.DEFAULT_PORT.getIntValue(),
                container,
                xMsgTopic.ANY);
        a.isContainer = true;
        return a;
    }

    public static ClaraComponent service(String dpeHost, int dpePort, String dpeLang, String container, String engine){
        ClaraComponent a = new ClaraComponent(dpeLang,
                dpeHost,
                dpePort,
                container,
                engine);
        a.isService = true;
        return a;
    }

    public static ClaraComponent service(String dpeHost, String dpeLang, String container, String engine){
        ClaraComponent a = new ClaraComponent(dpeLang,
                dpeHost,
                xMsgConstants.DEFAULT_PORT.getIntValue(),
                container,
                engine);
        a.isService = true;
        return a;
    }

    public static ClaraComponent service(String dpeHost, String container, String engine){
        ClaraComponent a = new ClaraComponent(CConstants.JAVA_LANG,
                dpeHost,
                xMsgConstants.DEFAULT_PORT.getIntValue(),
                container,
                engine);
        a.isService = true;
        return a;
    }

    public static ClaraComponent service(String container, String engine) throws IOException {
        ClaraComponent a = new ClaraComponent(CConstants.JAVA_LANG,
                xMsgUtil.localhost(),
                xMsgConstants.DEFAULT_PORT.getIntValue(),
                container,
                engine);
        a.isService = true;
        return a;
    }

    public xMsgTopic getTopic() {
        return topic;
    }

    public String getDpeLang() {
        return dpeLang;
    }

    public String getDpeHost() {
        return dpeHost;
    }

    public int getDpePort() {
        return dpePort;
    }

    public String getDpeName() {
        return dpeName;
    }

    public String getContainerName() {
        return containerName;
    }

    public String getEngineName() {
        return engineName;
    }

    public String getName(){
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isOrchestrator() {
        return isOrchestrator;
    }

    public boolean isDpe() {
        return isDpe;
    }

    public boolean isContainer() {
        return isContainer;
    }

    public boolean isService() {
        return isService;
    }

    public xMsgPrxAddress getProxyAddress(){
        return new xMsgPrxAddress(getDpeHost(), getDpePort());
    }
}
