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
public class ClaraAddress {

    private xMsgTopic topic;

    private String dpeHost;
    private int dpePort;
    private String dpeName;
    private String containerName;
    private String engineName;
    private String name;

    private ClaraAddress(String dpeHost, int dpePort, String container, String engine){
        this.dpeHost = dpeHost;
        this.dpePort = dpePort;
        this.dpeName = dpeHost+ CConstants.PRXHOSTPORT_SEP+Integer.toString(dpePort);
        this.containerName = container;
        this.engineName = engine;
        topic = xMsgTopic.build(dpeName,containerName,engineName);
        name = topic.toString();
    }

    public static ClaraAddress orchestrator(String name, String dpeHost, int dpePort){
        ClaraAddress a =  new ClaraAddress(dpeHost,
                dpePort,
                xMsgTopic.ANY,
                xMsgTopic.ANY);
        a.setName(name);
        return a;
    }

    public static ClaraAddress orchestrator(String name) throws IOException {
        ClaraAddress a =  new ClaraAddress(xMsgUtil.localhost(),
                xMsgConstants.DEFAULT_PORT.getIntValue(),
                xMsgTopic.ANY,
                xMsgTopic.ANY);
        a.setName(name);
        return a;
    }

    public static ClaraAddress orchestrator(String name, String dpeHost){
        ClaraAddress a =  new ClaraAddress(dpeHost,
                xMsgConstants.DEFAULT_PORT.getIntValue(),
                xMsgTopic.ANY,
                xMsgTopic.ANY);
        a.setName(name);
        return a;
    }

    public static ClaraAddress dpe(String dpeHost, int dpePort){
        return new ClaraAddress(dpeHost,
                dpePort,
                xMsgTopic.ANY,
                xMsgTopic.ANY);
    }

    public static ClaraAddress dpe(String dpeHost){
        return new ClaraAddress(dpeHost,
                xMsgConstants.DEFAULT_PORT.getIntValue(),
                xMsgTopic.ANY,
                xMsgTopic.ANY);
    }

    public static ClaraAddress dpe() throws IOException {
        return new ClaraAddress(xMsgUtil.localhost(),
                xMsgConstants.DEFAULT_PORT.getIntValue(),
                xMsgTopic.ANY,
                xMsgTopic.ANY);
    }

    public static ClaraAddress container(String dpeHost, int dpePort, String container){
        return new ClaraAddress(dpeHost,
                dpePort,
                container,
                xMsgTopic.ANY);
    }

    public static ClaraAddress container(String dpeHost, String container){
        return new ClaraAddress(dpeHost,
                xMsgConstants.DEFAULT_PORT.getIntValue(),
                container,
                xMsgTopic.ANY);
    }

    public static ClaraAddress container(String container) throws IOException {
        return new ClaraAddress(xMsgUtil.localhost(),
                xMsgConstants.DEFAULT_PORT.getIntValue(),
                container,
                xMsgTopic.ANY);
    }

    public static ClaraAddress service(String dpeHost, int dpePort, String container, String engine){
        return new ClaraAddress(dpeHost,
                dpePort,
                container,
                engine);
    }

    public static ClaraAddress service(String dpeHost, String container, String engine){
        return new ClaraAddress(dpeHost,
                xMsgConstants.DEFAULT_PORT.getIntValue(),
                container,
                engine);
    }

    public static ClaraAddress service(String container, String engine) throws IOException {
        return new ClaraAddress(xMsgUtil.localhost(),
                xMsgConstants.DEFAULT_PORT.getIntValue(),
                container,
                engine);
    }

    public xMsgTopic getTopic() {
        return topic;
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

    public boolean isDpe(){
        return getName().contains(xMsgTopic.SEPARATOR) &&
                getName().contains(CConstants.PRXHOSTPORT_SEP);
    }
    public boolean isContainer(){
        String n = getName();
        int count = n.length() - n.replace(xMsgTopic.SEPARATOR, "").length();
        return count == 1;
    }

    public boolean isService(){
        String n = getName();
        int count = n.length() - n.replace(xMsgTopic.SEPARATOR, "").length();
        return count == 2;

    }

    public xMsgPrxAddress getProxyAddress(){
        return new xMsgPrxAddress(getDpeHost(), getDpePort());
    }
}
