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

import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.util.CConstants;
import org.jlab.clara.util.ClaraUtil;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;

import java.io.IOException;

/**
 *  Clara component. This is used to define
 *  service, container, DPE and orchestrator components.
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
    private String engineClass = CConstants.UNDEFINED;

    private String name;

    private int subscriptionPoolSize;

    private boolean isOrchestrator = false;
    private boolean isDpe = false;
    private boolean isContainer = false;
    private boolean isService = false;

    private ClaraComponent(String dpeLang, String dpeHost, int dpePort, String container, String engine, int subscriptionPoolSize){
        this.dpeLang = dpeLang;
        this.subscriptionPoolSize = subscriptionPoolSize;
        this.dpeHost = dpeHost;
        this.dpePort = dpePort;
        this.dpeName = dpeHost + CConstants.PRXHOSTPORT_SEP +
                Integer.toString(dpePort) + CConstants.LANG_SEP + dpeLang;
        this.containerName = container;
        this.engineName = engine;
        topic = xMsgTopic.build(dpeName,containerName,engineName);
        name = topic.toString();
    }


    public static ClaraComponent orchestrator(String name, String dpeHost, int dpePort, String dpeLang, int subscriptionPoolSize){
        ClaraComponent a = new ClaraComponent(dpeLang,
                dpeHost,
                dpePort,
                xMsgTopic.ANY,
                xMsgTopic.ANY,
                subscriptionPoolSize);
        a.setName(name);
        a.isOrchestrator = true;
        return a;
    }

    public static ClaraComponent orchestrator(String name, String dpeHost, String dpeLang, int subscriptionPoolSize){
        ClaraComponent a = new ClaraComponent(dpeLang,
                dpeHost,
                xMsgConstants.DEFAULT_PORT.getIntValue(),
                xMsgTopic.ANY,
                xMsgTopic.ANY,
                subscriptionPoolSize);
        a.setName(name);
        a.isOrchestrator = true;
        return a;
    }

    public static ClaraComponent orchestrator(String name, String dpeHost, int subscriptionPoolSize){
        ClaraComponent a = new ClaraComponent(CConstants.JAVA_LANG,
                dpeHost,
                xMsgConstants.DEFAULT_PORT.getIntValue(),
                xMsgTopic.ANY,
                xMsgTopic.ANY,
                subscriptionPoolSize);
        a.setName(name);
        a.isOrchestrator = true;
        return a;
    }

    public static ClaraComponent orchestrator(String name, int subscriptionPoolSize) throws IOException {
        ClaraComponent a = new ClaraComponent(CConstants.JAVA_LANG,
                xMsgUtil.localhost(),
                xMsgConstants.DEFAULT_PORT.getIntValue(),
                xMsgTopic.ANY,
                xMsgTopic.ANY,
                subscriptionPoolSize);
        a.setName(name);
        a.isOrchestrator = true;
        return a;
    }


    public static ClaraComponent dpe(String dpeHost, int dpePort, String dpeLang, int subscriptionPoolSize){
        ClaraComponent a = new ClaraComponent(dpeLang,
                dpeHost,
                dpePort,
                xMsgTopic.ANY,
                xMsgTopic.ANY,
                subscriptionPoolSize);
        a.isDpe = true;
        return a;
    }

    public static ClaraComponent dpe(String dpeHost, String dpeLang, int subscriptionPoolSize){
        ClaraComponent a = new ClaraComponent(dpeLang,
                dpeHost,
                xMsgConstants.DEFAULT_PORT.getIntValue(),
                xMsgTopic.ANY,
                xMsgTopic.ANY,
                subscriptionPoolSize);
        a.isDpe = true;
        return a;
    }

    public static ClaraComponent dpe(String dpeHost, int subscriptionPoolSize){
        ClaraComponent a = new ClaraComponent(CConstants.JAVA_LANG,
                dpeHost,
                xMsgConstants.DEFAULT_PORT.getIntValue(),
                xMsgTopic.ANY,
                xMsgTopic.ANY,
                subscriptionPoolSize);
        a.isDpe = true;
        return a;
    }

    public static ClaraComponent dpe(int subscriptionPoolSize) throws IOException {
        ClaraComponent a = new ClaraComponent(CConstants.JAVA_LANG,
                xMsgUtil.localhost(),
                xMsgConstants.DEFAULT_PORT.getIntValue(),
                xMsgTopic.ANY,
                xMsgTopic.ANY,
                subscriptionPoolSize);
        a.isDpe = true;
        return a;
    }

    public static ClaraComponent dpe(String dpeCanonicalName) throws ClaraException {
        if (!ClaraUtil.isCanonicalName(dpeCanonicalName)) {
            throw new ClaraException("Clara-Error: not a canonical name.");
        }
        return dpe(ClaraUtil.getDpeHost(dpeCanonicalName),
                ClaraUtil.getDpePort(dpeCanonicalName),
                ClaraUtil.getDpeLang(dpeCanonicalName),
                xMsgConstants.DEFAULT_POOL_SIZE.getIntValue());

    }


    public static ClaraComponent container(String dpeHost, int dpePort, String dpeLang, String container, int subscriptionPoolSize){
        ClaraComponent a = new ClaraComponent(dpeLang,
                dpeHost,
                dpePort,
                container,
                xMsgTopic.ANY,
                subscriptionPoolSize);
        a.isContainer = true;
        return a;
    }

    public static ClaraComponent container(String dpeHost, String dpeLang, String container, int subscriptionPoolSize){
        ClaraComponent a = new ClaraComponent(dpeLang,
                dpeHost,
                xMsgConstants.DEFAULT_PORT.getIntValue(),
                container,
                xMsgTopic.ANY,
                subscriptionPoolSize);
        a.isContainer = true;
        return a;
    }

    public static ClaraComponent container(String dpeHost, String container, int subscriptionPoolSize){
        ClaraComponent a = new ClaraComponent(CConstants.JAVA_LANG,
                dpeHost,
                xMsgConstants.DEFAULT_PORT.getIntValue(),
                container,
                xMsgTopic.ANY,
                subscriptionPoolSize);
        a.isContainer = true;
        return a;
    }

    public static ClaraComponent container(String container, int subscriptionPoolSize) throws IOException {
        ClaraComponent a = new ClaraComponent(CConstants.JAVA_LANG,
                xMsgUtil.localhost(),
                xMsgConstants.DEFAULT_PORT.getIntValue(),
                container,
                xMsgTopic.ANY,
                subscriptionPoolSize);
        a.isContainer = true;
        return a;
    }

    public static ClaraComponent container(String containerCanonicalName) throws ClaraException {
        if (!ClaraUtil.isCanonicalName(containerCanonicalName)) {
            throw new ClaraException("Clara-Error: not a canonical name.");
        }
        return container(ClaraUtil.getDpeHost(containerCanonicalName),
        ClaraUtil.getDpePort(containerCanonicalName),
        ClaraUtil.getDpeLang(containerCanonicalName),
                ClaraUtil.getContainerName(containerCanonicalName), xMsgConstants.DEFAULT_POOL_SIZE.getIntValue());

    }

    public static ClaraComponent service(String dpeHost, int dpePort, String dpeLang, String container,
                                         String engine, String engineClass, int subscriptionPoolSize){
        ClaraComponent a = new ClaraComponent(dpeLang,
                dpeHost,
                dpePort,
                container,
                engine,
                subscriptionPoolSize);
        a.isService = true;
        a.engineClass = engineClass;
        return a;
    }

    public static ClaraComponent service(String dpeHost, String dpeLang, String container,
                                         String engine, String engineClass,int subscriptionPoolSize){
        ClaraComponent a = new ClaraComponent(dpeLang,
                dpeHost,
                xMsgConstants.DEFAULT_PORT.getIntValue(),
                container,
                engine,
                subscriptionPoolSize);
        a.isService = true;
        a.engineClass = engineClass;
        return a;
    }

    public static ClaraComponent service(String dpeHost, String container,
                                         String engine, String engineClass, int subscriptionPoolSize){
        ClaraComponent a = new ClaraComponent(CConstants.JAVA_LANG,
                dpeHost,
                xMsgConstants.DEFAULT_PORT.getIntValue(),
                container,
                engine,
                subscriptionPoolSize);
        a.isService = true;
        a.engineClass = engineClass;
        return a;
    }

    public static ClaraComponent service(String container,
                                         String engine, String engineClass, int subscriptionPoolSize) throws IOException {
        ClaraComponent a = new ClaraComponent(CConstants.JAVA_LANG,
                xMsgUtil.localhost(),
                xMsgConstants.DEFAULT_PORT.getIntValue(),
                container,
                engine,
                subscriptionPoolSize);
        a.isService = true;
        a.engineClass = engineClass;
        return a;
    }

    public static ClaraComponent service(String serviceCanonicalName) throws ClaraException {
        if (!ClaraUtil.isCanonicalName(serviceCanonicalName)) {
            throw new ClaraException("Clara-Error: not a canonical name.");
        }
        return service(ClaraUtil.getDpeHost(serviceCanonicalName),
                ClaraUtil.getDpePort(serviceCanonicalName),
                ClaraUtil.getDpeLang(serviceCanonicalName),
                ClaraUtil.getContainerName(serviceCanonicalName),
                ClaraUtil.getEngineName(serviceCanonicalName),
                CConstants.UNDEFINED, xMsgConstants.DEFAULT_POOL_SIZE.getIntValue());
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

    public String getEngineClass() {
        return engineClass;
    }

    public void setEngineClass(String engineClass) {
        this.engineClass = engineClass;
    }

    public String getName(){
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getSubscriptionPoolSize() {
        return subscriptionPoolSize;
    }

    public void setSubscriptionPoolSize(int subscriptionPoolSize) {
        this.subscriptionPoolSize = subscriptionPoolSize;
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

    public xMsgProxyAddress getProxyAddress() {
        return new xMsgProxyAddress(getDpeHost(), getDpePort());
    }
}
