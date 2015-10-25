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

    private String dpeCanonicalName;
    private String containerName;
    private String engineName;
    private String engineClass = CConstants.UNDEFINED;

    private String canonicalName;
    private String description;
    private String initialState;

    private int subscriptionPoolSize;

    private boolean isOrchestrator = false;
    private boolean isDpe = false;
    private boolean isContainer = false;
    private boolean isService = false;

    private ClaraComponent(String dpeLang, String dpeHost, int dpePort,
                           String container, String engine, String engineClass,
                           int subscriptionPoolSize, String description,
                           String initialState) {
        this.dpeLang = dpeLang;
        this.subscriptionPoolSize = subscriptionPoolSize;
        this.dpeHost = dpeHost;
        this.dpePort = dpePort;
        this.dpeCanonicalName = dpeHost + xMsgConstants.PRXHOSTPORT_SEP +
                Integer.toString(dpePort) + xMsgConstants.LANG_SEP + dpeLang;
        this.containerName = container;
        this.engineName = engine;
        this.engineClass = engineClass;
        topic = xMsgTopic.build(dpeCanonicalName, containerName, engineName);
        canonicalName = topic.toString();
        this.description = description;
        this.initialState = initialState;
    }


    public static ClaraComponent orchestrator(String name, String dpeHost, int dpePort, String dpeLang, int subscriptionPoolSize, String description) {
        ClaraComponent a = new ClaraComponent(dpeLang,
                dpeHost,
                dpePort,
                xMsgTopic.ANY,
                xMsgTopic.ANY,
                CConstants.UNDEFINED,
                subscriptionPoolSize, description,
                CConstants.UNDEFINED);
        a.setCanonicalName(name);
        a.isOrchestrator = true;
        return a;
    }

    public static ClaraComponent orchestrator(String name, String dpeHost, String dpeLang, int subscriptionPoolSize, String description) {
        ClaraComponent a = new ClaraComponent(dpeLang,
                dpeHost,
                xMsgConstants.DEFAULT_PORT,
                xMsgTopic.ANY,
                xMsgTopic.ANY,
                CConstants.UNDEFINED,
                subscriptionPoolSize, description,
                CConstants.UNDEFINED);
        a.setCanonicalName(name);
        a.isOrchestrator = true;
        return a;
    }

    public static ClaraComponent orchestrator(String name, String dpeHost, int subscriptionPoolSize, String description) {
        ClaraComponent a = new ClaraComponent(CConstants.JAVA_LANG,
                dpeHost,
                xMsgConstants.DEFAULT_PORT,
                xMsgTopic.ANY,
                xMsgTopic.ANY,
                CConstants.UNDEFINED,
                subscriptionPoolSize, description,
                CConstants.UNDEFINED);
        a.setCanonicalName(name);
        a.isOrchestrator = true;
        return a;
    }

    public static ClaraComponent orchestrator(String name, int subscriptionPoolSize, String description) throws IOException {
        ClaraComponent a = new ClaraComponent(CConstants.JAVA_LANG,
                xMsgUtil.localhost(),
                xMsgConstants.DEFAULT_PORT,
                xMsgTopic.ANY,
                xMsgTopic.ANY,
                CConstants.UNDEFINED,
                subscriptionPoolSize, description,
                CConstants.UNDEFINED);
        a.setCanonicalName(name);
        a.isOrchestrator = true;
        return a;
    }


    public static ClaraComponent dpe(String dpeHost, int dpePort, String dpeLang, int subscriptionPoolSize, String description) {
        ClaraComponent a = new ClaraComponent(dpeLang,
                dpeHost,
                dpePort,
                xMsgTopic.ANY,
                xMsgTopic.ANY,
                CConstants.UNDEFINED,
                subscriptionPoolSize, description,
                CConstants.UNDEFINED);
        a.isDpe = true;
        return a;
    }

    public static ClaraComponent dpe(String dpeHost, String dpeLang, int subscriptionPoolSize, String description) {
        ClaraComponent a = new ClaraComponent(dpeLang,
                dpeHost,
                xMsgConstants.DEFAULT_PORT,
                xMsgTopic.ANY,
                xMsgTopic.ANY,
                CConstants.UNDEFINED,
                subscriptionPoolSize, description,
                CConstants.UNDEFINED);
        a.isDpe = true;
        return a;
    }

    public static ClaraComponent dpe(String dpeHost, int subscriptionPoolSize, String description) {
        ClaraComponent a = new ClaraComponent(CConstants.JAVA_LANG,
                dpeHost,
                xMsgConstants.DEFAULT_PORT,
                xMsgTopic.ANY,
                xMsgTopic.ANY,
                CConstants.UNDEFINED,
                subscriptionPoolSize, description,
                CConstants.UNDEFINED);
        a.isDpe = true;
        return a;
    }

    public static ClaraComponent dpe(int subscriptionPoolSize, String description) throws IOException {
        ClaraComponent a = new ClaraComponent(CConstants.JAVA_LANG,
                xMsgUtil.localhost(),
                xMsgConstants.DEFAULT_PORT,
                xMsgTopic.ANY,
                xMsgTopic.ANY,
                CConstants.UNDEFINED,
                subscriptionPoolSize, description,
                CConstants.UNDEFINED);
        a.isDpe = true;
        return a;
    }

    public static ClaraComponent dpe() throws IOException {
        ClaraComponent a = new ClaraComponent(CConstants.JAVA_LANG,
                xMsgUtil.localhost(),
                xMsgConstants.DEFAULT_PORT,
                xMsgTopic.ANY,
                xMsgTopic.ANY,
                CConstants.UNDEFINED,
                1, CConstants.UNDEFINED,
                CConstants.UNDEFINED);
        a.isDpe = true;
        return a;
    }

    public static ClaraComponent dpe(String dpeCanonicalName, String description) throws ClaraException {
        if (!ClaraUtil.isCanonicalName(dpeCanonicalName)) {
            throw new ClaraException("Clara-Error: not a canonical name.");
        }
        ClaraComponent a = dpe(ClaraUtil.getDpeHost(dpeCanonicalName),
                ClaraUtil.getDpePort(dpeCanonicalName),
                ClaraUtil.getDpeLang(dpeCanonicalName),
                xMsgConstants.DEFAULT_POOL_SIZE, description);
        a.isDpe = true;
        return a;

    }

    public static ClaraComponent dpe(String dpeCanonicalName) throws ClaraException {
        if (!ClaraUtil.isCanonicalName(dpeCanonicalName)) {
            throw new ClaraException("Clara-Error: not a canonical name.");
        }
        ClaraComponent a = dpe(ClaraUtil.getDpeHost(dpeCanonicalName),
                ClaraUtil.getDpePort(dpeCanonicalName),
                ClaraUtil.getDpeLang(dpeCanonicalName),
                xMsgConstants.DEFAULT_POOL_SIZE, CConstants.UNDEFINED);
        a.isDpe = true;
        return a;

    }


    public static ClaraComponent container(String dpeHost, int dpePort, String dpeLang, String container, int subscriptionPoolSize, String description) {
        ClaraComponent a = new ClaraComponent(dpeLang,
                dpeHost,
                dpePort,
                container,
                xMsgTopic.ANY,
                CConstants.UNDEFINED,
                subscriptionPoolSize, description,
                CConstants.UNDEFINED);
        a.isContainer = true;
        return a;
    }

    public static ClaraComponent container(String dpeHost, String dpeLang, String container, int subscriptionPoolSize, String description) {
        ClaraComponent a = new ClaraComponent(dpeLang,
                dpeHost,
                xMsgConstants.DEFAULT_PORT,
                container,
                xMsgTopic.ANY,
                CConstants.UNDEFINED,
                subscriptionPoolSize, description,
                CConstants.UNDEFINED);
        a.isContainer = true;
        return a;
    }

    public static ClaraComponent container(String dpeHost, String container, int subscriptionPoolSize, String description) {
        ClaraComponent a = new ClaraComponent(CConstants.JAVA_LANG,
                dpeHost,
                xMsgConstants.DEFAULT_PORT,
                container,
                xMsgTopic.ANY,
                CConstants.UNDEFINED,
                subscriptionPoolSize, description,
                CConstants.UNDEFINED);
        a.isContainer = true;
        return a;
    }

    public static ClaraComponent container(String container, int subscriptionPoolSize, String description) throws IOException {
        ClaraComponent a = new ClaraComponent(CConstants.JAVA_LANG,
                xMsgUtil.localhost(),
                xMsgConstants.DEFAULT_PORT,
                container,
                xMsgTopic.ANY,
                CConstants.UNDEFINED,
                subscriptionPoolSize, description,
                CConstants.UNDEFINED);
        a.isContainer = true;
        return a;
    }

    public static ClaraComponent container(String containerCanonicalName, String description) throws ClaraException {
        if (!ClaraUtil.isCanonicalName(containerCanonicalName)) {
            throw new ClaraException("Clara-Error: not a canonical name.");
        }
        return container(ClaraUtil.getDpeHost(containerCanonicalName),
        ClaraUtil.getDpePort(containerCanonicalName),
        ClaraUtil.getDpeLang(containerCanonicalName),
                ClaraUtil.getContainerName(containerCanonicalName), xMsgConstants.DEFAULT_POOL_SIZE, description);

    }

    public static ClaraComponent service(String dpeHost, int dpePort, String dpeLang,
                                         String container,
                                         String engine, String engineClass,
                                         int subscriptionPoolSize, String description,
                                         String initialState) {
        ClaraComponent a = new ClaraComponent(dpeLang,
                dpeHost,
                dpePort,
                container,
                engine,
                engineClass,
                subscriptionPoolSize,
                description,
                initialState);
        a.isService = true;
        return a;
    }

    public static ClaraComponent service(String dpeHost, int dpePort, String dpeLang, String container,
                                         String engine) {
        ClaraComponent a = new ClaraComponent(dpeLang,
                dpeHost,
                dpePort,
                container,
                engine,
                CConstants.UNDEFINED,
                1, CConstants.UNDEFINED,
                CConstants.UNDEFINED);
        a.isService = true;
        return a;
    }

    public static ClaraComponent service(String dpeHost, String dpeLang, String container,
                                         String engine) {
        ClaraComponent a = new ClaraComponent(dpeLang,
                dpeHost,
                xMsgConstants.DEFAULT_PORT,
                container,
                engine,
                CConstants.UNDEFINED,
                1, CConstants.UNDEFINED,
                CConstants.UNDEFINED);
        a.isService = true;
        return a;
    }

    public static ClaraComponent service(String container,
                                         String engine) throws IOException {
        ClaraComponent a = new ClaraComponent(CConstants.JAVA_LANG,
                xMsgUtil.localhost(),
                xMsgConstants.DEFAULT_PORT,
                container,
                engine,
                CConstants.UNDEFINED,
                1, CConstants.UNDEFINED,
                CConstants.UNDEFINED);
        a.isService = true;
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
                CConstants.UNDEFINED, xMsgConstants.DEFAULT_POOL_SIZE,
                CConstants.UNDEFINED, CConstants.UNDEFINED);
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

    public String getDpeCanonicalName() {
        return dpeCanonicalName;
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

    public String getCanonicalName() {
        return canonicalName;
    }

    public void setCanonicalName(String canonicalName) {
        this.canonicalName = canonicalName;
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

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getInitialState() {
        return initialState;
    }

    public void setInitialState(String initialState) {
        this.initialState = initialState;
    }
}
