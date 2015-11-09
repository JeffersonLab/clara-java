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


    /**
     * Creates and returns Clara orchestrator
     *
     * @param name                 of the orchestrator
     * @param dpeHost              host of the PDE to communicate with
     * @param dpePort              port of the DPE to communicate with
     * @param dpeLang              language of the DPE
     * @param subscriptionPoolSize pool size for the
     *                             orchestrator to be used for subscriptions
     * @param description          textual description of this orchestrator
     * @return orchestrator {@link org.jlab.clara.base.ClaraComponent} object
     */
    public static ClaraComponent orchestrator(String name, String dpeHost, int dpePort, String dpeLang,
                                              int subscriptionPoolSize, String description) {
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

    /**
     * Creates and returns Clara orchestrator. Uses default DPE port.
     *
     * @param name                 of the orchestrator
     * @param dpeHost              host of the PDE to communicate with
     * @param dpeLang              language of the DPE
     * @param subscriptionPoolSize pool size for the
     *                             orchestrator to be used for subscriptions
     * @param description          textual description of this orchestrator
     * @return orchestrator {@link org.jlab.clara.base.ClaraComponent} object
     */
    public static ClaraComponent orchestrator(String name, String dpeHost, String dpeLang,
                                              int subscriptionPoolSize, String description) {
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

    /**
     * Creates and returns Clara orchestrator. Default port of the DP and Java lang is used.
     *
     * @param name                 of the orchestrator
     * @param dpeHost              host of the PDE to communicate with
     * @param subscriptionPoolSize pool size for the
     *                             orchestrator to be used for subscriptions
     * @param description          textual description of this orchestrator
     * @return orchestrator {@link org.jlab.clara.base.ClaraComponent} object
     */
    public static ClaraComponent orchestrator(String name, String dpeHost,
                                              int subscriptionPoolSize, String description) {
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

    /**
     * Creates and returns Clara orchestrator. DPE on the local host, with
     * the default port and Java lang is used.
     *
     * @param name of the orchestrator
     * @param subscriptionPoolSize pool size for the
     *                             orchestrator to be used for subscriptions
     * @param description textual description of this orchestrator
     * @return orchestrator {@link org.jlab.clara.base.ClaraComponent} object
     */
    public static ClaraComponent orchestrator(String name, int subscriptionPoolSize, String description)
            throws IOException {
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

    /**
     * Creates and returns Clara DPE component
     *
     * @param dpeHost              host where the DPE will run
     * @param dpePort              port of the DPE will use
     * @param dpeLang              language of the DPE
     * @param subscriptionPoolSize pool size for the
     *                             DPE to be used for subscriptions
     * @param description          textual description of the DPE
     * @return DPE {@link org.jlab.clara.base.ClaraComponent} object
     */
    public static ClaraComponent dpe(String dpeHost, int dpePort, String dpeLang,
                                     int subscriptionPoolSize, String description) {
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

    /**
     * Creates and returns Clara DPE component. The default DPE port is used.
     *
     * @param dpeHost host where the DPE will run
     * @param dpeLang language of the DPE
     * @param subscriptionPoolSize pool size for the
     *                             DPE to be used for subscriptions
     * @param description textual description of the DPE
     * @return DPE {@link org.jlab.clara.base.ClaraComponent} object
     */
    public static ClaraComponent dpe(String dpeHost, String dpeLang,
                                     int subscriptionPoolSize, String description) {
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

    /**
     * Creates and returns Clara DPE component. The default DPE port and Java lang is used.
     *
     * @param dpeHost host where the DPE will run
     * @param subscriptionPoolSize pool size for the
     *                             DPE to be used for subscriptions
     * @param description textual description of the DPE
     * @return DPE {@link org.jlab.clara.base.ClaraComponent} object
     */
    public static ClaraComponent dpe(String dpeHost,
                                     int subscriptionPoolSize, String description) {
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

    /**
     * Creates and returns Clara DPE component. The local host, default DPE port and Java lang is used.
     *
     * @param subscriptionPoolSize pool size for the
     *                             DPE to be used for subscriptions
     * @param description textual description of the DPE
     * @return DPE {@link org.jlab.clara.base.ClaraComponent} object
     */
    public static ClaraComponent dpe(int subscriptionPoolSize, String description)
            throws IOException {
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

    /**
     * Creates and returns Clara DPE component. DPE default settings are used, including pool-size = 1.
     *
     * @return DPE {@link org.jlab.clara.base.ClaraComponent} object
     * @throws IOException
     */
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

    /**
     * Creates and returns Clara DPE component from the Clara component
     * canonical name.. DPE default pool-size = 1 is used.
     *
     * @param canonicalName The canonical name of a component
     * @param description textual description of the DPE
     * @return {@link org.jlab.clara.base.ClaraComponent} object
     * @throws ClaraException
     */
    public static ClaraComponent dpe(String canonicalName, String description)
            throws ClaraException {
        if (!ClaraUtil.isCanonicalName(canonicalName)) {
            throw new ClaraException("Clara-Error: not a canonical name.");
        }
        ClaraComponent a = dpe(ClaraUtil.getDpeHost(canonicalName),
                ClaraUtil.getDpePort(canonicalName),
                ClaraUtil.getDpeLang(canonicalName),
                xMsgConstants.DEFAULT_POOL_SIZE, description);
        a.isDpe = true;
        return a;

    }

    /**
     * Creates and returns Clara DPE component from the Clara component
     * canonical name.. DPE default pool-size = 1 is used, leaving description
     * of the DPE undefined.
     *
     * @param canonicalName The canonical name of a component
     * @return {@link org.jlab.clara.base.ClaraComponent} object
     * @throws ClaraException
     */
    public static ClaraComponent dpe(String canonicalName) throws ClaraException {
        if (!ClaraUtil.isCanonicalName(canonicalName)) {
            throw new ClaraException("Clara-Error: not a canonical name.");
        }
        ClaraComponent a = dpe(ClaraUtil.getDpeHost(canonicalName),
                ClaraUtil.getDpePort(canonicalName),
                ClaraUtil.getDpeLang(canonicalName),
                xMsgConstants.DEFAULT_POOL_SIZE, CConstants.UNDEFINED);
        a.isDpe = true;
        return a;
    }

    /**
     * Creates and returns Clara Container component
     *
     * @param dpeHost              host of the DPE where container is/(will be) deployed
     * @param dpePort              port of the DPE where container is/(will be) deployed
     * @param dpeLang              language of the DPE
     * @param container            the name of the container
     * @param subscriptionPoolSize pool size for the
     *                             container to be used for subscriptions
     * @param description          textual description of the container
     * @return Container {@link org.jlab.clara.base.ClaraComponent} object
     */
    public static ClaraComponent container(String dpeHost, int dpePort, String dpeLang,
                                           String container, int subscriptionPoolSize,
                                           String description) {
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

    /**
     * Creates and returns Clara Container component. The default DPE port is used.
     *
     * @param dpeHost host of the DPE where container is/(will be) deployed
     * @param dpeLang language of the DPE
     * @param container the name of the container
     * @param subscriptionPoolSize pool size for the
     *                             container to be used for subscriptions
     * @param description textual description of the container
     * @return Container {@link org.jlab.clara.base.ClaraComponent} object
     */
    public static ClaraComponent container(String dpeHost, String dpeLang,
                                           String container, int subscriptionPoolSize,
                                           String description) {
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

    /**
     * Creates and returns Clara Container component. The default DPE port and Java lang is used.
     *
     * @param dpeHost host of the DPE where container is/(will be) deployed
     * @param container the name of the container
     * @param subscriptionPoolSize pool size for the
     *                             container to be used for subscriptions
     * @param description textual description of the container
     * @return Container {@link org.jlab.clara.base.ClaraComponent} object
     */
    public static ClaraComponent container(String dpeHost, String container,
                                           int subscriptionPoolSize, String description) {
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

    /**
     * Creates and returns Clara Container component. The DPE running on a local host,
     * default DPE port and Java lang is used.
     *
     * @param container the name of the container
     * @param subscriptionPoolSize pool size for the
     *                             container to be used for subscriptions
     * @param description textual description of the container
     * @return Container {@link org.jlab.clara.base.ClaraComponent} object
     */
    public static ClaraComponent container(String container, int subscriptionPoolSize,
                                           String description)
            throws IOException {
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

    /**
     * Creates and returns Clara Container component, using the container canonical name.
     * Default subscriptions pool-size = 1 is used.
     *
     * @param description textual description of the container
     * @return Container {@link org.jlab.clara.base.ClaraComponent} object
     */
    public static ClaraComponent container(String containerCanonicalName, String description)
            throws ClaraException {
        if (!ClaraUtil.isCanonicalName(containerCanonicalName)) {
            throw new ClaraException("Clara-Error: not a canonical name.");
        }
        return container(ClaraUtil.getDpeHost(containerCanonicalName),
                ClaraUtil.getDpePort(containerCanonicalName),
                ClaraUtil.getDpeLang(containerCanonicalName),
                ClaraUtil.getContainerName(containerCanonicalName), xMsgConstants.DEFAULT_POOL_SIZE, description);
    }

    /**
     * Creates and returns Clara Service component
     *
     * @param dpeHost host of the DPE where service is/(will be) deployed
     * @param dpePort port of the DPE where service is/(will be) deployed
     * @param dpeLang language of the DPE
     * @param container the name of the container of the service
     * @param engine the name of the service engine
     * @param engineClass engine full class name (package name)
     * @param subscriptionPoolSize pool size for the
     *                             service to be used for subscriptions
     * @param description textual description of the service
     * @param initialState the initial state of the service
     * @return Service {@link org.jlab.clara.base.ClaraComponent} object
     */
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

    /**
     * Creates and returns Clara Service component. Default pool-size=1 is used.
     * The description of the service is undefined.
     *
     * @param dpeHost host of the DPE where service is/(will be) deployed
     * @param dpePort port of the DPE where service is/(will be) deployed
     * @param dpeLang language of the DPE
     * @param container the name of the container of the service
     * @param engine the name of the service engine
     * @return Service {@link org.jlab.clara.base.ClaraComponent} object
     */
    public static ClaraComponent service(String dpeHost, int dpePort, String dpeLang,
                                         String container, String engine) {
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

    /**
     * Creates and returns Clara Service component. DPE default port and default pool-size=1 is used.
     * The description of the service is undefined.
     *
     * @param dpeHost host of the DPE where service is/(will be) deployed
     * @param dpeLang language of the DPE
     * @param container the name of the container of the service
     * @param engine the name of the service engine
     * @return Service {@link org.jlab.clara.base.ClaraComponent} object
     */
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

    /**
     * Creates and returns Clara Service component. DPE running on a local host with the
     * default port and default pool-size=1 is used. The description of the service is undefined.
     *
     * @param container the name of the container of the service
     * @param engine the name of the service engine
     * @return Service {@link org.jlab.clara.base.ClaraComponent} object
     */
    public static ClaraComponent service(String container,
                                         String engine)
    throws IOException {
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

    /**
     * Creates and returns Clara Service component. DPE running on a local host with the
     * default port used. The description of the service is undefined.
     *
     * @param container the name of the container of the service
     * @param engine the name of the service engine
     * @param poolSize pool size for the service subscriptions
     * @return Service {@link org.jlab.clara.base.ClaraComponent} object
     */
    public static ClaraComponent service(String container,
                                         String engine, int poolSize)
    throws IOException {
        ClaraComponent a = new ClaraComponent(CConstants.JAVA_LANG,
                xMsgUtil.localhost(),
                xMsgConstants.DEFAULT_PORT,
                container,
                engine,
                CConstants.UNDEFINED,
                poolSize, CConstants.UNDEFINED,
                CConstants.UNDEFINED);
        a.isService = true;
        return a;
    }

    /**
     * Creates and returns Clara Service component, using the service canonical name.
     * Default subscriptions pool-size = 1 is used. The description of the service is undefined.
     *
     * @return Service {@link org.jlab.clara.base.ClaraComponent} object
     */
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

    /**
     * Returns the topic of the Clara component, i.e. the topic of the xMsg subscriber.
     * Note that all Clara components are registered as xMsg subscribers.
     *
     * @return {@link org.jlab.coda.xmsg.core.xMsgTopic} object
     */
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

    /**
     * Returns DPE canonical, constructed as "dpeHost % dpePort _ dpeLang"
     *
     * @return DPE canonical name
     */
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

    /**
     * Sets the engine class which is the package name of the class.
     *
     * @param engineClass package name of the class
     */
    public void setEngineClass(String engineClass) {
        this.engineClass = engineClass;
    }

    public String getCanonicalName() {
        return canonicalName;
    }

    /**
     * Note. candidate to be deprecated. Do not use to define the
     * canonical names for DPE, container or service.
     *
     * The canonical name of a Clara component is defined internally,
     * yet this method is used to set the name of an orchestrator, which
     * considers to be non critical.
     *
     * @param canonicalName canonical name
     */
    public void setCanonicalName(String canonicalName) {
        this.canonicalName = canonicalName;
    }

    public int getSubscriptionPoolSize() {
        return subscriptionPoolSize;
    }

    /**
     * Sets the subscription pool-size of the component.
     *
     * @param subscriptionPoolSize pool size
     */
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

    /**
     * Returns the DPE proxy address
     *
     * @return {@link org.jlab.coda.xmsg.net.xMsgProxyAddress} object
     */
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
