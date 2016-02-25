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

package org.jlab.clara.base;

import org.jlab.clara.base.ClaraRequests.DeployContainerRequest;
import org.jlab.clara.base.ClaraRequests.DeployServiceRequest;
import org.jlab.clara.base.ClaraRequests.ExitRequest;
import org.jlab.clara.base.ClaraRequests.ServiceConfigRequestBuilder;
import org.jlab.clara.base.ClaraRequests.ServiceExecuteRequestBuilder;
import org.jlab.clara.base.ClaraSubscriptions.GlobalSubscriptionBuilder;
import org.jlab.clara.base.ClaraSubscriptions.ServiceSubscriptionBuilder;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.util.CConstants;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgSubscription;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.excp.xMsgException;

import javax.annotation.ParametersAreNonnullByDefault;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;


/**
 * Base class for orchestration of applications.
 */
@ParametersAreNonnullByDefault
public class BaseOrchestrator {

    //Set of user defined data types, that provide data specific serialization routines.
    private final Set<EngineDataType> dataTypes = new HashSet<>();

    // Map of subscription objects. Key = Clara_component_canonical_name # topic_of_subscription
    private final Map<String, xMsgSubscription> subscriptions = new HashMap<>();

    // ClaraBase reference
    private ClaraBase base = null;


    /**
     * Creates a new orchestrator.
     * Uses a random name and the local node as front-end.
     *
     * @throws IOException if localhost could not be obtained
     * @throws ClaraException if the orchestrator could not be created
     */
    public BaseOrchestrator() {
        this(xMsgConstants.DEFAULT_POOL_SIZE);
    }

    /**
     * Creates a new orchestrator.
     * Uses a random name and receives the location of the front-end.
     *
     * @param subPoolSize set the size of the pool for processing subscriptions on background
     * @throws IOException if localhost could not be obtained
     * @throws ClaraException if the orchestrator could not be created
     */
    public BaseOrchestrator(int subPoolSize) {
        this(ClaraUtil.getUniqueName(),
             new DpeName(ClaraUtil.localhost(), ClaraLang.JAVA),
             subPoolSize);
    }

    /**
     * Creates a new orchestrator.
     * Uses a random name and receives the location of the front-end.
     *
     * @param frontEnd use this front-end for communication with the Clara cloud
     * @throws IOException if localhost could not be obtained
     * @throws ClaraException if the orchestrator could not be created
     */
    public BaseOrchestrator(DpeName frontEnd) {
        this(ClaraUtil.getUniqueName(),
             frontEnd,
             xMsgConstants.DEFAULT_POOL_SIZE);
    }

    /**
     * Creates a new orchestrator.
     * Uses a random name and receives the location of the front-end.
     *
     * @param frontEnd use this front-end for communication with the Clara cloud
     * @param subPoolSize set the size of the pool for processing subscriptions on background
     * @throws ClaraException if the orchestrator could not be created
     */
    public BaseOrchestrator(DpeName frontEnd, int subPoolSize) {
        this(ClaraUtil.getUniqueName(),
             frontEnd,
             subPoolSize);
    }

    /**
     * Creates a new orchestrator.
     *
     * @param name the identification of this orchestrator
     * @param frontEnd use this front-end for communication with the Clara cloud
     * @param subPoolSize set the size of the pool for processing subscriptions on background
     * @throws ClaraException if the orchestrator could not be created
     */
    public BaseOrchestrator(String name, DpeName frontEnd, int subPoolSize) {
        base = getClaraBase(name, frontEnd, subPoolSize);
    }

    /**
     * Creates the internal base object.
     * It can be overridden to return a mock for testing purposes.
     *
     * @throws ClaraException
     * @throws IOException
     */
    ClaraBase getClaraBase(String name, DpeName frontEnd, int poolSize) {
        try {
            String localhost = ClaraUtil.localhost();
            ClaraComponent o = ClaraComponent.orchestrator(name, localhost, poolSize, "");
            ClaraComponent fe = ClaraComponent.dpe(frontEnd.canonicalName());
            ClaraBase b = new ClaraBase(o, fe) {
                @Override
                public void start(ClaraComponent component) {
                    // Nothing
                }

                @Override
                public void end() {
                    // Nothing
                }
            };
            return b;
        } catch (ClaraException e) {
            throw new IllegalArgumentException("Invalid front-end: " + frontEnd);
        }
    }

    /**
     * Returns the map of subscriptions for testing purposes.
     *
     * @return {@link org.jlab.coda.xmsg.core.xMsgSubscription} objects
     *          mapped by the key = Key = Clara_component_canonical_name # topic_of_subscription
     */
    Map<String, xMsgSubscription> getSubscriptions() {
        return subscriptions;
    }


    /**
     * Registers the necessary data-types to communicate data to services.
     * {@link org.jlab.clara.engine.EngineDataType} object contains user
     * provided data serialization routine
     *
     * @param dataTypes service engine data types
     */
    public void registerDataTypes(EngineDataType... dataTypes) {
        Collections.addAll(this.dataTypes, dataTypes);
    }

    /**
     * Registers the necessary data-types to communicate data to services.
     * {@link org.jlab.clara.engine.EngineData} object contains user
     * provided data serialization routine
     *
     * @param dataTypes set of {@link org.jlab.clara.engine.EngineDataType} objects
     */
    public void registerDataTypes(Set<EngineDataType> dataTypes) {
        this.dataTypes.addAll(dataTypes);
    }


    /**
     * Tells a Clara DPE component to consider the passed Clara component as a front end.
     * This method is used at run-time to define/redefine front end DPE.
     *
     * @param dpe receiver DPE
     * @param frontEnd info about the front end DPE
     */
    public void setFrontEnd(ClaraComponent dpe, ClaraComponent frontEnd)
            throws IOException, xMsgException {

        xMsgTopic topic = ClaraUtil.buildTopic(CConstants.DPE, base.getFrontEnd().getCanonicalName());

        String data = ClaraUtil.buildData(CConstants.SET_FRONT_END_REMOTE, dpe.getDpeHost(),
                dpe.getDpePort(), dpe.getDpeLang(), frontEnd.getDpeHost(),
                frontEnd.getDpePort(), frontEnd.getDpeLang());
        base.send(base.getFrontEnd(), ClaraBase.createRequest(topic, data));
    }

    /**
     * Sends a message to the front-end DPE asking to start a DPE. The new DPE info,
     * such as where DPE should start, on what port, language, pool size, etc, is defined
     * in ClaraComponent object. Note that front end is set/defined by the user from one
     * of the cloud DPEs.
     *
     * @param comp    DPE that must be started as a {@link org.jlab.clara.base.ClaraComponent} object
     * @param regHost registration service host that future DPE will use to register it's components
     * @param regPort registration service port number
     * @throws ClaraException
     * @throws xMsgException
     * @throws IOException
     * @throws TimeoutException
     */
    public void deployDpe(ClaraComponent comp, String regHost, int regPort)
            throws ClaraException, xMsgException, IOException, TimeoutException {
        if (comp.isDpe()) {
            xMsgTopic topic = ClaraUtil.buildTopic(CConstants.DPE, base.getFrontEnd().getCanonicalName());


            String data = ClaraUtil.buildData(CConstants.START_DPE,
                    comp.getDpeHost(),
                    comp.getDpePort(),
                    comp.getDpeLang(),
                    comp.getSubscriptionPoolSize(),
                    regHost, regPort,
                    comp.getDescription());
            base.send(base.getFrontEnd(), ClaraBase.createRequest(topic, data));
        }
    }

    /**
     * Requests a DPE to exit.
     * The request is sent to a running DPE of the given language.
     * If no DPE is running in the node, the message is lost.
     *
     * @throws ClaraException if the request could not be sent
     */
    public void exitFrontEnd()
            throws ClaraException, xMsgException, IOException, TimeoutException {
        xMsgTopic topic = ClaraUtil.buildTopic(CConstants.DPE, base.getFrontEnd().getCanonicalName());

        String data = CConstants.STOP_DPE;
        base.send(base.getFrontEnd(), ClaraBase.createRequest(topic, data));
    }

    public DeployContainerRequest deploy(ContainerName container) throws ClaraException {
        String dpeName = ClaraUtil.getDpeName(container.canonicalName());
        ClaraComponent targetDpe = ClaraComponent.dpe(dpeName);
        return new DeployContainerRequest(base, targetDpe, container);
    }

    public DeployServiceRequest deploy(ServiceName service, String classPath) throws ClaraException {
        String dpeName = ClaraUtil.getDpeName(service.canonicalName());
        ClaraComponent targetDpe = ClaraComponent.dpe(dpeName);
        return new DeployServiceRequest(base, targetDpe, service, classPath);
    }


    public ExitRequest exit(DpeName dpe) throws ClaraException {
        ClaraComponent targetDpe = ClaraComponent.dpe(dpe.canonicalName());
        return new ExitRequest(base, targetDpe, dpe);
    }

    public ExitRequest exit(ContainerName container) throws ClaraException {
        String dpeName = ClaraUtil.getDpeName(container.canonicalName());
        ClaraComponent targetDpe = ClaraComponent.dpe(dpeName);
        return new ExitRequest(base, targetDpe, container);
    }

    public ExitRequest exit(ServiceName service) throws ClaraException {
        String dpeName = ClaraUtil.getDpeName(service.canonicalName());
        ClaraComponent targetDpe = ClaraComponent.dpe(dpeName);
        return new ExitRequest(base, targetDpe, service);
    }


    /**
     * Pings a DPE. This is a sync request.
     *
     * @param dpe a DPE
     * @param timeout sync request timeout
     * @return message {@link org.jlab.coda.xmsg.core.xMsgMessage}
     *         indicating the status of the sync operation.
     * @throws ClaraException
     * @throws xMsgException
     * @throws IOException
     * @throws TimeoutException
     */
    public xMsgMessage pingDpe(ClaraComponent dpe, int timeout)
            throws ClaraException, xMsgException, IOException, TimeoutException {
        return base.pingDpe(dpe, timeout);
    }

    /**
     * Returns a request builder to configure the given service.
     *
     * @param service the Clara service to be configured
     * @return a builder to choose how to configure the service
     *         (with data, with report frequency, etc)
     */
    public ServiceConfigRequestBuilder configure(ServiceName service) throws ClaraException {
        String dpeName = ClaraUtil.getDpeName(service.canonicalName());
        ClaraComponent targetDpe = ClaraComponent.dpe(dpeName);
        return new ServiceConfigRequestBuilder(base, targetDpe, service, dataTypes);
    }


    /**
     * Returns a request builder to execute the given service.
     *
     * @param service the Clara service to be executed
     * @return a builder to setup the execution request
     *         (with data, data types, etc)
     */
    public ServiceExecuteRequestBuilder execute(ServiceName service) throws ClaraException {
        String dpeName = ClaraUtil.getDpeName(service.canonicalName());
        ClaraComponent targetDpe = ClaraComponent.dpe(dpeName);
        return new ServiceExecuteRequestBuilder(base, targetDpe, service, dataTypes);
    }

    /**
     * Returns a request builder to execute the given composition.
     *
     * @param composition the Clara composition to be executed
     * @return a builder to to configure the execute request
     *         (with data, data types, etc)
     */
    public ServiceExecuteRequestBuilder execute(Composition composition) throws ClaraException {
        String dpeName = ClaraUtil.getDpeName(composition.firstService());
        ClaraComponent targetDpe = ClaraComponent.dpe(dpeName);
        return new ServiceExecuteRequestBuilder(base, targetDpe, composition, dataTypes);
    }


    /**
     * Returns a subscription builder to select what type of reports of the
     * given service shall be listened, and what action should be called when a
     * report is received.
     *
     * @param service the service to be listened
     */
    public ServiceSubscriptionBuilder listen(ClaraName service) {
        return new ServiceSubscriptionBuilder(base, subscriptions, dataTypes,
                                              base.getFrontEnd(), service);
    }


    /**
     * Returns a subscription builder to select what type of global reports by
     * the front-end shall be listened, and what action should be called when a
     * report is received.
     */
    public GlobalSubscriptionBuilder listen() {
        return new GlobalSubscriptionBuilder(base, subscriptions, base.getFrontEnd());
    }



    /**
     * Uses a default registrar service address (defined at the
     * constructor) to ask the Set of registered DPEs.
     *
     * @return Set of DPE names: String
     * @throws ClaraException
     * @throws xMsgException
     */
    public Set<String> getDpeNames() throws ClaraException, xMsgException {
        xMsgTopic topic = xMsgTopic.build(CConstants.DPE);
        Set<xMsgRegistration> rs = base.findSubscribers(topic);
        HashSet<String> result = new HashSet<>();
        for (xMsgRegistration r : rs) {
            result.add(r.getName());
        }
        return result;
    }

    /**
     * Returns the names of all service containers of a particular DPE.
     * Request goes to the default registrar service, defined at the constructor.
     *
     * @param dpeName canonical name of a DPE
     * @return Set of container names of a DPE
     * @throws ClaraException
     * @throws xMsgException
     */
    public Set<String> getContainerNames(String dpeName) throws ClaraException, xMsgException {
        xMsgTopic topic = xMsgTopic.build(CConstants.CONTAINER, dpeName);
        Set<xMsgRegistration> rs = base.findSubscribers(topic);
        HashSet<String> result = new HashSet<>();
        for (xMsgRegistration r : rs) {
            result.add(r.getName());
        }
        return result;
    }

    /**
     * Returns service engine names of a particular container of a particular DPE.
     * Request goes to the default registrar service, defined at the constructor.
     *
     * @param dpeName  canonical name of a DPE
     * @param containerName canonical name of a container
     * @return Set of service engine names
     * @throws ClaraException
     * @throws xMsgException
     */
    public Set<String> getEngineNames(String dpeName, String containerName) throws ClaraException, xMsgException {
        xMsgTopic topic = xMsgTopic.build(dpeName, containerName);
        Set<xMsgRegistration> rs = base.findSubscribers(topic);
        HashSet<String> result = new HashSet<>();
        for (xMsgRegistration r : rs) {
            result.add(r.getName());
        }
        return result;
    }

    /**
     * Returns the registration information of a selected Clara actor.
     * The actor can be a DPE, a container or a service.
     *
     * @param canonicalName the name of the actor
     * @return Set of {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration} objects
     */
    public Set<xMsgRegistration> getRegistrationInfo(String canonicalName)
            throws ClaraException, xMsgException {
        xMsgTopic topic = xMsgTopic.wrap(canonicalName);
        return base.findSubscribers(topic);
    }


    public String meta2Json(xMsgMeta meta) throws ClaraException {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public String reg2Json(xMsgRegistration regData) throws ClaraException {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    /**
     * Returns this orchestrator name.
     */
    public String getName() {
        return base.getName();
    }
}
