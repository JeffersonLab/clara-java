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

import org.jlab.clara.base.ClaraQueries.ClaraQueryBuilder;
import org.jlab.clara.base.ClaraRequests.DeployContainerRequest;
import org.jlab.clara.base.ClaraRequests.DeployServiceRequest;
import org.jlab.clara.base.ClaraRequests.ExitRequest;
import org.jlab.clara.base.ClaraRequests.ServiceConfigRequestBuilder;
import org.jlab.clara.base.ClaraRequests.ServiceExecuteRequestBuilder;
import org.jlab.clara.base.ClaraSubscriptions.GlobalSubscriptionBuilder;
import org.jlab.clara.base.ClaraSubscriptions.ServiceSubscriptionBuilder;
import org.jlab.clara.base.core.ClaraBase;
import org.jlab.clara.base.core.ClaraComponent;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgSubscription;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;


/**
 * Base class for orchestration of applications.
 */
@ParametersAreNonnullByDefault
public class BaseOrchestrator implements AutoCloseable {

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
     * @throws java.io.UncheckedIOException if localhost could not be obtained
     */
    public BaseOrchestrator() {
        this(xMsgConstants.DEFAULT_POOL_SIZE);
    }

    /**
     * Creates a new orchestrator.
     * Uses a random name and receives the location of the front-end.
     *
     * @param subPoolSize set the size of the pool for processing subscriptions on background
     * @throws java.io.UncheckedIOException if localhost could not be obtained
     */
    public BaseOrchestrator(int subPoolSize) {
        this(getUniqueName(),
             new DpeName(ClaraUtil.localhost(), ClaraLang.JAVA),
             subPoolSize);
    }

    /**
     * Creates a new orchestrator.
     * Uses a random name and receives the location of the front-end.
     *
     * @param frontEnd use this front-end for communication with the CLARA cloud
     * @throws java.io.UncheckedIOException if localhost could not be obtained
     */
    public BaseOrchestrator(DpeName frontEnd) {
        this(getUniqueName(), frontEnd, xMsgConstants.DEFAULT_POOL_SIZE);
    }

    /**
     * Creates a new orchestrator.
     * Uses a random name and receives the location of the front-end.
     *
     * @param frontEnd use this front-end for communication with the CLARA cloud
     * @param subPoolSize set the size of the pool for processing subscriptions on background
     */
    public BaseOrchestrator(DpeName frontEnd, int subPoolSize) {
        this(getUniqueName(), frontEnd, subPoolSize);
    }

    /**
     * Creates a new orchestrator.
     *
     * @param name the identification of this orchestrator
     * @param frontEnd use this front-end for communication with the CLARA cloud
     * @param subPoolSize set the size of the pool for processing subscriptions on background
     */
    public BaseOrchestrator(String name, DpeName frontEnd, int subPoolSize) {
        base = getClaraBase(name, frontEnd, subPoolSize);
    }

    /**
     * Creates the internal base object.
     * It can be overridden to return a mock for testing purposes.
     */
    ClaraBase getClaraBase(String name, DpeName frontEnd, int poolSize) {
        String localhost = ClaraUtil.localhost();
        ClaraComponent o = ClaraComponent.orchestrator(name, localhost, poolSize, "");
        ClaraComponent fe = ClaraComponent.dpe(frontEnd.canonicalName());
        ClaraBase b = new ClaraBase(o, fe) {
            @Override
            public void start() {
                // Nothing
            }

            @Override
            protected void end() {
                // Nothing
            }
        };
        return b;
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
     * Unsubscribes all running subscriptions,
     * terminates all running callbacks and closes all connections.
     */
    @Override
    public void close() {
        base.close();
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
     * Creates a request to start the given container.
     *
     * @param container the container to start
     * @return the request to start the container
     */
    public DeployContainerRequest deploy(ContainerName container) {
        String dpeName = ClaraUtil.getDpeName(container.canonicalName());
        ClaraComponent targetDpe = ClaraComponent.dpe(dpeName);
        return new DeployContainerRequest(base, targetDpe, container);
    }

    /**
     * Creates a request to start the given service engine.
     *
     * @param service the service to start
     * @param classPath the path to the engine class that needs to be loaded
     *        to create the engine
     * @return the request to start the service
     */
    public DeployServiceRequest deploy(ServiceName service, String classPath) {
        String dpeName = ClaraUtil.getDpeName(service.canonicalName());
        ClaraComponent targetDpe = ClaraComponent.dpe(dpeName);
        return new DeployServiceRequest(base, targetDpe, service, classPath);
    }


    /**
     * Creates a request to stop the given DPE.
     *
     * @param dpe the DPE to stop
     * @return the request to stop the DPE
     */
    public ExitRequest exit(DpeName dpe) {
        ClaraComponent targetDpe = ClaraComponent.dpe(dpe.canonicalName());
        return new ExitRequest(base, targetDpe, dpe);
    }

    /**
     * Creates a request to stop the given container.
     *
     * @param container the container to stop
     * @return the request to stop the container
     */
    public ExitRequest exit(ContainerName container) {
        String dpeName = ClaraUtil.getDpeName(container.canonicalName());
        ClaraComponent targetDpe = ClaraComponent.dpe(dpeName);
        return new ExitRequest(base, targetDpe, container);
    }

    /**
     * Creates a request to stop the given service.
     *
     * @param service the service to stop
     * @return the request to stop the service
     */
    public ExitRequest exit(ServiceName service) {
        String dpeName = ClaraUtil.getDpeName(service.canonicalName());
        ClaraComponent targetDpe = ClaraComponent.dpe(dpeName);
        return new ExitRequest(base, targetDpe, service);
    }


    /**
     * Returns a request builder to configure the given service.
     *
     * @param service the CLARA service to be configured
     * @return a builder to choose how to configure the service
     *         (with data, with report frequency, etc)
     */
    public ServiceConfigRequestBuilder configure(ServiceName service) {
        String dpeName = ClaraUtil.getDpeName(service.canonicalName());
        ClaraComponent targetDpe = ClaraComponent.dpe(dpeName);
        return new ServiceConfigRequestBuilder(base, targetDpe, service, dataTypes);
    }


    /**
     * Returns a request builder to execute the given service.
     *
     * @param service the CLARA service to be executed
     * @return a builder to setup the execution request
     *         (with data, data types, etc)
     */
    public ServiceExecuteRequestBuilder execute(ServiceName service) {
        String dpeName = ClaraUtil.getDpeName(service.canonicalName());
        ClaraComponent targetDpe = ClaraComponent.dpe(dpeName);
        return new ServiceExecuteRequestBuilder(base, targetDpe, service, dataTypes);
    }

    /**
     * Returns a request builder to execute the given composition.
     *
     * @param composition the CLARA composition to be executed
     * @return a builder to to configure the execute request
     *         (with data, data types, etc)
     */
    public ServiceExecuteRequestBuilder execute(Composition composition) {
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
     * @return a builder to select a service subscription
     */
    public ServiceSubscriptionBuilder listen(ClaraName service) {
        return new ServiceSubscriptionBuilder(base, subscriptions, dataTypes,
                                              base.getFrontEnd(), service);
    }


    /**
     * Returns a subscription builder to select what type of global reports by
     * the front-end shall be listened, and what action should be called when a
     * report is received.
     *
     * @return a builder to select a global subscription
     */
    public GlobalSubscriptionBuilder listen() {
        return new GlobalSubscriptionBuilder(base, subscriptions, base.getFrontEnd());
    }


    /**
     * Returns a query builder to query the registration/runtime database to
     * discover registered components or obtain the registration/runtime
     * information.
     * <p>
     * To create the query use a filter from {@link ClaraFilters} to filter
     * which components should be selected, or a {@link ClaraName} for a
     * specific component.
     *
     * @return a builder to create the desired query
     */
    public ClaraQueryBuilder query() {
        return new ClaraQueryBuilder(base, base.getFrontEnd());
    }


    /**
     * Returns this orchestrator name.
     *
     * @return the name of the orchestrator
     */
    public String getName() {
        return base.getName();
    }


    /**
     * Returns the front-end used by this orchestrator.
     *
     * @return the name of the front-end DPE
     */
    public DpeName getFrontEnd() {
        return new DpeName(base.getFrontEnd().getCanonicalName());
    }


    /**
     * Gets the size of the thread-pool that process subscription callbacks.
     *
     * @return the maximum size of the thread-pool
     */
    public int getPoolSize() {
        return base.getPoolSize();
    }


    private static String getUniqueName() {
        return UUID.randomUUID().toString();
    }
}
