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

package org.jlab.clara.std.orchestrators;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.jlab.clara.base.BaseOrchestrator;
import org.jlab.clara.base.ClaraFilters;
import org.jlab.clara.base.ClaraName;
import org.jlab.clara.base.Composition;
import org.jlab.clara.base.ContainerName;
import org.jlab.clara.base.DpeName;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.base.EngineCallback;
import org.jlab.clara.base.GenericCallback;
import org.jlab.clara.base.ServiceName;
import org.jlab.clara.base.ServiceRuntimeData;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.engine.EngineStatus;
import org.json.JSONObject;

class CoreOrchestrator {

    private final BaseOrchestrator base;

    private final Set<ContainerName> userContainers;
    private final Map<ServiceName, DeployInfo> userServices;


    CoreOrchestrator(OrchestratorSetup setup, int poolSize) {
        base = new BaseOrchestrator(setup.frontEnd, poolSize);

        userContainers = Collections.newSetFromMap(new ConcurrentHashMap<ContainerName, Boolean>());
        userServices = new ConcurrentHashMap<>();

        base.registerDataTypes(EngineDataType.JSON,
                               EngineDataType.STRING,
                               EngineDataType.SFIXED32);
        base.registerDataTypes(setup.dataTypes);
    }


    void deployService(DeployInfo service) {
        try {
            ContainerName containerName = service.name.container();
            if (!userContainers.contains(containerName)) {
                deployContainer(containerName);
                userContainers.add(containerName);
            }
            base.deploy(service.name, service.classPath).withPoolsize(service.poolSize).run();
            userServices.put(service.name, service);
        } catch (ClaraException e) {
            String errorMsg = String.format("failed request to deploy service = %s  class = %s",
                                            service.name, service.classPath);
            throw new OrchestratorException(errorMsg, e);
        }
    }


    private void deployContainer(ContainerName container) throws ClaraException {
        base.deploy(container).run();

        final int maxAttempts = 10;
        int counter = 0;
        while (true) {
            Set<ContainerName> regContainers = getRegisteredContainers(container.dpe());
            for (ContainerName c : regContainers) {
                if (container.equals(c)) {
                    return;
                }
            }
            counter++;
            if (counter == 6) {
                base.deploy(container).run();
            }
            if (counter == maxAttempts) {
                throw new OrchestratorException("could not start container = " + container);
            }
            sleep(200);
        }
    }


    private Set<ContainerName> getRegisteredContainers(DpeName dpe) {
        try {
            return base.query()
                       .canonicalNames(ClaraFilters.containersByDpe(dpe))
                       .syncRun(3, TimeUnit.SECONDS);
        } catch (TimeoutException | ClaraException e) {
            throw new OrchestratorException(e);
        }
    }


    private Set<ServiceName> getRegisteredServices(DpeName dpe) {
        try {
            return base.query()
                       .canonicalNames(ClaraFilters.servicesByDpe(dpe))
                       .syncRun(3, TimeUnit.SECONDS);
        } catch (TimeoutException | ClaraException e) {
            throw new OrchestratorException(e);
        }
    }


    private Set<ServiceName> findMissingServices(Set<ServiceName> services,
                                                 Set<ServiceName> regServices) {
        Set<ServiceName> missingServices = new HashSet<>();
        for (ServiceName s : services) {
            if (!regServices.contains(s)) {
                missingServices.add(s);
            }
        }
        return missingServices;
    }


    void checkServices(DpeName dpe, Set<ServiceName> services) {
        final int sleepTime = 2000;
        final int totalConnectTime = 1000 * 10 * services.size();
        final int maxAttempts = totalConnectTime / sleepTime;
        final int retryAttempts = maxAttempts / 2;

        int counter = 1;
        while (true) {
            Set<ServiceName> regServices = getRegisteredServices(dpe);
            Set<ServiceName> missingServices = findMissingServices(services, regServices);
            if (missingServices.isEmpty()) {
                return;
            } else {
                if (counter == retryAttempts) {
                    reDeploy(regServices, missingServices);
                }
                counter++;
                if (counter > maxAttempts) {
                    throw new OrchestratorException(reportUndeployed(missingServices));
                }
                sleep(sleepTime);
            }
        }
    }


    private void reDeploy(Set<ServiceName> regServices, Set<ServiceName> missingServices) {
        // Remove user containers that were not started
        Set<ContainerName> regContainers = new HashSet<>();
        for (ServiceName service : regServices) {
            regContainers.add(service.container());
        }
        for (ServiceName missing : missingServices) {
            ContainerName cont = missing.container();
            if (!regContainers.contains(cont)) {
                userContainers.remove(cont);
            }
        }
        // Re-deploy missing services
        for (ServiceName missing : missingServices) {
            DeployInfo deployInfo = userServices.get(missing);
            Logging.info("Service " + missing + " was not found. Trying to redeploy...");
            deployService(deployInfo);
        }
    }


    private String reportUndeployed(Set<ServiceName> missingServices) {
        StringBuilder sb = new StringBuilder();
        sb.append("undeployed service");
        if (missingServices.size() > 1) {
            sb.append("s");
        }
        sb.append(" = '");
        Iterator<ServiceName> iter = missingServices.iterator();
        sb.append(iter.next());
        while (iter.hasNext()) {
            sb.append(", ");
            sb.append(iter.next());
        }
        sb.append("'");
        return sb.toString();
    }


    boolean findServices(DpeName dpe, Set<ServiceName> services) {
        return findMissingServices(services, getRegisteredServices(dpe)).isEmpty();
    }


    void syncConfig(ServiceName service, JSONObject data, int wait, TimeUnit unit)
            throws ClaraException, TimeoutException {
        EngineData input = new EngineData();
        input.setData(EngineDataType.JSON.mimeType(), data.toString());
        syncConfig(service, input, wait, unit);
    }


    void syncConfig(ServiceName service, EngineData data, int wait, TimeUnit unit)
            throws ClaraException, TimeoutException {
        base.configure(service).withData(data).syncRun(wait, unit);
    }


    void syncEnableRing(ServiceName service, int wait, TimeUnit unit)
            throws ClaraException, TimeoutException {
        base.configure(service).startDataRingReporting().syncRun(wait, unit);
    }


    void send(Composition composition, EngineData data) throws ClaraException {
        base.execute(composition).withData(data).run();
    }


    EngineData syncSend(ServiceName service, String data, int wait, TimeUnit unit)
            throws ClaraException, TimeoutException {
        EngineData input = new EngineData();
        input.setData(EngineDataType.STRING.mimeType(), data);
        return syncSend(service, input, wait, unit);
    }


    EngineData syncSend(ServiceName service, JSONObject data, int wait, TimeUnit unit)
            throws ClaraException, TimeoutException {
        EngineData input = new EngineData();
        input.setData(EngineDataType.JSON.mimeType(), data.toString());
        return syncSend(service, input, wait, unit);
    }


    EngineData syncSend(ServiceName service, EngineData input, int wait, TimeUnit unit)
            throws ClaraException, TimeoutException {
        EngineData output = base.execute(service).withData(input).syncRun(wait, unit);
        if (output.getStatus() == EngineStatus.ERROR) {
            throw new ClaraException(output.getDescription());
        }
        return output;
    }


    void startDoneReporting(ServiceName service, int frequency) throws ClaraException {
        base.configure(service).startDoneReporting(frequency).run();
    }


    void stopDoneReporting(ServiceName service) throws ClaraException {
        base.configure(service).stopDoneReporting().run();
    }


    void subscribeDpes(DpeCallBack callback, String session) {
        try {
            DpeCallbackWrapper dpeCallback = new DpeCallbackWrapper(callback);
            base.listen().aliveDpes(session).start(dpeCallback);
        } catch (ClaraException e) {
            String msg = "Could not subscribe to front-end to get running DPEs";
            throw new OrchestratorException(msg, e);
        }
    }


    void subscribeErrors(ClaraName name, EngineCallback callback) {
        try {
            base.listen(name).status(EngineStatus.ERROR).start(callback);
        } catch (ClaraException e) {
            throw new OrchestratorException("Could not subscribe to services", e);
        }
    }


    void subscribeDone(ServiceName service, EngineCallback callback) {
        try {
            base.listen(service).done().start(callback);
        } catch (ClaraException e) {
            throw new OrchestratorException("Could not subscribe to services", e);
        }
    }


    Set<DpeName> getRegisteredDpes(int seconds) {
        try {
            return base.query()
                       .canonicalNames(ClaraFilters.allDpes())
                       .syncRun(seconds, TimeUnit.SECONDS);
        } catch (TimeoutException | ClaraException e) {
            String msg = "cannot connect with front-end: " + base.getFrontEnd();
            throw new OrchestratorException(msg, e);
        }
    }


    Set<ServiceRuntimeData> getReport(DpeName dpe) {
        try {
            return base.query()
                       .runtimeData(ClaraFilters.servicesByDpe(dpe))
                       .syncRun(5, TimeUnit.SECONDS);
        } catch (ClaraException | TimeoutException e) {
            throw new OrchestratorException(e);
        }
    }


    DpeName getFrontEnd() {
        return base.getFrontEnd();
    }


    public void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }



    interface DpeCallBack {
        void callback(DpeInfo dpe);
    }



    private class DpeCallbackWrapper implements GenericCallback {

        final DpeCallBack callback;

        DpeCallbackWrapper(DpeCallBack callback) {
            this.callback = callback;
        }

        @Override
        public void callback(String data) {
            try {
                DpeInfo dpe = data.startsWith("{") ? parseJson(data) : parseTokens(data);
                callback.callback(dpe);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private DpeInfo parseJson(String data) {
            JSONObject json = new JSONObject(data);
            DpeName name = new DpeName(json.getString("name"));
            int ncores = json.getInt("n_cores");
            String claraHome = json.getString("clara_home");
            return new DpeInfo(name, ncores, claraHome);
        }

        // keep support for old DPE versions
        private DpeInfo parseTokens(String data) {
            StringTokenizer st = new StringTokenizer(data, "?");
            DpeName name = new DpeName(st.nextToken());
            int ncores = Integer.parseInt(st.nextToken());
            String claraHome = st.nextToken();
            return new DpeInfo(name, ncores, claraHome);
        }
    }
}
