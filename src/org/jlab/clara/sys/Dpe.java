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

package org.jlab.clara.sys;

import org.jlab.clara.base.CException;
import org.jlab.clara.base.ClaraLang;
import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.util.CConstants;
import org.jlab.clara.util.CUtility;
import org.jlab.clara.util.RequestParser;
import org.jlab.coda.xmsg.core.xMsgCallBack;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgAddress;
import org.jlab.coda.xmsg.net.xMsgConnection;
import org.jlab.coda.xmsg.xsys.xMsgRegistrar;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Clara data processing environment. It can play the role of the Front-End
 * (FE), which is the static point of the entire cloud. It creates and manages
 * the registration database (local and case of being assigned as an FE: global
 * database). Note this is a copy of the subscribers database resident in the
 * xMsg registration database. This also creates a shared memory for
 * communicating Clara transient data objects between services within the same
 * process (this avoids data serialization and de-serialization).
 *
 * @author gurjyan
 * @version 1.x
 * @since 1/30/15
 */
public class Dpe extends CBase {

    private xMsgRegistrar registrar;

    // The name of this dpe. usually it is the IP address
    // of a node where this class is instantiated.
    private String
            dpeName = xMsgConstants.UNDEFINED.toString();

    // The name of the Front-End dpe
    private String
            feHostIp = xMsgConstants.UNDEFINED.toString();

    // Indicates if this DPE is assigned to be the Front-End of the cloud.
    private Boolean isFE = false;

    // Local database of names, stores canonical names of
    // cloud DPEs, containers and services.
    // Note. if this is not a FE then these list will contain
    // only current dpe relevant containers and services, and _myDPEs
    // will contain only a single record (for the current DPE).
    // key = DPE_name
    // value = Map (key = containerName, value = Set of service names
    private Map<String, Map<String, Set<String>>> _myCloud = new HashMap<>();

    private HeartBeatReport heartBeat;


    public static void main(String[] args) {
        String frontEnd = "";
        boolean cloudController = false;
        int i = 0;
        while (i < args.length) {
            switch (args[i++]) {
                case "-fh":
                    if (i < args.length) {
                        frontEnd = args[i++];
                    } else {
                        usage();
                        System.exit(1);
                    }
                    break;
                case "-cc":
                    cloudController = true;
                    break;
                default:
                    usage();
                    System.exit(1);
            }
        }

        try {
            String localAddress = xMsgUtil.localhost();
            if (cloudController) {
                new Dpe(localAddress, true);
            } else if (frontEnd.isEmpty()) {
                new Dpe(localAddress, false);
            } else {
                frontEnd = xMsgUtil.toHostAddress(frontEnd);
                new Dpe(localAddress, frontEnd);
            }
        } catch (xMsgException | IOException e) {
            System.out.println(e.getMessage());
            System.out.println("exiting...");
            System.exit(1);
        }
    }

    public static void usage() {
        System.err.println("Usage: j_dpe [ -cc | -fh <front_end> ]");
    }


    /**
     * Constructor for a standalone or Front-End DPE.
     *
     * @param isFE true if this DPE is assumed the role of the FE
     * @throws xMsgException
     * @throws IOException
     */
    public Dpe(String localAddress, Boolean isFE) throws xMsgException, IOException {
        super(ClaraUtil.formDpeName(localAddress, ClaraLang.JAVA), localAddress, localAddress);

        dpeName = getName();
        feHostIp = getFrontEndAddress();
        this.isFE = isFE;

        printLogo();

        // If this DEP is required to be the FE then start a
        // thread that periodically checks the health of cloud DPEs
        if (isFE) {
            ScheduledExecutorService scheduledPingService = Executors.newScheduledThreadPool(3);
            scheduledPingService.schedule(new Runnable() {
                @Override
                public void run() {
                    for (String dpn : _myCloud.keySet()) {
                        if (!dpn.equals(getName())) {
                            try {
                                Object response = syncPing(dpn, 2);
                                if (response == null) {
                                    System.out.println("Warning: no response from DPE= " + dpn);
                                    // removeDpe(dpn);
                                    // removeContainerRecord(dpn);
                                }
                                /**
                                 // check to see if the response is ACTIVE
                                 if (response instanceof String) {
                                 String s = (String) response;
                                 if (!s.equals(CConstants.ALIVE)) {
                                 removeDpe(dpn);
                                 removeContainerRecord(dpn);
                                 }
                                 }
                                 **/
                            } catch (xMsgException | IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }, 10, TimeUnit.SECONDS);
        }

        // Subscribe messages published to this container
        xMsgTopic topic = xMsgTopic.wrap(CConstants.DPE + ":" + getName());
        genericReceive(topic, new DpeCallBack());

        // Create the xMsgNode object that will provide
        // dpe registration and discovery service.
        registrar = new xMsgRegistrar();
        registrar.start();
        _myCloud.put(dpeName, new HashMap<String, Set<String>>());

        startHeartBeatReport();
    }

    /**
     * Constructor for the DPE that is part of the Clara cloud.
     *
     * @param frontEndAddress the address of the front-end DPE
     * @throws xMsgException
     * @throws IOException
     */
    public Dpe(String localAddress, String frontEndAddress) throws xMsgException, IOException {
        super(ClaraUtil.formDpeName(localAddress, ClaraLang.JAVA), localAddress, frontEndAddress);

        dpeName = getName();
        feHostIp = getFrontEndAddress();
        printLogo();

        // Send dpe_up message to the FE
        try {
            String data = CConstants.DPE_UP + "?" + getName();
            xMsgTopic topic = xMsgTopic.wrap(CConstants.DPE + ":" + feHostIp);
            xMsgMessage msg = new xMsgMessage(topic, data);
            genericSend(feHostIp, msg);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Subscribe messages published to this container
        xMsgTopic topic = xMsgTopic.wrap(CConstants.DPE + ":" + getName());
        genericReceive(topic, new DpeCallBack());

        // Create the xMsgNode object that will provide
        // dpe registration and discovery service.
        registrar = new xMsgRegistrar();
        registrar.start();
        _myCloud.put(dpeName, new HashMap<String, Set<String>>());

        startHeartBeatReport();
    }


    /**
     * DPE callback.
     */
    private class DpeCallBack implements xMsgCallBack {

        @Override
        public xMsgMessage callback(xMsgMessage msg) {
            xMsgMessage returnMsg = null;
            xMsgMeta.Builder metadata = msg.getMetaData();
            try {
                String sender = metadata.getSender();
                String returnTopic = metadata.getReplyTo();

                RequestParser parser = RequestParser.build(msg);
                String cmd = parser.nextString();

                switch (cmd) {
                    case CConstants.ACCEPT_FE:
                        turnFE(parser.nextString());
                        break;

                    // Sent from orchestrator. Assuming this is the FE
                    case CConstants.START_DPE:
                        runDpe(parser.nextString());
                        break;

                    // Sent from orchestrator.
                    case CConstants.STOP_DPE:
                        stopDpe(parser.nextString());
                        break;

                    // Sent from master DPE (FE). In this case the value
                    // is the canonical name of the master DPE (FE)
                    case CConstants.DPE_PING:
                        pingDpe(parser.nextString(), sender, returnTopic);
                        break;

                    // Sent by some other dpe, assuming this is a master DPE.
                    // The value is the canonical name of a dpe
                    case CConstants.DPE_UP:
                        dbRegisterDpe(parser.nextString());
                        break;

                    // Sent by the dpe assuming this is a master DPE.
                    // The value is the canonical name of a dpe
                    case CConstants.DPE_DOWN:
                        dbRemoveDpe(parser.nextString());
                        break;

                    // Sent from orchestrator. Value is the name (not canonical)of the container.
                    case CConstants.START_CONTAINER:
                        runContainer(parser.nextString());
                        break;

                    // Sent from orchestrator. Value is the name of the container.
                    // Note that the container name should be a canonical name
                    case CConstants.STOP_CONTAINER:
                        stopContainer(parser.nextString());
                        break;

                    // Sent by the container assuming this is a master DPE.
                    // The value is the canonical name of a container
                    case CConstants.CONTAINER_UP:
                        dbRegisterContainer(parser.nextString(), msg);
                        break;

                    // Sent by the container assuming this is a master DPE.
                    // The value is the canonical name of a container
                    case CConstants.CONTAINER_DOWN:
                        dbRemoveContainer(parser.nextString(), msg);
                        break;

                    case CConstants.START_SERVICE:
                        runService(parser.nextString(), parser.nextString(), parser.nextString());
                        break;

                    case CConstants.STOP_SERVICE:
                        stopService(parser.nextString());
                        break;

                    // Sent by the service assuming this is a master DPE.
                    // The value is the canonical name of a service
                    case CConstants.SERVICE_UP:
                        dbRegisterService(parser.nextString(), msg);
                        break;

                    // Sent by the container assuming this is a master DPE.
                    // The value is the canonical name of a container
                    case CConstants.SERVICE_DOWN:
                        dbRemoveService(parser.nextString(), msg);
                        break;

                    // TODO Implement these requests

                    case CConstants.LIST_DPES:
                        dbListDpes(returnTopic);
                        break;

                    case CConstants.LIST_CONTAINERS:
                        dbListContainers(returnTopic);
                        break;

                    case CConstants.LIST_SERVICES:
                        dbListServices(returnTopic);
                        break;

                    default:
                        break;
                }
            } catch (CException | IOException | xMsgException e) {
                e.printStackTrace();
            }
            return returnMsg;
        }
    }


    private class HeartBeatReport extends Thread {
        @Override
        public void run() {
            try {
                int availableProcessors = Runtime.getRuntime().availableProcessors();
                String claraHome = System.getenv("CLARA_HOME");

                xMsgTopic topic = xMsgTopic.wrap(CConstants.DPE_ALIVE + ":" + feHostIp);
                String data = dpeName + "?" + availableProcessors + "?" + claraHome;

                xMsgAddress address = new xMsgAddress(feHostIp);
                xMsgConnection socket = getNewConnection(address);

                while (true) {
                    xMsgMessage msg = new xMsgMessage(topic, data);
                    genericSend(socket, msg);
                    Thread.sleep(5000);
                }
            } catch (IOException | xMsgException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }


    private void printLogo() {
        System.out.println("================================");
        if (isFE) {
            System.out.println("             CLARA FE         ");
        } else {
            System.out.println("             CLARA DPE        ");
        }
        System.out.println("================================");
        System.out.println(" Binding = Java");
        System.out.println(" Date    = " + CUtility.getCurrentTimeInH());
        System.out.println(" Host    = " + getLocalAddress());
        System.out.println("================================");
    }

    private void startHeartBeatReport() {
        heartBeat = new HeartBeatReport();
        heartBeat.start();
    }

    private void turnFE(String feAddress) {
        feHostIp = feAddress;
        isFE = feHostIp.equals(getLocalAddress());
    }

    private void runDpe(String dpe) {
        if (isFE) {
            startRemoteDpe(dpe);
        }
    }

    private void stopDpe(String dpe)
            throws IOException, xMsgException {
        if (dpeName.equals(dpe)) {
            if (!isFE) {
                reportFE(CConstants.DPE_DOWN);
            }
            System.exit(1);
        } else if (isFE) {
            removeRemoteDpe(dpe);
        }
    }

    private void pingDpe(String dpe, String sender, String returnTopic)
            throws xMsgException, IOException, CException {
        System.out.println("Info: got pinged from DPE = " + sender);
        xMsgTopic topic = xMsgTopic.wrap(CConstants.DPE + ":" + dpe);
        xMsgMessage amsg = new xMsgMessage(topic, CConstants.ALIVE);
        String adpe = ClaraUtil.getHostName(returnTopic);
        genericSend(adpe, amsg);
    }

    private void runContainer(String container)
            throws CException, xMsgException, IOException {
        if (!ClaraUtil.isCanonicalName(container)) {
            // if container name is not canonical we assume it to be started in this DPE
            container = dpeName + ":" + container;
        }
        String tmpDpeName = ClaraUtil.getDpeName(container);
        if (_myCloud.get(tmpDpeName).containsKey(container)) {
            System.err.println("Warning: container = " + container +
                    " is registered on this Dpe. No new container is created.");
        } else {

            if (tmpDpeName.equals(dpeName)) {
                if (feHostIp.equals(xMsgConstants.UNDEFINED.toString())) {
                    startContainer(container);
                } else {
                    startContainer(container, feHostIp);
                    if (!feHostIp.equals(dpeName)) {
                        // report FE container is up
                        reportFE(CConstants.CONTAINER_UP + "?" + container);
                    }
                }
            } else if (isFE) {
                startRemoteContainer(tmpDpeName, container);
                if (_myCloud.containsKey(tmpDpeName)) {
                    _myCloud.get(tmpDpeName).put(container, new HashSet<String>());
                } else {
                    System.out.println("Warning: DPE = " + tmpDpeName +
                            " was not registered previously.");
                    Map<String, Set<String>> tmpContainer = new HashMap<>();
                    tmpContainer.put(container, new HashSet<String>());
                    _myCloud.put(tmpDpeName, tmpContainer);
                }
            }
        }
    }

    private void stopContainer(String container)
            throws CException, xMsgException, IOException {
        if (!ClaraUtil.isCanonicalName(container)) {
            container = dpeName + ":" + container;
        }
        String tmpDpeName = ClaraUtil.getDpeName(container);
        if (tmpDpeName.equals(dpeName)) {
            removeContainer(dpeName, container);
            if (!feHostIp.equals(xMsgConstants.UNDEFINED.toString()) &&
                    !feHostIp.equals(dpeName)) {

                // report FE container is down
                reportFE(CConstants.CONTAINER_DOWN + "?" + container);
            }
        } else if (isFE) {
            removeContainer(tmpDpeName, container);
            if (_myCloud.containsKey(tmpDpeName)) {
                _myCloud.get(tmpDpeName).remove(container);
                System.out.println("Warning: Container = " + container + " is down.");
            }
        }
    }

    private void runService(String service, String classPath, String poolSize)
            throws CException, xMsgException, IOException {
        // in this case value1 is the canonical name of the service
        // and value 2 is the pull path to the class
        // we do not accept non canonical names in this case.
        // The 3rd value is the pool size
        if (ClaraUtil.isCanonicalName(service)) {
            String tmpDpeName = ClaraUtil.getDpeName(service);
            String tmpContainerName = ClaraUtil.getContainerName(service);
            if (tmpDpeName.equals(dpeName)) {
                startService(tmpDpeName, service, classPath, poolSize);
                if (!feHostIp.equals(xMsgConstants.UNDEFINED.toString()) &&
                        !feHostIp.equals(dpeName)) {

                    // report FE container is down
                    reportFE(CConstants.SERVICE_UP + "?" + service);
                }
            } else if (isFE) {
                startService(tmpDpeName, service, classPath, poolSize);
                if (_myCloud.containsKey(tmpDpeName)) {
                    if (_myCloud.get(tmpDpeName).containsKey(tmpContainerName)) {
                        _myCloud.get(tmpDpeName).get(tmpContainerName).add(service);
                    }
                }
            }
        }
    }

    private void stopService(String service)
            throws CException, xMsgException, IOException {
        if (ClaraUtil.isCanonicalName(service)) {
            String tmpDpeName = ClaraUtil.getDpeName(service);
            String tmpContainerName = ClaraUtil.getContainerName(service);
            if (tmpDpeName.equals(dpeName)) {
                removeService(tmpDpeName, service);
                if (!feHostIp.equals(xMsgConstants.UNDEFINED.toString()) &&
                        !feHostIp.equals(dpeName)) {

                    // report FE container is down
                    reportFE(CConstants.SERVICE_DOWN + "?" + service);
                }
            } else if (isFE) {
                removeService(tmpDpeName, service);
                if (_myCloud.containsKey(tmpDpeName)) {
                    if (_myCloud.get(tmpDpeName).containsKey(tmpContainerName)) {
                        _myCloud.get(tmpDpeName).get(tmpContainerName).remove(service);
                    }
                }
            }
        }
    }

    private void dbRegisterDpe(String dpe) {
        if (isFE && !dpe.equals(dpeName)) {
            _myCloud.put(dpe, new HashMap<String, Set<String>>());
        }
    }

    private void dbRemoveDpe(String dpe) {
        if (isFE && !dpe.equals(dpeName)) {
            _myCloud.remove(dpe);
        }
    }

    private void dbRegisterContainer(String container, xMsgMessage msg)
            throws CException, xMsgException, IOException {
        if (!ClaraUtil.isCanonicalName(container)) {
            String tmpDpeName = ClaraUtil.getDpeName(container);

            if (_myCloud.containsKey(tmpDpeName)) {
                _myCloud.get(tmpDpeName).put(container, new HashSet<String>());
            } else {
                System.out.println("Warning: DPE = " + tmpDpeName +
                        " was not registered previously.");
                Map<String, Set<String>> tmpContainer = new HashMap<>();
                tmpContainer.put(container, new HashSet<String>());
                _myCloud.put(tmpDpeName, tmpContainer);
            }
            System.out.println("Info: Container = " + container + " is up.");

            // Forward this to FE DPE
            if (!isFE) {
                genericSend(feHostIp, msg);
            }
        }
    }

    private void dbRemoveContainer(String container, xMsgMessage msg)
            throws CException, xMsgException, IOException {
        if (!ClaraUtil.isCanonicalName(container)) {
            String tmpDpeName = ClaraUtil.getDpeName(container);

            if (_myCloud.containsKey(tmpDpeName)) {
                _myCloud.get(tmpDpeName).remove(container);
                System.out.println("Warning: Container = " + container + " is down.");
            }

            // Forward this to FE DPE
            if (!isFE) {
                genericSend(feHostIp, msg);
            }
        }
    }

    private void dbRegisterService(String service, xMsgMessage msg)
            throws CException, xMsgException, IOException {
        if (ClaraUtil.isCanonicalName(service)) {
            String tmpDpeName = ClaraUtil.getDpeName(service);
            String tmpContainerName = ClaraUtil.getContainerName(service);
            if (_myCloud.containsKey(tmpDpeName)) {
                if (_myCloud.get(tmpDpeName).containsKey(tmpContainerName)) {
                    _myCloud.get(tmpDpeName).get(tmpContainerName).add(service);
                }
            }
            // Forward this to FE DPE
            if (!isFE) {
                genericSend(feHostIp, msg);
            }
        }
    }

    private void dbRemoveService(String service, xMsgMessage msg)
            throws CException, xMsgException, IOException {
        if (ClaraUtil.isCanonicalName(service)) {
            String tmpDpeName = ClaraUtil.getDpeName(service);
            String tmpContainerName = ClaraUtil.getContainerName(service);
            if (_myCloud.containsKey(tmpDpeName)) {
                if (_myCloud.get(tmpDpeName).containsKey(tmpContainerName)) {
                    _myCloud.get(tmpDpeName).get(tmpContainerName).remove(service);
                }
            }
            // Forward this to FE DPE
            if (!isFE) {
                genericSend(feHostIp, msg);
            }
        }
    }

    private void dbListDpes(String returnTopic) {
//        returnMsg = new xMsgMessage(xMsgTopic.wrap(returnTopic),
//                _myCloud.keySet().toArray(
//                        new String[_myCloud.keySet().size()]));
    }

    private void dbListContainers(String returnTopic) {
//        String tmpDpeName = ClaraUtil.getDpeName(value1);
//        if (tmpDpeName != null && xMsgUtil.isIP(tmpDpeName)) {
//            if (_myCloud.containsKey(tmpDpeName)) {
//                returnMsg = new xMsgMessage(xMsgTopic.wrap(returnTopic),
//                        _myCloud.get(tmpDpeName).keySet().toArray(
//                                new String[_myCloud.get(tmpDpeName).keySet().size()]));
//            }
//        }
    }

    private void dbListServices(String returnTopic) {
//        if (ClaraUtil.isCanonicalName(value1)) {
//            try {
//                String tmpDpeN = ClaraUtil.getDpeName(value1);
//                List<String> tmpServices = new ArrayList<>();
//
//                if (tmpDpeN != null) {
//
//                    // all DPEs
//                    if (tmpDpeN.equals("*")) {
//                        for (Map<String, Set<String>> m : _myCloud.values()) {
//                            String tmpContainerName = ClaraUtil.getContainerName(value1);
//
//                            // all containers
//                            if (tmpContainerName.equals("*")) {
//                                for (Set<String> ser : m.values()) {
//                                    for (String s : ser) {
//                                        tmpServices.add(s);
//                                    }
//                                }
//                                // specific container
//                            } else {
//                                Set<String> ser = m.get(tmpContainerName);
//                                for (String s : ser) {
//                                    tmpServices.add(s);
//                                }
//                            }
//                        }
//
//                        // specific DPE
//                    } else {
//                        String tmpContainerName = ClaraUtil.getContainerName(value1);
//
//                        // all containers
//                        if (tmpContainerName.equals("*")) {
//                            for (Set<String> ser : _myCloud.get(tmpDpeN).values()) {
//                                for (String s : ser) {
//                                    tmpServices.add(s);
//                                }
//                            }
//                            // specific container
//                        } else {
//                            Set<String> ser = _myCloud.get(tmpDpeN).get(tmpContainerName);
//                            for (String s : ser) {
//                                tmpServices.add(s);
//                            }
//                        }
//                    }
//
//                }
//                returnMsg = new xMsgMessage(xMsgTopic.wrap(returnTopic),
//                        tmpServices.toArray(new String[tmpServices.size()]));
//            } catch (CException e) {
//                e.printStackTrace();
//            }
//        }
    }
}
