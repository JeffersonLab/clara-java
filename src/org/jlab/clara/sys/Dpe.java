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
import org.jlab.clara.util.CConstants;
import org.jlab.clara.util.CUtility;
import org.jlab.coda.xmsg.core.xMsgCallBack;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgD;
import org.jlab.coda.xmsg.data.xMsgM;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgAddress;
import org.jlab.coda.xmsg.net.xMsgConnection;
import org.jlab.coda.xmsg.xsys.xMsgRegistrar;

import java.io.IOException;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *     Clara data processing environment. It can play the role of
 *     the Front-End (FE), which is the static point of the entire cloud.
 *     It creates and manages the registration database (local and
 *     case of being assigned as an FE: global database). Note this is a
 *     copy of the subscribers database resident in the xMsg registration
 *     database. This also creates a shared memory for communicating Clara
 *     transient data objects between services within the same process
 *     (this avoids data serialization and de-serialization).
 * </p>
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
            dpeName = xMsgConstants.UNDEFINED.getStringValue();

    // The name of the Front-End dpe
    private String
            feHostIp = xMsgConstants.UNDEFINED.getStringValue();

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


    /**
     * <p>
     * Constructor for a standalone or Front-End DPE
     * </p>
     *
     * @param isFE true if this DPE is assumed the role of the FE
     * @throws xMsgException
     * @throws SocketException
     */
    public Dpe(Boolean isFE) throws xMsgException, SocketException {
        super();

        dpeName = xMsgUtil.getLocalHostIps().get(0);
        setMyName(dpeName);
        this.isFE = isFE;
        if (isFE) {
            feHostIp = dpeName;
        }

        printLogo();

        // If this DEP is required to be the FE then start a
        // thread that periodically checks the health of cloud DPEs
        if (isFE) {
            ScheduledExecutorService scheduledPingService = Executors.newScheduledThreadPool(3);
            scheduledPingService.schedule(new Runnable() {
                @Override
                public void run() {
                    for (String dpn : _myCloud.keySet()) {
                        if (!dpn.equals(getMyName())) {
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
        genericReceive(CConstants.DPE + ":" + getMyName(),
                new DpeCallBack());

        // Create the xMsgNode object that will provide
        // dpe registration and discovery service.
        registrar = new xMsgRegistrar();
        _myCloud.put(dpeName, new HashMap<String, Set<String>>());

        startHeartBeatReport();
    }

    /**
     * <p>
     * Constructor for the DPE that is part of the Clara cloud.
     * </p>
     *
     * @param feName the name, i.e. IP address of the FE DPE
     * @throws xMsgException
     * @throws SocketException
     */
    public Dpe(String feName) throws xMsgException, SocketException {
        super();

        dpeName = xMsgUtil.getLocalHostIps().get(0);
        setMyName(dpeName);
        feHostIp = xMsgUtil.host_to_ip(feName);

        printLogo();

        // Send dpe_up message to the FE
        try {
            xMsgMessage msg = new xMsgMessage(CConstants.DPE + ":" + feHostIp,
                    CConstants.DPE_UP + "?" + getMyName());
            genericSend(feHostIp, msg);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Subscribe messages published to this container
        genericReceive(CConstants.DPE + ":" + getMyName(),
                new DpeCallBack());

        // Create the xMsgNode object that will provide
        // dpe registration and discovery service.
        registrar = new xMsgRegistrar();
        _myCloud.put(dpeName, new HashMap<String, Set<String>>());

        startHeartBeatReport();
    }

    public static void main(String[] args) {
        if (args.length == 2) {
            switch (args[0]) {
                case "-fh":
                    try {
                        new Dpe(args[1]);
                    } catch (xMsgException | SocketException e) {
                        System.out.println(e.getMessage());
                        System.out.println("exiting...");
                    }
                    break;
                case "-cc":
                    try {
                        new Dpe(true);
                    } catch (xMsgException | SocketException e) {
                        e.printStackTrace();
                    }
                    break;
                case "-h":
                case "-help":
                    System.out.println("synopsis: j_dpe [-cc (start as a cloud controller)] " +
                            "[-fh (host IP of the cloud controller, i.e.FE)] " +
                            "[-h or help] ");
                    break;
            }
        } else if (args.length == 0) {
            try {
                new Dpe(false);
            } catch (xMsgException | SocketException e) {
                System.out.println(e.getMessage());
                System.out.println("exiting...");
            }
        }
    }

    /**
     * <p>
     * Prints some entry information
     * </p>
     */
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
        System.out.println(" Host    = " + getMyName());
        System.out.println("================================");
    }


    /**
     * <p>
     * DPE callback. This will react to messages such as:
     * <ul>
     * <li>
     * start/stop DPE/Containers/Services
     * </li>
     * <li>
     * DPE is up/down
     * </li>
     * <li>
     * Container is up/down
     * </li>
     * <li>
     * Service is up/down
     * </li>
     * <li>
     * etc.
     * </li>
     * </ul>
     * Note: The message comes with the xMsg envelope,
     * with xMsgMeta and xMsgData. data type is defined as string
     * in xMsgMeta and data value in xMsgData has a
     * pre_defined_command?value structure
     * </p>
     */
    private class DpeCallBack implements xMsgCallBack {

        @Override
        public xMsgMessage callback(xMsgMessage msg) {

            xMsgMessage returnMsg = null;

            final xMsgM.xMsgMeta.Builder metadata = msg.getMetaData();
            String sender = metadata.getSender();
            String returnTopic = metadata.getReplyTo();

            if (metadata.getDataType().equals(xMsgM.xMsgMeta.DataType.X_Object)) {
                final xMsgD.xMsgData.Builder data = (xMsgD.xMsgData.Builder) msg.getData();
                if (data.getType().equals(xMsgD.xMsgData.Type.T_STRING)) {

                    String cmdData = data.getSTRING();
                    String cmd = null, value1 = null, value2 = null, value3 = null;
                    try {
                        StringTokenizer st = new StringTokenizer(cmdData, "?");
                        cmd = st.nextToken();
                        if (st.hasMoreTokens()) value1 = st.nextToken();
                        if (st.hasMoreTokens()) value2 = st.nextToken();
                        if (st.hasMoreTokens()) value3 = st.nextToken();
                    } catch (NoSuchElementException e) {
                        e.printStackTrace();
                    }
                    if (cmd != null && value1 != null) {
                        switch (cmd) {

                            case CConstants.ACCEPT_FE:
                                feHostIp = value1;
                                isFE = feHostIp.equals(dpeName);
                                break;

                            // Sent from orchestrator. Assuming this is the FE
                            case CConstants.START_DPE:
                                if (isFE) startRemoteDpe(value1);
                                break;

                            // Sent from orchestrator.
                            case CConstants.STOP_DPE:
                                if (dpeName.equals(value1)) {
                                    if (!isFE) {
                                        try {
                                            reportFE(CConstants.DPE_DOWN);
                                        } catch (IOException | xMsgException e) {
                                            e.printStackTrace();
                                        }
                                    }
                                    System.exit(1);
                                } else if (isFE) {
                                    try {
                                        removeRemoteDpe(value1);
                                    } catch (IOException | xMsgException e) {
                                        e.printStackTrace();
                                    }
                                }
                                break;

                            // Sent from master DPE (FE). In this case the value
                            // is the canonical name of the master DPE (FE)
                            case CConstants.DPE_PING:
                                System.out.println("Info: got pinged from DPE = " + sender);
                                try {
                                    xMsgMessage amsg = new xMsgMessage(CConstants.DPE + ":" + value1,
                                            CConstants.ALIVE);
                                    String dpe = CUtility.getDpeName(returnTopic);
                                    genericSend(dpe, amsg);
                                } catch (xMsgException | IOException | CException e) {
                                    e.printStackTrace();
                                }
                                break;

                            // Sent by some other dpe, assuming this is a master DPE.
                            // The value is the canonical name of a dpe
                            case CConstants.DPE_UP:
                                if (isFE && !value1.equals(dpeName)) {
                                    _myCloud.put(value1, new HashMap<String, Set<String>>());
                                }
                                break;

                            // Sent by the dpe assuming this is a master DPE.
                            // The value is the canonical name of a dpe
                            case CConstants.DPE_DOWN:
                                if (isFE && !value1.equals(dpeName)) {
                                    _myCloud.remove(value1);

                                }
                                break;

                            // Sent from orchestrator. Value is the name (not canonical)of the container.
                            case CConstants.START_CONTAINER:
                                if (!CUtility.isCanonical(value1)) {
                                    // if container name is not canonical we assume it to be started in this DPE
                                    value1 = dpeName + ":" + value1;
                                }
                                try {
                                    String tmpDpeName = CUtility.getDpeName(value1);
                                    if (_myCloud.get(tmpDpeName).containsKey(value1)) {
                                        System.err.println("Warning: container = " + value1 +
                                                " is registered on this Dpe. No new container is created.");
                                    } else {

                                        if (tmpDpeName.equals(dpeName)) {
                                            if (feHostIp.equals(xMsgConstants.UNDEFINED.getStringValue())) {
                                                startContainer(value1);
                                            } else {
                                                startContainer(value1, feHostIp);
                                                if (!feHostIp.equals(dpeName)) {
                                                    // report FE container is up
                                                    reportFE(CConstants.CONTAINER_UP + "?" + value1);
                                                }
                                            }
                                        } else if (isFE) {
                                            startRemoteContainer(tmpDpeName, value1);
                                            if (_myCloud.containsKey(tmpDpeName)) {
                                                _myCloud.get(tmpDpeName).put(value1, new HashSet<String>());
                                            } else {
                                                System.out.println("Warning: DPE = " + tmpDpeName +
                                                        " was not registered previously.");
                                                Map<String, Set<String>> tmpContainer = new HashMap<>();
                                                tmpContainer.put(value1, new HashSet<String>());
                                                _myCloud.put(tmpDpeName, tmpContainer);
                                            }
                                        }
                                    }
                                } catch (CException | xMsgException | IOException e) {
                                    e.printStackTrace();
                                }
                                break;


                            // Sent from orchestrator. Value is the name of the container.
                            // Note that the container name should be a canonical name
                            case CConstants.STOP_CONTAINER:
                                if (!CUtility.isCanonical(value1)) {
                                    value1 = dpeName + ":" + value1;
                                }
                                try {
                                    String tmpDpeName = CUtility.getDpeName(value1);
                                    if (tmpDpeName.equals(dpeName)) {
                                        removeContainer(dpeName, value1);
                                        if (!feHostIp.equals(xMsgConstants.UNDEFINED.getStringValue()) &&
                                                !feHostIp.equals(dpeName)) {

                                            // report FE container is down
                                            reportFE(CConstants.CONTAINER_DOWN + "?" + value1);
                                        }
                                    } else if (isFE) {
                                        removeContainer(tmpDpeName, value1);
                                        if (_myCloud.containsKey(tmpDpeName)) {
                                            _myCloud.get(tmpDpeName).remove(value1);
                                            System.out.println("Warning: Container = " + value1 + " is down.");
                                        }
                                    }
                                } catch (CException | xMsgException | IOException e) {
                                    e.printStackTrace();
                                }
                                break;

                            // Sent by the container assuming this is a master DPE.
                            // The value is the canonical name of a container
                            case CConstants.CONTAINER_UP:
                                if (!CUtility.isCanonical(value1)) {
                                    try {
                                        String tmpDpeName = CUtility.getDpeName(value1);

                                        if (_myCloud.containsKey(tmpDpeName)) {
                                            _myCloud.get(tmpDpeName).put(value1, new HashSet<String>());
                                        } else {
                                            System.out.println("Warning: DPE = " + tmpDpeName +
                                                    " was not registered previously.");
                                            Map<String, Set<String>> tmpContainer = new HashMap<>();
                                            tmpContainer.put(value1, new HashSet<String>());
                                            _myCloud.put(tmpDpeName, tmpContainer);
                                        }
                                        System.out.println("Info: Container = " + value1 + " is up.");

                                        // Forward this to FE DPE
                                        if (!isFE) {
                                            genericSend(feHostIp, msg);
                                        }
                                    } catch (CException | xMsgException | IOException e) {
                                        e.printStackTrace();
                                    }
                                }
                                break;

                            // Sent by the container assuming this is a master DPE.
                            // The value is the canonical name of a container
                            case CConstants.CONTAINER_DOWN:
                                if (!CUtility.isCanonical(value1)) {

                                    try {
                                        String tmpDpeName = CUtility.getDpeName(value1);

                                        if (_myCloud.containsKey(tmpDpeName)) {
                                            _myCloud.get(tmpDpeName).remove(value1);
                                            System.out.println("Warning: Container = " + value1 + " is down.");
                                        }

                                        // Forward this to FE DPE
                                        if (!isFE) {
                                            genericSend(feHostIp, msg);
                                        }
                                    } catch (CException | xMsgException | IOException e) {
                                        e.printStackTrace();
                                    }
                                }
                                break;

                            case CConstants.START_SERVICE:
                                // in this case value1 is the canonical name of the service
                                // and value 2 is the pull path to the class
                                // we do not accept non canonical names in this case.
                                // The 3rd value is the pool size
                                if (CUtility.isCanonical(value1)) {
                                    try {
                                        String tmpDpeName = CUtility.getDpeName(value1);
                                        String tmpContainerName = CUtility.getContainerName(value1);
                                        if (tmpDpeName.equals(dpeName)) {
                                            startService(tmpDpeName, value1, value2, value3);
                                            if (!feHostIp.equals(xMsgConstants.UNDEFINED.getStringValue()) &&
                                                    !feHostIp.equals(dpeName)) {

                                                // report FE container is down
                                                reportFE(CConstants.SERVICE_UP + "?" + value1);
                                            }
                                        } else if (isFE) {
                                            startService(tmpDpeName, value1, value2, value3);
                                            if (_myCloud.containsKey(tmpDpeName)) {
                                                if (_myCloud.get(tmpDpeName).containsKey(tmpContainerName)) {
                                                    _myCloud.get(tmpDpeName).get(tmpContainerName).add(value1);
                                                }
                                            }
                                        }
                                    } catch (CException | xMsgException | IOException e) {
                                        e.printStackTrace();
                                    }

                                }
                                break;

                            case CConstants.STOP_SERVICE:
                                if (CUtility.isCanonical(value1)) {
                                    try {
                                        String tmpDpeName = CUtility.getDpeName(value1);
                                        String tmpContainerName = CUtility.getContainerName(value1);
                                        if (tmpDpeName.equals(dpeName)) {
                                            removeService(tmpDpeName, value1);
                                            if (!feHostIp.equals(xMsgConstants.UNDEFINED.getStringValue()) &&
                                                    !feHostIp.equals(dpeName)) {

                                                // report FE container is down
                                                reportFE(CConstants.SERVICE_DOWN + "?" + value1);
                                            }
                                        } else if (isFE) {
                                            removeService(tmpDpeName, value1);
                                            if (_myCloud.containsKey(tmpDpeName)) {
                                                if (_myCloud.get(tmpDpeName).containsKey(tmpContainerName)) {
                                                    _myCloud.get(tmpDpeName).get(tmpContainerName).remove(value1);
                                                }
                                            }
                                        }
                                    } catch (CException | xMsgException | IOException e) {
                                        e.printStackTrace();
                                    }
                                }
                                break;

                            // Sent by the service assuming this is a master DPE.
                            // The value is the canonical name of a service
                            case CConstants.SERVICE_UP:
                                if (CUtility.isCanonical(value1)) {
                                    try {
                                        String tmpDpeName = CUtility.getDpeName(value1);
                                        String tmpContainerName = CUtility.getContainerName(value1);
                                        if (_myCloud.containsKey(tmpDpeName)) {
                                            if (_myCloud.get(tmpDpeName).containsKey(tmpContainerName)) {
                                                _myCloud.get(tmpDpeName).get(tmpContainerName).add(value1);
                                            }
                                        }
                                        // Forward this to FE DPE
                                        if (!isFE) {
                                            genericSend(feHostIp, msg);
                                        }

                                    } catch (CException | xMsgException | IOException e) {
                                        e.printStackTrace();
                                    }
                                }
                                break;

                            // Sent by the container assuming this is a master DPE.
                            // The value is the canonical name of a container
                            case CConstants.SERVICE_DOWN:
                                if (CUtility.isCanonical(value1)) {
                                    try {
                                        String tmpDpeName = CUtility.getDpeName(value1);
                                        String tmpContainerName = CUtility.getContainerName(value1);
                                        if (_myCloud.containsKey(tmpDpeName)) {
                                            if (_myCloud.get(tmpDpeName).containsKey(tmpContainerName)) {
                                                _myCloud.get(tmpDpeName).get(tmpContainerName).remove(value1);
                                            }
                                        }

                                        // Forward this to FE DPE
                                        if (!isFE) {
                                            genericSend(feHostIp, msg);
                                        }
                                    } catch (CException | xMsgException | IOException e) {
                                        e.printStackTrace();
                                    }
                                }
                                break;

                            case CConstants.LIST_DPES:
                                returnMsg = new xMsgMessage(returnTopic,
                                        _myCloud.keySet().toArray(
                                                new String[_myCloud.keySet().size()]));
                                break;

                            case CConstants.LIST_CONTAINERS:
                                String tmpDpeName = CUtility.getIPAddress(value1);
                                if (tmpDpeName != null && xMsgUtil.isIP(tmpDpeName)) {
                                    if (_myCloud.containsKey(tmpDpeName)) {
                                        returnMsg = new xMsgMessage(returnTopic,
                                                _myCloud.get(tmpDpeName).keySet().toArray(
                                                        new String[_myCloud.get(tmpDpeName).keySet().size()]));
                                    }
                                }
                                break;

                            case CConstants.LIST_SERVICES:
                                if (CUtility.isCanonical(value1)) {
                                    try {
                                        String tmpDpeN = CUtility.getIPAddress(CUtility.getDpeName(value1));
                                        List<String> tmpServices = new ArrayList<>();

                                        if (tmpDpeN != null) {

                                            // all DPEs
                                            if (tmpDpeN.equals("*")) {
                                                for (Map<String, Set<String>> m : _myCloud.values()) {
                                                    String tmpContainerName = CUtility.getContainerName(value1);

                                                    // all containers
                                                    if (tmpContainerName.equals("*")) {
                                                        for (Set<String> ser : m.values()) {
                                                            for (String s : ser) {
                                                                tmpServices.add(s);
                                                            }
                                                        }
                                                        // specific container
                                                    } else {
                                                        Set<String> ser = m.get(tmpContainerName);
                                                        for (String s : ser) {
                                                            tmpServices.add(s);
                                                        }
                                                    }
                                                }

                                                // specific DPE
                                            } else {
                                                String tmpContainerName = CUtility.getContainerName(value1);

                                                // all containers
                                                if (tmpContainerName.equals("*")) {
                                                    for (Set<String> ser : _myCloud.get(tmpDpeN).values()) {
                                                        for (String s : ser) {
                                                            tmpServices.add(s);
                                                        }
                                                    }
                                                    // specific container
                                                } else {
                                                    Set<String> ser = _myCloud.get(tmpDpeN).get(tmpContainerName);
                                                    for (String s : ser) {
                                                        tmpServices.add(s);
                                                    }
                                                }
                                            }

                                        }
                                        returnMsg = new xMsgMessage(returnTopic,
                                                tmpServices.toArray(new String[tmpServices.size()]));
                                    } catch (CException e) {
                                        e.printStackTrace();
                                    }
                                }
                                break;
                        }
                    }
                }
            }
            return returnMsg;
        }
    }



    private void startHeartBeatReport() {
        heartBeat = new HeartBeatReport();
        heartBeat.start();
    }



    private class HeartBeatReport extends Thread {
        @Override
        public void run() {
            try {
                int availableProcessors = Runtime.getRuntime().availableProcessors();
                String claraHome = System.getenv("CLARA_HOME");

                String topic = CConstants.DPE_ALIVE + ":" + feHostIp;
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
}
