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

import org.jlab.clara.base.CBase;
import org.jlab.clara.base.CException;
import org.jlab.clara.util.CConstants;
import org.jlab.clara.util.CUtility;
import org.jlab.coda.xmsg.core.xMsgCallBack;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgD;
import org.jlab.coda.xmsg.data.xMsgR;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.xsys.xMsgNode;

import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import java.util.concurrent.*;

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
public class Dpe extends CBase{

    // Shared memory used by the services
    // deployed in the JVM where this DPE is running.
    public static ConcurrentHashMap<String, xMsgD.Data.Builder>
            sharedMemory = new ConcurrentHashMap<>();

    // Shared map used by the services to store (un-serialized)
    // user data objects passed within the transient data.
    // Note that serialized user object are passed as byte array
    public static ConcurrentHashMap<String, Object>
            sharedDataObject = new ConcurrentHashMap<>();

    // The name of this dpe. usually it is the IP address
    // of a node where this class is instantiated.
    private String
            dpeName = xMsgConstants.UNDEFINED.getStringValue();

    // The name of the Front-End dpe
    private String
            feHostIp = xMsgConstants.UNDEFINED.getStringValue();

    // Indicates if this DPE is assigned to be the Front-End of the cloud.
    private Boolean isFE = false;

    // Registration database. stores information
    // of cloud DPEs, containers and services.
    // Note. if this is not a FE then the database
    // will contain only a single record (for the current DPE)
    private Map<String, Map<String, Map<String, xMsgR.xMsgRegistrationData.Builder>>>
            _db = new HashMap<>();

    private ScheduledExecutorService scheduledPingService;




    /**
     * <p>
     *     Constructor for a standalone or Front-End DPE
     * </p>
     * @param isFE true if this DPE is assumed the role of the FE
     * @throws xMsgException
     * @throws SocketException
     */
    public Dpe(Boolean isFE) throws xMsgException, SocketException {
        super();
        dpeName = xMsgUtil.getLocalHostIps().get(0);
        setName(dpeName);
        this.isFE = isFE;
        feHostIp = dpeName;

        printLogo();

        // Create the xMsgNode object that will provide
        // dpe registration and discovery service.
        new xMsgNode(false);

        // create the local database
        _db.put(getName(),
                new HashMap<String, Map<String,xMsgR.xMsgRegistrationData.Builder>>());

        // If this DEP is required to be the FE then start a
        // thread that periodically checks the health of cloud DPEs
        if(isFE){
            scheduledPingService = Executors.newScheduledThreadPool(3);
            scheduledPingService.schedule(new Runnable() {
                @Override
                public void run() {
                    for(String dpn:_db.keySet()) {
                        if(!dpn.equals(getName())) {
                            try {
                                Object response = syncPing(dpn,2);
                                if(response!=null) {
                                    if (response instanceof String) {
                                        String s = (String) response;
                                        if (!s.equals(CConstants.ALIVE)) {
                                            removeDpe(dpn);
                                            removeContainerRecord(dpn);
                                        }
                                    }
                                } else {
                                    removeDpe(dpn);
                                    removeContainerRecord(dpn);
                                }
                            } catch (xMsgException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }, 10, TimeUnit.SECONDS);
        }

        // Subscribe messages published to this container
        genericReceive(CConstants.DPE + ":" + getName(),
                new DpeCallBack());
    }

    /**
     * <p>
     *     Constructor for the DPE that is part of the Clara cloud.
     * </p>
     * @param feName the name, i.e. IP address of the FE DPE
     * @throws xMsgException
     * @throws SocketException
     */
    public Dpe(String feName) throws xMsgException, SocketException {
        super();
        dpeName = xMsgUtil.getLocalHostIps().get(0);
        setName(dpeName);
        feHostIp = feName;

        printLogo();
        // Create the xMsgNode object that will provide
        // dpe registration and discovery service.
        new xMsgNode(feName, false);

        // create the local database
        _db.put(getName(),
                new HashMap<String, Map<String,xMsgR.xMsgRegistrationData.Builder>>());

        // Send dpe_up message to the FE
        genericSend(feHostIp,
                CConstants.DPE + ":" + feHostIp,
                CConstants.DPE_UP+"?"+getName());

        // Subscribe messages published to this container
        genericReceive(CConstants.DPE + ":" + getName(),
                new DpeCallBack());
    }

    public static void main(String[] args) {
        if (args.length == 2) {
            if (args[0].equals("-fe_host")) {
                try {
                    new Dpe(args[1]);
                } catch (xMsgException | SocketException e) {
                    System.out.println(e.getMessage());
                    System.out.println("exiting...");
                }
            } else {
                System.out.println("wrong option. Accepts -fe_host option only.");
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
     *     Prints some entry information
     * </p>
     */
    private void printLogo(){
        System.out.println("================================");
        if(isFE){
            System.out.println("             CLARA FE         ");
        }else {
            System.out.println("             CLARA DPE        ");
        }
        System.out.println("================================");
        System.out.println(" Binding = Java");
        System.out.println(" Date    = "+ CUtility.getCurrentTimeInH());
        System.out.println(" Host    = " + getName());
        System.out.println("================================");
    }

    private Object syncPing(String dpeName, int timeOut)
            throws xMsgException {
       try {
            return genericSyncSend(CConstants.DPE + ":" + dpeName,
                   CConstants.DPE_PING,
                   timeOut );
        } catch (TimeoutException e) {
            return null;
        }
    }

    private void removeContainer(String name){
        // remove all services of the container
        try {
            // find dpe name form the container canonical name
            String dpn = CUtility.getDpeName(name);
            // go over all containers of the dpe
            for(String cn:_db.get(dpn).keySet())
                if(cn.equals(name)) {
                    // for the required container get the map of deployed services
                    Map<String, xMsgR.xMsgRegistrationData.Builder> sm = _db.get(dpn).get(cn);
                    // go over all services of the container and send remove service request
                    for(String sn:sm.keySet()) {
                        String dpeHost = CUtility.getDpeName(sn);
                        genericSend(dpeHost,
                                CConstants.CONTAINER + ":" + cn,
                                CConstants.REMOVE_SERVICE+"?"+sn);
                    }
                }
        } catch (CException | xMsgException | SocketException e) {
            e.printStackTrace();
        }
    }

    private void removeDpe(String name){
        // remove all services of the container
        // go over all containers of the dpe
        for(String cn:_db.get(name).keySet()){
            removeContainer(cn);
        }
    }

    private void removeDpeRecord(String name){
        _db.remove(name);
    }

    private void removeContainerRecord(String name){
        try {
            // find dpe name form the container canonical name
            String dpn = CUtility.getDpeName(name);
            if(_db.containsKey(dpn)) {
                _db.get(dpn).remove(name);
            }
        } catch (CException e) {
            e.printStackTrace();
        }
    }

    private void removeServiceRecord(String name){
        try {
            // find dpe name form the container canonical name
            String dpn = CUtility.getDpeName(name);
            if(_db.containsKey(dpn)) {
                String cn = CUtility.getContainerCanonicalName(name);
                _db.get(dpn).get(cn).remove(name);
            }
        } catch (CException e) {
            e.printStackTrace();
        }

    }

    /**
     * <p>
     *     DPE callback. This will react to messages such as:
     *     <ul>
     *         <li>
     *             start/stop container
     *         </li>
     *         <li>
     *             DPE is up/down
     *         </li>
     *         <li>
     *             Container is up/down
     *         </li>
     *         <li>
     *             Service is up/down
     *         </li>
     *     </ul>
     *     Note: The message comes with the xMsg envelope,
     *           where envelope data type is "string" and envelope
     *           payload has a structure:
     *     pre_defined_command?value
     * </p>
     */
    private class DpeCallBack implements xMsgCallBack {

        @Override
        public Object callback(xMsgMessage msg) {

            final String dataType = msg.getDataType();
            final Object data = msg.getData();
            if(dataType.equals(xMsgConstants.ENVELOPE_DATA_TYPE_STRING.getStringValue())) {
                String cmdData = (String)data;
                String cmd = null, value = null;
                try {
                    StringTokenizer st = new StringTokenizer(cmdData, "?");
                    cmd = st.nextToken();
                    value = st.nextToken();
                } catch (NoSuchElementException e){
                    e.printStackTrace();
                }
                if(cmd!=null && value!=null) {
                    switch (cmd) {

                        // Sent from orchestrator. Value is the name (not canonical)of the container.
                        case CConstants.START_CONTAINER:
                            // create canonical name for the container
                            value = getName() + ":" + value;
                            if(_db.get(getName()).keySet().contains(value)){
                                System.err.println("Warning: container = "+ value +
                                        " is registered on this Dpe. No new container is created.");
                                return null;
                            }
                            try {
                                if(feHostIp.equals(xMsgConstants.UNDEFINED.getStringValue())) {
                                    new Container(value);
                                } else {
                                    new Container(value, feHostIp);
                                }
                                // add a record for a new container that will host services
                                _db.get(getName()).put(value, new HashMap<String, xMsgR.xMsgRegistrationData.Builder>());
                            } catch (xMsgException | SocketException e) {
                                e.printStackTrace();
                            }
                            break;

                        // Sent from orchestrator. Value is the name of the container.
                        // Note that the container name should be a canonical name
                        case CConstants.REMOVE_CONTAINER:
                            if(CUtility.isCanonical(value)){
                                removeContainer(value);
                                removeContainerRecord(value);
                                System.out.println(CUtility.getCurrentTimeInH() + ": Stopped container = " + value);
                            }
                            break;

                        // Sent from master DPE (FE). In this case the value
                        // is the canonical name of the master DPE (FE)
                        case CConstants.DPE_PING:
                            try {
                                genericSend(feHostIp,
                                        CConstants.DPE + ":" + value,CConstants.ALIVE);
                            } catch (xMsgException | SocketException e) {
                                e.printStackTrace();
                            }
                            break;

                        // Sent by the dpe assuming this is a master DPE.
                        // The value is the canonical name of a dpe
                        case CConstants.DPE_UP:
                            if(!value.equals(getName())){
                                _db.put(value,
                                        new HashMap<String, Map<String,xMsgR.xMsgRegistrationData.Builder>>());
                            }
                            break;

                        // Sent by the dpe assuming this is a master DPE.
                        // The value is the canonical name of a dpe
                        case CConstants.DPE_DOWN:
                            if(!value.equals(getName())){
                                removeDpe(value);
                                removeDpeRecord(value);
                            }
                            break;

                        // Sent by the container assuming this is a master DPE.
                        // The value is the canonical name of a container
                        case CConstants.CONTAINER_UP:
                            // find dpe name form the container canonical name
                            try {
                                String dpn = CUtility.getDpeName(value);
                                Map<String,Map<String,xMsgR.xMsgRegistrationData.Builder>> cm = _db.get(dpn);
                                if(cm==null){
                                    System.err.printf("Error: DPE for the container = " + value +
                                            " is not registered with the master DPE");
                                    return null;
                                }
                                cm.put(value, new HashMap<String,xMsgR.xMsgRegistrationData.Builder>());
                            } catch (CException e) {
                                e.printStackTrace();
                            }
                            break;

                        // Sent by the container assuming this is a master DPE.
                        // The value is the canonical name of a container
                        case CConstants.CONTAINER_DOWN:
                            removeContainerRecord(value);
                            break;

                        // Sent by the service assuming this is a master DPE.
                        // The value is the canonical name of a service
                        case CConstants.SERVICE_UP:
                            // find dpe and container canonical names form the service canonical name
                            try {
                                String dpn = CUtility.getDpeName(value);
                                String cn = CUtility.getContainerCanonicalName(value);
                                Map<String,Map<String,xMsgR.xMsgRegistrationData.Builder>> cm = _db.get(dpn);
                                if(cm==null){
                                    System.err.printf("Error: DPE for the container = " + value +
                                            " is not registered with the master DPE");
                                    return null;
                                }
                                if(cm.containsKey(cn)) {
                                    cm.get(cn).put(value,xMsgR.xMsgRegistrationData.newBuilder());

                                } else {
                                    System.err.printf("Error: Container for the service = " + value +
                                            " is not registered with the master DPE");
                                    return null;
                                }
                                cm.put(value, new HashMap<String,xMsgR.xMsgRegistrationData.Builder>());
                            } catch (CException e) {
                                e.printStackTrace();
                            }
                            break;

                        // Sent by the container assuming this is a master DPE.
                        // The value is the canonical name of a container
                        case CConstants.SERVICE_DOWN:
                            removeServiceRecord(value);
                            break;
                    }
                }
            }
            return null;
        }
    }

}
