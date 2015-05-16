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
import org.jlab.clara.engine.EDataType;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.ICEngine;
import org.jlab.clara.util.CClassLoader;
import org.jlab.clara.util.CConstants;
import org.jlab.clara.util.CServiceSysConfig;
import org.jlab.clara.util.CUtility;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;
import org.jlab.coda.xmsg.excp.xMsgException;

import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 * </p>
 *
 * @author gurjyan
 * @version 1.x
 * @since 1/30/15
 */
public class Service extends CBase {


    //
    public AtomicBoolean isAvailable;
    // Already recorded (previous) composition
    private String
            p_composition = xMsgConstants.UNDEFINED.getStringValue();
    // user provided engine class container class name
    private String
            engine_class_name = xMsgConstants.UNDEFINED.getStringValue();
    // Engine instantiated object
    private ICEngine
            engine_object = null;
    // key in the shared memory map of DPE to
    // locate this service resulting data object
    private String
            sharedMemoryKey = xMsgConstants.UNDEFINED.getStringValue();
    // Simple average of the service engine
    // execution times over all received requests
    private long _avEngineExecutionTime;
    // Number of received requests to this service.
    // Note: common for different compositions
    private long _numberOfRequests;

    private CompositionAnalyser compositionAnalyser;


    /**
     * <p>
     * Constructor
     * </p>
     *
     * @param packageName service engine package name
     * @param name   Clara service canonical name
     *               (such as dep:container:engine)
     * @param sharedMemoryKey key in the shared memory map of DPE to
     *                        locate this service resulting data object
     * @param feHost front-end host name. This is the host that holds
     *               centralized registration database.
     * @throws xMsgException
     */
    public Service(String packageName,
                   String name,
                   String sharedMemoryKey,
                   String feHost)
            throws xMsgException,
            CException,
            SocketException,
            IllegalAccessException,
            InstantiationException,
            ClassNotFoundException {
        super(feHost);
        setName(name);

        this.sharedMemoryKey = sharedMemoryKey;

        this.engine_class_name = packageName+"."+CUtility.getEngineName(getName());

        // Dynamic loading of the Clara engine class
        // Note: using system class loader
        CClassLoader cl = new CClassLoader(ClassLoader.getSystemClassLoader());
        engine_object = cl.load(engine_class_name);


        // Create a socket connections
        // to the local dpe proxy
        connect();

        isAvailable = new AtomicBoolean(true);

        System.out.println("Service = " + getName()+" is up.");

        // create an object of the composition parser
        compositionAnalyser = new CompositionAnalyser(getName());
    }

    /**
     * <p>
     * Constructor
     * </p>
     *
     * @param packageName service engine package name
     * @param name Clara service canonical name
     *             (such as dep:container:engine)
     * @param sharedMemoryKey key in the shared memory map of DPE to
     *                        locate this service resulting data object
     * @throws xMsgException
     */
    public Service(String packageName,
                   String name,
                   String sharedMemoryKey)
            throws xMsgException,
            CException,
            SocketException,
            IllegalAccessException,
            InstantiationException,
            ClassNotFoundException {
        super();
        setName(name);
        this.sharedMemoryKey = sharedMemoryKey;
        this.engine_class_name = packageName+"."+CUtility.getEngineName(getName());

        // Dynamic loading of the Clara engine class
        // Note: using system class loader
        Class exampleClass = Class.forName(engine_class_name);
        engine_object = (ICEngine) exampleClass.newInstance();

        // Create a socket connections
        // to the local dpe proxy
        connect();

        isAvailable = new AtomicBoolean(true);

        System.out.println("Service = " + getName()+" is up.");

        // create an object of the composition parser
        compositionAnalyser = new CompositionAnalyser(getName());
    }

    public void configure(xMsgMeta.Builder metadata,
                          Object data,
                          AtomicInteger configureCountDown)
            throws CException,
            xMsgException,
            InterruptedException,
            IOException,
            ClassNotFoundException {

        if (metadata.getAction().equals(xMsgMeta.ControlAction.CONFIGURE)) {

            engine_object.configure(new EngineData(metadata, data));

            // If this is a sync request, send done to the requester
            if (!metadata.getReplyTo().equals(xMsgConstants.UNDEFINED.getStringValue()) &&
                    CUtility.isCanonical(metadata.getReplyTo())) {
                int remainingInstances = configureCountDown.decrementAndGet();
                if (remainingInstances == 0) {
                    xMsgMessage msg = new xMsgMessage(metadata.getReplyTo(), xMsgConstants.DONE.getStringValue());
                    String dpe = CUtility.getDpeName(metadata.getReplyTo());
                    genericSend(dpe, msg);
                }
            }
        }
    }

    /**
     * Service process method. Note that configure
     * will never be execute within this method.
     */
    public void process(CServiceSysConfig config,
                        xMsgMeta.Builder metadata,
                        Object data)
            throws CException,
            xMsgException,
            IOException,
            InterruptedException,
            ClassNotFoundException {

        isAvailable.set(false);

        // Increment request count in the sysConfig object
        config.addRequest();

        // Variables to measure service
        // engine execution time
        long startTime;
        long endTime;

        String c_composition = metadata.getComposition();
        String senderService = metadata.getSender();

        if (metadata.getAction().equals(xMsgMeta.ControlAction.EXECUTE)) {

            long execTime;

            if (!c_composition.equals(p_composition)) {
                // analyze composition
                compositionAnalyser.analyzeComposition(c_composition);
                p_composition = c_composition;
            }

            // Execute service engine
            EngineData serviceIn = new EngineData(metadata, data);
            EngineData serviceOut = null;

            for (String com : compositionAnalyser.getInLinks().keySet()) {

                // find a sub_composition that sender
                // service is listed as a an input service
                if (com.contains(senderService)) {

                    // Find if the data from this input service
                    // is required to be logically ANDed with
                    // other input service.
                    // Go over all sub_compositions that require
                    // logical AND of inputs
                    if (compositionAnalyser.getInAndNameList().containsKey(com)) {

                        // Get that sub composition and check against
                        // the received service name the list of service
                        // that are required to be logically ANDed
                        for (String ser : compositionAnalyser.getInAndNameList().get(com)) {
                            if (ser.equals(senderService)) {
                                if (compositionAnalyser.getInAndDataList().containsKey(com)) {
                                    HashMap<String, EngineData> dm = compositionAnalyser.getInAndDataList().get(com);
                                    dm.put(senderService, serviceIn);
                                } else {
                                    HashMap<String, EngineData> dm = new HashMap<>();
                                    dm.put(senderService, serviceIn);
                                    compositionAnalyser.getInAndDataList().put(com, dm);
                                }
                            }
                        }

                        // Now check the size of received data list
                        // with the required input name list.
                        // If equal we will execute the service.
                        if (compositionAnalyser.getInAndNameList().get(com).size() ==
                                compositionAnalyser.getInAndDataList().get(com).size()) {

                            List<EngineData> ddl = new ArrayList<>();

                            for (HashMap<String, EngineData> m : compositionAnalyser.getInAndDataList().values()) {
                                for (EngineData d : m.values()) {
                                    ddl.add(d);
                                }
                            }
//                            System.out.println(senderService + ": Executing engine (logAND) = " + engine_class_name);
                            try {
                                // increment request count
                                _numberOfRequests++;
                                // get engine execution start time
                                startTime = System.nanoTime();

                                serviceOut = engine_object.execute_group(ddl);

                                // get engine execution end time
                                endTime = System.nanoTime();
                                // service engine execution time
                                execTime = endTime - startTime;
                                // Calculate a simple average for the execution time
                                _avEngineExecutionTime = (_avEngineExecutionTime + execTime) / _numberOfRequests;

                            } catch (Throwable t) {
                                serviceIn.getMetaData().setDescription(t.getMessage());
                                serviceIn.getMetaData().setStatus(xMsgMeta.Status.ERROR);
                                serviceIn.getMetaData().setSeverityId(3);
                                report_problem(serviceIn);
                                return;
                            }
                            // Clear inAnd data hash map for the satisfied composition
                            compositionAnalyser.getInAndDataList().remove(com);
                            break;
                        }
                    } else {

                        // sub-composition does not require logical
                        // AND operations at the input of this service
//                        System.out.println(senderService + ": Executing engine = " + engine_class_name);
                        try {
                            // increment request count
                            _numberOfRequests++;
                            // get engine execution start time
                            startTime = System.nanoTime();

                            serviceOut = engine_object.execute(serviceIn);

                            // get engine execution end time
                            endTime = System.nanoTime();
                            // service engine execution time
                            execTime = endTime - startTime;
                            // Calculate a simple average for the execution time
                            _avEngineExecutionTime = (_avEngineExecutionTime + execTime) / _numberOfRequests;

                        } catch (Throwable t) {
                            serviceIn.getMetaData().setDescription(t.getMessage());
                            serviceIn.getMetaData().setStatus(xMsgMeta.Status.ERROR);
                            serviceIn.getMetaData().setSeverityId(3);
                            report_problem(serviceIn);
                            return;
                        }
                        break;
                    }

                } else if (senderService.startsWith("orchestrator")) {
//                    System.out.println(" Orchestrator: Executing engine = " + engine_class_name);
                    try {
                        // increment request count
                        _numberOfRequests++;
                        // get engine execution start time
                        startTime = System.nanoTime();

                        serviceOut = engine_object.execute(serviceIn);

                        // get engine execution end time
                        endTime = System.nanoTime();
                        // service engine execution time
                        execTime = endTime - startTime;
                        // Calculate a simple average for the execution time
                        _avEngineExecutionTime = (_avEngineExecutionTime + execTime) / _numberOfRequests;

                    } catch (Throwable t) {
                        serviceIn.getMetaData().setDescription(t.getMessage());
                        serviceIn.getMetaData().setStatus(xMsgMeta.Status.ERROR);
                        serviceIn.getMetaData().setSeverityId(3);
                        report_problem(serviceIn);
                        return;
                    }
                    break;
                }
            }

            if (serviceOut == null) {
                serviceOut = serviceIn;
                serviceOut.getMetaData().setStatus(xMsgMeta.Status.WARNING);
                serviceOut.getMetaData().setSeverityId(1);
                serviceOut.newData(EDataType.UNDEFINED, null);
            }
            serviceOut.getMetaData().setSender(getName());


            // Send service engine execution data to the services that are linked
            callLinked(config, serviceOut);

            // If this is a sync request send data also to the requester
            if (!serviceOut.getMetaData().getReplyTo().equals(xMsgConstants.UNDEFINED.getStringValue())) {
                String dpeHost = CUtility.getDpeName(serviceOut.getMetaData().getReplyTo());
                xMsgMessage transit;
                if (serviceOut.getMetaData().getDataType().equals(xMsgMeta.DataType.X_Object)) {
                    transit = new xMsgMessage(serviceOut.getMetaData().getReplyTo(), serviceOut.getMetaData(), serviceOut.getxData());
                } else {
                    transit = new xMsgMessage(serviceOut.getMetaData().getReplyTo(), serviceOut.getMetaData(), serviceOut.getData());
                }
                genericSend(dpeHost, transit);
            }

            // If engine defines status error
            // or warning broadcast exception
            if (serviceOut.getMetaData().getStatus().equals(xMsgMeta.Status.ERROR) ||
                    serviceOut.getMetaData().getStatus().equals(xMsgMeta.Status.WARNING)) {
                report_problem(serviceOut);
            }
        }
        isAvailable.set(true);
    }

    /**
     * <p>
     * Calls a service that is linked according to the composition.
     * </p>
     *
     * @param config     additional pre-ordered actions,
     *                   such as reportData or reportDone
     * @param engineData output data of this service
     *                   that is going to be an input
     *                   for the linked service
     * @throws xMsgException
     * @throws IOException
     * @throws CException
     */
    private void callLinked(CServiceSysConfig config,
                            EngineData engineData)
            throws xMsgException, IOException, CException {

        // External broadcast data
        if (config.isDataRequest()){
            report_data(engineData);
            config.resetDataRequestCount();
        }

        // External done broadcasting
        if (config.isDoneRequest()) {
            report_done(engineData);
            config.resetDoneRequestCount();
        }

        // Send to all output-linked services.
        // Note: multiple sub compositions

        // Create transit data
        xMsgMessage transit;
        if (engineData.getMetaData().getDataType().equals(xMsgMeta.DataType.X_Object)) {
            transit = new xMsgMessage(engineData.getMetaData().getReplyTo(), engineData.getMetaData(), engineData.getxData());
        } else {
            transit = new xMsgMessage(engineData.getMetaData().getReplyTo(), engineData.getMetaData(), engineData.getData());
        }

        for (List<String> ls : compositionAnalyser.getOutLinks().values()){
            for(String ss:ls) {
                if (CUtility.isRemoteService(ss)) {
                    engineData.getMetaData().setIsDataSerialized(true);
                } else {
                    engineData.getMetaData().setIsDataSerialized(false);
                }
                transit.setTopic(ss);
                serviceSend(transit);
            }
        }
    }


    /**
     * <p>
     *    Broadcasts the average engine execution time to
     *    done:<service_name></service_name>, averageExecutionTime
     *
     * </p>
     */
    public void report_done(EngineData data)
            throws xMsgException, IOException {

        // we are not sending data
        data.newData(EDataType.UNDEFINED, null);

        // Create transit data
        xMsgMessage transit;
        if (data.getMetaData().getDataType().equals(xMsgMeta.DataType.X_Object)) {
            transit = new xMsgMessage(data.getMetaData().getReplyTo(), data.getMetaData(), data.getxData());
        } else {
            transit = new xMsgMessage(data.getMetaData().getReplyTo(), data.getMetaData(), data.getData());
        }
        transit.setTopic(xMsgConstants.DONE.getStringValue() + ":" + getName());

        String dpe = "localhost";
        if(!getFeHostName().equals(xMsgConstants.UNDEFINED.getStringValue())) {
            dpe = getFeHostName();
        }
        // send always serialized. We want to keep shared memory for data only.
        transit.setIsDataSerialized(true);
        genericSend(dpe, transit);

    }

    /**
     * <p>
     *     Broadcasts a xMsgData transient data
     *     containing data generated by the engine,
     *     i.e. unaltered user engine output data.
     *     Severity = 1 is used to report data.
     *    Note: that the data contains service engine
     *    execution current/instantaneous time
     * </p>
     * @param data EngineData object
     */
    public void report_data(EngineData data)
            throws xMsgException, IOException {

        // Create transit data
        xMsgMessage transit;
        if (data.getMetaData().getDataType().equals(xMsgMeta.DataType.X_Object)) {
            transit = new xMsgMessage(data.getMetaData().getReplyTo(), data.getMetaData(), data.getxData());
        } else {
            transit = new xMsgMessage(data.getMetaData().getReplyTo(), data.getMetaData(), data.getData());
        }
        transit.setTopic(xMsgConstants.DATA.getStringValue() + ":" + getName());

        String dpe = "localhost";
        if (!getFeHostName().equals(xMsgConstants.UNDEFINED.getStringValue())) {
            dpe = getFeHostName();
        }
        // send always serialized. We want to keep shared memory for application data only.
        transit.setIsDataSerialized(true);
        genericSend(dpe, transit);

    }

    /**
     * <p>
     *    Broadcasts the average engine execution time to
     *    done:<service_name></service_name>, averageExecutionTime
     *
     * </p>
     */
    public void report_problem(EngineData data)
            throws xMsgException, IOException {

        // we are not sending data
        data.newData(EDataType.UNDEFINED, null);

        // Create transit data
        xMsgMessage transit;
        if (data.getMetaData().getDataType().equals(xMsgMeta.DataType.X_Object)) {
            transit = new xMsgMessage(data.getMetaData().getReplyTo(), data.getMetaData(), data.getxData());
        } else {
            transit = new xMsgMessage(data.getMetaData().getReplyTo(), data.getMetaData(), data.getData());
        }
        if (data.getMetaData().getStatus().equals(xMsgMeta.Status.ERROR)) {
            transit.setTopic(xMsgConstants.ERROR.getStringValue() + ":" + getName());
        } else if (data.getMetaData().getStatus().equals(xMsgMeta.Status.WARNING)) {
            transit.setTopic(xMsgConstants.WARNING.getStringValue() + ":" + getName());
        }
        String dpe = "localhost";
        if (!getFeHostName().equals(xMsgConstants.UNDEFINED.getStringValue())) {
            dpe = getFeHostName();
        }
        // send always serialized. We want to keep shared memory for data only.
        transit.setIsDataSerialized(true);
        genericSend(dpe, transit);

    }

    /**
     * <p>
     *  Removes service xMsg registration
     * <p/>
     */
    public void remove_registration()
            throws xMsgException {

        removeSubscriberRegistration(getName(),
                xMsgUtil.getTopicDomain(getName()),
                xMsgUtil.getTopicSubject(getName()),
                xMsgUtil.getTopicType(getName()));
    }

    /**
     *
     * @throws xMsgException
     */
    public void dispose() throws xMsgException, IOException {
        remove_registration();

        // Send service_down message
        String localDpe = xMsgUtil.getLocalHostIps().get(0);

        xMsgMessage msg1 = new xMsgMessage(CConstants.SERVICE + ":" + localDpe,
                CConstants.SERVICE_DOWN + "?" + getName());

        genericSend(localDpe, msg1);
        if(!getFeHostName().equals(xMsgConstants.UNDEFINED.getStringValue())) {
            xMsgMessage msg2 = new xMsgMessage(CConstants.SERVICE + ":" + getFeHostName(),
                    CConstants.SERVICE_DOWN + "?" + getName());
            genericSend(getFeHostName(), msg2);
        }
    }

}
