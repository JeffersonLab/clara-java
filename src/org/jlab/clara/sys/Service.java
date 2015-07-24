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
import org.jlab.clara.engine.EDataType;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.ICEngine;
import org.jlab.clara.sys.ccc.*;
import org.jlab.clara.util.CClassLoader;
import org.jlab.clara.util.CConstants;
import org.jlab.clara.util.CServiceSysConfig;
import org.jlab.clara.util.CUtility;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;
import org.jlab.coda.xmsg.excp.xMsgException;

import java.io.IOException;
import java.net.SocketException;
import java.util.*;
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

    private ServiceState myServiceState =
            new ServiceState(getMyName(),xMsgConstants.UNDEFINED.toString());

    public AtomicBoolean isAvailable;
    // Already recorded (previous) composition
    private String
            p_composition = xMsgConstants.UNDEFINED.toString();
    // user provided engine class container class name
    private String
            engine_class_name = xMsgConstants.UNDEFINED.toString();
    // Engine instantiated object
    private ICEngine
            engine_object = null;

    // key in the shared memory map of DPE to
    // locate this service resulting data object
    private String
            sharedMemoryKey = xMsgConstants.UNDEFINED.toString();
    // Simple average of the service engine
    // execution times over all received requests
    private long _avEngineExecutionTime;
    // Number of received requests to this service.
    // Note: common for different compositions
    private long _numberOfRequests;

    private CCompiler compiler;


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
        super(name, feHost);

        this.sharedMemoryKey = sharedMemoryKey;

        this.engine_class_name = packageName+"."+CUtility.getEngineName(getMyName());

        // Dynamic loading of the Clara engine class
        // Note: using system class loader
        CClassLoader cl = new CClassLoader(ClassLoader.getSystemClassLoader());
        engine_object = cl.load(engine_class_name);


        // Create a socket connections
        // to the local dpe proxy
        connect();

        isAvailable = new AtomicBoolean(true);

        System.out.println("Service = " + getMyName()+" is up.");

        // create an object of the composition parser
        compiler = new CCompiler(getMyName());
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
        super(name);
        this.sharedMemoryKey = sharedMemoryKey;
        this.engine_class_name = packageName+"."+CUtility.getEngineName(getMyName());

        // Dynamic loading of the Clara engine class
        // Note: using system class loader
        Class exampleClass = Class.forName(engine_class_name);
        engine_object = (ICEngine) exampleClass.newInstance();

        // Create a socket connections
        // to the local dpe proxy
        connect();

        isAvailable = new AtomicBoolean(true);

        System.out.println("Service = " + getMyName()+" is up.");

        // create an object of the composition parser
        compiler = new CCompiler(getMyName());
    }

    public ServiceState getMyServiceState() {
        return myServiceState;
    }

    public void updateMyState(String state) {
        myServiceState.setState(state);
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
            if (!metadata.getReplyTo().equals(xMsgConstants.UNDEFINED.toString()) &&
                    CUtility.isCanonical(metadata.getReplyTo())) {
                int remainingInstances = configureCountDown.decrementAndGet();
                if (remainingInstances == 0) {
                    xMsgTopic topic = xMsgTopic.wrap(metadata.getReplyTo());
                    xMsgMessage msg = new xMsgMessage(topic, xMsgConstants.DONE.toString());
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

        String c_composition = metadata.getComposition();

        ServiceState senderServiceState =
                new ServiceState(metadata.getSender(), metadata.getSenderState());


        if (metadata.getAction().equals(xMsgMeta.ControlAction.EXECUTE)) {

            if (!c_composition.equals(p_composition)) {
                // analyze composition
                compiler.compile(c_composition);
                p_composition = c_composition;
            }


            for (Instruction inst : compiler.getInstructions()) {

                // get the condition of the instruction
                // if condition...
                Condition if_cnd = inst.getIfCondition();
                Condition elseif_cnd = inst.getElseifCondition();

                // the set of routing statements
                Set<Statement> r_stmt;

                if (if_cnd != null) {
                    // Conditional routing.

                    if (if_cnd.isTrue(getMyServiceState(), senderServiceState)) {
                        r_stmt = inst.getIfCondStatements();
                    } else if (elseif_cnd.isTrue(getMyServiceState(), senderServiceState)) {
                        r_stmt = inst.getElseifCondStatements();
                    } else {
                        r_stmt = inst.getElseCondStatements();
                    }
                } else {

                    // unconditional routing
                    r_stmt = inst.getUnCondStatements();
                }

                // execute service engine and route the statements
                // note that service engine will not be executed if
                // data for all inputs are present in the logical AND case.
                // Execute service engine
                EngineData inData = new EngineData(metadata, data);

                execAndRoute(config, r_stmt, senderServiceState, inData);
            }
        }
        isAvailable.set(true);
    }

    private EngineData executeEngine( Set<EngineData> inData)
            throws IOException, xMsgException {
        EngineData outData = null;

        // Variables to measure service
        // engine execution time
        long startTime;
        long endTime;
        long execTime;

        try {
            // increment request count
            _numberOfRequests++;
            // get engine execution start time
            startTime = System.nanoTime();

            if(inData.size()==1) {
                outData = engine_object.execute(inData.iterator().next());
            } else {
                outData = engine_object.execute_group(inData);

            }
            // get engine execution end time
            endTime = System.nanoTime();
            // service engine execution time
            execTime = endTime - startTime;
            // Calculate a simple average for the execution time
            _avEngineExecutionTime = (_avEngineExecutionTime + execTime) / _numberOfRequests;

            // update service state based on the engine set state
            updateMyState(outData.getState());

        } catch (Throwable t) {
            EngineData fst = inData.iterator().next();
            fst.getMetaData().setDescription(t.getMessage());
            fst.getMetaData().setStatus(xMsgMeta.Status.ERROR);
            fst.getMetaData().setSeverityId(3);
            report_problem(fst);
            t.printStackTrace();
        }
        return outData;
    }

    private void execAndRoute(CServiceSysConfig config,
                              Set<Statement> r_stmt,
                              ServiceState inServiceState,
                              EngineData inData)
            throws IOException, xMsgException, CException {

        EngineData outData;
        for (Statement st : r_stmt) {
            if(st.getInputLinks().contains(inServiceState.getName())) {

                Set<EngineData> ens = new HashSet<>();
                ens.add(inData);
                outData =  executeEngine(ens);

                callLinked(config, outData, st.getOutputLinks());

            } else if(st.getLogAndInputs().containsKey(inServiceState.getName())){

                st.getLogAndInputs().put(inServiceState.getName(), inData);

                // check to see if all required data is present (are not null)

                boolean groupExecute = true;
                for(EngineData ed : st.getLogAndInputs().values()){
                    if(ed == null) {
                        groupExecute = false;
                        break;
                    }
                }

                if(groupExecute){

                    Set<EngineData> ens = new HashSet<>();
                    // engine group execute

                    for(EngineData ed: st.getLogAndInputs().values()){
                            ens.add(ed);
                    }
                    outData = executeEngine(ens);

                    callLinked(config,outData,st.getOutputLinks());

                    // reset data in the logAndInputs map
                    for(String s : st.getLogAndInputs().keySet()){
                        st.getLogAndInputs().put(s,null);
                    }
                }

            }
        }
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
                            EngineData engineData,
                            Set<String> outLinks)
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
        xMsgMessage transit = engineDataToxMsg(engineData);

        for(String ss:outLinks) {
            if (CUtility.isRemoteService(ss)) {
                engineData.getMetaData().setIsDataSerialized(true);
            } else {
                engineData.getMetaData().setIsDataSerialized(false);
            }
            transit.setTopic(xMsgTopic.wrap(ss));
            serviceSend(transit);
        }
    }


    private xMsgMessage engineDataToxMsg(EngineData engineData){
        xMsgTopic topic = xMsgTopic.wrap(engineData.getMetaData().getReplyTo());
        xMsgMessage transit = new xMsgMessage(topic);
        transit.setMetaData(engineData.getMetaData());
        transit.setData(engineData.getxData().build());
        return transit;
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
        xMsgTopic topic = xMsgTopic.wrap(xMsgConstants.DONE.toString() + ":" + getMyName());
        xMsgMessage transit = new xMsgMessage(topic);
        transit.setMetaData(data.getMetaData());
        transit.setData(data.getxData().build());

        String dpe = "localhost";
        if(!getFeHostName().equals(xMsgConstants.UNDEFINED.toString())) {
            dpe = getFeHostName();
        }
        // send always serialized. We want to keep shared memory for data only.
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
        xMsgTopic topic = xMsgTopic.wrap(xMsgConstants.DATA.toString() + ":" + getMyName());
        xMsgMessage transit = new xMsgMessage(topic);
        transit.setMetaData(data.getMetaData());
        transit.setData(data.getxData().build());

        String dpe = "localhost";
        if (!getFeHostName().equals(xMsgConstants.UNDEFINED.toString())) {
            dpe = getFeHostName();
        }
        // send always serialized. We want to keep shared memory for application data only.
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
        xMsgTopic topic;
        if (data.getMetaData().getStatus().equals(xMsgMeta.Status.ERROR)) {
            topic = xMsgTopic.wrap(xMsgConstants.ERROR.toString() + ":" + getMyName());
        } else if (data.getMetaData().getStatus().equals(xMsgMeta.Status.WARNING)) {
            topic = xMsgTopic.wrap(xMsgConstants.WARNING.toString() + ":" + getMyName());
        } else {
            return;
        }

        xMsgMessage transit = new xMsgMessage(topic);
        transit.setMetaData(data.getMetaData());
        transit.setData(data.getxData().build());
        String dpe = "localhost";
        if (!getFeHostName().equals(xMsgConstants.UNDEFINED.toString())) {
            dpe = getFeHostName();
        }
        // send always serialized. We want to keep shared memory for data only.
        genericSend(dpe, transit);
    }



    /**
     * <p>
     *  Removes service xMsg registration
     * <p/>
     */
    public void remove_registration()
            throws xMsgException {

        removeSubscriber(xMsgTopic.wrap(getMyName()));
    }

    /**
     *
     * @throws xMsgException
     */
    public void dispose() throws xMsgException, IOException {
        remove_registration();

        String data = CConstants.SERVICE_DOWN + "?" + getMyName();

        // Send service_down message
        String localDpe = xMsgUtil.getLocalHostIps().get(0);
        xMsgTopic topic = xMsgTopic.wrap(CConstants.SERVICE + ":" + localDpe);
        xMsgMessage msg1 = new xMsgMessage(topic, data);

        genericSend(localDpe, msg1);

        if(!getFeHostName().equals(xMsgConstants.UNDEFINED.toString())) {
            xMsgTopic topic2 = xMsgTopic.wrap(CConstants.SERVICE + ":" + getFeHostName());
            xMsgMessage msg2 = new xMsgMessage(topic2, data);
            genericSend(getFeHostName(), msg2);
        }
    }

}
