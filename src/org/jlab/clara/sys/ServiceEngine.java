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
import org.jlab.clara.sys.ccc.CCompiler;
import org.jlab.clara.sys.ccc.Condition;
import org.jlab.clara.sys.ccc.Instruction;
import org.jlab.clara.sys.ccc.ServiceState;
import org.jlab.clara.sys.ccc.Statement;
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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Service engine.
 * A Service can have multiple engines.
 * The Service distributes its requests to the available engines.
 * Every engine process a request in its own thread.
 *
 * @author gurjyan
 * @version 1.x
 * @since 1/30/15
 */
public class ServiceEngine extends CBase {

    private ServiceState myServiceState =
            new ServiceState(getName(),xMsgConstants.UNDEFINED.toString());

    public AtomicBoolean isAvailable;

    // Already recorded (previous) composition
    private String prevComposition = xMsgConstants.UNDEFINED.toString();

    // user provided engine class container class name
    private String engineClassPath = xMsgConstants.UNDEFINED.toString();

    // Engine instantiated object
    private final ICEngine engineObject;

    // key in the shared memory map of DPE to
    // locate this service resulting data object
    private String sharedMemoryKey = xMsgConstants.UNDEFINED.toString();

    // Simple average of the service engine
    // execution times over all received requests
    private long averageExecutionTime;
    // Number of received requests to this service.
    // Note: common for different compositions
    private long numberOfRequests;

    private CCompiler compiler;


    /**
     * Constructor.
     *
     * @param packageName service engine package name
     * @param name the service canonical name
     * @param sharedMemoryKey key in the shared memory map of DPE to
     *                        locate this service resulting data object
     * @param feHost front-end host name. This is the host that holds
     *               centralized registration database.
     * @throws xMsgException
     */
    public ServiceEngine(String name,
                         String classPath,
                         String localAddress,
                         String frontEndAddres,
                         String sharedMemoryKey)
            throws CException {
        super(name, localAddress, frontEndAddres);

        this.sharedMemoryKey = sharedMemoryKey;

        // Dynamic loading of the Clara engine class
        // Note: using system class loader
        try {
            CClassLoader cl = new CClassLoader(ClassLoader.getSystemClassLoader());
            engineClassPath = classPath;
            engineObject = cl.load(engineClassPath);
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            throw new CException(e.getMessage());
        }

        // Create a socket connections
        // to the local dpe proxy
        connect();

        isAvailable = new AtomicBoolean(true);

        // create an object of the composition parser
        compiler = new CCompiler(getName());
        System.out.println(CUtility.getCurrentTimeInH()+": Started service = "+getName());
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

            engineObject.configure(new EngineData(metadata, data));
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

        String currentComposition = metadata.getComposition();

        ServiceState senderServiceState =
                new ServiceState(metadata.getSender(), metadata.getSenderState());


        if (metadata.getAction().equals(xMsgMeta.ControlAction.EXECUTE)) {

            if (!currentComposition.equals(prevComposition)) {
                // analyze composition
                compiler.compile(currentComposition);
                prevComposition = currentComposition;
            }


            for (Instruction inst : compiler.getInstructions()) {

                // get the condition of the instruction
                // if condition...
                Condition ifCond = inst.getIfCondition();
                Condition elseifCond = inst.getElseifCondition();

                // the set of routing statements
                Set<Statement> routingStatements;

                if (ifCond != null) {
                    // Conditional routing.

                    if (ifCond.isTrue(getMyServiceState(), senderServiceState)) {
                        routingStatements = inst.getIfCondStatements();
                    } else if (elseifCond.isTrue(getMyServiceState(), senderServiceState)) {
                        routingStatements = inst.getElseifCondStatements();
                    } else {
                        routingStatements = inst.getElseCondStatements();
                    }
                } else {

                    // unconditional routing
                    routingStatements = inst.getUnCondStatements();
                }

                // execute service engine and route the statements
                // note that service engine will not be executed if
                // data for all inputs are present in the logical AND case.
                // Execute service engine
                EngineData inData = new EngineData(metadata, data);

                execAndRoute(config, routingStatements, senderServiceState, inData);
            }
        }
        isAvailable.set(true);
    }

    private EngineData executeEngine(Set<EngineData> inData)
            throws IOException, xMsgException {
        EngineData outData = null;

        // Variables to measure service
        // engine execution time
        long startTime;
        long endTime;
        long execTime;

        try {
            // increment request count
            numberOfRequests++;
            // get engine execution start time
            startTime = System.nanoTime();

            if(inData.size()==1) {
                outData = engineObject.execute(inData.iterator().next());
            } else {
                outData = engineObject.execute_group(inData);

            }
            // get engine execution end time
            endTime = System.nanoTime();
            // service engine execution time
            execTime = endTime - startTime;
            // Calculate a simple average for the execution time
            averageExecutionTime = (averageExecutionTime + execTime) / numberOfRequests;

            // update service state based on the engine set state
            updateMyState(outData.getState());

        } catch (Throwable t) {
            EngineData fst = inData.iterator().next();
            fst.getMetaData().setDescription(t.getMessage());
            fst.getMetaData().setStatus(xMsgMeta.Status.ERROR);
            fst.getMetaData().setSeverityId(3);
            reportProblem(fst);
            t.printStackTrace();
        }
        return outData;
    }

    private void execAndRoute(CServiceSysConfig config,
                              Set<Statement> routingStatements,
                              ServiceState inServiceState,
                              EngineData inData)
            throws IOException, xMsgException, CException {

        EngineData outData;
        for (Statement st : routingStatements) {
            if (st.getInputLinks().contains(inServiceState.getName())) {

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
     * Calls a service that is linked according to the composition.
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
        if (config.isDataRequest()) {
            reportData(engineData);
            config.resetDataRequestCount();
        }

        // External done broadcasting
        if (config.isDoneRequest()) {
            reportDone(engineData);
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
     * Broadcast a done report of an engine execution.
     */
    public void reportDone(EngineData data)
            throws xMsgException, IOException {

        // we are not sending data
        data.newData(EDataType.UNDEFINED, null);

        // Create transit data
        xMsgTopic topic = xMsgTopic.wrap(xMsgConstants.DONE.toString() + ":" + getName());
        xMsgMessage transit = new xMsgMessage(topic);
        transit.setMetaData(data.getMetaData());
        transit.setData(data.getxData().build());

        String dpe = "localhost";
        if(!getFrontEndAddress().equals(xMsgConstants.UNDEFINED.toString())) {
            dpe = getFrontEndAddress();
        }
        // send always serialized. We want to keep shared memory for data only.
        genericSend(dpe, transit);
    }

    /**
     * Broadcasts the output data generated by the engine.
     * Severity = 1 is used to report data.
     * Note: that the data contains service engine execution current/instantaneous time.
     *
     * @param data the output data of the engine
     */
    public void reportData(EngineData data)
            throws xMsgException, IOException {

        // Create transit data
        xMsgTopic topic = xMsgTopic.wrap(xMsgConstants.DATA.toString() + ":" + getName());
        xMsgMessage transit = new xMsgMessage(topic);
        transit.setMetaData(data.getMetaData());
        transit.setData(data.getxData().build());

        String dpe = "localhost";
        if (!getFrontEndAddress().equals(xMsgConstants.UNDEFINED.toString())) {
            dpe = getFrontEndAddress();
        }
        // send always serialized. We want to keep shared memory for application data only.
        genericSend(dpe, transit);
    }

    /**
     * Broadcasts a problem reported by the engine.
     *
     * @param data the output data of the engine
     */
    public void reportProblem(EngineData data)
            throws xMsgException, IOException {

        // we are not sending data
        data.newData(EDataType.UNDEFINED, null);

        // Create transit data
        xMsgTopic topic;
        if (data.getMetaData().getStatus().equals(xMsgMeta.Status.ERROR)) {
            topic = xMsgTopic.wrap(xMsgConstants.ERROR.toString() + ":" + getName());
        } else if (data.getMetaData().getStatus().equals(xMsgMeta.Status.WARNING)) {
            topic = xMsgTopic.wrap(xMsgConstants.WARNING.toString() + ":" + getName());
        } else {
            return;
        }

        xMsgMessage transit = new xMsgMessage(topic);
        transit.setMetaData(data.getMetaData());
        transit.setData(data.getxData().build());
        String dpe = "localhost";
        if (!getFrontEndAddress().equals(xMsgConstants.UNDEFINED.toString())) {
            dpe = getFrontEndAddress();
        }
        // send always serialized. We want to keep shared memory for data only.
        genericSend(dpe, transit);
    }



    /**
     * Removes service xMsg registration.
     */
    public void removeRegistration()
            throws xMsgException {

        removeSubscriber(xMsgTopic.wrap(getName()));
    }

    /**
     * Destroys the engine.
     *
     * @throws xMsgException
     * @throws IOException
     */
    public void dispose() throws xMsgException, IOException {
        removeRegistration();

        String data = CConstants.SERVICE_DOWN + "?" + getName();

        // Send service_down message
        String localDpe = xMsgUtil.getLocalHostIps().get(0);
        xMsgTopic topic = xMsgTopic.wrap(CConstants.SERVICE + ":" + localDpe);
        xMsgMessage msg1 = new xMsgMessage(topic, data);

        genericSend(localDpe, msg1);

        if(!getFrontEndAddress().equals(xMsgConstants.UNDEFINED.toString())) {
            xMsgTopic topic2 = xMsgTopic.wrap(CConstants.SERVICE + ":" + getFrontEndAddress());
            xMsgMessage msg2 = new xMsgMessage(topic2, data);
            genericSend(getFrontEndAddress(), msg2);
        }
    }

}
