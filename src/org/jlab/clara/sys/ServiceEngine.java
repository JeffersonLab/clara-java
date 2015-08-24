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
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.Engine;
import org.jlab.clara.engine.EngineStatus;
import org.jlab.clara.sys.ccc.ServiceState;
import org.jlab.clara.sys.ccc.SimpleCompiler;
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

    private CServiceSysConfig sysConfig;

    private ServiceState myServiceState =
            new ServiceState(getName(), xMsgConstants.UNDEFINED.toString());

    public AtomicBoolean isAvailable;

    // Already recorded (previous) composition
    private String prevComposition = xMsgConstants.UNDEFINED.toString();

    // user provided engine class container class name
    private String engineClassPath = xMsgConstants.UNDEFINED.toString();

    // Engine instantiated object
    private final Engine engineObject;

    // key in the shared memory map of DPE to
    // locate this service resulting data object
    private String sharedMemoryKey = xMsgConstants.UNDEFINED.toString();

    // Simple average of the service engine
    // execution times over all received requests
    private long averageExecutionTime;
    // Number of received requests to this service.
    // Note: common for different compositions
    private long numberOfRequests;

    private SimpleCompiler compiler;


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
                         CServiceSysConfig config,
                         String localAddress,
                         String frontEndAddres,
                         String sharedMemoryKey)
            throws CException {
        super(name, localAddress, frontEndAddres);

        this.sysConfig = config;
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
        compiler = new SimpleCompiler(getName());
        System.out.println(CUtility.getCurrentTimeInH() + ": Started service = " + getName());
    }

    public ServiceState getMyServiceState() {
        return myServiceState;
    }

    public void updateMyState(String state) {
        myServiceState.setState(state);
    }

    public void configure(xMsgMessage message, AtomicInteger configureCountDown)
            throws CException,
            xMsgException,
            InterruptedException,
            IOException,
            ClassNotFoundException {

        engineObject.configure(parseFrom(message, engineObject.getInputDataTypes()));
        // If this is a sync request, send done to the requester
        String replyTo = message.getMetaData().getReplyTo();
        if (!replyTo.equals(xMsgConstants.UNDEFINED.toString()) &&
                CUtility.isCanonical(replyTo)) {
            int remainingInstances = configureCountDown.decrementAndGet();
            if (remainingInstances == 0) {
                xMsgTopic topic = xMsgTopic.wrap(replyTo);
                xMsgMessage outMsg = new xMsgMessage(topic, xMsgConstants.DONE.toString());
                String dpe = CUtility.getDpeName(replyTo);
                genericSend(dpe, outMsg);
            }
        }
    }

//    /**
//     * Service process method. Note that configure
//     * will never be execute within this method.
//     */
//    public void process(xMsgMessage message)
//            throws CException,
//            xMsgException,
//            IOException,
//            InterruptedException,
//            ClassNotFoundException {
//
//        isAvailable.set(false);
//
//        // Increment request count in the sysConfig object
//        sysConfig.addRequest();
//
//        xMsgMeta.Builder metadata = message.getMetaData();
//
//        String currentComposition = metadata.getComposition();
//        if (!currentComposition.equals(prevComposition)) {
//            // analyze composition
//            compiler.compile(currentComposition);
//            prevComposition = currentComposition;
//        }
//
//        ServiceState senderServiceState =
//                new ServiceState(metadata.getSender(), metadata.getSenderState());
//
//        for (Instruction inst : compiler.getInstructions()) {
//
//            // get the condition of the instruction
//            // if condition...
//            Condition ifCond = inst.getIfCondition();
//            Condition elseifCond = inst.getElseifCondition();
//
//            // the set of routing statements
//            Set<Statement> routingStatements;
//
//            if (ifCond != null) {
//                // Conditional routing.
//
//                if (ifCond.isTrue(getMyServiceState(), senderServiceState)) {
//                    routingStatements = inst.getIfCondStatements();
//                } else if (elseifCond.isTrue(getMyServiceState(), senderServiceState)) {
//                    routingStatements = inst.getElseifCondStatements();
//                } else {
//                    routingStatements = inst.getElseCondStatements();
//                }
//            } else {
//
//                // unconditional routing
//                routingStatements = inst.getUnCondStatements();
//            }
//
//            // execute service engine and route the statements
//            // note that service engine will not be executed if
//            // data for all inputs are present in the logical AND case.
//            // Execute service engine
//            EngineData inData = parseFrom(message, engineObject.getInputDataTypes());
//
//            execAndRoute(routingStatements, senderServiceState, inData);
//        }
//        isAvailable.set(true);
//    }
//
//    private void execAndRoute(Set<Statement> routingStatements,
//                              ServiceState inServiceState,
//                              EngineData inData)
//            throws IOException, xMsgException, CException {
//
//        EngineData outData;
//        for (Statement st : routingStatements) {
//            if (st.getInputLinks().contains(inServiceState.getName())) {
//
//                Set<EngineData> ens = new HashSet<>();
//                ens.add(inData);
//                outData =  executeEngine(ens);
//
//                callLinked(outData, st.getOutputLinks());
//
//            } else if (st.getLogAndInputs().containsKey(inServiceState.getName())) {
//
//                st.getLogAndInputs().put(inServiceState.getName(), inData);
//
//                // check to see if all required data is present (are not null)
//
//                boolean groupExecute = true;
//                for (EngineData ed : st.getLogAndInputs().values()) {
//                    if (ed == null) {
//                        groupExecute = false;
//                        break;
//                    }
//                }
//
//                if (groupExecute) {
//
//                    Set<EngineData> ens = new HashSet<>();
//                    // engine group execute
//
//                    for (EngineData ed : st.getLogAndInputs().values()) {
//                        ens.add(ed);
//                    }
//                    outData = executeEngine(ens);
//
//                    callLinked(outData, st.getOutputLinks());
//
//                    // reset data in the logAndInputs map
//                    for (String s : st.getLogAndInputs().keySet()) {
//                        st.getLogAndInputs().put(s, null);
//                    }
//                }
//
//            }
//        }
//    }

    public void execute(xMsgMessage message)
            throws CException, xMsgException, IOException {

        isAvailable.set(false);

        // Increment request count in the sysConfig object
        sysConfig.addRequest();
        try {
            EngineData inData = parseFrom(message, engineObject.getInputDataTypes());
            EngineData outData;

            parseComposition(inData);

            outData = executeEngine(inData);
            updateMetadata(inData, outData);

            reportProblem(outData);
            if (outData.getStatus() == EngineStatus.ERROR) {
                return;
            }

            sendReports(outData);
            sendResponse(outData, getLinks(inData, outData));

        } finally {
            isAvailable.set(true);
        }
    }

    private void parseComposition(EngineData inData) throws CException {
        String currentComposition = inData.getComposition();
        if (!currentComposition.equals(prevComposition)) {
            // analyze composition
            compiler.compile(currentComposition);
            prevComposition = currentComposition;
        }
    }

    private Set<String> getLinks(EngineData inData, EngineData outData) {
        return compiler.getOutputs();
    }

    private EngineData executeEngine(EngineData inData)
            throws IOException, xMsgException, CException {
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

            outData = engineObject.execute(inData);

            // get engine execution end time
            endTime = System.nanoTime();
            // service engine execution time
            execTime = endTime - startTime;
            // Calculate a simple average for the execution time
            averageExecutionTime = (averageExecutionTime + execTime) / numberOfRequests;

        } catch (Throwable t) {
            EngineData fst = inData;
            fst.setDescription(t.getMessage());
            fst.setStatus(EngineStatus.ERROR, 3);
            reportProblem(fst);
            t.printStackTrace();
        }
        return outData;
    }

    private void updateMetadata(EngineData inData, EngineData outData) {
        xMsgMeta.Builder outMeta = getMetadata(outData);
        outMeta.setAuthor(getName());
        outMeta.setVersion(engineObject.getVersion());

        if (!outMeta.hasCommunicationId()) {
            outMeta.setCommunicationId(inData.getCommunicationId());
        }
        outMeta.setComposition(inData.getComposition());
        outMeta.setExecutionTime(averageExecutionTime);

        if (outMeta.hasSenderState()) {
            updateMyState(outMeta.getSenderState());
        }
    }

    private void sendReports(EngineData outData)
            throws xMsgException, IOException, CException {
        // External broadcast data
        if (sysConfig.isDataRequest()) {
            reportData(outData);
            sysConfig.resetDataRequestCount();
        }

        // External done broadcasting
        if (sysConfig.isDoneRequest()) {
            reportDone(outData);
            sysConfig.resetDoneRequestCount();
        }
    }

    private void sendResponse(EngineData outData, Set<String> outLinks)
            throws xMsgException, IOException, CException {
        for (String ss : outLinks) {
            xMsgMessage transit = new xMsgMessage(xMsgTopic.wrap(ss));
            serialize(outData, transit, engineObject.getOutputDataTypes());
            serviceSend(transit);
        }
    }

    /**
     * Broadcast a done report of an engine execution.
     */
    public void reportDone(EngineData data)
            throws xMsgException, IOException, CException {

        // we are not sending data
        data.setData(xMsgConstants.UNDEFINED.toString(), null);

        // Create transit data
        xMsgTopic topic = xMsgTopic.wrap(xMsgConstants.DONE.toString() + ":" + getName());
        xMsgMessage transit = new xMsgMessage(topic);
        serialize(data, transit, engineObject.getOutputDataTypes());

        String dpe = "localhost";
        if (!getFrontEndAddress().equals(xMsgConstants.UNDEFINED.toString())) {
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
            throws xMsgException, IOException, CException {

        // Create transit data
        xMsgTopic topic = xMsgTopic.wrap(xMsgConstants.DATA.toString() + ":" + getName());
        xMsgMessage transit = new xMsgMessage(topic);
        serialize(data, transit, engineObject.getOutputDataTypes());

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
            throws xMsgException, IOException, CException {

        // we are not sending data
        data.setData(xMsgConstants.UNDEFINED.toString(), null);

        // Create transit data
        xMsgTopic topic;
        if (data.getStatus().equals(xMsgMeta.Status.ERROR)) {
            topic = xMsgTopic.wrap(xMsgConstants.ERROR.toString() + ":" + getName());
        } else if (data.getStatus().equals(xMsgMeta.Status.WARNING)) {
            topic = xMsgTopic.wrap(xMsgConstants.WARNING.toString() + ":" + getName());
        } else {
            return;
        }

        xMsgMessage transit = new xMsgMessage(topic);
        serialize(data, transit, engineObject.getOutputDataTypes());
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

        if (!getFrontEndAddress().equals(xMsgConstants.UNDEFINED.toString())) {
            xMsgTopic topic2 = xMsgTopic.wrap(CConstants.SERVICE + ":" + getFrontEndAddress());
            xMsgMessage msg2 = new xMsgMessage(topic2, data);
            genericSend(getFrontEndAddress(), msg2);
        }
    }
}
