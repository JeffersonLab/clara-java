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
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.engine.EngineStatus;
import org.jlab.clara.sys.ccc.ServiceState;
import org.jlab.clara.sys.ccc.SimpleCompiler;
import org.jlab.clara.util.CConstants;
import org.jlab.clara.util.CServiceSysConfig;
import org.jlab.clara.util.CUtility;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta.ControlAction;
import org.jlab.coda.xmsg.excp.xMsgException;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Semaphore;

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

    // Engine instantiated object
    private final Engine engineObject;

    private CServiceSysConfig sysConfig;

    private ServiceState myServiceState =
            new ServiceState(getName(), xMsgConstants.UNDEFINED.toString());

    private Semaphore semaphore = new Semaphore(1);

    // Already recorded (previous) composition
    private String prevComposition = xMsgConstants.UNDEFINED.toString();

    // Simple average of the service engine
    // execution times over all received requests
    private long averageExecutionTime;
    // Number of received requests to this service.
    // Note: common for different compositions
    private long numberOfRequests;

    private SimpleCompiler compiler;


    /**
     * Constructor.
     */
    public ServiceEngine(String name,
                         Engine userEngine,
                         CServiceSysConfig config,
                         String localAddress,
                         String frontEndAddres)
            throws CException {
        super(name, localAddress, frontEndAddres);

        this.engineObject = userEngine;
        this.sysConfig = config;

        // Create a socket connections
        // to the local dpe proxy
        connect();

        // create an object of the composition parser
        compiler = new SimpleCompiler(getName());
    }

    public ServiceState getMyServiceState() {
        return myServiceState;
    }

    public void updateMyState(String state) {
        myServiceState.setState(state);
    }

    public void configure(xMsgMessage message)
            throws CException,
            xMsgException,
            InterruptedException,
            IOException,
            ClassNotFoundException {

        EngineData outData = engineObject.configure(getEngineData(message));
        String replyTo = message.getMetaData().getReplyTo();
        if (!replyTo.equals(xMsgConstants.UNDEFINED.toString()) &&
                CUtility.isCanonical(replyTo)) {
            xMsgMessage outMsg = new xMsgMessage(xMsgTopic.wrap(replyTo));
            if (outData == null) {
                outMsg.setData("done");
            } else {
                putEngineData(outData, replyTo, outMsg);
            }
            genericSend(getLocalAddress(), outMsg);
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

        // Increment request count in the sysConfig object
        sysConfig.addRequest();

        EngineData inData = getEngineData(message);
        EngineData outData;

        parseComposition(inData);

        outData = executeEngine(inData);
        updateMetadata(inData, outData);

        String replyTo = message.getMetaData().getReplyTo();
        if (!replyTo.equals(xMsgConstants.UNDEFINED.toString()) &&
                CUtility.isCanonical(replyTo)) {
            xMsgMessage outMsg = new xMsgMessage(xMsgTopic.wrap(replyTo));
            putEngineData(outData, replyTo, outMsg);
            genericSend(getLocalAddress(), outMsg);
            return;
        }

        reportProblem(outData);
        if (outData.getStatus() == EngineStatus.ERROR) {
            return;
        }

        sendReports(outData);
        sendResponse(outData, getLinks(inData, outData));
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
        outMeta.setAction(ControlAction.EXECUTE);

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
            putEngineData(outData, ss, transit);
            serviceSend(transit);
        }
    }

    /**
     * Broadcast a done report of an engine execution.
     */
    public void reportDone(EngineData data)
            throws xMsgException, IOException, CException {

        String mt = data.getMimeType();
        Object ob = data.getData();

        // we are not sending data
        data.setData(EngineDataType.STRING.mimeType(), "done");

        // Create transit data
        xMsgTopic topic = xMsgTopic.wrap(xMsgConstants.DONE.toString() + ":" + getName());
        xMsgMessage transit = new xMsgMessage(topic);

        // send always serialized. We want to keep shared memory for data only.
        serialize(data, transit, engineObject.getOutputDataTypes());

        genericSend(getFrontEndAddress(), transit);

        data.setData(mt, ob);
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

        // send always serialized. We want to keep shared memory for application data only.
        serialize(data, transit, engineObject.getOutputDataTypes());

        genericSend(getFrontEndAddress(), transit);
    }

    /**
     * Broadcasts a problem reported by the engine.
     *
     * @param data the output data of the engine
     */
    public void reportProblem(EngineData data)
            throws xMsgException, IOException, CException {

        // Create transit data
        xMsgTopic topic;
        if (data.getStatus().equals(EngineStatus.ERROR)) {
            topic = xMsgTopic.wrap(xMsgConstants.ERROR.toString() + ":" + getName());
        } else if (data.getStatus().equals(EngineStatus.WARNING)) {
            topic = xMsgTopic.wrap(xMsgConstants.WARNING.toString() + ":" + getName());
        } else {
            return;
        }

        xMsgMessage transit = new xMsgMessage(topic);
        // send always serialized. We want to keep shared memory for data only.
        serialize(data, transit, engineObject.getOutputDataTypes());

        genericSend(getFrontEndAddress(), transit);
    }

    private EngineData getEngineData(xMsgMessage message) throws CException {
        xMsgMeta.Builder metadata = message.getMetaData();
        String mimeType = metadata.getDataType();
        if (mimeType.equals(CConstants.SHARED_MEMORY_KEY)) {
            String sender = metadata.getSender();
            int id = metadata.getCommunicationId();
            return SharedMemory.getEngineData(getName(), sender, id);
        } else {
            return parseFrom(message, engineObject.getInputDataTypes());
        }
    }

    private void putEngineData(EngineData data, String receiver, xMsgMessage message)
            throws CException {
        if (SharedMemory.containsReceiver(receiver)) {
            int id = data.getCommunicationId();
            SharedMemory.putEngineData(receiver, getName(), id, data);

            xMsgMeta.Builder metadata = xMsgMeta.newBuilder();
            metadata.setDataType(CConstants.SHARED_MEMORY_KEY);
            metadata.setSender(getName());
            metadata.setCommunicationId(id);
            metadata.setAction(xMsgMeta.ControlAction.EXECUTE);

            message.setMetaData(metadata);
            message.setData(CConstants.SHARED_MEMORY_KEY);
        } else {
            serialize(data, message, engineObject.getOutputDataTypes());
        }
    }

    public boolean tryAcquire() {
        return semaphore.tryAcquire();
    }

    public void release() {
        semaphore.release();
    }
}
