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
import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.Engine;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.engine.EngineStatus;
import org.jlab.clara.sys.ccc.ServiceState;
import org.jlab.clara.sys.ccc.SimpleCompiler;
import org.jlab.clara.util.CConstants;
import org.jlab.clara.util.CServiceSysConfig;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;
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

    private SimpleCompiler compiler;

    // The last execution time
    private long executionTime;


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

        EngineData inputData = getEngineData(message);
        EngineData outData = configureEngine(inputData);

        String replyTo = getReplyTo(message);
        if (replyTo != null) {
            send(getLocalAddress(), replyTo, outData);
        }
    }


    private EngineData configureEngine(EngineData inputData) {
        EngineData outData = engineObject.configure(inputData);

        if (outData == null) {
            outData = new EngineData();
        }
        if (outData.getData() ==  null) {
            outData.setData(EngineDataType.STRING.mimeType(), "done");
        }

        return outData;
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
        updateMetadata(message.getMetaData(), getMetadata(outData));
        resetClock();

        String replyTo = getReplyTo(message);
        if (replyTo != null) {
            send(getLocalAddress(), replyTo, outData);
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

        try {
            long startTime = startClock();

            outData = engineObject.execute(inData);

            stopClock(startTime);

            if (outData == null) {
                throw new CException("null engine result");
            }
            if (outData.getData() == null) {
                throw new CException("empty engine result");
            }

        } catch (Throwable t) {
            EngineData fst = inData;
            fst.setDescription(t.getMessage());
            fst.setStatus(EngineStatus.ERROR, 3);
            reportProblem(fst);
            t.printStackTrace();
        }
        return outData;
    }

    private void updateMetadata(xMsgMeta.Builder inMeta, xMsgMeta.Builder outMeta) {
        outMeta.setAuthor(getName());
        outMeta.setVersion(engineObject.getVersion());

        if (!outMeta.hasCommunicationId()) {
            outMeta.setCommunicationId(inMeta.getCommunicationId());
        }
        outMeta.setComposition(inMeta.getComposition());
        outMeta.setExecutionTime(executionTime);
        outMeta.setAction(inMeta.getAction());

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
            send(ClaraUtil.getHostName(ss), ss, outData);
        }
    }

    private void reportDone(EngineData data)
            throws xMsgException, IOException, CException {
        String mt = data.getMimeType();
        Object ob = data.getData();
        data.setData(EngineDataType.STRING.mimeType(), "done");

        report(xMsgConstants.DONE.toString(), data);

        data.setData(mt, ob);
    }

    private void reportData(EngineData data)
            throws xMsgException, IOException, CException {
        report(xMsgConstants.DATA.toString(), data);
    }

    private void reportProblem(EngineData data)
            throws xMsgException, IOException, CException {
        EngineStatus status = data.getStatus();
        if (status.equals(EngineStatus.ERROR)) {
            report(xMsgConstants.ERROR.toString(), data);
        } else if (status.equals(EngineStatus.WARNING)) {
            report(xMsgConstants.WARNING.toString(), data);
        }
    }


    private void send(String host, String receiver, EngineData data)
            throws xMsgException, CException, IOException {
        xMsgMessage message = new xMsgMessage(xMsgTopic.wrap(receiver));
        putEngineData(data, receiver, message);
        genericSend(host, message);
    }

    private void report(String topicPrefix, EngineData data)
            throws CException, xMsgException, IOException {
        xMsgTopic topic = xMsgTopic.wrap(topicPrefix + ":" + getName());
        xMsgMessage transit = new xMsgMessage(topic);
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


    private String getReplyTo(xMsgMessage message) {
        String replyTo = message.getMetaData().getReplyTo();
        if (replyTo.equals(xMsgConstants.UNDEFINED.toString())) {
            return null;
        }
        return replyTo;
    }


    private void resetClock() {
        executionTime = 0;
    }

    private long startClock() {
        return System.nanoTime();
    }

    private void stopClock(long watch) {
        executionTime = (System.nanoTime() - watch) / 1000;
    }


    public boolean tryAcquire() {
        return semaphore.tryAcquire();
    }

    public void release() {
        semaphore.release();
    }
}
