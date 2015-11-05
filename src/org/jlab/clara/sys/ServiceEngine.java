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

import org.jlab.clara.base.ClaraBase;
import org.jlab.clara.base.ClaraComponent;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.Engine;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.engine.EngineStatus;
import org.jlab.clara.sys.ccc.CCompiler;
import org.jlab.clara.sys.ccc.ServiceState;
import org.jlab.clara.util.CConstants;
import org.jlab.clara.util.ClaraUtil;
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
 * Every engine process a request in its own thread.
 *
 * @author gurjyan
 * @version 4.x
 */
public class ServiceEngine extends ClaraBase {

    // Engine instantiated object
    private final Engine engineObject;

    private ServiceSysConfig sysConfig;

    private Semaphore semaphore = new Semaphore(1);

    // Already recorded (previous) composition
    private String prevComposition = xMsgConstants.UNDEFINED;

    private CCompiler compiler;

    // The last execution time
    private long executionTime;


    /**
     * Constructor.
     */
    public ServiceEngine(ClaraComponent comp,
                         Engine userEngine,
                         ServiceSysConfig config
    )
            throws ClaraException, IOException {
        super(comp);

        this.engineObject = userEngine;
        this.sysConfig = config;

        // Create a socket connections
        // to the local dpe proxy
        connect();

        // create an object of the composition parser
        compiler = new CCompiler(comp.getCanonicalName());
    }

    @Override
    public void end() {

    }

    @Override
    public void start(ClaraComponent component) {

    }

    public void configure(xMsgMessage message)
            throws ClaraException,
            xMsgException,
            InterruptedException,
            IOException,
            ClassNotFoundException {

        EngineData inputData;
        EngineData outData = null;
        try {
            inputData = getEngineData(message);
            outData = configureEngine(inputData);
        } catch (Exception e) {
            outData = reportSystemError("unhandled exception", -4, ClaraUtil.reportException(e));
        } finally {
            updateMetadata(message.getMetaData(), getMetadata(outData));
            resetClock();
        }

        String replyTo = getReplyTo(message);
        if (replyTo != null) {
            ClaraComponent comp = ClaraComponent.dpe(replyTo);
            xMsgMessage msOut = message.response();
            putEngineData(outData, replyTo, msOut);
            send(comp, msOut);
        } else {
            reportProblem(outData);
        }
    }


    private EngineData configureEngine(EngineData inputData) {
        long startTime = startClock();

        EngineData outData = engineObject.configure(inputData);

        stopClock(startTime);

        if (outData == null) {
            outData = new EngineData();
        }
        if (outData.getData() ==  null) {
            outData.setData(EngineDataType.STRING.mimeType(), "done");
        }

        return outData;
    }


    public void execute(xMsgMessage message)
            throws ClaraException, xMsgException, IOException {

        // Increment request count in the sysConfig object
        sysConfig.addRequest();

        EngineData inData = null;
        EngineData outData = null;

        try {
            inData = getEngineData(message);
            parseComposition(inData);
            outData = executeEngine(inData);
        } catch (Exception e) {
            outData = reportSystemError("unhandled exception", -4, ClaraUtil.reportException(e));
        } finally {
            updateMetadata(message.getMetaData(), getMetadata(outData));
            resetClock();
        }

        String replyTo = getReplyTo(message);
        if (replyTo != null) {
            xMsgMessage msgReply = message.response(outData);
            ClaraComponent comp = ClaraComponent.service(replyTo);
            send(comp, msgReply);
            return;
        }

        reportProblem(outData);
        if (outData.getStatus() == EngineStatus.ERROR) {
            return;
        }

        sendReports(outData);
        sendResponse(outData, getLinks(inData, outData));
    }

    private void parseComposition(EngineData inData) throws ClaraException {
        String currentComposition = inData.getComposition();
        if (!currentComposition.equals(prevComposition)) {
            // analyze composition
            compiler.compile(currentComposition);
            prevComposition = currentComposition;
        }
    }

    private Set<String> getLinks(EngineData inData, EngineData outData) {

        // service-states for conditional routing
        ServiceState ownerSS = new ServiceState(outData.getEngineName(), outData.getEngineState());
        ServiceState inputSS = new ServiceState(inData.getEngineName(), inData.getEngineState());

        return compiler.getLinks(ownerSS, inputSS);
    }

    private EngineData executeEngine(EngineData inData)
            throws ClaraException {
        long startTime = startClock();

        EngineData outData = engineObject.execute(inData);

        stopClock(startTime);

        if (outData == null) {
            throw new ClaraException("null engine result");
        }
        if (outData.getData() == null) {
            if (outData.getStatus() == EngineStatus.ERROR) {
                outData.setData(EngineDataType.STRING.mimeType(),
                        xMsgConstants.UNDEFINED);
            } else {
                throw new ClaraException("empty engine result");
            }
        }

        return outData;
    }

    private void updateMetadata(xMsgMeta.Builder inMeta, xMsgMeta.Builder outMeta) {
        outMeta.setAuthor(getMe().getCanonicalName());
        outMeta.setVersion(engineObject.getVersion());

        if (!outMeta.hasCommunicationId()) {
            outMeta.setCommunicationId(inMeta.getCommunicationId());
        }
        outMeta.setComposition(inMeta.getComposition());
        outMeta.setExecutionTime(executionTime);
        outMeta.setAction(inMeta.getAction());

        if (outMeta.hasSenderState()) {
            sysConfig.updateState(outMeta.getSenderState());
        }
    }


    private void sendReports(EngineData outData)
            throws xMsgException, IOException, ClaraException {
        // External send data
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
            throws xMsgException, IOException, ClaraException {
        for (String ss : outLinks) {
            ClaraComponent comp = ClaraComponent.dpe(ss);
            xMsgMessage msOut = new xMsgMessage(xMsgTopic.wrap(ss), null);
            putEngineData(outData, ss, msOut);
            send(comp, msOut);
        }
    }

    private void reportDone(EngineData data)
            throws xMsgException, IOException, ClaraException {
        String mt = data.getMimeType();
        Object ob = data.getData();
        data.setData(EngineDataType.STRING.mimeType(), xMsgConstants.DONE);

        report(xMsgConstants.DONE, data);

        data.setData(mt, ob);
    }

    private void reportData(EngineData data)
            throws xMsgException, IOException, ClaraException {
        report(xMsgConstants.DATA, data);
    }

    private void reportProblem(EngineData data)
            throws xMsgException, IOException, ClaraException {
        EngineStatus status = data.getStatus();
        if (status.equals(EngineStatus.ERROR)) {
            report(xMsgConstants.ERROR, data);
        } else if (status.equals(EngineStatus.WARNING)) {
            report(xMsgConstants.WARNING, data);
        }
    }


    private void report(String topicPrefix, EngineData data)
            throws ClaraException, xMsgException, IOException {
        xMsgTopic topic = xMsgTopic.wrap(topicPrefix + xMsgConstants.TOPIC_SEP + getName());
        xMsgMessage transit = new xMsgMessage(topic, null);
        serialize(data, transit, engineObject.getOutputDataTypes());
        send(getFrontEnd(), transit);
    }


    private EngineData getEngineData(xMsgMessage message) throws ClaraException {
        xMsgMeta.Builder metadata = message.getMetaData();
        String mimeType = metadata.getDataType();
        if (mimeType.equals(CConstants.SHARED_MEMORY_KEY)) {
            String sender = metadata.getSender();
            int id = metadata.getCommunicationId();
            return SharedMemory.getEngineData(getName(), sender, id);
        } else {
            return deSerialize(message, engineObject.getInputDataTypes());
        }
    }

    private void putEngineData(EngineData data, String receiver, xMsgMessage message)
            throws ClaraException {
        if (SharedMemory.containsReceiver(receiver)) {
            int id = data.getCommunicationId();
            SharedMemory.putEngineData(receiver, getName(), id, data);

            xMsgMeta.Builder metadata = message.getMetaData();
            metadata.setSender(getName());
            metadata.setComposition(data.getComposition());
            metadata.setCommunicationId(id);
            metadata.setAction(xMsgMeta.ControlAction.EXECUTE);

            message.setData(CConstants.SHARED_MEMORY_KEY.getBytes());
        } else {
            serialize(data, message, engineObject.getOutputDataTypes());
        }
    }


    private String getReplyTo(xMsgMessage message) {
        String replyTo = message.getMetaData().getReplyTo();
        if (replyTo.equals(xMsgConstants.UNDEFINED)) {
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
