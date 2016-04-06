/*
 *   Copyright (c) 2016.  Jefferson Lab (JLab). All rights reserved. Permission
 *   to use, copy, modify, and distribute  this software and its documentation for
 *   educational, research, and not-for-profit purposes, without fee and without a
 *   signed licensing agreement.
 *
 *   IN NO EVENT SHALL JLAB BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL
 *   INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING
 *   OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF JLAB HAS
 *   BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 *   JLAB SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 *   THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 *   PURPOSE. THE CLARA SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY,
 *   PROVIDED HEREUNDER IS PROVIDED "AS IS". JLAB HAS NO OBLIGATION TO PROVIDE
 *   MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 *   This software was developed under the United States Government license.
 *   For more information contact author at gurjyan@jlab.org
 *   Department of Experimental Nuclear Physics, Jefferson Lab.
 */

package org.jlab.clara.sys;

import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.clara.base.core.ClaraBase;
import org.jlab.clara.base.core.ClaraComponent;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.Engine;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.engine.EngineStatus;
import org.jlab.clara.sys.ccc.CompositionCompiler;
import org.jlab.clara.sys.ccc.ServiceState;
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
class ServiceEngine extends ClaraBase {

    // Engine instantiated object
    private final Engine engineObject;

    private ServiceSysConfig sysConfig;

    private Semaphore semaphore = new Semaphore(1);

    // Already recorded (previous) composition
    private String prevComposition = xMsgConstants.UNDEFINED;

    private CompositionCompiler compiler;

    // The last execution time
    private long executionTime;


    /**
     * Constructor.
     */
    ServiceEngine(ClaraComponent comp,
                  ClaraComponent frontEnd,
                  Engine userEngine,
                  ServiceSysConfig config) throws ClaraException {
        super(comp, frontEnd);

        this.engineObject = userEngine;
        this.sysConfig = config;

        // Create a socket connection to the local dpe proxy
        cacheConnection();

        // create an object of the composition parser
        compiler = new CompositionCompiler(comp.getCanonicalName());
    }

    @Override
    public void start() throws ClaraException {
        // nothing
    }

    @Override
    protected void end() {
        // nothing
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
            e.printStackTrace();
            outData = buildSystemErrorData("unhandled exception", -4, ClaraUtil.reportException(e));
        } finally {
            updateMetadata(message.getMetaData(), getMetadata(outData));
            resetClock();
        }

        String replyTo = getReplyTo(message);
        if (replyTo != null) {
            xMsgMessage msgOut = putEngineData(outData, replyTo);
            send(msgOut);
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
            e.printStackTrace();
            outData = buildSystemErrorData("unhandled exception", -4, ClaraUtil.reportException(e));
        } finally {
            updateMetadata(message.getMetaData(), getMetadata(outData));
            resetClock();
        }

        String replyTo = getReplyTo(message);
        if (replyTo != null) {
            xMsgTopic topic = xMsgTopic.wrap(replyTo);
            xMsgMessage msgReply = serialize(topic, outData, engineObject.getOutputDataTypes());
            send(msgReply);
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
            xMsgMessage msg = putEngineData(outData, ss);
            send(comp, msg);
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
        xMsgMessage transit = serialize(topic, data, engineObject.getOutputDataTypes());
        send(getFrontEnd(), transit);
    }


    private EngineData getEngineData(xMsgMessage message) throws ClaraException {
        xMsgMeta.Builder metadata = message.getMetaData();
        String mimeType = metadata.getDataType();
        if (mimeType.equals(ClaraConstants.SHARED_MEMORY_KEY)) {
            String sender = metadata.getSender();
            int id = metadata.getCommunicationId();
            return SharedMemory.getEngineData(getName(), sender, id);
        } else {
            return deSerialize(message, engineObject.getInputDataTypes());
        }
    }

    private xMsgMessage putEngineData(EngineData data, String receiver)
            throws ClaraException {
        xMsgTopic topic = xMsgTopic.wrap(receiver);
        if (SharedMemory.containsReceiver(receiver)) {
            int id = data.getCommunicationId();
            SharedMemory.putEngineData(receiver, getName(), id, data);

            xMsgMeta.Builder metadata = xMsgMeta.newBuilder();
            metadata.setSender(getName());
            metadata.setComposition(data.getComposition());
            metadata.setCommunicationId(id);
            metadata.setAction(xMsgMeta.ControlAction.EXECUTE);
            metadata.setDataType(ClaraConstants.SHARED_MEMORY_KEY);

            return new xMsgMessage(topic, metadata, ClaraConstants.SHARED_MEMORY_KEY.getBytes());
        } else {
            return serialize(topic, data, engineObject.getOutputDataTypes());
        }
    }


    private String getReplyTo(xMsgMessage message) {
        xMsgMeta.Builder meta = message.getMetaData();
        if (meta.hasReplyTo()) {
            return meta.getReplyTo();
        }
        return null;
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
