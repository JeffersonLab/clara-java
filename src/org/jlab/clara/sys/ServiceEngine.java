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

import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.clara.base.core.ClaraComponent;
import org.jlab.clara.base.core.DataUtil;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.Engine;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.engine.EngineStatus;
import org.jlab.clara.sys.ccc.CompositionCompiler;
import org.jlab.clara.sys.ccc.ServiceState;
import org.jlab.clara.util.report.ServiceReport;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;

import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * A Service engine.
 * Every engine process a request in its own thread.
 *
 * @author gurjyan
 * @version 4.x
 */
class ServiceEngine {

    private final Engine engine;
    private final ServiceActor base;

    private final ServiceSysConfig sysConfig;
    private final ServiceReport sysReport;

    private final Semaphore semaphore = new Semaphore(1);

    private final CompositionCompiler compiler;

    // Already recorded (previous) composition
    private String prevComposition = ClaraConstants.UNDEFINED;

    // The last execution time
    private long executionTime;


    ServiceEngine(Engine userEngine,
                  ServiceActor base,
                  ServiceSysConfig config,
                  ServiceReport report) {
        this.base = base;
        this.engine = userEngine;
        this.sysConfig = config;
        this.sysReport = report;
        this.compiler = new CompositionCompiler(base.getName());
    }

    void start() throws ClaraException {
        // nothing
    }

    void stop() {
        // nothing
    }

    public void configure(xMsgMessage message) throws ClaraException {

        EngineData inputData;
        EngineData outData = null;
        try {
            inputData = getEngineData(message);
            outData = configureEngine(inputData);
        } catch (Exception e) {
            Logging.error("UNHANDLED EXCEPTION ON SERVICE CONFIGURATION: %s", base.getName());
            e.printStackTrace();
            outData = DataUtil.buildErrorData("unhandled exception", 4, e);
        } catch (Throwable e) {
            Logging.error("UNHANDLED CRITICAL ERROR ON SERVICE CONFIGURATION: %s", base.getName());
            e.printStackTrace();
            outData = DataUtil.buildErrorData("unhandled critical error", 4, e);
        } finally {
            updateMetadata(message.getMetaData(), DataUtil.getMetadata(outData));
            resetClock();
        }

        String replyTo = getReplyTo(message);
        if (replyTo != null) {
            sendResponse(outData, replyTo);
        } else {
            reportProblem(outData);
        }
    }


    private EngineData configureEngine(EngineData inputData) {
        long startTime = startClock();

        EngineData outData = engine.configure(inputData);

        stopClock(startTime);

        if (outData == null) {
            outData = new EngineData();
        }
        if (outData.getData() ==  null) {
            outData.setData(EngineDataType.STRING.mimeType(), "done");
        }

        return outData;
    }


    public void execute(xMsgMessage message) throws ClaraException {
        sysConfig.addRequest();
        sysReport.incrementRequestCount();

        EngineData inData = null;
        EngineData outData = null;

        try {
            inData = getEngineData(message);
            parseComposition(inData);
            outData = executeEngine(inData);
            sysReport.addExecutionTime(executionTime);
        } catch (Exception e) {
            Logging.error("UNHANDLED EXCEPTION ON SERVICE EXECUTION: %s", base.getName());
            e.printStackTrace();
            outData = DataUtil.buildErrorData("unhandled exception", 4, e);
        } catch (Throwable e) {
            Logging.error("UNHANDLED CRITICAL ERROR ON SERVICE EXECUTION: %s", base.getName());
            e.printStackTrace();
            outData = DataUtil.buildErrorData("unhandled critical error", 4, e);
        } finally {
            updateMetadata(message.getMetaData(), DataUtil.getMetadata(outData));
            resetClock();
        }

        String replyTo = getReplyTo(message);
        if (replyTo != null) {
            sendResponse(outData, replyTo);
            return;
        }

        reportProblem(outData);
        if (outData.getStatus() == EngineStatus.ERROR) {
            sysReport.incrementFailureCount();
            return;
        }

        reportResult(outData);
        sendResult(outData, getLinks(inData, outData));
    }

    private void parseComposition(EngineData inData) throws ClaraException {
        String currentComposition = inData.getComposition();
        if (!currentComposition.equals(prevComposition)) {
            compiler.compile(currentComposition);
            prevComposition = currentComposition;
        }
    }

    private Set<String> getLinks(EngineData inData, EngineData outData) {
        ServiceState ownerSS = new ServiceState(outData.getEngineName(),
                                                outData.getExecutionState());
        ServiceState inputSS = new ServiceState(inData.getEngineName(),
                                                inData.getExecutionState());

        return compiler.getLinks(ownerSS, inputSS);
    }

    private EngineData executeEngine(EngineData inData)
            throws ClaraException {
        long startTime = startClock();

        EngineData outData = engine.execute(inData);

        stopClock(startTime);

        if (outData == null) {
            throw new ClaraException("null engine result");
        }
        if (outData.getData() == null) {
            if (outData.getStatus() == EngineStatus.ERROR) {
                outData.setData(EngineDataType.STRING.mimeType(),
                                ClaraConstants.UNDEFINED);
            } else {
                throw new ClaraException("empty engine result");
            }
        }

        return outData;
    }

    private void updateMetadata(xMsgMeta.Builder inMeta, xMsgMeta.Builder outMeta) {
        outMeta.setAuthor(base.getName());
        outMeta.setVersion(engine.getVersion());

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

    private void reportResult(EngineData outData) throws ClaraException {
        if (sysConfig.isDataRequest()) {
            reportData(outData);
            sysConfig.resetDataRequestCount();
        }
        if (sysConfig.isDoneRequest()) {
            reportDone(outData);
            sysConfig.resetDoneRequestCount();
        }
    }

    private void sendResponse(EngineData outData, String replyTo) throws ClaraException {
        base.send(putEngineData(outData, replyTo));
    }

    private void sendResult(EngineData outData, Set<String> outLinks) throws ClaraException {
        for (String ss : outLinks) {
            ClaraComponent comp = ClaraComponent.dpe(ss);
            xMsgMessage msg = putEngineData(outData, ss);
            base.send(comp.getProxyAddress(), msg);
        }
    }

    private void reportDone(EngineData data) throws ClaraException {
        String mt = data.getMimeType();
        Object ob = data.getData();
        data.setData(EngineDataType.STRING.mimeType(), ClaraConstants.DONE);

        sendReport(ClaraConstants.DONE, data);

        data.setData(mt, ob);
    }

    private void reportData(EngineData data) throws ClaraException {
        sendReport(ClaraConstants.DATA, data);
    }

    private void reportProblem(EngineData data) throws ClaraException {
        EngineStatus status = data.getStatus();
        if (status.equals(EngineStatus.ERROR)) {
            sendReport(ClaraConstants.ERROR, data);
        } else if (status.equals(EngineStatus.WARNING)) {
            sendReport(ClaraConstants.WARNING, data);
        }
    }


    private void sendReport(String topicPrefix, EngineData data) throws ClaraException {
        xMsgTopic topic = xMsgTopic.wrap(topicPrefix + xMsgConstants.TOPIC_SEP + base.getName());
        xMsgMessage transit = DataUtil.serialize(topic, data, engine.getOutputDataTypes());
        base.send(base.getFrontEnd(), transit);
    }


    private EngineData getEngineData(xMsgMessage message) throws ClaraException {
        xMsgMeta.Builder metadata = message.getMetaData();
        String mimeType = metadata.getDataType();
        if (mimeType.equals(ClaraConstants.SHARED_MEMORY_KEY)) {
            sysReport.incrementShrmReads();
            String sender = metadata.getSender();
            int id = metadata.getCommunicationId();
            return SharedMemory.getEngineData(base.getName(), sender, id);
        } else {
            sysReport.addBytesReceived(message.getDataSize());
            return DataUtil.deserialize(message, engine.getInputDataTypes());
        }
    }

    private xMsgMessage putEngineData(EngineData data, String receiver)
            throws ClaraException {
        xMsgTopic topic = xMsgTopic.wrap(receiver);
        if (SharedMemory.containsReceiver(receiver)) {
            int id = data.getCommunicationId();
            SharedMemory.putEngineData(receiver, base.getName(), id, data);
            sysReport.incrementShrmWrites();

            xMsgMeta.Builder metadata = xMsgMeta.newBuilder();
            metadata.setAuthor(base.getName());
            metadata.setComposition(data.getComposition());
            metadata.setCommunicationId(id);
            metadata.setAction(xMsgMeta.ControlAction.EXECUTE);
            metadata.setDataType(ClaraConstants.SHARED_MEMORY_KEY);

            return new xMsgMessage(topic, metadata, ClaraConstants.SHARED_MEMORY_KEY.getBytes());
        } else {
            xMsgMessage output = DataUtil.serialize(topic, data, engine.getOutputDataTypes());
            sysReport.addBytesSent(output.getDataSize());
            return output;
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
        executionTime = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - watch);
    }


    public boolean tryAcquire() {
        return semaphore.tryAcquire();
    }

    public void release() {
        semaphore.release();
    }
}
