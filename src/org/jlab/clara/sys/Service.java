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
import org.jlab.clara.base.core.MessageUtils;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.Engine;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.sys.RequestParser.RequestException;
import org.jlab.coda.xmsg.core.xMsgCallBack;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgSubscription;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;
import org.jlab.coda.xmsg.excp.xMsgException;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * A Clara service listening and executing requests.
 * <p>
 * An internal object pool contains N number of {@link ServiceEngine} objects,
 * where N is user specified value (usually equals to the number of cores).
 * A thread pool contains threads to run each object within.
 * Number of threads in the pool is equal to the size of the object pool.
 * Thread pool is fixed size, however object pool is capable of expanding.
 */
class Service extends ClaraBase {

    private final String name;

    private ExecutorService executionPool;
    private ServiceEngine[] enginePool;
    private Engine userEngine;

    private ServiceSysConfig sysConfig;
    private xMsgSubscription subscription;


    /**
     * Constructor of a service.
     * <p>
     * Create thread pool to run requests to this service.
     * Create object pool to hold the engines this service.
     * Object pool size is set to be 2 in case it was requested
     * to be 0 or negative number.
     *
     * @throws ClaraException
     * @throws xMsgException
     */
    Service(ClaraComponent comp, ClaraComponent frontEnd)
                throws ClaraException, xMsgException {

        super(comp, frontEnd);

        // Create a socket connections to the dpe proxy
        releaseConnection(getConnection());

        this.name = comp.getCanonicalName();
        this.sysConfig = new ServiceSysConfig(name, comp.getInitialState());

        // Dynamic loading of the Clara engine class
        // Note: using system class loader
        try {
            EngineLoader cl = new EngineLoader(ClassLoader.getSystemClassLoader());
            userEngine = cl.load(comp.getEngineClass());
            validateEngine(userEngine);
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            throw new ClaraException("Clara-Error: Could not load engine", e.getCause());
        }

        // Creating thread pool
        this.executionPool = xMsgUtil.newFixedThreadPool(comp.getSubscriptionPoolSize(), name);

        // Creating service object pool
        this.enginePool = new ServiceEngine[comp.getSubscriptionPoolSize()];

        // Fill the object pool
        for (int i = 0; i < comp.getSubscriptionPoolSize(); i++) {
            ServiceEngine engine = new ServiceEngine(getMe(), getFrontEnd(), userEngine, sysConfig);
            enginePool[i] = engine;
        }

        // Register with the shared memory
        SharedMemory.addReceiver(name);

        // subscription

        this.subscription = listen(comp.getTopic(), new ServiceCallBack());
        System.out.printf("%s: Started service = %s  pool_size = %d%n",
                ClaraUtil.getCurrentTimeInH(), name, comp.getSubscriptionPoolSize());

        // Register this subscriber
        register(comp.getTopic(), comp.getDescription());
        System.out.printf("%s: Registered service = %s%n", ClaraUtil.getCurrentTimeInH(), name);
    }


    @Override
    public void start() throws ClaraException {
        // nothing
    }


    @Override
    protected void end() {
        try {
            executionPool.shutdown();
            userEngine.destroy();

            removeRegistration(getMe().getTopic());
            stopListening(subscription);
            System.out.println(ClaraUtil.getCurrentTimeInH() + ": Removed service = " + name + "\n");
        } catch (ClaraException e) {
            e.printStackTrace();
        }

    }


    private void validateEngine(Engine engine) throws ClaraException {
        validateDataTypes(engine.getInputDataTypes(), "input data types");
        validateDataTypes(engine.getOutputDataTypes(), "output data types");
        validateString(engine.getDescription(), "description");
        validateString(engine.getVersion(), "version");
        validateString(engine.getAuthor(), "author");
    }


    private void validateString(String value, String field) throws ClaraException {
        if (value == null || value.isEmpty()) {
            throw new ClaraException("Clara-Error: missing engine " + field);
        }
    }


    private void validateDataTypes(Set<EngineDataType> types, String field) throws ClaraException {
        if (types == null || types.isEmpty()) {
            throw new ClaraException("Clara-Error: missing engine " + field);
        }
        for (EngineDataType dt : types) {
            if (dt == null) {
                throw new ClaraException("Clara-Error: null data type on engine " + field);
            }
        }
    }


    private void configure(final xMsgMessage msg) throws Exception {
        while (true) {
            for (final ServiceEngine engine : enginePool) {
                if (engine.tryAcquire()) {
                    executionPool.submit(() -> {
                        try {
                            engine.configure(msg);
                        } catch (Exception e) {
                            printUnhandledException(e);
                        } finally {
                            engine.release();
                        }
                    });
                    return;
                }
            }
        }
    }


    private void execute(final xMsgMessage msg) {
        while (true) {
            for (final ServiceEngine engine : enginePool) {
                if (engine.tryAcquire()) {
                    executionPool.submit(() -> {
                        try {
                            engine.execute(msg);
                        } catch (Exception e) {
                            printUnhandledException(e);
                        } finally {
                            engine.release();
                        }
                    });
                    return;
                }
            }
        }
    }


    private void setup(xMsgMessage msg) throws RequestException {
        RequestParser setup = RequestParser.build(msg);
        String report = setup.nextString();
        int value = setup.nextInteger();
        switch (report) {
            case ClaraConstants.SERVICE_REPORT_DONE:
                sysConfig.setDoneRequest(true);
                sysConfig.setDoneReportThreshold(value);
                sysConfig.resetDoneRequestCount();
                break;
            case ClaraConstants.SERVICE_REPORT_DATA:
                sysConfig.setDataRequest(true);
                sysConfig.setDataReportThreshold(value);
                sysConfig.resetDataRequestCount();
                break;
            default:
                throw new RequestException("Invalid report request: " + report);
        }
        if (msg.getMetaData().hasReplyTo()) {
            sendResponse(msg, xMsgMeta.Status.INFO, setup.request());
        }
    }


    private void printUnhandledException(Exception e) {
        StringWriter errors = new StringWriter();
        errors.write(name + ": Clara error: ");
        e.printStackTrace(new PrintWriter(errors));
        System.err.println(errors.toString());
    }


    private void sendResponse(xMsgMessage msg, xMsgMeta.Status status, String data) {
        try {
            xMsgTopic topic = xMsgTopic.wrap(msg.getMetaData().getReplyTo());
            xMsgMessage repMsg = MessageUtils.buildRequest(topic, data);
            repMsg.getMetaData().setStatus(status);
            send(repMsg);
        } catch (IOException | xMsgException e) {
            e.printStackTrace();
        }
    }


    private class ServiceCallBack implements xMsgCallBack {

        @Override
        public void callback(xMsgMessage msg) {
            try {
                xMsgMeta.Builder metadata = msg.getMetaData();
                if (!metadata.hasAction()) {
                    setup(msg);
                } else if (metadata.getAction().equals(xMsgMeta.ControlAction.CONFIGURE)) {
                    configure(msg);
                } else {
                    execute(msg);
                }
            } catch (Exception e) {
                e.printStackTrace();
                if (msg.getMetaData().hasReplyTo()) {
                    sendResponse(msg, xMsgMeta.Status.ERROR, e.getMessage());
                }
            }
        }
    }
}
