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

import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.Engine;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.util.CClassLoader;
import org.jlab.clara.util.CConstants;
import org.jlab.clara.util.ClaraUtil;
import org.jlab.clara.util.xml.RequestParser;
import org.jlab.coda.xmsg.core.*;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;
import org.jlab.coda.xmsg.excp.xMsgException;

import java.io.IOException;
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
public class Service extends CBase {

    private final String name;

    private ExecutorService executionPool;
    private ServiceEngine[] enginePool;
    private Engine userEngine;

    private ServiceSysConfig sysConfig;
    private xMsgSubscription subscription;


    /**
     * Starts a service.
     * <p>
     * Create thread pool to run requests to this service.
     * Create object pool to hold the engines this service.
     * Object pool size is set to be 2 in case it was requested
     * to be 0 or negative number.
     *
     * @param name the service canonical name
     * @param className the class path of the service engine
     * @param localAddress the address of the local Clara node
     * @param frontEndAddress the name of the front-end Clara node
     * @param poolSize the size of the engines pool
     * @param initialState initial state of this service
     * @throws ClaraException if the engine could not be loaded
     * @throws IOException
     */
    public Service(String name,
                   String className,
                   String localAddress,
                   String frontEndAddress,
                   int poolSize,
                   String initialState)
            throws ClaraException, xMsgException {

        super(name, localAddress, frontEndAddress);

        this.name = name;
        this.sysConfig = new ServiceSysConfig(name, initialState);

        // Dynamic loading of the Clara engine class
        // Note: using system class loader
        try {
            CClassLoader cl = new CClassLoader(ClassLoader.getSystemClassLoader());
            userEngine = cl.load(className);
            validateEngine(userEngine);
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            throw new ClaraException("Could not load engine", e);
        }

        // Creating thread pool
        this.executionPool = xMsgUtil.newFixedThreadPool(poolSize, name);

        // Creating service object pool
        this.enginePool = new ServiceEngine[poolSize];

        // Fill the object pool
        for (int i = 0; i < poolSize; i++) {
            ServiceEngine engine = new ServiceEngine(name,
                                                     userEngine,
                                                     sysConfig,
                                                     localAddress,
                                                     frontEndAddress);
            enginePool[i] = engine;
        }

        // Register with the shared memory
        SharedMemory.addReceiver(name);

        xMsgTopic topic = xMsgTopic.wrap(getName());
        this.subscription = genericReceive(topic, new ServiceCallBack());
        System.out.printf("%s: Started service = %s  poolsize = %d%n",
                          ClaraUtil.getCurrentTimeInH(), getName(), poolSize);

        registerLocalSubscriber(topic, "Clara Service");
        System.out.printf("%s: Registered service = %s%n",
                          ClaraUtil.getCurrentTimeInH(), getName());
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
            throw new ClaraException("missing engine " + field);
        }
    }


    private void validateDataTypes(Set<EngineDataType> types, String field) throws ClaraException {
        if (types == null || types.isEmpty()) {
            throw new ClaraException("missing engine " + field);
        }
        for (EngineDataType dt : types) {
            if (dt == null) {
                throw new ClaraException("null data type on engine " + field);
            }
        }
    }


    public void exit() throws ClaraException {
        boolean error = false;
        executionPool.shutdown();
        userEngine.destroy();

        try {
            unregister();
        } catch (xMsgException | IOException e) {
            e.printStackTrace();
            error = true;
        }

        try {
            unsubscribe(subscription);
        } catch (xMsgException e) {
            e.printStackTrace();
            error = true;
        }

        if (error) {
            throw new ClaraException("Error removing service = " + name);
        }

        System.out.println(ClaraUtil.getCurrentTimeInH() + ": Removed service = " + name + "\n");
    }


    private void configure(final xMsgMessage msg) throws Exception {
        while (true) {
            for (final ServiceEngine engine : enginePool) {
                if (engine.tryAcquire()) {
                    executionPool.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                engine.configure(msg);
                            } catch (Exception e) {
                                e.printStackTrace();
                            } finally {
                                engine.release();
                            }
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
                    executionPool.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                engine.execute(msg);
                            } catch (Exception e) {
                                e.printStackTrace();
                            } finally {
                                engine.release();
                            }
                        }
                    });
                    return;
                }
            }
        }
    }


    private void setup(xMsgMessage msg) throws ClaraException {
        RequestParser setup = RequestParser.build(msg);
        String report = setup.nextString();
        int value = setup.nextInteger();
        switch (report) {
            case CConstants.SERVICE_REPORT_DONE:
                sysConfig.setDoneRequest(true);
                sysConfig.setDoneReportThreshold(value);
                sysConfig.resetDoneRequestCount();
                break;
            case CConstants.SERVICE_REPORT_DATA:
                sysConfig.setDataRequest(true);
                sysConfig.setDataReportThreshold(value);
                sysConfig.resetDataRequestCount();
                break;
            default:
                throw new ClaraException("Invalid report request: " + report);
        }
    }


    public void register()
            throws xMsgException, IOException {
        String description = "Clara Service";
        String feHost = getFrontEndAddress();
        registerLocalSubscriber(xMsgTopic.wrap(name), description);
        if (!xMsgUtil.getLocalHostIps().contains(feHost)) {
            registerSubscriber(xMsgTopic.wrap(name), description);
        }

        if (!feHost.equals(xMsgConstants.UNDEFINED.toString())) {
            xMsgTopic topic = xMsgTopic.wrap(CConstants.SERVICE + ":" + feHost);
            String data = CConstants.SERVICE_UP + "?" + name;
            // Send service_up message to the FE
            xMsgMessage msg = new xMsgMessage(topic, data);
            genericSend(feHost, msg);
        }

        System.out.println(ClaraUtil.getCurrentTimeInH() + ": Registered service = " + name);
    }


    public void unregister() throws xMsgException, IOException {
        removeLocalSubscriber(xMsgTopic.wrap(name));
        removeSubscriber(xMsgTopic.wrap(name));

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


    private class ServiceCallBack implements xMsgCallBack {

        @Override
        public xMsgMessage callback(xMsgMessage msg) {
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
            }
            return null;
        }
    }
}
