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

import org.jlab.clara.base.CBase;
import org.jlab.clara.base.CException;
import org.jlab.clara.util.CConstants;
import org.jlab.clara.util.CServiceSysConfig;
import org.jlab.clara.util.CUtility;
import org.jlab.coda.xmsg.core.*;
import org.jlab.coda.xmsg.data.xMsgD.xMsgData;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;
import org.jlab.coda.xmsg.excp.xMsgException;

import java.io.IOException;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 *      Container has a map of ObjectPools, one for each service in the container.
 *      Each ObjectPool contains n number of Service objects, where n is user
 *      specified value (usually equals to the number of cores). It also holds the
 *      thread pool for each service. Thread pool contains threads to run each object
 *      within. Number of threads in the pool is equal to the size of the object pool.
 *      Thread pool is fixed size, however object pool is capable of expanding.
 * </p>
 *
 * @author gurjyan
 * @version 1.x
 * @since 1/30/15
 */
public class Container extends CBase {

    // Map containing service object pools for every service in the container
    private HashMap<String, Service[]>
            _objectPoolMap = new HashMap<>();

    // Map of thread pools for every service in the container
    private HashMap<String, ExecutorService>
            _threadPoolMap = new HashMap<>();

    // stores pool size for every service
    private HashMap<String, Integer> _poolSizeMap = new HashMap<>();

    // stores system config objects for every service of this container
    private ConcurrentHashMap<String, CServiceSysConfig>
            _sysConfigs = new ConcurrentHashMap<>();

    private String feHost =
            xMsgConstants.UNDEFINED.getStringValue();

    // Unique id for services within the container
    private AtomicInteger uniqueId = new AtomicInteger(0);

    private SubscriptionHandler subscriptionHandler;

    private Thread subscriptionThread;

    private Map<String, ServiceDispatcher>
            _myServiceDispatchers = new HashMap<>();

    /**
     * <p>
     *     Constructor
     * </p>
     *
     * @param name Clara service canonical name (such as dep:container:engine)
     * @param feHost front-end host name. This is the host that holds
     *               centralized registration database.
     * @throws xMsgException
     */
    public Container(String name,
                     String feHost)
            throws xMsgException, IOException {
        super(feHost);
        this.feHost = feHost;

        setName(name);

        // Create a socket connections to the local dpe proxy
        connect();

        System.out.println(CUtility.getCurrentTimeInH() +
                ": Started container = " + getName() + "\n");

        //register container
        System.out.println(CUtility.getCurrentTimeInH() + ": " + getName() +
                " container sending registration request.");
        registerSubscriber(getName(),
                xMsgUtil.getTopicDomain(getName()),
                xMsgUtil.getTopicSubject(getName()),
                xMsgConstants.UNDEFINED.getStringValue(),
                "Service Container");


        subscriptionThread = new Thread(new Runnable() {
            public void run() {
                // Subscribe messages published to this container
                try {
                    subscriptionHandler = genericReceive(CConstants.CONTAINER + ":" + getName(),
                            new ContainerCallBack());
                } catch (xMsgException e) {
                    e.printStackTrace();
                }
            }
        });
        subscriptionThread.start();

    }

    /**
     * <p>
     *     Constructor
     * </p>
     *
     * @param name  Clara service canonical name (such as dep:container:engine)
     *
     * @throws xMsgException
     */
    public Container(String name)
            throws xMsgException, SocketException {
        super();

        setName(name);

        // Create a socket connections to the local dpe proxy
        connect();

        System.out.println(CUtility.getCurrentTimeInH() +
                ": Started container = " + getName() + "\n");

        //register container
        System.out.println(CUtility.getCurrentTimeInH() + ": " + getName() +
                " container sending registration request.");
        registerSubscriber(getName(),
                xMsgUtil.getTopicDomain(getName()),
                xMsgUtil.getTopicSubject(getName()),
                xMsgConstants.UNDEFINED.getStringValue(),
                "Service Container");

        subscriptionThread = new Thread(new Runnable() {
            public void run() {
                // Subscribe messages published to this container
                try {
                    subscriptionHandler = genericReceive(CConstants.CONTAINER + ":" + getName(),
                            new ContainerCallBack());
                } catch (xMsgException e) {
                    e.printStackTrace();
                }
            }
        });
        subscriptionThread.start();
    }

    public void exitContainer() throws xMsgException, IOException {

        reportFE(CConstants.CONTAINER_DOWN + "?" + getName());

        subscriptionHandler.unsubscribe();
        subscriptionThread.stop();
        removeSubscriberRegistration(getName(),
                xMsgUtil.getTopicDomain(getName()),
                xMsgUtil.getTopicSubject(getName()),
                xMsgConstants.UNDEFINED.getStringValue());

        for (ServiceDispatcher sd : _myServiceDispatchers.values()) {
            sd.exitDispatcher();
        }
        _myServiceDispatchers.clear();
        _myServiceDispatchers = null;
        _objectPoolMap.clear();
        _objectPoolMap = null;
        _threadPoolMap.clear();
        _threadPoolMap = null;
        _poolSizeMap.clear();
        _poolSizeMap = null;
        _sysConfigs.clear();
        _sysConfigs = null;

        feHost = null;
        uniqueId = null;
        subscriptionThread = null;
        subscriptionHandler = null;
    }

    /**
     * <p>
     *     Check to see if the passed name is a canonical
     *     name of a service or just a service engine name
     *     Note: we assume that if name contains ":" then it is
     *     properly formed service canonical name. Also if it
     *     does not then the passed string is the service
     *     engine name.
     *     Create thread pool to run requested service objects
     *     Create object pool to hold objects of this requested service.
     *     Object pool size is set to be 2 in case it was requested
     *     to be 0 or negative number.
     * </p>
     *
     * @param packageName service engine full path
     * @param engineClassName service engine class name
     * @param objectPoolSize size of the object pool
     */
    public void addService(String packageName,
                           String engineClassName,
                           int objectPoolSize)
            throws CException,
            xMsgException,
            IOException,
            IllegalAccessException,
            ClassNotFoundException,
            InstantiationException {


        // We need final variables to pass
        // abstract method implementation
        final String canonical_name = getName() + ":" + engineClassName;

        if(_threadPoolMap.containsKey(canonical_name)){
            throw new CException("service exists");
        }

        // Create and add sys config object for a service
        _sysConfigs.put(canonical_name, new CServiceSysConfig());

        final String fe = feHost;

        // Define the key in the shared
        // memory map (defined in the DPE).
        int id = uniqueId.incrementAndGet();
        String sharedMemoryLocation = canonical_name+id;


        // Object pool size is set to be 2 in case
        // it was requested to be 0 or negative number.
        if(objectPoolSize <= 0) {
            objectPoolSize = 1;
        }

        // Creating thread pool
        _threadPoolMap.put(canonical_name, Executors.newFixedThreadPool(objectPoolSize));


        _poolSizeMap.put(canonical_name,objectPoolSize);

        // Creating service object pool
        Service[] sop = new Service[objectPoolSize];

        // Fill the object pool
        for(int i=0; i<objectPoolSize; i++){
            Service service;

            // Create an object of the Service class by passing
            // service name as a parameter. service name = canonical
            // name of this container + engine name of a service
            if(feHost.equals(xMsgConstants.UNDEFINED.getStringValue())) {
                service =  new Service(packageName, canonical_name, sharedMemoryLocation);
            } else {
                service =  new Service(packageName, canonical_name, sharedMemoryLocation, fe);
            }
            // add object to the pool
            sop[i] = service;
        }

        // Add the object pool to the pools map
        _objectPoolMap.put(canonical_name, sop);

        _myServiceDispatchers.put(canonical_name,
                new ServiceDispatcher(canonical_name, "Clara service"));

        System.out.println(CUtility.getCurrentTimeInH() +
                ": Started service = " + canonical_name + "\n");
    }

    public void removeService(String name)
            throws xMsgException, InterruptedException, CException, IOException {

        // Check to see if the passed name is a canonical
        // name of a service or just a service engine name
        // Note: we assume that if name contains ":" then it is
        // properly formed service canonical name. Also if it
        // does not then the passed string is the service
        // engine name.
        if(!CUtility.isCanonical(name)) {
            throw new CException("not a canonical names");
        }

        if(_threadPoolMap.containsKey(name)) {
            // clear and shut down thread pool for the requested service
            ExecutorService threadPool = _threadPoolMap.get(name);
            threadPool.shutdown();
            _threadPoolMap.remove(name);
        }

        if(_objectPoolMap.containsKey(name)) {
            // Clear object pool of a service by calling dispose
            // method of a service (for every service object
            // in the pool)
            Service[] op = _objectPoolMap.get(name);
            for(Service s:op) {
                // remove registration of a service and exit
                s.dispose();
            }
        }

        // remove sys config object for a service
        if(_sysConfigs.containsKey(name)){
            _sysConfigs.remove(name);
        }

        if (_myServiceDispatchers.containsKey(name)) {
            _myServiceDispatchers.get(name).exitDispatcher();
            _myServiceDispatchers.remove(name);
        }

    }

    private class ContainerCallBack implements xMsgCallBack {

        @Override
        // This method serves to start/deploy a service on
        // this container. In the future it will report
        // service specific statistics on a request
        public xMsgMessage callback(xMsgMessage msg) {

            final xMsgMeta.Builder metadata = msg.getMetaData();
            if (metadata.getDataType().equals(xMsgMeta.DataType.X_Object)) {
                final xMsgData.Builder data = (xMsgData.Builder) msg.getData();
                if (data.getType().equals(xMsgData.Type.T_STRING)) {
                    String cmdData = data.getSTRING();
                    String cmd = null, seName = null, objectPoolSize = null;
                    try {
                        StringTokenizer st = new StringTokenizer(cmdData, "?");
                        cmd = st.nextToken();
                        seName = st.nextToken();
                        objectPoolSize = st.nextToken();

                    } catch (NoSuchElementException e) {
                        System.out.println(e.getMessage());
//                    e.printStackTrace();
                    }
                    if (cmd != null && seName != null) {
                        switch (cmd) {
                            case CConstants.DEPLOY_SERVICE:
                                // Note: in this case seName is the pull path to the engine class
                                if (!seName.contains(".")) {
                                    System.out.println("Warning: Deployment failed. " +
                                            "Clara accepts fully qualified class names only.");
                                    return null;
                                }


                                if (objectPoolSize == null) {

                                    // if object pool size is not defined set
                                    // the size equal to the number of cores
                                    // in the node where this container is deployed
                                    int ps = Runtime.getRuntime().availableProcessors();
                                    objectPoolSize = String.valueOf(ps);
                                }
                                try {
                                    String packageName = seName.substring(0, seName.lastIndexOf("."));
                                    String className = seName.substring((seName.lastIndexOf(".")) + 1, seName.length());

                                    addService(packageName, className, Integer.parseInt(objectPoolSize));
                                } catch (xMsgException | NumberFormatException | CException | ClassNotFoundException |
                                        InstantiationException | IllegalAccessException | IOException e) {
                                    e.printStackTrace();
                                }
                                break;
                            case CConstants.REMOVE_SERVICE:
                                // Note: in this case seName is the canonical name of the service
                                try {
                                    removeService(seName);
                                } catch (xMsgException | InterruptedException | CException | IOException e) {
                                    e.printStackTrace();
                                }
                                break;
                            case CConstants.REMOVE_CONTAINER:
                                try {
                                    exitContainer();
                                } catch (xMsgException | IOException e) {
                                    e.printStackTrace();
                                }
                                break;
                        }
                    }
                }
            }
            return null;
        }
    }

    /**
     * <p>
     * Service representative in the container.
     * Starts a thread to run service specific callback.
     * </p>
     */
    private class ServiceDispatcher extends CBase {

        private String description = xMsgConstants.UNDEFINED.getStringValue();
        private String myName = xMsgConstants.UNDEFINED.getStringValue();
        private Thread dispatcherThread;
        private SubscriptionHandler serviceSubscriptionHandler;

        public ServiceDispatcher(final String serviceCanonicalName,
                                 String description)
                throws xMsgException,
                IOException {
            super();

            this.description = description;
            this.myName = serviceCanonicalName;

            dispatcherThread = new Thread(new Runnable() {
                public void run() {
                    // Subscribe messages published to this service
                    try {
                        serviceSubscriptionHandler = serviceReceive(serviceCanonicalName,
                                new ServiceCallBack());
                    } catch (xMsgException | CException e) {
                        e.printStackTrace();
                    }
                }
            });
            dispatcherThread.start();

            // register with the registrar
            register();

            if(!xMsgUtil.getLocalHostIps().contains(feHost)){
                register(feHost);
            }

            if(!feHost.equals(xMsgConstants.UNDEFINED.getStringValue())) {
                // Send service_up message to the FE
                xMsgMessage msg = new xMsgMessage(CConstants.SERVICE + ":" + feHost,
                        CConstants.SERVICE_UP + "?" + myName);
                genericSend(feHost, msg);
            }
        }

        /**
         * <p>
         * Note that Clara topic for services are constructed as:
         * dpe_host:container:engine
         * <p/>
         */
        public void register()
                throws xMsgException {
            System.out.println(CUtility.getCurrentTimeInH() + ": " + myName +
                    " sending registration request.");
            registerSubscriber(myName,
                    xMsgUtil.getTopicDomain(myName),
                    xMsgUtil.getTopicSubject(myName),
                    xMsgUtil.getTopicType(myName),
                    description);
        }

        /**
         * <p>
         * Note that Clara topic for services are constructed as:
         * dpe_host:container:engine
         * <p/>
         */
        public void register(String feHost)
                throws xMsgException {
            System.out.println(CUtility.getCurrentTimeInH() + ": " + getName() +
                    " sending registration request.");
            registerSubscriber(myName,
                    feHost,
                    xMsgConstants.DEFAULT_PORT.getIntValue(),
                    xMsgUtil.getTopicDomain(myName),
                    xMsgUtil.getTopicSubject(myName),
                    xMsgUtil.getTopicType(myName),
                    description);
        }

        /**
         * <p>
         * THis will unregister also from the FE
         * </p>
         *
         * @throws xMsgException
         */
        public void unregister() throws xMsgException {
            removeSubscriberRegistration(myName,
                    xMsgUtil.getTopicDomain(myName),
                    xMsgUtil.getTopicSubject(myName),
                    xMsgUtil.getTopicType(myName));
        }

        public void exitDispatcher() throws xMsgException {
            serviceSubscriptionHandler.unsubscribe();
            dispatcherThread.stop();
            unregister();
            description = null;
            myName = null;
        }

    }

    /**
     * <p>
     *     Service call-back, started as a thread by the ServiceDispatcher
     * </p>
     */
    private class ServiceCallBack implements xMsgCallBack {

        @Override
        // This method plays a role of a message dispatcher.
        // It uses a thread from the thread pool and the
        // service object from the object pool and runs the
        // service object serviceRequest method by passing
        // sender/dataType and dataObject of the xMsg transport.
        public xMsgMessage callback(xMsgMessage msg) {

            try {
                String receiver;
                receiver = msg.getTopic();

                final xMsgMeta.Builder metadata = msg.getMetaData();
                final Object data = msg.getData();

                // Take object and thread pool for a service.
                // This will block if there is no available object in the pool
                final Service[] requestedServiceObjectPool = _objectPoolMap.get(receiver);
                ExecutorService threadPool = _threadPoolMap.get(receiver);

                final xMsgData.Builder xData;

                if (metadata.getDataType().equals(xMsgMeta.DataType.X_Object)) {
                    xData = (xMsgData.Builder) data;

                    // Check to see if this is a service external
                    // request (outside of the composition chain request)
                    // these are request for e.g.
                    // xMsg envelope = <service_canonical_name, string, serviceReportDone?1000>
                    // that will tell service to report done messages every 1000 events.
                    if (xData != null && xData.getType().equals(xMsgData.Type.T_STRING)) {
                        String cmdData = xData.getSTRING();
                        String cmd = null, param1 = null, param2 = null, param3 = null;
                        if (cmdData.contains("?")) {
                            try {
                                StringTokenizer st = new StringTokenizer(cmdData, "?");
                                cmd = st.nextToken();
                                if (st.hasMoreTokens()) param1 = st.nextToken();
                                if (st.hasMoreTokens()) param2 = st.nextToken();
                                if (st.hasMoreTokens()) param3 = st.nextToken();
                            } catch (NoSuchElementException e) {
                                System.out.println(e.getMessage());
                            }
                            assert cmd != null;
                            switch (cmd) {
                                case CConstants.SERVICE_REPORT_DONE:
                                    if (_sysConfigs.containsKey(receiver) && param1 != null) {
                                        CServiceSysConfig sc = _sysConfigs.get(receiver);
                                        sc.setDoneRequest(true);
                                        sc.setDoneReportThreshold(Integer.parseInt(param1));
                                        sc.resetDoneRequestCount();
                                    }
                                    break;
                                case CConstants.SERVICE_REPORT_DATA:
                                    if (_sysConfigs.containsKey(receiver) && param1 != null) {
                                        CServiceSysConfig sc = _sysConfigs.get(receiver);
                                        sc.setDataRequest(true);
                                        sc.setDataReportThreshold(Integer.parseInt(param1));
                                        sc.resetDataRequestCount();
                                    }
                                    break;
                            }
                        }
                    }
                }
                // service configure
                if (metadata.getAction().equals(xMsgMeta.ControlAction.CONFIGURE)) {
                    // pool size for a specific service
                    int sps = _poolSizeMap.get(receiver);
                    if (requestedServiceObjectPool.length != sps)
                        throw new CException("service is busy. Can not configure.");

                    final AtomicInteger rps = new AtomicInteger();
                    for (int i = 0; i < _poolSizeMap.get(receiver); i++) {
                        rps.set(_poolSizeMap.get(receiver) - i);
                        final Service ser = requestedServiceObjectPool[i];
                        threadPool.submit(new Runnable() {
                                              public void run() {
                                                  try {
                                                      ser.configure(metadata, data, rps);
                                                  } catch (xMsgException | InterruptedException | CException |
                                                          ClassNotFoundException | IOException e) {
                                                      e.printStackTrace();
                                                  }
                                              }
                                          }
                        );
                    }

                    // service execute
                } else {
                    boolean _of = false;
                    do {
                        for (final Service ser : requestedServiceObjectPool) {
                            if (ser.isAvailable.get()) {
                                _of = true;
                                final CServiceSysConfig serConfig = _sysConfigs.get(receiver);

                                threadPool.submit(new Runnable() {
                                                      public void run() {
                                                          try {
                                                              ser.process(serConfig, metadata, data);
                                                          } catch (xMsgException | InterruptedException | CException |
                                                                  IOException | ClassNotFoundException e) {
                                                              e.printStackTrace();
                                                          }
                                                      }
                                                  }
                                );
                                break;
                            }
                        }
                    } while (!_of);

                }

            } catch (CException e) {
                e.printStackTrace();
            }
            return null;
        }
    }
}

