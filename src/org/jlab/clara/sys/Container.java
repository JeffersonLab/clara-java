package org.jlab.clara.sys;

import org.jlab.clara.base.CBase;
import org.jlab.clara.base.CException;
import org.jlab.clara.util.CConstants;
import org.jlab.clara.util.CTransit;
import org.jlab.clara.util.CUtility;
import org.jlab.coda.xmsg.core.xMsgCallBack;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.data.xMsgD;
import org.jlab.coda.xmsg.excp.xMsgException;

import java.net.SocketException;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 *      Container have a map of ObjectPools, one for each service in the container.
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
    private HashMap<String, LinkedBlockingQueue<Service>>
            _objectPoolMap = new HashMap<>();

    // Map of thread pools for every service in the container
    private HashMap<String, ExecutorService>
            _threadPoolMap = new HashMap<>();

    // stores pool size for every service
    private HashMap<String, Integer> _poolSizeMap = new HashMap<>();

    private String feHost =
            xMsgConstants.UNDEFINED.getStringValue();

    // Unique id for services within the container
    private AtomicInteger uniqueId = new AtomicInteger(0);

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
            throws xMsgException, SocketException {
        super(feHost);
        this.feHost = feHost;

        setName(name);

        // Create a socket connections to the local dpe proxy
        connect();

        System.out.println(CUtility.getCurrentTimeInH()+": Started container = "+getName());

        // Send container_up message to the FE
        genericSend(CConstants.CONTAINER + ":" + feHost, CConstants.CONTAINER_UP+"?"+getName());

        Thread t1 = new Thread(new Runnable() {
            public void run() {
                // Subscribe messages published to this container
                try {
                    genericReceive(CConstants.CONTAINER + ":" + getName(),
                            new ContainerCallBack());
                } catch (xMsgException e) {
                    e.printStackTrace();
                }
            }
        });
        t1.start();
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

        System.out.println(CUtility.getCurrentTimeInH()+": Started container = "+getName());

        Thread t1 = new Thread(new Runnable() {
            public void run() {
                // Subscribe messages published to this container
                try {
                    genericReceive(CConstants.CONTAINER + ":" + getName(),
                            new ContainerCallBack());
                } catch (xMsgException e) {
                    e.printStackTrace();
                }
            }
        });
        t1.start();

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
     * @param packageName service engine package name
     * @param name service engine class name
     * @param objectPoolSize size of the object pool
     */
    public void addService(String packageName,
                           String name,
                           int objectPoolSize)
            throws CException,
            xMsgException,
            SocketException,
            IllegalAccessException,
            ClassNotFoundException,
            InstantiationException {


        // We need final variables to pass
        // abstract method implementation
        final String canonical_name = getName() + ":" + name;

        if(_threadPoolMap.containsKey(canonical_name)){
            throw new CException("service exists");
        }

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

        // Creating object pool
        LinkedBlockingQueue<Service> lbq = new LinkedBlockingQueue<>(objectPoolSize);

        _poolSizeMap.put(canonical_name,objectPoolSize);

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
            lbq.add(service);
        }

        // Add the object pool to the pools map
        _objectPoolMap.put(canonical_name, lbq);

        new ServiceDispatcher(canonical_name);

    }

    public void removeService(String name)
            throws xMsgException, InterruptedException, CException {

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
            LinkedBlockingQueue op = _objectPoolMap.get(name);
            Service ser = (Service) op.take();

            // remove registration of a service and exit
            ser.dispose();

            // dispose the object pool for the service
            op.clear();
        }

    }

    private class ContainerCallBack implements xMsgCallBack {

        @Override
        // This method serves to start/deploy a service on
        // this container. In the future it will report
        // service specific statistics on a request
        public Object callback(xMsgMessage msg) {

            final String dataType = msg.getDataType();
            final Object data = msg.getData();
            if(dataType.equals(xMsgConstants.ENVELOPE_DATA_TYPE_STRING.getStringValue())) {
//                System.out.println("DDD: container_request: " + msg);
                String cmdData = (String)data;
                String cmd = null, seName = null, objectPoolSize = null;
                try {
                    StringTokenizer st = new StringTokenizer(cmdData, "?");
                    cmd = st.nextToken();
                    seName = st.nextToken();
                    objectPoolSize = st.nextToken();
                } catch (NoSuchElementException e){
                    System.out.println(e.getMessage());
//                    e.printStackTrace();
                }
                if(cmd!=null && seName!=null) {
                    switch (cmd) {
                        case CConstants.DEPLOY_SERVICE:
                            if(objectPoolSize==null){

                                // if object pool size is not defined set
                                // the size equal to the number of cores
                                // in the node where this container is deployed
                                int ps = Runtime.getRuntime().availableProcessors();
                                objectPoolSize = String.valueOf(ps);
                            }
                            try {
                                String packageName = seName.substring(0,seName.lastIndexOf("."));
                                String className = seName.substring((seName.lastIndexOf("."))+1, seName.length());

                                addService(packageName, className, Integer.parseInt(objectPoolSize));
                            } catch (xMsgException | NumberFormatException | CException |
                                    SocketException | ClassNotFoundException |
                                    InstantiationException | IllegalAccessException e) {
                                e.printStackTrace();
                            }
                            break;
                        case CConstants.REMOVE_SERVICE:
                            try {
                                removeService(seName);
                            } catch (xMsgException | InterruptedException | CException e) {
                                e.printStackTrace();
                            }
                            break;
                    }
                }
            }
            return null;
        }
    }

    private class ServiceDispatcher extends CBase {

        public ServiceDispatcher(final String serviceCanonicalName) throws xMsgException, SocketException {
            super();

            Thread t1 = new Thread(new Runnable() {
                public void run() {
                    // Subscribe messages published to this service
                    try {
                        serviceReceive(serviceCanonicalName,
                                new ServiceCallBack());
                    } catch (xMsgException e) {
                        e.printStackTrace();
                    }
                }
            });
            t1.start();

        }
    }

    private class ServiceCallBack implements xMsgCallBack {

        @Override
        // This method plays a role of a message dispatcher.
        // It uses a thread from the thread pool and the
        // service object from the object pool and runs the
        // service object serviceRequest method by passing
        // sender/dataType and dataObject of the xMsg transport.
        public Object callback(xMsgMessage msg) {

            try {
                String receiver;
                receiver = CUtility.form_service_name(msg.getDomain(),
                        msg.getSubject(),
                        msg.getType());

                final String dataType = msg.getDataType();
                final Object data = msg.getData();

                // If this is a sync request getSyncRequesterAddress()
                // will return address NOT equal to "undefined".
                // Service process method will check this
                final String syncReceiver = msg.getSyncRequesterAddress();

                // Take object and thread pool for a service.
                // This will block if there is no available object in the pool
                final LinkedBlockingQueue<Service> op = _objectPoolMap.get(receiver);
                ExecutorService threadPool = _threadPoolMap.get(receiver);


                // service configure
                xMsgD.Data.Builder inData = null;
                if (dataType.equals(xMsgConstants.ENVELOPE_DATA_TYPE_XMSGDATA.getStringValue())) {
                    inData = (xMsgD.Data.Builder) data;
                    if (inData == null) throw new CException("unknown data type");

                }
                // check to see if this is a configure request
                if (inData!=null && inData.getAction().equals(xMsgD.Data.ControlAction.CONFIGURE)){
                    // pool size for a specific service
                    int sps = _poolSizeMap.get(receiver);
                    if(op.size()!=sps)throw new CException("service is busy. Can not configure.");

                    for(int i=0; i<_poolSizeMap.get(receiver);i++){
                        final Service ser = op.take();
                        threadPool.submit(new Runnable() {
                                              public void run() {
                                                  try {
                                                      ser.configure(op, dataType, data, syncReceiver);
                                                  } catch (xMsgException | InterruptedException | CException e) {
                                                      e.printStackTrace();
                                                  }
                                              }
                                          }
                        );
                    }

                    // service execute
                } else {

                    final Service ser = op.take();

                    threadPool.submit(new Runnable() {
                                          public void run() {
                                              try {
                                                  ser.process(op, dataType, data, syncReceiver, -1);
                                              } catch (xMsgException | SocketException | InterruptedException | CException e) {
                                                  e.printStackTrace();
                                              }
                                          }
                                      }
                    );
                }
            } catch (CException | InterruptedException  e) {
                e.printStackTrace();
            }
            return null;
        }
    }

}
