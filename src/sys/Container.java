package sys;

import base.CBase;
import base.CException;
import org.jlab.coda.xmsg.core.xMsgCallBack;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnection;
import util.CUtility;

import java.net.SocketException;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
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

    // Map containing object pools for every service in the container
    private HashMap<String, LinkedBlockingDeque<Service>>
            _objectPoolMap = new HashMap<>();

    // Map of thread pools for every service in the container
    private HashMap<String, ExecutorService>
            _threadPoolMap = new HashMap<>();

    // Socket connections to the local proxy and to the FE if requested
    private xMsgConnection proxy_connection;

    private String fe_host =
            xMsgConstants.UNDEFINED.getStringValue();

    // Unique id for services within the container
    private AtomicInteger uniqueId = new AtomicInteger(0);

    /**
     * <p>
     *     Constructor
     * </p>
     *
     * @param name given name of the service. This can be Clara
     *             service canonical name (such as dep:container:engine),
     *             or simply the engine name of a service.
     * @param feHost front-end host name. This is the host that holds
     *               centralized registration database.
     * @throws xMsgException
     */
    public Container(String name,
                     String feHost)
            throws xMsgException {
        super(feHost);
        this.fe_host = feHost;
        setName(CUtility.form_container_name(name));

        // Create a socket connections to the local dpe proxy
        proxy_connection =  connect();
    }

    /**
     * <p>
     *     Constructor
     * </p>
     *
     * @param name given name of the service. This can be Clara
     *             service canonical name (such as dep:container:engine),
     *             or simply the engine name of a service.
     * @throws xMsgException
     */
    public Container(String name)
            throws xMsgException {
        super();
        setName(CUtility.form_container_name(name));

        // Create a socket connections to the local dpe proxy
        proxy_connection =  connect();
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
     *     Create object pool to hold objects of this requested service
     * </p>
     * @param name
     * @param objectPoolSize
     */
    public void addService(String name,
                           int objectPoolSize)
            throws xMsgException, SocketException {

        // Check the passed service name
        if(!name.contains(":")) {
            try {
                name = CUtility.form_service_name("localhost",
                        getName(),
                        name);
            } catch (xMsgException e) {
                e.printStackTrace();
            }
        }

        // Define the key in the shared
        // memory map (defined in the DPE).
        int id = uniqueId.incrementAndGet();
        String sharedMemoryLocation = name+id;

        // Creating thread pool
        _threadPoolMap.put(name, Executors.newFixedThreadPool(objectPoolSize));

        // We need final variables to pass
        // abstract method implementation
        final String n = name;
        final String fe = fe_host;

        // Creating object pool
        LinkedBlockingDeque<Service> lbq = new LinkedBlockingDeque<>(objectPoolSize);

        // Fill the object pool
        for(int i=0; i<objectPoolSize; i++){
            Service service;

            // Create an object of the Service class by passing
            // service name as a parameter
            if(fe_host.equals(xMsgConstants.UNDEFINED.getStringValue())) {
                service =  new Service(n, sharedMemoryLocation);
            } else {
                service =  new Service(n, sharedMemoryLocation, fe);
            }
            // add object to the pool
            lbq.add(service);
        }

        // Add the object pool to the pools map
        _objectPoolMap.put(name, lbq);

        // Subscribe messages published to this service
        subscribe(proxy_connection,
                xMsgUtil.getTopicDomain(getName()),
                xMsgUtil.getTopicSubject(getName()),
                xMsgUtil.getTopicType(getName()),
                new ServiceCallBack(),
                true);

    }

    public void removeService(String name)
            throws xMsgException, InterruptedException {

        // Check to see if the passed name is a canonical
        // name of a service or just a service engine name
        // Note: we assume that if name contains ":" then it is
        // properly formed service canonical name. Also if it
        // does not then the passed string is the service
        // engine name.
        if(!name.contains(":")) {
            try {
                name = CUtility.form_service_name("localhost",
                        getName(),
                        name);
            } catch (xMsgException e) {
                e.printStackTrace();
            }
        }

        // clear and shut down thread pool for the requested service
        ExecutorService threadPool = _threadPoolMap.get(name);
        threadPool.shutdown();
        _threadPoolMap.remove(name);

        // Clear object pool of a service by calling dispose
        // method of a service (for every service object
        // in the pool)
        LinkedBlockingDeque op = _objectPoolMap.get(name);
        Service ser = (Service)op.take();

        // remove registration of a service and exit
        ser.dispose();

        // dispose the object pool for the service
        op.clear();

    }

    private class ServiceCallBack implements xMsgCallBack {

        @Override
        // This method plays a role of a message dispatcher.
        // It uses a thread from the thread pool and the
        // service object from the object pool and runs the
        // service object serviceRequest method by passing
        // sender/dataType and dataObject of the xMsg transport.
        public void callback(xMsgMessage msg) {

            try {
                String receiver = CUtility.form_service_name(msg.getDomain(),
                        msg.getSubject(),
                        msg.getType());

                final String dataType = msg.getDataType();
                final Object data = msg.getData();

                final LinkedBlockingDeque<Service> op = _objectPoolMap.get(receiver);

                // This will block if there is no available object in the pool
                final Service ser = op.take();

                ExecutorService threadPool = _threadPoolMap.get(receiver);
                threadPool.submit(new Runnable() {
                                      public void run() {
                                          try {
                                              ser.process(op, dataType, data, -1);
                                          } catch (CException | xMsgException | SocketException | InterruptedException e) {
                                              e.printStackTrace();
                                          }
                                      }
                                  }
                );


            } catch (xMsgException | InterruptedException  e) {
                e.printStackTrace();
            }

            System.out.println(msg);
        }
    }
}
