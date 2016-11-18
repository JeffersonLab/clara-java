package org.jlab.clara.util.report;

//import org.influxdb.dto.Point;
//import org.jlab.clara.base.ClaraUtil;
//import org.jlab.clara.base.core.ClaraConstants;
//import org.jlab.coda.jinflux.JinFlux;
//import org.jlab.coda.jinflux.JinFluxException;
//import org.jlab.coda.jinflux.JinTime;
//
//import java.util.HashMap;
//import java.util.Map;

/**
 * Class description here....
 * <p>
 *
 * @author gurjyan
 *         Date 11/18/16
 * @version 4.x
 */
public class JinfluxReport {
//public class JinfluxReport extends JinFlux implements ExternalReport {
//
//    private String dbName;
//    private boolean jinFxConnected = true;
//
//    public JinfluxReport(String dbNode, String dbName, String user, String password) throws JinFluxException {
//        super(dbNode, user, password);
//        this.dbName = dbName;
//        try {
//            if (!existsDB(dbName)) {
//                createDB(dbName, 1, JinTime.HOURE);
//            }
//        } catch (Exception e) {
//            jinFxConnected = false;
//        }
//
//    }
//
//    public JinfluxReport(String dbNode, String dbName) throws JinFluxException {
//        super(dbNode);
//        this.dbName = dbName;
//
//        try {
//            if (!existsDB(dbName)) {
//                createDB(dbName, 1, JinTime.HOURE);
//            }
//
//        } catch (Exception e) {
//            jinFxConnected = false;
//        }
//    }
//
//
//    public void push(DpeReport dpeData) {
//        try {
//
//            if (jinFxConnected) {
//
//                for (ContainerReport cr : dpeData.getContainers()) {
//
//                    for (ServiceReport sr : cr.getServices()) {
//                        Map<String, String> tags = new HashMap<>();
//                        tags.put(ClaraConstants.DPE, dpeData.getHost());
//                        tags.put(ClaraConstants.SESSION, dpeData.getAuthor());
//                        Point.Builder p = openTB("clas12", tags);
//
//                        addDP(p, "language", dpeData.getLang());
//                        addDP(p, "clara_home", dpeData.getClaraHome());
//                        addDP(p, "n_cores", dpeData.getCoreCount());
//                        addDP(p, "memory_size", dpeData.getMemorySize());
//                        addDP(p, "start_time", dpeData.getStartTime());
//
//                        addDP(p, "host_name", dpeData.getHost());
//                        addDP(p, "cpu_usage", dpeData.getCpuUsage());
//                        addDP(p, "memory_usage", dpeData.getMemoryUsage());
//                        addDP(p, "load", dpeData.getLoad());
//
////                        addDP(p,"class_name", sr.getClassName());
//                        addDP(p, "engine_name", sr.getEngineName());
//                        addDP(p, "author", sr.getAuthor());
//                        addDP(p, "version", sr.getVersion());
////                        addDP(p,"description", sr.getDescription());
////                        addDP(p, "language", sr.getLang());
////                        addDP(p,"start_time", sr.getStartTime());
//
//                        long serviceRequests = sr.getRequestCount();
//
//                        addDP(p,"name", sr.getName());
//                        addDP(p,"n_requests", serviceRequests);
//                        addDP(p,"n_failures", sr.getFailureCount());
//                        addDP(p,"shm_reads", sr.getShrmReads());
//                        addDP(p,"shm_writes", sr.getShrmWrites());
//                        addDP(p,"bytes_recv", sr.getBytesReceived());
//                        addDP(p,"bytes_sent", sr.getBytesSent());
//                        addDP(p,"exec_time", sr.getExecutionTime());
//
//                        write(dbName, p);
//                        ClaraUtil.sleep(1);
//                    }
//                }
//            }
//        } catch (Exception e) {
//            System.out.println("DDD: Error writing into influxDB");
//        }
//
//    }
//
//
//    public boolean isConnected() {
//
//        return jinFxConnected;
//    }
//
//    public boolean isServerUp(int timeout) throws Exception {
//        jinFxConnected = ping(timeout);
//        return jinFxConnected;
//    }
//
//    public void checkCreate() {
//        try {
//            if (isServerUp(1)) {
//                // connect to the database
//                // create database if it does not exists
//                if (!existsDB(dbName)) {
//                    createDB(dbName, 1, JinTime.HOURE);
//                }
//            }
//        } catch (Exception e) {
//            jinFxConnected = false;
//        }
//    }
//
//
//    @Override
//    public String generateReport(DpeReport dpeData) {
//        return null;
//    }

}
