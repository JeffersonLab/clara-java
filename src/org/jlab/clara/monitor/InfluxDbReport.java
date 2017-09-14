package org.jlab.clara.monitor;

import org.influxdb.dto.Point;
import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.coda.jinflux.JinFlux;
import org.jlab.coda.jinflux.JinFluxException;
import org.jlab.coda.jinflux.JinTime;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by gurjyan on 4/25/17.
 */
public class InfluxDbReport extends DpeListenerAndReporter {

    private final String dbName = "clara";
    private String dbNode;
    private JinFlux jinFlux;
    private boolean jinFxConnected = true;
    private Map<String, String> tags = new HashMap<>();
    private Point.Builder p;

    InfluxDbReport(String name, String proxyHost, int proxyPort, String dbNode){
        super(name,new xMsgProxyAddress(proxyHost, proxyPort));
        this.dbNode = dbNode;
        try {
            jinFlux = new JinFlux(dbNode);
            if (!jinFlux.existsDB(dbName)) {
                jinFlux.createDB(dbName, 1, JinTime.HOURE);
            }

        } catch (JinFluxException e) {
            jinFxConnected = false;
            e.printStackTrace();
        }
    }

    @Override
    public void report(String jsonString) {
        long totalExecTime = 0;

        try {
            jinFlux = new JinFlux(dbNode);
            if (!jinFlux.existsDB(dbName)) {
                jinFlux.createDB(dbName, 1, JinTime.HOURE);
            }

        } catch (JinFluxException e) {
            jinFxConnected = false;
            e.printStackTrace();
        }

        if(jinFxConnected) {

            int pool_size = 1;
            tags.clear();

            JSONObject base = new JSONObject(jsonString);

            // registration information
            JSONObject registration = base.getJSONObject("DPERegistration");
            JSONArray reg_containers = registration.getJSONArray("containers");
            JSONObject reg_container = reg_containers.getJSONObject(0);
            JSONArray cont_services = reg_container.getJSONArray("services");
            for (int i = 0; i < cont_services.length(); i++) {
                JSONObject service = cont_services.getJSONObject(i);
//                System.out.println("start_time = " + service.getString("start_time"));
//                System.out.println("author = " + service.getString("author"));
//                System.out.println("pool_size = " + service.getInt("pool_size"));
//                System.out.println("description = " + service.getString("description"));
//                System.out.println("engine_name = " + service.getString("engine_name"));
//                System.out.println("language = " + service.getString("language"));
//                System.out.println("class_name = " + service.getString("class_name"));
//                System.out.println("version = " + service.getString("version"));
                pool_size = service.getInt("pool_size");
                if(pool_size>1) break;
            }

            // runtime information
            JSONObject runtime = base.getJSONObject("DPERuntime");
            String dpeName = runtime.getString("hostname");
            String session = runtime.getString("session");
            String description = runtime.getString("description");
            Long memUse = runtime.getLong("memory_usage");
            Integer cpuUse = runtime.getInt("cpu_usage");

            System.out.println("dpeName = "+dpeName);
            System.out.println("session = "+session);
            System.out.println("description = "+description);
            System.out.println("memUse = "+memUse);
            System.out.println("cpuUse = "+cpuUse);
            System.out.println("pool_size = "+pool_size);


            tags.put(ClaraConstants.SESSION, session + "-" + description);

            JSONArray rt_containers = runtime.getJSONArray("containers");
            JSONObject rt_container = rt_containers.getJSONObject(0);
            JSONArray rt_services = rt_container.getJSONArray("services");
            for (int i = 0; i < rt_services.length(); i++) {

                JSONObject service = rt_services.getJSONObject(i);

                String ser_name = service.getString("name");
                tags.put("service_name", ser_name.substring(ser_name.lastIndexOf(":")+1));
                p = jinFlux.openTB("clas12", tags);
                jinFlux.addDP(p, "cpu_usage", cpuUse);
                jinFlux.addDP(p, "memory_usage", memUse);

//                System.out.println("n_requests = " + service.getInt("n_requests"));
                jinFlux.addDP(p, "n_requests", service.getInt("n_requests"));

//                System.out.println("n_failures = " + service.getInt("n_failures"));
                jinFlux.addDP(p, "n_failures", service.getInt("n_failures"));

//                System.out.println("shm_reads = " + service.getInt("shm_reads"));
                int ser_sher_m_reads = service.getInt("shm_reads");
                jinFlux.addDP(p, "shm_reads", ser_sher_m_reads);

//                System.out.println("shm_writes = " + service.getInt("shm_writes"));
                jinFlux.addDP(p, "shm_writes", service.getInt("shm_writes"));

//                System.out.println("bytes_recv = " + service.getInt("bytes_recv"));
                jinFlux.addDP(p, "bytes_recv", service.getInt("bytes_recv"));

//                System.out.println("bytes_sent = " + service.getInt("bytes_sent"));
                jinFlux.addDP(p, "bytes_sent", service.getInt("bytes_sent"));

                Long ser_exec_time = service.getLong("exec_time");

                jinFlux.addDP(p, "pool_size", pool_size);

                if (ser_sher_m_reads > 0) {
                    long execTime = ser_exec_time / ser_sher_m_reads;
                    jinFlux.addDP(p, "exec_time", execTime);
                    totalExecTime = totalExecTime + execTime;
                }
                try {
                    jinFlux.write(dbName, p);
                } catch (JinFluxException e) {
                    e.printStackTrace();
                }

            }

            jinFlux.addDP(p, "total_exec_time", totalExecTime);
            if (pool_size > 0) jinFlux.addDP(p, "average_exec_time", totalExecTime / pool_size);

            try {
                jinFlux.write(dbName, p);
            } catch (JinFluxException e) {
                e.printStackTrace();
            }
//        System.out.println(base.toString(4));
        }
    }

    public static void main(String[] args) {
        IDROptionParser options = new IDROptionParser();
            options.parse(args);
            if (options.hasHelp()) {
                System.out.println(options.usage());
                System.exit(0);
            }

            String name = UUID.randomUUID().toString();
            try (InfluxDbReport rep = new InfluxDbReport(name, options.getM_host(), options.getM_port(), options.getDb_host())) {
            rep.start();
        } catch (xMsgException e) {
            e.printStackTrace();
        }
    }
}
