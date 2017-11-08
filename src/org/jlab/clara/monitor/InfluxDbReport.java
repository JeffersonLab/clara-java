package org.jlab.clara.monitor;

import org.influxdb.dto.Point;
import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.coda.jinflux.JinFlux;
import org.jlab.coda.jinflux.JinFluxException;
import org.jlab.coda.jinflux.JinTime;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.json.JSONArray;
import org.json.JSONObject;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by gurjyan on 4/25/17.
 */
public class InfluxDbReport extends DpeListenerAndReporter {

    private final String dbName = "clara";
    private final String dbNode;

    private JinFlux jinFlux;
    private boolean jinFxConnected = true;

    private Map<String, String> tags = new HashMap<>();
    private Point.Builder p;

    private DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    InfluxDbReport(String name, String proxyHost, int proxyPort, String dbNode) {
        super(name, new xMsgProxyAddress(proxyHost, proxyPort));
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

        if (jinFxConnected) {

            int poolSize = 1;
            tags.clear();

            JSONObject base = new JSONObject(jsonString);

            // registration information
            JSONObject registration = base.getJSONObject("DPERegistration");
            JSONArray regContainers = registration.getJSONArray("containers");
            if (regContainers.length() <= 0) {
                return;
            }
            JSONObject regContainer = regContainers.getJSONObject(0);
            JSONArray contServices = regContainer.getJSONArray("services");
            for (int i = 0; i < contServices.length(); i++) {
                JSONObject service = contServices.getJSONObject(i);
                poolSize = service.getInt("pool_size");
                if (poolSize > 1) {
                    break;
                }
            }

            // runtime information
            JSONObject runtime = base.getJSONObject("DPERuntime");
            Long memUse = runtime.getLong("memory_usage");
            Integer cpuUse = runtime.getInt("cpu_usage");

            String session = registration.getString("session");
            System.out.println(dateFormat.format(new Date()) + ": reporting for " + session);

            tags.put(ClaraConstants.SESSION, session);

            JSONArray rtContainers = runtime.getJSONArray("containers");
            JSONObject rtContainer = rtContainers.getJSONObject(0);
            JSONArray rtServices = rtContainer.getJSONArray("services");
            for (int i = 0; i < rtServices.length(); i++) {

                JSONObject service = rtServices.getJSONObject(i);

                String serName = service.getString("name");
                tags.put("service_name", ClaraUtil.getEngineName(serName));
                p = jinFlux.openTB("clas12", tags);
                jinFlux.addDP(p, "cpu_usage", cpuUse);
                jinFlux.addDP(p, "memory_usage", memUse);

                jinFlux.addDP(p, "n_requests", service.getInt("n_requests"));
                jinFlux.addDP(p, "n_failures", service.getInt("n_failures"));

                int serShmReads = service.getInt("shm_reads");

                jinFlux.addDP(p, "shm_reads", serShmReads);
                jinFlux.addDP(p, "shm_writes", service.getInt("shm_writes"));
                jinFlux.addDP(p, "bytes_recv", service.getInt("bytes_recv"));
                jinFlux.addDP(p, "bytes_sent", service.getInt("bytes_sent"));
                jinFlux.addDP(p, "pool_size", poolSize);

                Long serExecTime = service.getLong("exec_time");
                if (serShmReads > 0) {
                    long execTime = serExecTime / serShmReads;
                    jinFlux.addDP(p, "exec_time", execTime);
                    totalExecTime += execTime;
                }

                try {
                    jinFlux.write(dbName, p);
                } catch (JinFluxException e) {
                    e.printStackTrace();
                }
            }

            jinFlux.addDP(p, "total_exec_time", totalExecTime);
            if (poolSize > 0) {
                jinFlux.addDP(p, "average_exec_time", totalExecTime / poolSize);
            }

            try {
                jinFlux.write(dbName, p);
            } catch (JinFluxException e) {
                e.printStackTrace();
            }
        }
    }

    private static InfluxDbReport createInfluxReporter(IDROptionParser parser) {
        String name = UUID.randomUUID().toString();
        return new InfluxDbReport(
                name,
                parser.getProxyHost(),
                parser.getProxyPort(),
                parser.getDataBaseHost()
        );
    }

    public static void main(String[] args) {
        IDROptionParser options = new IDROptionParser();
        if (!options.parse(args)) {
            System.exit(1);
        }
        if (options.hasHelp()) {
            System.out.println(options.usage());
            System.exit(0);
        }
        try (InfluxDbReport rep = createInfluxReporter(options)) {
            rep.start();
        } catch (xMsgException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
