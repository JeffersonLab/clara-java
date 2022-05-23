package org.jlab.clara.std.monitor;

import org.influxdb.dto.Point;
import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.base.ContainerRegistrationData;
import org.jlab.clara.base.ContainerRuntimeData;
import org.jlab.clara.base.DataRingAddress;
import org.jlab.clara.base.DpeRegistrationData;
import org.jlab.clara.base.DpeRuntimeData;
import org.jlab.clara.base.ServiceName;
import org.jlab.clara.base.ServiceRegistrationData;
import org.jlab.clara.base.ServiceRuntimeData;
import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.clara.std.orchestrators.DpeReportHandler;
import org.jlab.clara.std.orchestrators.MonitorOrchestrator;
import org.jlab.clara.util.OptUtils;
import org.jlab.coda.jinflux.JinFlux;
import org.jlab.coda.jinflux.JinFluxException;
import org.jlab.coda.jinflux.JinTime;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import static java.util.Arrays.asList;

/**
 * Saves the DPE reports data into a time series database.
 */
public class InfluxDbReporter implements DpeReportHandler {

    private static final String DEFAULT_DB_HOST = "claraweb.jlab.org";
    private static final String DEFAULT_MON_HOST = "129.57.70.24";
    private static final int DEFAULT_MON_PORT = ClaraConstants.MONITOR_PORT;

    private final String dbName = "clara";

    private JinFlux jinFlux;
    private boolean jinFxConnected = true;

    /**
     * Main of the InfluxDB  reporter class.
     * @param args param
     */
    public static void main(String[] args) {
        IDROptionParser parser = new IDROptionParser();
        if (!parser.parse(args)) {
            System.exit(1);
        }
        if (parser.hasHelp()) {
            System.out.println(parser.usage());
            System.exit(0);
        }
        try {
            DataRingAddress proxyAddress =
                    new DataRingAddress(parser.proxyHost(), parser.proxyPort());
            MonitorOrchestrator monitor = new MonitorOrchestrator(proxyAddress);
            InfluxDbReporter reporter = new InfluxDbReporter(parser.dataBaseHost());

            Runtime.getRuntime().addShutdownHook(new Thread(monitor::close));

            monitor.listenDpeReports(reporter);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }


    /**
     * Create a reporter to the InfluxDB database on the default JLab host.
     */
    public InfluxDbReporter() {
        this(DEFAULT_DB_HOST);
    }


    /**
     * Create a reporter to the InfluxDB database on the given host.
     *
     * @param dbNode the database host
     */
    public InfluxDbReporter(String dbNode) {
        try {
            jinFlux = new JinFlux(dbNode);
            if (!jinFlux.existsDB(dbName)) {
                jinFlux.createDB(dbName, 1, JinTime.HOURE);
            }
        } catch (JinFluxException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void handleReport(DpeRegistrationData dpeRegistration, DpeRuntimeData dpeRuntime) {

        if (jinFxConnected) {
            Map<String, String> tags = new HashMap<>();

            Set<ContainerRegistrationData> regContainers = dpeRegistration.containers();
            if (regContainers.isEmpty()) {
                return;
            }

            String session = dpeRegistration.session();
            System.out.printf("%s: reporting for DPE = '%s'  session = '%s'%n",
                      ClaraUtil.getCurrentTime(), dpeRegistration.name(), session);

            tags.put(ClaraConstants.SESSION, session);

            int poolSize = 1;
            for (ContainerRegistrationData containerReg : regContainers) {
                for (ServiceRegistrationData serviceReg : containerReg.services()) {
                    poolSize = serviceReg.poolSize();
                    if (poolSize > 1) {
                        break;
                    }
                }
            }

            long memUse = dpeRuntime.memoryUsage();
            double cpuUse = dpeRuntime.cpuUsage();
            long totalExecTime = 0;

            Point.Builder p = null;

            for (ContainerRuntimeData container : dpeRuntime.containers()) {
                for (ServiceRuntimeData service : container.services()) {
                    ServiceName serName = service.name();
                    tags.put("service_name", serName.name());
                    p = Point.measurement("clas12").tag(tags);
                    p.addField("cpu_usage", cpuUse);
                    p.addField("memory_usage", memUse);

                    p.addField("n_requests", service.numRequests());
                    p.addField("n_failures", service.numFailures());

                    long serShmReads = service.sharedMemoryReads();

                    p.addField("shm_reads", serShmReads);
                    p.addField("shm_writes", service.sharedMemoryWrites());
                    p.addField("bytes_recv", service.bytesReceived());
                    p.addField("bytes_sent", service.bytesSent());
                    p.addField("pool_size", poolSize);

                    long serExecTime = service.executionTime();
                    if (serShmReads > 0) {
                        long execTime = serExecTime / serShmReads;
                        p.addField("exec_time", execTime);
                        totalExecTime += execTime;
                    }

                    try {
                        jinFlux.write(dbName, p);
                    } catch (JinFluxException e) {
                        e.printStackTrace();
                    }
                }
            }

            if (p == null) {
                return;
            }

            p.addField("total_exec_time", totalExecTime);
            p.addField("average_exec_time", totalExecTime / poolSize);

            try {
                jinFlux.write(dbName, p);
            } catch (JinFluxException e) {
                e.printStackTrace();
            }
        }
    }


    private static class IDROptionParser {

        private final OptionSpec<String> proxyHost;
        private final OptionSpec<Integer> proxyPort;
        private final OptionSpec<String> dbHost;

        private final OptionParser parser;
        private OptionSet options;

        IDROptionParser() {
            parser = new OptionParser();

            proxyHost = parser.acceptsAll(asList("fe-host", "m-host"))
                    .withRequiredArg()
                    .defaultsTo(DEFAULT_MON_HOST);

            proxyPort = parser.acceptsAll(asList("fe-port", "m-port"))
                    .withRequiredArg()
                    .ofType(Integer.class)
                    .defaultsTo(DEFAULT_MON_PORT);

            dbHost = parser.acceptsAll(asList("db-host"))
                    .withRequiredArg()
                    .defaultsTo(DEFAULT_DB_HOST);

            parser.acceptsAll(asList("h", "help")).forHelp();
        }

        public boolean parse(String[] args) {
            try {
                options = parser.parse(args);

                if (!options.has(proxyHost)) {
                    System.out.println("Using the default proxy host: " + DEFAULT_MON_PORT);
                }
                if (!options.has(proxyPort)) {
                    System.out.println("Using the default proxy port: " + DEFAULT_MON_PORT);
                }
                if (!options.has(dbHost)) {
                    System.out.println("Using the default db host: " + DEFAULT_DB_HOST);
                }

                return true;
            } catch (OptionException e) {
                System.err.println("error: " + e.getMessage());
                return false;
            }
        }

        public String proxyHost() {
            return options.valueOf(proxyHost);
        }

        public int proxyPort() {
            return options.valueOf(proxyPort);
        }

        public String dataBaseHost() {
            return options.valueOf(dbHost);
        }

        public boolean hasHelp() {
            return options.has("help");
        }

        public String usage() {
            return String.format("usage: j_idr [options]%n%n  Options:%n")
                    + OptUtils.optionHelp(proxyHost, "hostname", "the monitoring proxy host")
                    + OptUtils.optionHelp(proxyPort, "port", "the monitoring proxy port")
                    + OptUtils.optionHelp(dbHost, "hostname", "the database host");
        }
    }
}
