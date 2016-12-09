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
import org.jlab.clara.base.core.ClaraComponent;
import org.jlab.clara.base.core.MessageUtil;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.sys.DpeOptionsParser.DpeOptionsException;
import org.jlab.clara.sys.RequestParser.RequestException;
import org.jlab.clara.util.report.DpeReport;
import org.jlab.clara.util.report.JinFluxReportBuilder;
import org.jlab.clara.util.report.JsonReportBuilder;
import org.jlab.coda.jinflux.JinFluxException;
import org.jlab.coda.xmsg.core.xMsgCallBack;
import org.jlab.coda.xmsg.core.xMsgConnection;
import org.jlab.coda.xmsg.core.xMsgConnectionPool;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgSubscription;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgContext;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

/**
 * Clara data processing environment. It can play the role of the Front-End
 * (FE), which is the static point of the entire cloud. It creates and manages
 * the registration database (local and case of being assigned as an FE: global
 * database). Note this is a copy of the subscribers database resident in the
 * xMsg registration database. This also creates a shared memory for
 * communicating Clara transient data objects between services within the same
 * process (this avoids data serialization and de-serialization).
 *
 * @author gurjyan
 * @version 4.x
 */
public final class Dpe extends AbstractActor {

    static final int DEFAULT_PROXY_PORT = ClaraConstants.JAVA_PORT;

    static final int DEFAULT_MAX_CORES = Runtime.getRuntime().availableProcessors();
    static final int DEFAULT_POOL_SIZE = 2;
    static final long DEFAULT_REPORT_PERIOD = 10_000;

    static final int DEFAULT_MAX_SOCKETS = 1024;
    static final int DEFAULT_IO_THREADS = 1;

    // these are guarded by start/stop synchronized blocks on parent
    private Proxy proxy = null;
    private FrontEnd frontEnd = null;
    private xMsgSubscription subscriptionHandler;

    // a shared connection pool between all services
    private volatile xMsgConnectionPool servicesConnectionPool;

    // session ID
    private volatile String session = "undefined";

    // The containers running on this DPE
    private final ConcurrentMap<String, Container> myContainers = new ConcurrentHashMap<>();

    private final ReportService reportService;
    private final int maxCores;


    public static void main(String[] args) {
        DpeOptionsParser options = new DpeOptionsParser();
        try {
            options.parse(args);
            if (options.hasHelp()) {
                System.out.println(options.usage());
                System.exit(0);
            }

            // config ZMQ context
            xMsgContext.setIOThreads(options.ioThreads());
            xMsgContext.setMaxSockets(options.maxSockets());

            // start a dpe
            Dpe dpe = new Dpe(options.isFrontEnd(), options.localAddress(), options.frontEnd(),
                              options.config(), options.session(), options.description());

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    dpe.stop();
                }
            });

            dpe.start();

        } catch (DpeOptionsException e) {
            System.err.println(e.getMessage());
            System.err.println();
            System.err.println(options.usage());
            System.exit(1);
        } catch (ClaraException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }


    /**
     * Helps constructing a {@link Dpe DPE}.
     */
    public static class Builder {

        final boolean isFrontEnd;

        xMsgProxyAddress localAddress;
        xMsgProxyAddress frontEndAddress;

        String session = "";

        int poolSize = DEFAULT_POOL_SIZE;
        int maxCores = DEFAULT_MAX_CORES;
        long reportPeriod = DEFAULT_REPORT_PERIOD;
        String description = "";

        /**
         * Creates a builder for a front-end DPE.
         * The front-end DPE contains the registration service used by the
         * orchestrators to find running worker DPEs and engines.
         * <p>
         * A front-end DPE is also a worker DPE that can run user engines,
         * so it is recommended to run a front-end when using CLARA on a local box.
         * In multi-node CLARA distributions it is mostly used a discovery and
         * gateway for workers DPEs.
         */
        public Builder() {
            isFrontEnd = true;
            localAddress = new xMsgProxyAddress(ClaraUtil.localhost(), DEFAULT_PROXY_PORT);
            frontEndAddress = localAddress;
        }

        /**
         * Creates a builder for a worker DPE.
         * A worker DPE mainly runs user engines as part of a cloud of DPEs.
         * All worker DPEs must register with the main front-end DPE.
         * <p>
         * When running CLARA on single node, a front-end DPE must be used
         * instead of a worker DPE.
         *
         * @param frontEndHost the host address of the front-end
         */
        public Builder(String frontEndHost) {
            this(frontEndHost, DEFAULT_PROXY_PORT);
        }

        /**
         * Creates a builder for a worker DPE.
         * A worker DPE mainly runs user engines as part of a cloud of DPEs.
         * All worker DPEs must register with the main front-end DPE.
         * <p>
         * When running CLARA on single node, a front-end DPE must be used
         * instead of a worker DPE.
         *
         * @param frontEndHost the host address of the front-end
         * @param frontEndPort the port number of the front-end
         */
        public Builder(String frontEndHost, int frontEndPort) {
            isFrontEnd = false;
            localAddress = new xMsgProxyAddress(ClaraUtil.localhost(), DEFAULT_PROXY_PORT);
            frontEndAddress = new xMsgProxyAddress(frontEndHost, frontEndPort);
        }

        /**
         * Uses the given host for the local address.
         */
        public Builder withHost(String host) {
            localAddress = new xMsgProxyAddress(host, localAddress.pubPort());
            if (isFrontEnd) {
                frontEndAddress = localAddress;
            }
            return this;
        }

        /**
         * Uses the given port for the local address.
         */
        public Builder withPort(int port) {
            localAddress = new xMsgProxyAddress(localAddress.host(), port);
            if (isFrontEnd) {
                frontEndAddress = localAddress;
            }
            return this;
        }

        /**
         * Sets a session for this DPE.
         */
        public Builder withSession(String id) {
            Objects.requireNonNull(id, "id parameter is null");
            this.session = id;
            return this;
        }

        /**
         * Sets the interval of time between publishing reports.
         */
        public Builder withReportPeriod(long interval, TimeUnit unit) {
            if (interval <= 0) {
                throw new IllegalArgumentException("Invalid report interval: " + interval);
            }
            this.reportPeriod = unit.toMillis(interval);
            return this;
        }

        /**
         * Sets the size of the thread-pool that will process requests.
         */
        public Builder withPoolSize(int poolSize) {
            if (poolSize <= 0) {
                throw new IllegalArgumentException("Invalid pool size: " + poolSize);
            }
            this.poolSize = poolSize;
            return this;
        }

        /**
         * Sets the number of cores that a service can use in parallel.
         */
        public Builder withMaxCores(int maxCores) {
            if (maxCores <= 0) {
                throw new IllegalArgumentException("Invalid number of cores: " + maxCores);
            }
            this.maxCores = maxCores;
            return this;
        }


        /**
         * Sets a description for this DPE.
         */
        public Builder withDescription(String description) {
            Objects.requireNonNull(description, "description parameter is null");
            this.description = description;
            return this;
        }

        /**
         * Creates the DPE.
         */
        public Dpe build() {
            DpeConfig config = new DpeConfig(poolSize, maxCores, reportPeriod);
            return new Dpe(isFrontEnd, localAddress, frontEndAddress,
                           config, session, description);
        }
    }


    /**
     * Constructor of a DPE.
     *
     * @param isFrontEnd true if this DPE is the front-end
     * @param proxyAddress address of local proxy
     * @param frontEndAddress address of front-end proxy
     * @param description textual description of the DPE
     */
    private Dpe(boolean isFrontEnd,
                xMsgProxyAddress proxyAddress,
                xMsgProxyAddress frontEndAddress,
                DpeConfig config,
                String session,
                String description) {

        super(ClaraComponent.dpe(proxyAddress.host(),
                                 proxyAddress.pubPort(),
                                 ClaraConstants.JAVA_LANG,
                                 config.poolSize(),
                                 description),
              ClaraComponent.dpe(frontEndAddress.host(),
                      frontEndAddress.pubPort(),
                      ClaraConstants.JAVA_LANG,
                      1, "Front End"));

        AbstractActor.isFrontEnd.set(isFrontEnd);
        this.reportService = new ReportService(config.reportPeriod(), session);
        this.session = session;
        this.maxCores = config.maxCores();
    }

    /**
     * Starts this DPE.
     * <p>
     * Starts a local xMsg proxy server, and a local xMsg registrar service
     * in case it is a front-end. Does proper subscriptions to receive requests
     * and starts heart beat reporting thread.
     * <p>
     * It is never legal to start a DPE more than once. More specifically,
     * a DPE should not be restarted once it has being stopped.
     *
     * @throws ClaraException if the DPE could not be started
     */
    @Override
    public void start() throws ClaraException {
        super.start();
        cacheConnections();
    }

    /**
     * Shuts down this DPE.
     * <p>
     * Stops accepting new requests, stops the reporting thread,
     * and waits for all containers and services to shut down.
     * The local xMsg proxy server, and the local xMsg registrar service in case
     * it is a front-end, are destroyed last.
     */
    @Override
    public void stop() {
        super.stop();
    }

    @Override
    void startMsg() {
        printLogo();
    }

    @Override
    void stopMsg() {
        Logging.info("shutdown DPE");
    }

    @Override
    void initialize() throws ClaraException {
        if (proxy == null) {
            try {
                startProxyAndFrontEnd();
                startConnectionPool();
                startSubscription();
                startHeartBeatReport();
            } catch (ClaraException e) {
                stop();
                throw e;
            }
        }
    }

    @Override
    void end() {
        isShutDown.set(true);
        if (proxy != null) {
            stopHeartBeatReport();
            stopSubscription();
            stopContainers();
            stopConnectionPool();
            stopProxyAndFrontEnd();
        }
    }

    private void startProxyAndFrontEnd() throws ClaraException {
        // start the proxy
        proxy = new Proxy(base.getMe());
        proxy.start();

        // start the front-end
        if (isFrontEnd.get()) {
            frontEnd = new FrontEnd(base.getMe());
            frontEnd.start();
        }
    }

    private void startConnectionPool() throws ClaraException {
        servicesConnectionPool = xMsgConnectionPool.newBuilder()
                .withProxy(base.getDefaultProxyAddress())
                .withPreConnectionSetup(s -> {
                    s.setRcvHWM(0);
                    s.setSndHWM(0);
                })
                .withPostConnectionSetup(() -> xMsgUtil.sleep(100))
                .build();
    }

    private void cacheConnections() throws ClaraException {
        base.cacheConnection();

        int createdConnections = IntStream.range(0, maxCores)
                .parallel()
                .map(i -> cacheLocalConnection())
                .reduce(0, Integer::sum);

        if (createdConnections < maxCores * 0.75) {
            Logging.error("could not cache enough connections to local proxy");
        }
    }

    private int cacheLocalConnection() {
        try (xMsgConnection con = servicesConnectionPool.getConnection()) {
            return 1;
        } catch (xMsgException e) {
            return 0;
        }
    }

    private void startSubscription() throws ClaraException {
        xMsgTopic topic = xMsgTopic.build(ClaraConstants.DPE, base.getName());
        xMsgCallBack callback = new DpeCallBack();
        String description = base.getDescription();
        subscriptionHandler = startRegisteredSubscription(topic, callback, description);
    }

    private void startHeartBeatReport() {
        reportService.start();
    }

    private void stopHeartBeatReport() {
        reportService.stop();
    }

    private void stopSubscription() {
        if (subscriptionHandler != null) {
            base.stopListening(subscriptionHandler);
            if (shouldDeregister()) {
                try {
                    base.removeRegistration(base.getMe().getTopic());
                } catch (ClaraException e) {
                    Logging.error("%s", e.getMessage());
                }
            }
            base.stopCallbacks();
        }
    }

    private void stopContainers() {
        myContainers.values().parallelStream().forEach(Container::stop);
        myContainers.clear();
    }

    private void stopConnectionPool() {
        servicesConnectionPool.close();
    }

    private void stopProxyAndFrontEnd() {
        proxy.stop();
        proxy = null;

        if (frontEnd != null) {
            frontEnd.stop();
            frontEnd = null;
        }
    }

    private void printLogo() {
        System.out.println("=========================================");
        if (isFrontEnd.get()) {
            System.out.println("              CLARA FE/DPE               ");
        } else {
            System.out.println("                CLARA DPE                ");
        }
        System.out.println("=========================================");
        System.out.println(" Name             = " + base.getName());
        if (!session.isEmpty()) {
            System.out.println(" Session          = " + session);
        }
        System.out.println(" Date             = " + ClaraUtil.getCurrentTime());
        System.out.println(" Version          = 4.3");
        System.out.println(" Lang             = Java");
        System.out.println(" Pool size        = " + base.getPoolSize());
        if (!base.getDescription().isEmpty()) {
            System.out.println(" Description      = " + base.getDescription());
        }
        System.out.println();
        System.out.println(" Proxy Host       = " + base.getMe().getDpeHost());
        System.out.println(" Proxy Port       = " + base.getMe().getDpePort());
        if (!isFrontEnd.get()) {
            System.out.println();
            System.out.println(" FrontEnd Host    = " + base.getFrontEnd().getDpeHost());
            System.out.println(" FrontEnd Port    = " + base.getFrontEnd().getDpePort());
            System.out.println(" FrontEnd Lang    = " + base.getFrontEnd().getDpeLang());
        }
        System.out.println("=========================================");
    }


    private void startContainer(RequestParser parser)
            throws RequestException, DpeException {

        String containerName = parser.nextString();
        int poolSize = parser.nextInteger();
        String description = parser.nextString();
        if (poolSize <= 0) {
            poolSize = base.getPoolSize();
        }

        ClaraComponent contComp = ClaraComponent.container(
                base.getMe().getDpeHost(),
                base.getMe().getDpePort(),
                ClaraConstants.JAVA_LANG,
                containerName,
                poolSize,
                description);

        Container container = myContainers.get(containerName);
        if (container == null) {
            container = new Container(contComp, base.getFrontEnd());
            Container result = myContainers.putIfAbsent(containerName, container);
            if (result == null) {
                try {
                    container.start();
                    reportService.addContainer(container);
                } catch (ClaraException e) {
                    container.stop();
                    myContainers.remove(containerName, container);
                    throw new DpeException("could not start container = " + contComp, e);
                }
            }
        } else {
            Logging.error("container = %s already exists. No new container is created", contComp);
        }
    }


    private void startService(RequestParser parser)
            throws RequestException, DpeException {
        String containerName = parser.nextString();
        String engineName = parser.nextString();
        String engineClass = parser.nextString();
        int poolSize = parser.nextInteger();
        String description = parser.nextString();
        String initialState = parser.nextString();
        if (poolSize <= 0) {
            poolSize = 1;
        } else if (poolSize > maxCores) {
            poolSize = maxCores;
        }
        ClaraComponent serComp = ClaraComponent.service(base.getMe().getDpeHost(),
                                                        base.getMe().getDpePort(),
                                                        ClaraConstants.JAVA_LANG,
                                                        containerName,
                                                        engineName,
                                                        engineClass,
                                                        poolSize,
                                                        description,
                                                        initialState);

        Container container = myContainers.get(containerName);
        if (container == null) {
            throw new RequestException("could not start service = " + serComp +
                                       ": missing container");
        }
        try {
            container.addService(serComp, base.getFrontEnd(), servicesConnectionPool);
        } catch (ClaraException e) {
            throw new DpeException("could not start service " + serComp, e);
        }
    }

    private void stopService(RequestParser parser)
            throws RequestException, DpeException {
        String containerName = parser.nextString();
        String engineName = parser.nextString();
        String serviceName = MessageUtil.buildTopic(base.getName(), containerName, engineName)
                                        .toString();

        Container container = myContainers.get(containerName);
        if (container == null) {
            throw new RequestException("could not stop service = " + serviceName +
                                       ": missing container");
        }
        boolean removed = container.removeService(serviceName);
        if (!removed) {
            throw new RequestException("could not stop service = " + serviceName +
                                       ": service doesn't exist");
        }
    }

    private void stopContainer(RequestParser parser)
            throws RequestException, DpeException {
        String containerName = parser.nextString();
        Container container = myContainers.remove(containerName);
        if (container == null) {
            String canonName = base.getName() + ":" + containerName;
            throw new RequestException("could not stop container = " + canonName +
                                       ": container doesn't exist");
        }
        container.stop();
        reportService.removeContainer(container);
    }


    private void setSession(RequestParser parser) throws RequestException {
        session = parser.nextString("");
    }


    private void setFrontEnd(RequestParser parser) throws RequestException {
        String frontEndHost = parser.nextString();
        int frontEndPort = parser.nextInteger();
        String frontEndLang = parser.nextString();

        ClaraComponent frontEnd = ClaraComponent.dpe(frontEndHost, frontEndPort, frontEndLang,
                                                     1, ClaraConstants.UNDEFINED);
        base.setFrontEnd(frontEnd);
        for (Container cont : myContainers.values()) {
            cont.setFrontEnd(frontEnd);
            for (Service ser : cont.geServices().values()) {
                ser.setFrontEnd(frontEnd);
            }
        }
    }


    private String reportRuntime(RequestParser parser) {
        return reportService.jsonReport();
    }


    /**
     * Periodically publishes reports to the front-end.
     */
    private class ReportService {

        private final DpeReport myReport;
        private final JsonReportBuilder myReportBuilder = new JsonReportBuilder();
//        private final JinFluxReportBuilder myFluxReportBuilder;

        private final ScheduledExecutorService scheduledPingService;
        private final AtomicBoolean isReporting = new AtomicBoolean();
        private final long reportPeriod;
        private String session;

        ReportService(long periodMillis, String session) {
            myReport = new DpeReport(base, session);
            myReport.setPoolSize(base.getPoolSize());
//            myFluxReportBuilder = fluxReport("claraweb.jlab.org", "clara", session);
            scheduledPingService = Executors.newSingleThreadScheduledExecutor();
            reportPeriod = periodMillis;
            this.session = session;
        }

//        private JinFluxReportBuilder fluxReport(String node, String name, String session) {
//            try {
//                return new JinFluxReportBuilder(node, name, session);
//            } catch (JinFluxException e) {
//                e.printStackTrace();
//                return null;
//            }
//        }

        public void start() {
            isReporting.set(true);
            scheduledPingService.schedule(() -> run(), 5, TimeUnit.SECONDS);
        }

        public void stop() {
            isReporting.set(false);

            scheduledPingService.shutdown();
            try {
                if (!scheduledPingService.awaitTermination(100, TimeUnit.MILLISECONDS)) {
                    scheduledPingService.shutdownNow();
                    if (!scheduledPingService.awaitTermination(1, TimeUnit.SECONDS)) {
                        System.err.println("reporting thread did not terminate");
                    }
                }
            } catch (InterruptedException ie) {
                scheduledPingService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        public void addContainer(Container container) {
            myReport.addContainer(container.getReport());
        }

        public void removeContainer(Container container) {
            myReport.removeContainer(container.getReport());
        }

        public String aliveReport() {
            return myReport.getAliveData();
        }

        public String jsonReport() {
            return myReportBuilder.generateReport(myReport);
        }

//        public String jinFluxReport() {
//            return myFluxReportBuilder.generateReport(myReport);
//        }

        private xMsgMessage aliveMessage() {
            xMsgTopic topic = xMsgTopic.build(ClaraConstants.DPE_ALIVE, session, base.getName());
            return MessageUtil.buildRequest(topic, aliveReport());
        }

        private xMsgMessage jsonMessage() {
            xMsgTopic topic = xMsgTopic.build(ClaraConstants.DPE_REPORT, session, base.getName());
            return MessageUtil.buildRequest(topic, jsonReport());
        }

        private void run() {
            try {
                xMsgProxyAddress feHost = base.getFrontEnd().getProxyAddress();
                xMsgConnection con = base.getConnection(feHost);
                xMsgUtil.sleep(100);
                try {
                    while (isReporting.get()) {
                        base.send(con, aliveMessage());
                        base.send(con, jsonMessage());
                        // @todo report to influxDB database 11.18.16.
                        // THis is temporary solution and must be removed.
//                        jinFluxReport();
                        xMsgUtil.sleep(reportPeriod);
                    }
                } catch (xMsgException e) {
                    System.err.println("Could not publish DPE report:" + e.getMessage());
                } finally {
                    con.close();
                }
            } catch (xMsgException e) {
                System.err.println("Could not start DPE reporting thread:");
                e.printStackTrace();
            } catch (Exception e) {
                System.err.println("Error running DPE reporting thread:");
                e.printStackTrace();
            }
        }
    }


    /**
     * A problem that occurs processing a valid request.
     */
    private static class DpeException extends Exception {

        DpeException(String msg, Throwable cause) {
            super(msg, cause);
        }

        @Override
        public String getMessage() {
            StringBuilder sb = new StringBuilder();
            sb.append(super.getMessage());
            for (Throwable e: ClaraUtil.getThrowableList(getCause())) {
                sb.append(": ").append(e.getMessage());
            }
            return sb.toString();
        }
    }


    /**
     * DPE callback.
     * <p>
     * The topic of this subscription is:
     * <code>CConstants.DPE + ":" + dpeCanonicalName</code>
     * <p>
     * The following are accepted message data:
     * <li>
     *     CConstants.STOP_DPE
     * </li>
     * <li>
     *     CConstants.SET_FRONT_END ?
     *     frontEndHost ? frontEndPort ? frontEndLang
     * </li>
     * <li>
     *     CConstants.PING_DPE
     * </li>
     * <li>
     *     CConstants.START_CONTAINER
     *     ? containerName ? poolSize ? description
     * </li>
     * <li>
     *     CConstants.STOP_CONTAINER ?
     *     containerName
     * </li>
     * <li>
     *     CConstants.START_SERVICE ?
     *     containerName ? engineName ? engineClass ? poolSize ? description ? initialState
     * </li>
     * <li>
     *     CConstants.STOP_SERVICE ?
     *     containerName ? engineName
     * </li>
     */
    private class DpeCallBack implements xMsgCallBack {

        @Override
        public void callback(xMsgMessage msg) {
            try {
                RequestParser parser = RequestParser.build(msg);
                String cmd = parser.nextString();
                String response = parser.request();

                switch (cmd) {

                    case ClaraConstants.STOP_DPE:
                        new Thread(() -> stop()).start();
                        break;

                    case ClaraConstants.SET_FRONT_END:
                        setFrontEnd(parser);
                        break;

                    case ClaraConstants.SET_SESSION:
                        setSession(parser);
                        break;

                    case ClaraConstants.PING_DPE:
                        response = reportService.aliveReport();
                        break;

                    case ClaraConstants.START_CONTAINER:
                        startContainer(parser);
                        break;

                    case ClaraConstants.STOP_CONTAINER:
                        stopContainer(parser);
                        break;

                    case ClaraConstants.START_SERVICE:
                        startService(parser);
                        break;

                    case ClaraConstants.STOP_SERVICE:
                        stopService(parser);
                        break;

                    case ClaraConstants.REPORT_RUNTIME:
                        response = reportRuntime(parser);
                        break;

                    default:
                        break;
                }

                if (msg.hasReplyTopic()) {
                    sendResponse(msg, xMsgMeta.Status.INFO, response);
                }

            } catch (RequestException | DpeException e) {
                Logging.error("%s", e.getMessage());
                if (msg.hasReplyTopic()) {
                    sendResponse(msg, xMsgMeta.Status.ERROR, e.getMessage());
                }
            }
        }
    }
}
