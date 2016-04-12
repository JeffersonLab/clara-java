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
import org.jlab.clara.sys.DpeOptionsParser.DpeOptionsException;
import org.jlab.clara.sys.RequestParser.RequestException;
import org.jlab.clara.util.report.DpeReport;
import org.jlab.clara.util.report.JsonReportBuilder;
import org.jlab.clara.util.report.SystemStats;
import org.jlab.coda.xmsg.core.xMsgCallBack;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgSubscription;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnection;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
public final class Dpe extends ClaraBase {

    static final int DEFAULT_PROXY_PORT = xMsgConstants.DEFAULT_PORT;
    static final int DEFAULT_POOL_SIZE = 2;
    static final long DEFAULT_REPORT_WAIT = 10_000;

    private final boolean isFrontEnd;

    private Proxy proxy = null;
    private FrontEnd frontEnd = null;
    private xMsgSubscription subscriptionHandler;

    // The containers running on this DPE
    private final Map<String, Container> myContainers = new HashMap<>();

    private final ReportService reportService;


    public static void main(String[] args) {
        DpeOptionsParser options = new DpeOptionsParser();
        try {
            options.parse(args);
            if (options.hasHelp()) {
                System.out.println(options.usage());
                System.exit(0);
            }

            // start a dpe
            Dpe dpe = new Dpe(options.isFrontEnd(), options.localAddress(), options.frontEnd(),
                              options.poolSize(), options.reportInterval(), options.description());

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    dpe.close();
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

        int poolSize = DEFAULT_POOL_SIZE;
        long reportInterval = DEFAULT_REPORT_WAIT;
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
         * Sets the interval of time between publishing reports.
         */
        public Builder withReportInterval(long interval, TimeUnit unit) {
            if (interval <= 0) {
                throw new IllegalArgumentException("Invalid report interval: " + interval);
            }
            this.reportInterval = unit.toMillis(interval);
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
         * Sets a description for this DPE.
         */
        public Builder withDescription(String description) {
            Objects.requireNonNull(description, "description parameter is null");
            this.description = description;
            return this;
        }

        /**
         * Creates the DPE.
         *
         * @throws ClaraException if the DPE could not be created
         */
        public Dpe build() throws ClaraException {
            return new Dpe(isFrontEnd, localAddress, frontEndAddress,
                           poolSize, reportInterval, description);
        }
    }


    /**
     * Constructor of a DPE.
     *
     * @param isFrontEnd true if this DPE is the front-end
     * @param proxyAddress address of local proxy
     * @param frontEndAddress address of front-end proxy
     * @param poolSize subscription pool size
     * @param reportInterval the time between publishing the reports
     * @param description textual description of the DPE
     * @throws ClaraException
     */
    private Dpe(boolean isFrontEnd,
                xMsgProxyAddress proxyAddress,
                xMsgProxyAddress frontEndAddress,
                int poolSize,
                long reportInterval,
                String description) {

        super(ClaraComponent.dpe(proxyAddress.host(),
                                 proxyAddress.pubPort(),
                                 ClaraConstants.JAVA_LANG,
                                 poolSize,
                                 description),
              ClaraComponent.dpe(frontEndAddress.host(),
                      frontEndAddress.pubPort(),
                      ClaraConstants.JAVA_LANG,
                      1, "Front End"));

        this.isFrontEnd = isFrontEnd;
        this.reportService = new ReportService(reportInterval);
    }

    /**
     * Starts this DPE.
     * <p>
     * Starts a local xMsg proxy server, and a local xMsg registrar service
     * in case it is a front-end. Does proper subscriptions to receive requests
     * and starts heart beat reporting thread.
     *
     * @throws ClaraException if the DPE could not be started
     */
    @Override
    public void start() throws ClaraException {
        startProxyAndFrontEnd();
        cacheConnection();
        startSubscription();
        startHeartBeatReport();
        printLogo();
    }

    @Override
    protected void end() {
        stopHeartBeatReport();
        stopSubscription();
        stopContainers();
        stopProxyAndFrontEnd();
    }

    private void startProxyAndFrontEnd() throws ClaraException {
        // start the proxy
        proxy = new Proxy(getMe().getProxyAddress());
        proxy.start();

        // start the front-end
        if (isFrontEnd) {
            frontEnd = new FrontEnd(getFrontEnd().getProxyAddress(),
                                    getPoolSize(),
                                    getMe().getDescription());
            frontEnd.start();
        }
    }

    private void startSubscription() throws ClaraException {
        xMsgTopic topic = xMsgTopic.build(ClaraConstants.DPE, getMe().getCanonicalName());
        subscriptionHandler = startRegisteredSubscription(topic,
                                                          new DpeCallBack(),
                                                          getMe().getDescription());
    }

    private void startHeartBeatReport() {
        reportService.start();
    }

    private void stopHeartBeatReport() {
        reportService.stop();
    }

    private void stopSubscription() {
        if (subscriptionHandler != null) {
            stopListening(subscriptionHandler);
            if (!isFrontEnd) {
                try {
                    removeRegistration(getMe().getTopic());
                } catch (ClaraException e) {
                    System.err.printf("%s: %s%n", ClaraUtil.getCurrentTimeInH(), e.getMessage());
                }
            }
        }
    }

    private void stopContainers() {
        for (Container cont : myContainers.values()) {
            cont.close();
        }
        myContainers.clear();
    }

    private void stopProxyAndFrontEnd() {
        if (proxy != null) {
            proxy.stop();
        }
        if (frontEnd != null) {
            frontEnd.stop();
        }
    }

    private void printLogo() {
        System.out.println("=========================================");
        System.out.println("                 CLARA DPE               ");
        System.out.println("=========================================");
        System.out.println(" Name             = " + getMe().getCanonicalName());
        System.out.println(" Date             = " + ClaraUtil.getCurrentTimeInH());
        System.out.println(" Version          = 4.3");
        System.out.println(" Lang             = Java");
        System.out.println(" Pool size        = " + getPoolSize());
        if (!getMe().getDescription().isEmpty()) {
            System.out.println(" Description      = " + getMe().getDescription());
        }
        System.out.println();
        System.out.println(" Proxy Host       = " + getDefaultProxyAddress().host());
        System.out.println(" Proxy Port       = " + getDefaultProxyAddress().pubPort());
        System.out.println();
        System.out.println(" FrontEnd Host    = " + getFrontEnd().getDpeHost());
        System.out.println(" FrontEnd Port    = " + getFrontEnd().getDpePort());
        System.out.println(" FrontEnd Lang    = " + getFrontEnd().getDpeLang());
        System.out.println("=========================================");
    }


    private void startContainer(RequestParser parser)
            throws RequestException, DpeException {

        String containerName = parser.nextString();
        int poolSize = parser.nextInteger();
        String description = parser.nextString();
        if (poolSize <= 0) {
            poolSize = getMe().getSubscriptionPoolSize();
        }

        ClaraComponent contComp = ClaraComponent.container(
                getMe().getDpeHost(),
                getMe().getDpePort(),
                ClaraConstants.JAVA_LANG,
                containerName,
                poolSize,
                description);

        Container container = myContainers.get(containerName);
        if (container == null) {
            container = new Container(contComp, getFrontEnd());
            Container result = myContainers.putIfAbsent(containerName, container);
            if (result == null) {
                try {
                    container.start();
                    reportService.addContainer(container);
                } catch (ClaraException e) {
                    container.close();
                    myContainers.remove(containerName, container);
                    throw new DpeException("could not start container = " + contComp, e);
                }
            }
        } else {
            String msg = "%s: container = %s already exists. No new container is created%n";
            System.err.printf(msg, ClaraUtil.getCurrentTimeInH(), contComp.getCanonicalName());
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
            poolSize = getMe().getSubscriptionPoolSize();
        }
        ClaraComponent serComp = ClaraComponent.service(getMe().getDpeHost(),
                                                        getMe().getDpePort(),
                                                        getMe().getDpeLang(),
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
            container.addService(serComp, getFrontEnd());
        } catch (ClaraException e) {
            throw new DpeException("could not start service " + serComp, e);
        }
    }

    private void stopService(RequestParser parser)
            throws RequestException, DpeException {
        String containerName = parser.nextString();
        String engineName = parser.nextString();
        String serviceName = MessageUtils.buildTopic(getMe().getCanonicalName(),
                                                     containerName,
                                                     engineName).toString();

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
            String canonName = getMe().getCanonicalName() + ":" + containerName;
            throw new RequestException("could not stop container = " + canonName +
                                       ": container doesn't exist");
        }
        container.close();
    }

    private void setFrontEnd(RequestParser parser) throws RequestException {
        String frontEndHost = parser.nextString();
        int frontEndPort = parser.nextInteger();
        String frontEndLang = parser.nextString();

        ClaraComponent frontEnd = ClaraComponent.dpe(frontEndHost, frontEndPort, frontEndLang,
                                                     1, ClaraConstants.UNDEFINED);
        setFrontEnd(frontEnd);
        for (Container cont : myContainers.values()) {
            cont.setFrontEnd(frontEnd);
            for (Service ser : cont.geServices().values()) {
                ser.setFrontEnd(frontEnd);
            }
        }
    }


    /**
     * Periodically publishes reports to the front-end.
     */
    private class ReportService {

        private final String aliveData;

        private final DpeReport myReport;
        private final JsonReportBuilder myReportBuilder = new JsonReportBuilder();

        private final AtomicBoolean isReporting = new AtomicBoolean();
        private final long reportWait;

        ReportService(long reportInterval) {
            int availableProcessors = Runtime.getRuntime().availableProcessors();
            String claraHome = System.getenv("CLARA_HOME");
            String dpeName = getMe().getCanonicalName();

            aliveData = dpeName + "?" + availableProcessors + "?" + claraHome;

            myReport = new DpeReport(dpeName);
            myReport.setHost(getMe().getCanonicalName());
            myReport.setLang(getMe().getDpeLang());
            myReport.setDescription(getMe().getDescription());
            myReport.setAuthor(System.getenv("USER"));

            reportWait = reportInterval;
        }

        public void start() {
            myReport.setStartTime(ClaraUtil.getCurrentTime());
            myReport.setMemorySize(Runtime.getRuntime().maxMemory());
            myReport.setCoreCount(Runtime.getRuntime().availableProcessors());

            isReporting.set(true);

            ScheduledExecutorService scheduledPingService = Executors.newScheduledThreadPool(3);
            scheduledPingService.schedule(() -> run(), 5, TimeUnit.SECONDS);
        }

        public void stop() {
            isReporting.set(false);
        }

        public void addContainer(Container container) {
            myReport.addContainerReport(container.getReport());
        }

        public String aliveReport() {
            return aliveData;
        }

        public String jsonReport() throws ClaraException {
            myReport.setMemoryUsage(SystemStats.getMemoryUsage());
            myReport.setCpuUsage(SystemStats.getCpuUsage());

            return myReportBuilder.generateReport(myReport);
        }

        private void run() {
            try {
                xMsgProxyAddress feHost = getFrontEnd().getProxyAddress();
                xMsgTopic jsonTopic = xMsgTopic.build(ClaraConstants.DPE_REPORT, feHost.host());
                xMsgTopic aliveTopic = xMsgTopic.build(ClaraConstants.DPE_ALIVE, feHost.host());

                xMsgConnection con = createConnection(feHost);
                xMsgUtil.sleep(100);

                try {
                    while (isReporting.get()) {
                        xMsgMessage msg = MessageUtils.buildRequest(aliveTopic, aliveData);
                        send(con, msg);

                        xMsgMessage reportMsg = MessageUtils.buildRequest(jsonTopic, jsonReport());
                        send(con, reportMsg);

                        xMsgUtil.sleep(reportWait);
                    }
                } finally {
                    destroyConnection(con);
                }
            } catch (xMsgException | ClaraException e) {
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
            if (getCause() != null) {
                sb.append(": ").append(getCause().getMessage());
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
                        end();
                        break;

                    case ClaraConstants.SET_FRONT_END:
                        setFrontEnd(parser);
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

                    default:
                        break;
                }

                if (msg.getMetaData().hasReplyTo()) {
                    sendResponse(msg, xMsgMeta.Status.INFO, response);
                }

            } catch (RequestException | DpeException e) {
                System.err.printf("%s: %s%n", ClaraUtil.getCurrentTimeInH(), e.getMessage());
                if (msg.getMetaData().hasReplyTo()) {
                    sendResponse(msg, xMsgMeta.Status.ERROR, e.getMessage());
                }
            }
        }

        private void sendResponse(xMsgMessage msg, xMsgMeta.Status status, String data) {
            try {
                xMsgTopic topic = xMsgTopic.wrap(msg.getMetaData().getReplyTo());
                xMsgMessage repMsg = MessageUtils.buildRequest(topic, data);
                repMsg.getMetaData().setStatus(status);
                send(repMsg);
            } catch (xMsgException e) {
                e.printStackTrace();
            }
        }
    }
}
