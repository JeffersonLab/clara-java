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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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

    // The containers running on this DPE
    private final Map<String, Container> myContainers = new HashMap<>();

    private xMsgSubscription subscriptionHandler;

    private final DpeReport myReport;
    private final JsonReportBuilder myReportBuilder = new JsonReportBuilder();

    private final AtomicBoolean isReporting = new AtomicBoolean();
    private final long reportWait;


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
                String description)
            throws ClaraException {

        super(ClaraComponent.dpe(proxyAddress.host(),
                                 proxyAddress.port(),
                                 ClaraConstants.JAVA_LANG,
                                 poolSize,
                                 description),
              ClaraComponent.dpe(frontEndAddress.host(),
                      frontEndAddress.port(),
                      ClaraConstants.JAVA_LANG,
                      1, "Front End"));

        this.isFrontEnd = isFrontEnd;

        myReport = new DpeReport(getMe().getCanonicalName());
        myReport.setHost(getMe().getCanonicalName());
        myReport.setLang(getMe().getDpeLang());
        myReport.setDescription(description);
        myReport.setAuthor(System.getenv("USER"));

        reportWait = reportInterval;
    }

    /**
     * Starts this DPE.
     * <p>
     * Starts a local xMsg proxy server, and a local xMsg registrar service
     * in case it is a front-end. Does proper subscriptions to receive requests
     * and starts heart beat reporting thread.
     */
    public void start() throws ClaraException {
        try {
            // start the proxy
            Proxy proxy = new Proxy(getMe().getProxyAddress());
            proxy.start();

            // start the front-end
            if (isFrontEnd) {
                FrontEnd fe = new FrontEnd(getFrontEnd().getProxyAddress(),
                                           getPoolSize(),
                                           getMe().getDescription());
                fe.start();
            }

            // Create a socket connections to the local dpe proxy
            releaseConnection(getConnection());

            // Subscribe and register
            xMsgTopic topic = xMsgTopic.build(ClaraConstants.DPE, getMe().getCanonicalName());
            subscriptionHandler = listen(topic, new DpeCallBack());
            register(topic, getMe().getDescription());
        } catch (xMsgException e) {
            throw new ClaraException("Could not start DPE", e);
        }

        myReport.setStartTime(ClaraUtil.getCurrentTime());
        myReport.setMemorySize(Runtime.getRuntime().maxMemory());
        myReport.setCoreCount(Runtime.getRuntime().availableProcessors());

        isReporting.set(true);

        startHeartBeatReport();

        printLogo();
    }

    @Override
    public void start(ClaraComponent component) {
        // nothing
    }

    @Override
    public void end() {
        try {
            for (Container cont : myContainers.values()) {
                cont.end();
            }
            stopListening(subscriptionHandler);

            isReporting.set(false);
            removeRegistration(getMe().getTopic());
        } catch (xMsgException | ClaraException e) {
            e.printStackTrace();
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
        System.out.println(" Proxy Port       = " + getDefaultProxyAddress().port());
        System.out.println();
        System.out.println(" FrontEnd Host    = " + getFrontEnd().getDpeHost());
        System.out.println(" FrontEnd Port    = " + getFrontEnd().getDpePort());
        System.out.println(" FrontEnd Lang    = " + getFrontEnd().getDpeLang());
        System.out.println("=========================================");
    }

    private void startHeartBeatReport() {
        ScheduledExecutorService scheduledPingService = Executors.newScheduledThreadPool(3);
        scheduledPingService.schedule(() -> report(), 5, TimeUnit.SECONDS);
    }

    private void report() {
        try {
            xMsgProxyAddress feHost = getFrontEnd().getProxyAddress();

            xMsgTopic reportTopic = xMsgTopic.build(ClaraConstants.DPE_REPORT, feHost.host());
            xMsgTopic aliveTopic = xMsgTopic.build(ClaraConstants.DPE_ALIVE, feHost.host());

            xMsgConnection con = createConnection(feHost);
            xMsgUtil.sleep(100);

            int availableProcessors = Runtime.getRuntime().availableProcessors();
            String claraHome = System.getenv("CLARA_HOME");
            String dpeName = getMe().getCanonicalName();
            String data = dpeName + "?" + availableProcessors + "?" + claraHome;

            while (isReporting.get()) {

                xMsgMessage msg = MessageUtils.buildRequest(aliveTopic, data);
                send(con, msg);

                myReport.setMemoryUsage(SystemStats.getMemoryUsage());
                myReport.setCpuUsage(SystemStats.getCpuUsage());

                String jsonData = myReportBuilder.generateReport(myReport);

                xMsgMessage reportMsg = MessageUtils.buildRequest(reportTopic, jsonData);
                send(con, reportMsg);

                xMsgUtil.sleep((int) reportWait);
            }

            destroyConnection(con);
        } catch (xMsgException | ClaraException e) {
            e.printStackTrace();
        }

    }

    private void startContainer(RequestParser parser)
            throws RequestException, ClaraException {

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

        if (myContainers.containsKey(containerName)) {
            String msg = "%s: Container %s already exists. No new container is created%n";
            System.err.printf(msg, ClaraUtil.getCurrentTimeInH(), contComp.getCanonicalName());
            return;
        }

        try {
            Container container = new Container(contComp, getFrontEnd());
            myContainers.put(containerName, container);
            myReport.addContainerReport(container.getReport());
        } catch (xMsgException | ClaraException e) {
            throw new ClaraException("Could not start container " + contComp, e);
        }
    }


    private void startService(RequestParser parser)
            throws RequestException, ClaraException {
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
        if (myContainers.containsKey(containerName)) {
            try {
                myContainers.get(containerName).addService(serComp, getFrontEnd());
            } catch (xMsgException | IOException e) {
                throw new ClaraException("Could not start service " + serComp, e);
            }
        } else {
            throw new ClaraException("Could not start service " + serComp + " (missing container)");
        }
    }

    private void stopService(RequestParser parser)
            throws RequestException, ClaraException {
        String containerName = parser.nextString();
        String engineName = parser.nextString();
        String serviceName = MessageUtils.buildTopic(getMe().getCanonicalName(),
                                                     containerName,
                                                     engineName).toString();
        if (myContainers.containsKey(containerName)) {
            try {
                myContainers.get(containerName).removeService(serviceName);
            } catch (xMsgException | IOException e) {
                throw new ClaraException("Could not start service " + serviceName, e);
            }
        } else {
            throw new ClaraException("Could not stop service " + serviceName + " (missing container)");
        }

    }

    private void stopContainer(RequestParser parser)
            throws RequestException, ClaraException {
        String containerName = parser.nextString();
        if (myContainers.containsKey(containerName)) {
            System.out.println("Removing container " + containerName);
            myContainers.get(containerName).end();
        } else {
            System.out.println("Clara-Warning: wrong address. Container = " + containerName);
        }
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
                switch (cmd) {

                    case ClaraConstants.STOP_DPE:
                        end();
                        break;

                    case ClaraConstants.SET_FRONT_END:
                        setFrontEnd(parser);
                        break;

                    case ClaraConstants.PING_DPE:
                        report();
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
                    sendResponse(msg, xMsgMeta.Status.INFO, parser.request());
                }

            } catch (RequestException | ClaraException e) {
                e.printStackTrace();
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
            } catch (IOException | xMsgException e) {
                e.printStackTrace();
            }
        }
    }
}
