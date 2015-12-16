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

import org.jlab.clara.base.ClaraBase;
import org.jlab.clara.base.ClaraComponent;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.sys.DpeOptionsParser.DpeOptionsException;
import org.jlab.clara.sys.RequestParser.RequestException;
import org.jlab.clara.util.CConstants;
import org.jlab.clara.util.ClaraUtil;
import org.jlab.clara.util.report.DpeReport;
import org.jlab.clara.util.report.JsonReportBuilder;
import org.jlab.coda.xmsg.core.xMsgCallBack;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgSubscription;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.jlab.coda.xmsg.xsys.xMsgProxy;
import org.zeromq.ZContext;

import javax.management.InstanceNotFoundException;
import javax.management.MalformedObjectNameException;
import javax.management.ReflectionException;
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
public class Dpe extends ClaraBase {

    // The containers running on this DPE
    private Map<String, Container> myContainers = new HashMap<>();

    private xMsgSubscription subscriptionHandler;

    private DpeReport myReport;
    private JsonReportBuilder myReportBuilder = new JsonReportBuilder();

    private AtomicBoolean isReporting = new AtomicBoolean();
    private int reportWait;

    /**
     * Constructor starts a DPE component, that connects to the local xMsg proxy server.
     * Does proper subscriptions nad starts heart beat reporting thread.
     *
     * @param proxyAddress address of local proxy
     * @param frontEndAddress address of front-end proxy
     * @param poolSize subscription pool size
     * @param description textual description of the DPE
     * @throws xMsgException
     * @throws IOException
     * @throws ClaraException
     */
    public Dpe(xMsgProxyAddress proxyAddress,
               xMsgProxyAddress frontEndAddress,
               int poolSize,
               String description,
               int reportInterval)
            throws xMsgException, IOException, ClaraException {
        super(ClaraComponent.dpe(proxyAddress.host(),
                                 proxyAddress.port(),
                                 CConstants.JAVA_LANG,
                                 poolSize,
                                 description),
              frontEndAddress.host(), xMsgConstants.REGISTRAR_PORT);
        ClaraComponent frontEnd = ClaraComponent.dpe(frontEndAddress.host(),
                                                     frontEndAddress.port(),
                                                     CConstants.JAVA_LANG,
                                                     1, "Front End");
        setFrontEnd(frontEnd);

        // Create a socket connections to the local dpe proxy
        connect();

        // Subscribe messages published to this dpe
        xMsgTopic topic = xMsgTopic.wrap(CConstants.DPE + ":" + getMe().getCanonicalName());

        // Register this subscriber
        register(frontEndAddress.host(), description);
        System.out.println(ClaraUtil.getCurrentTimeInH() + ": Registered DPE = " + getMe().getCanonicalName());

        // Subscribe by passing a callback to the subscription
        subscriptionHandler = listen(topic, new DpeCallBack());
        System.out.println(ClaraUtil.getCurrentTimeInH() + ": Started DPE = " + getMe().getCanonicalName());

        printLogo();

        myReport = new DpeReport(getMe().getCanonicalName());
        myReport.setHost(getMe().getDpeHost());
        myReport.setLang(getMe().getDpeLang());
        myReport.setDescription(description);
        myReport.setAuthor(System.getenv("USER"));
        myReport.setStartTime(ClaraUtil.getCurrentTime());
        myReport.setMemorySize(Runtime.getRuntime().maxMemory());
        myReport.setCoreCount(Runtime.getRuntime().availableProcessors());

        isReporting.set(true);
        reportWait = reportInterval * 1000;

        startHeartBeatReport();
    }

    public static void main(String[] args) {
        DpeOptionsParser options = new DpeOptionsParser();
        try {
            options.parse(args);
            if (options.hasHelp()) {
                System.out.println(options.usage());
                System.exit(0);
            }

            // start the proxy
            startProxy();

            // start the front-end
            if (options.isFrontEnd()) {
                new FrontEnd(options.frontEnd(), options.poolSize(), options.description());
            }

            // start a dpe
            new Dpe(options.localAddress(), options.frontEnd(),
                    options.poolSize(), options.description(), options.reportInterval());

        } catch (DpeOptionsException e) {
            System.err.println(e.getMessage());
            System.err.println();
            System.err.println(options.usage());
            System.exit(1);
        } catch (xMsgException | IOException | ClaraException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void startProxy() {
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    xMsgProxy proxy = new xMsgProxy();
                    proxy.startProxy(new ZContext());
                } catch (xMsgException e) {
                    e.printStackTrace();
                }
            }
        });
        t.start();
    }

    @Override
    public void end() {
        try {
            for (Container cont : myContainers.values()) {
                cont.end();
            }
            stopListening(subscriptionHandler);

            isReporting.set(false);
            removeRegistration();
        } catch (IOException | xMsgException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void start(ClaraComponent component) {
        // nothing
    }

    /**
     *
     */
    private void printLogo() {
        System.out.println("=========================================");
        System.out.println("                 CLARA DPE               ");
        System.out.println("=========================================");
        System.out.println(" Name             = " + getMe().getCanonicalName());
        System.out.println(" Date             = " + ClaraUtil.getCurrentTimeInH());
        System.out.println(" Version          = 4.x");
        System.out.println(" Description      = " + getMe().getDescription());
        if (getFrontEnd() != null) {
            System.out.println(" FrontEnd Host    = " + getFrontEnd().getDpeHost());
            System.out.println(" FrontEnd Port    = " + getFrontEnd().getDpePort());
            System.out.println(" FrontEnd Lang    = " + getFrontEnd().getDpeLang());
        }
        System.out.println("=========================================");
    }

    /**
     *
     */
    private void startHeartBeatReport() {
        ScheduledExecutorService scheduledPingService = Executors.newScheduledThreadPool(3);
        scheduledPingService.schedule(new Runnable() {
            @Override
            public void run() {
                report();
            }
        }, 5, TimeUnit.SECONDS);
    }

    /**
     * Builds a report for the heart beat reporting thread.
     */
    private void report() {
        try {
            xMsgTopic dpeReportTopic = ClaraUtil.buildTopic(CConstants.DPE_ALIVE, getDefaultProxyAddress().host());
            if (getFrontEnd() != null) {
                dpeReportTopic = ClaraUtil.buildTopic(CConstants.DPE_ALIVE, getFrontEnd().getDpeCanonicalName());
            }

            while (isReporting.get()) {

                myReport.setMemoryUsage(ClaraUtil.getMemoryUsage());
                myReport.setCpuUsage(ClaraUtil.getCpuUsage());
                myReport.setSnapshotTime(ClaraUtil.getCurrentTime());

                String jsonData = myReportBuilder.generateReport(myReport);

                xMsgMessage msg = new xMsgMessage(dpeReportTopic, jsonData);
                if (getFrontEnd() == null || getFrontEnd().getDpeHost().equals(xMsgUtil.localhost())) {
                    send(msg);
                } else {
                    send(getFrontEnd(), msg);
                }
                xMsgUtil.sleep(reportWait);
            }
        } catch (IOException | xMsgException | MalformedObjectNameException |
                ReflectionException | InstanceNotFoundException e) {
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

        ClaraComponent contComp = ClaraComponent.container(getMe().getDpeHost(),
                getMe().getDpePort(), CConstants.JAVA_LANG, containerName, poolSize, description);

        if (myContainers.containsKey(contComp.getCanonicalName())) {
            String msg = "%s Warning: container %s already exists. No new container is created%n";
            System.err.printf(msg, ClaraUtil.getCurrentTimeInH(), contComp.getCanonicalName());
            return;
        }

        System.out.println("Starting container " + contComp.getCanonicalName());
        try {
            Container container = new Container(contComp,
                    getDefaultRegistrarAddress().host(),
                    getDefaultRegistrarAddress().port());
            myContainers.put(containerName, container);
            myReport.addContainerReport(container.getReport());
        } catch (xMsgException | IOException e) {
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
                myContainers.get(containerName).addService(serComp,
                                                           getDefaultRegistrarAddress().host(),
                                                           getDefaultProxyAddress().port());
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
        String serviceName = ClaraUtil.buildTopic(getMe().getCanonicalName(),
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
                                                     1, CConstants.UNDEFINED);
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
        public xMsgMessage callback(xMsgMessage msg) {
            try {
                RequestParser parser = RequestParser.build(msg);
                String cmd = parser.nextString();
                switch (cmd) {

                    case CConstants.STOP_DPE:
                        end();
                        break;

                    case CConstants.SET_FRONT_END:
                        setFrontEnd(parser);
                        break;

                    case CConstants.PING_DPE:
                        report();
                        break;

                    case CConstants.START_CONTAINER:
                        startContainer(parser);
                        break;

                    case CConstants.STOP_CONTAINER:
                        stopContainer(parser);
                        break;

                    case CConstants.START_SERVICE:
                        startService(parser);
                        break;

                    case CConstants.STOP_SERVICE:
                        stopService(parser);
                        break;

                    default:
                        break;
                }
            } catch (RequestException e) {
                e.printStackTrace();
            } catch (ClaraException e) {
                e.printStackTrace();
            }
            return msg;
        }
    }
}
