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
import org.jlab.clara.util.CConstants;
import org.jlab.clara.util.ClaraUtil;
import org.jlab.clara.util.report.DpeReport;
import org.jlab.clara.util.report.JsonReportBuilder;
import org.jlab.clara.util.shell.ClaraFork;
import org.jlab.clara.util.xml.RequestParser;
import org.jlab.coda.xmsg.core.*;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.xsys.xMsgProxy;
import org.jlab.coda.xmsg.xsys.xMsgRegistrar;
import org.zeromq.ZContext;

import javax.management.InstanceNotFoundException;
import javax.management.MalformedObjectNameException;
import javax.management.ReflectionException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
    private xMsgRegistrar registrar;
    private xMsgProxy proxy;

    private DpeReport myReport;
    private JsonReportBuilder myReportBuilder = new JsonReportBuilder();

    private AtomicBoolean isReporting = new AtomicBoolean();

    /**
     * Constructor starts a DPE component, that connects to the local xMsg proxy server.
     * Does proper subscriptions nad starts heart beat reporting thread.
     *
     * @param dpePort xMsg local proxy server port
     * @param subPoolSize subscription pool size
     * @param regHost registrar service host
     * @param regPort registrar service port
     * @param description textual description of the DPE
     * @param cloudHost front-end DPE host
     * @param cloudPort front-end DPE port
     * @throws xMsgException
     * @throws IOException
     * @throws ClaraException
     */
    public Dpe(int dpePort,
               int subPoolSize,
               String description,
               String feHost, int fePort)
            throws xMsgException, IOException, ClaraException {
        super(ClaraComponent.dpe(xMsgUtil.localhost(),
                        dpePort,
                        CConstants.JAVA_LANG,
                        subPoolSize, description),
                feHost, xMsgConstants.REGISTRAR_PORT);

        proxy = new xMsgProxy();
        startProxy();

        registrar = new xMsgRegistrar(new ZContext());
        registrar.start();

        // Create a socket connections to the local dpe proxy
        connect();

        // Subscribe messages published to this dpe
        xMsgTopic topic = xMsgTopic.wrap(CConstants.DPE + ":" + getMe().getCanonicalName());

        // Register this subscriber
        register(feHost, description);
        System.out.println(ClaraUtil.getCurrentTimeInH() + ": Registered DPE = " + getMe().getCanonicalName());

        // Subscribe by passing a callback to the subscription
        subscriptionHandler = listen(topic, new DpeCallBack());
        System.out.println(ClaraUtil.getCurrentTimeInH() + ": Started DPE = " + getMe().getCanonicalName());

        ClaraComponent frontEnd = ClaraComponent.dpe(feHost, fePort, CConstants.JAVA_LANG, 1, "Front End");
        setFrontEnd(frontEnd);

        printLogo();

        myReport = new DpeReport(getMe().getCanonicalName());
        myReport.setHost(getMe().getDpeHost());
        myReport.setLang(getMe().getDpeLang());
        myReport.setDescription(description);
        myReport.setAuthor(System.getenv("USER"));
        myReport.setStartTime(ClaraUtil.getCurrentTimeInH());
        myReport.setMemorySize(Runtime.getRuntime().maxMemory());
        myReport.setCoreCount(Runtime.getRuntime().availableProcessors());

        isReporting.set(true);

        startHeartBeatReport();
    }

    /**
     *
     */
    public static void usage() {
        System.err.println("Usage: j_dpe [ -cc | -fh <front_end> ]");
    }

    public static void main(String[] args) {
        try {
            int i = 0;
            int dpePort = xMsgConstants.DEFAULT_PORT;
            int poolSize = xMsgConstants.DEFAULT_POOL_SIZE;
            String feHost = xMsgUtil.localhost();
            int fePort = xMsgConstants.DEFAULT_PORT;
            String description = "Clara DPE";

            while (i < args.length) {
                switch (args[i++]) {

                    case "-DpePort":
                        if (i < args.length) {
                            dpePort = Integer.parseInt(args[i++]);
                        } else {
                            usage();
                            System.exit(1);
                        }
                        break;

                    case "-PoolSize":
                        if (i < args.length) {
                            poolSize = Integer.parseInt(args[i++]);
                        } else {
                            usage();
                            System.exit(1);
                        }
                        break;

                    case "-feHost":
                        if (i < args.length) {
                            feHost = args[i++];
                        } else {
                            usage();
                            System.exit(1);
                        }
                        break;
                    case "-fePort":
                        if (i < args.length) {
                            fePort = Integer.parseInt(args[i++]);
                        } else {
                            usage();
                            System.exit(1);
                        }
                        break;
                    case "-Description":
                        if (i < args.length) {
                            description = args[i++];
                        } else {
                            usage();
                            System.exit(1);
                        }
                        break;
                    default:
                        usage();
                        System.exit(1);
                }
            }
            // start a dpe
            new Dpe(dpePort, poolSize, description, feHost, fePort);
        } catch (IOException | ClaraException | xMsgException e) {
            System.err.println(e.getMessage());
        }
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
        try {
            if (component.isContainer()) {
                startContainer(component.getContainerName(), component.getSubscriptionPoolSize(), component.getDescription());
            } else if (component.isService()) {
                startService(component.getContainerName(), component.getEngineName(),
                        component.getEngineClass(), component.getSubscriptionPoolSize(),
                        component.getDescription(), component.getInitialState());
            }
        } catch (ClaraException | xMsgException | IOException e) {
            e.printStackTrace();
        }

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


    private void startProxy() {
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    proxy.startProxy(new ZContext());
                } catch (xMsgException e) {
                    e.printStackTrace();
                }
            }
        });
        t.start();
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
        }, 10, TimeUnit.SECONDS);
    }

    /**
     * Builds a report for the heart beat reporting thread
     */
    private void report() {
        try {

            xMsgTopic dpeReportTopic = xMsgTopic.wrap(CConstants.DPE_ALIVE + ":" + getDefaultProxyAddress().host());
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
            }
        } catch (IOException | xMsgException | MalformedObjectNameException |
                ReflectionException | InstanceNotFoundException e) {
            e.printStackTrace();
        }

    }

    /**
     * Deploys a container. Note that container name is NOT a canonical name
     *
     * @param containerName the name of the container (not canonical)
     * @param poolSize size of the subscription pool
     * @param description textual description of the container
     * @throws ClaraException
     * @throws xMsgException
     * @throws IOException
     */
    private void startContainer(String containerName, int poolSize, String description)
            throws ClaraException, xMsgException, IOException {

        if (poolSize <= 0) {
            poolSize = getMe().getSubscriptionPoolSize();
        }

        ClaraComponent contComp = ClaraComponent.container(getMe().getDpeHost(),
                getMe().getDpePort(), CConstants.JAVA_LANG, containerName, poolSize, description);

        if (myContainers.containsKey(contComp.getCanonicalName())) {
            String msg = "%s Warning: container %s already exists. No new container is created%n";
            System.err.printf(msg, ClaraUtil.getCurrentTimeInH(), contComp.getCanonicalName());
        } else {
            // start a container
            System.out.println("Starting container " + contComp.getCanonicalName());
            Container container = new Container(contComp,
                    getDefaultRegistrarAddress().host(),
                    getDefaultRegistrarAddress().port());
            myContainers.put(containerName, container);
            myReport.addContainerReport(container.getReport());
        }
    }

    private void startService(String containerName, String engineName, String engineClass,
                              int poolSize, String description, String initialState)
            throws xMsgException, ClaraException, IOException {
        if (myContainers.containsKey(containerName)) {
            if (poolSize <= 0) {
                poolSize = getMe().getSubscriptionPoolSize();
            }
            ClaraComponent serComp = ClaraComponent.service(getMe().getDpeHost(),
                    getMe().getDpePort(), getMe().getDpeLang(), containerName, engineName, engineClass, poolSize, description, initialState);
            myContainers.get(containerName).addService(serComp,
                    getDefaultRegistrarAddress().host(),
                    getDefaultProxyAddress().port());
        } else {
            System.out.println("Clara-Error: container does not exists");
        }

    }

    private void stopService(String containerName, String engineName)
            throws ClaraException, IOException, xMsgException {
        if (myContainers.containsKey(containerName)) {
            String serviceCanonicalName =
                    ClaraUtil.buildTopic(getMe().getCanonicalName(), containerName, engineName).toString();
            myContainers.get(containerName).removeService(serviceCanonicalName);
        } else {
            System.out.println("Clara-Error: container does not exists");
        }

    }

    private void stopContainer(String containerName)
            throws ClaraException, xMsgException, IOException, TimeoutException {
        if (myContainers.containsKey(containerName)) {
            System.out.println("Removing container " + containerName);
            myContainers.get(containerName).end();
        } else {
            System.out.println("Clara-Warning: wrong address. Container = " + containerName);
        }
    }

    /**
     * DPE callback.
     * <p>
     * The topic of this subscription is:
     * topic = CConstants.DPE + ":" + dpeCanonicalName
     * <p>
     *     The following are accepted message data:
     * <li>
     *     Start dpe:
     *     <p>
     *     data = CConstants.START_DPE ? dpeHost ? dpePort ? dpeLang ? poolSize ? regHost ? regPort ? description
     * </li>
     * <li>
     *     Stop dpe:
     *     <p>
     *     Local:
     *     data = CConstants.STOP_DPE
     *     Remote:
     *     data = CConstants.STOP_REMOTE_DPE ? dpeHost ? dpePort ? dpeLang
     * </li>
     * <li>
     *     Set front end:
     *     <p>
     *     Local:
     *     data = CConstants.SET_FRONT_END ? frontEndHost ? frontEndPort ? frontEndLang
     *     Remote:
     *     data = CConstants.SET_FRONT_END_REMOTE ? dpeHost ? dpePort ? dpeLang ? frontEndHost ? frontEndPort ? frontEndLang
     * </li>
     * <li>
     *     Ping dpe:
     *     <p>
     *     Local:
     *     data = CConstants.PING_DPE
     *     Remote:
     *     data = CConstants.PING_REMOTE_DPE ? dpeHost ? dpePort ? dpeLang
     * </li>
     * <li>
     *     Start container:
     *     <p>
     *     Local:
     *     data = CConstants.START_CONTAINER ? containerName ? poolSize ? description
     *     remote:
     *     data = CConstants.START_REMOTE_CONTAINER ? dpeHost ? dpePort ? dpeLang ? containerName ? poolSize ? description
     * </li>
     * <li>
     *     Stop container:
     *     <p>
     *     Local:
     *     data = CConstants.STOP_CONTAINER ? containerName
     *     Remote:
     *     data = CConstants.STOP_REMOTE_CONTAINER ? dpeHost ? dpePort ? dpeLang ? containerName
     * </li>
     * <li>
     *     Start service:
     *     <p>
     *     Local:
     *     data = CConstants.START_SERVICE ? containerName ? engineName ? engineClass ? poolSize ? description ? initialState
     *     remote:
     *     data = CConstants.START_REMOTE_SERVICE ? dpeHost ? dpePort ? dpeLang ? containerName ? engineName ? engineClass ? poolSize ? description ? initialState
     * </li>
     * <li>
     *     Stop service:
     *     <p>
     *     Local:
     *     data = CConstants.STOP_SERVICE ? containerName ? engineName
     *     remote:
     *     data = CConstants.STOP_REMOTE_SERVICE ? dpeHost ? dpePort ? dpeLang ? containerName ? engineName
     * </li>
     */
    private class DpeCallBack implements xMsgCallBack {

        @Override
        public xMsgMessage callback(xMsgMessage msg) {
            xMsgMeta.Builder metadata = msg.getMetaData();
            try {
                String sender = metadata.getSender();
                String returnTopic = metadata.getReplyTo();

                RequestParser parser = RequestParser.build(msg);
                String cmd = parser.nextString();

                switch (cmd) {
                    // Sent from orchestrator.
                    case CConstants.START_DPE:
                        // This will start a remote DPE
                        // the string of the message has the following constructor:
                        // startDpe ? dpeHost ? dpePort ? dpeLang ? poolSize ? regHost ? regPort
                        String dpeHost, dpeLang, regHost, containerName, engineName, engineClass,
                                description, initialState;
                        String frontEndHost, frontEndLang;
                        int dpePort, poolSize, regPort, frontEndPort;
                        ClaraComponent dpe;
                        xMsgTopic topic;
                        String data;

                        try {
                            dpeHost = parser.nextString();
                            dpePort = parser.nextInteger();
                            dpeLang = parser.nextString();
                            poolSize = parser.nextInteger();
                            regHost = parser.nextString();
                            regPort = parser.nextInteger();
                            description = parser.nextString();
                        } catch (NoSuchElementException e) {
                            System.out.println("Clara-Warning: malformed startDpe request.");
                            break;
                        }
                        StringBuilder remCommand = new StringBuilder();
                        if (dpeLang.equals(CConstants.JAVA_LANG)) {
                            remCommand.append("ssh").append(" ").append(dpeHost).append(" ");
                            remCommand.append("-DpePort").append(" ").append(dpePort).append(" ");
                            remCommand.append("-PoolSize").append(" ").append(poolSize).append(" ");
                            remCommand.append("-RegHost").append(" ").append(regHost).append(" ");
                            remCommand.append("-RegPort").append(" ").append(regPort).append(" ");
                            remCommand.append("-Description").append(" ").append(description).append(" ");
                            remCommand.append("-CloudProxyHost").append(" ").append(getMe().getDpeHost()).append(" ");
                            remCommand.append("-CloudProxyPort").append(" ").append(getMe().getDpePort()).append(" ");
                        } else {
                            System.out.println("Clara-Warning: unsupported DPE language.");
                            break;
                        }

                        ClaraFork.fork(remCommand.toString(), false);
                        // sync request handling below
                        if (!returnTopic.equals(xMsgConstants.UNDEFINED)) {
                            xMsgMessage returnMsg = new xMsgMessage(xMsgTopic.wrap(returnTopic), null);
                            msg.getMetaData().setReplyTo(xMsgConstants.UNDEFINED);

                            // sends back "Done" string
                            returnMsg.updateData("Done");
                            send(returnMsg);
                        }
                        break;

                    case CConstants.STOP_DPE:
                        end();
                        break;

                    case CConstants.STOP_REMOTE_DPE:
                        try {
                            dpeHost = parser.nextString();
                            dpePort = parser.nextInteger();
                            dpeLang = parser.nextString();
                            dpe = ClaraComponent.dpe(dpeHost, dpePort, dpeLang, 1, CConstants.UNDEFINED);
                            exit(dpe);
                        } catch (NoSuchElementException e) {
                            System.out.println("Clara-Warning: malformed stopRemoteDpe request.");
                            break;
                        } catch (ClaraException e1) {
                            e1.printStackTrace();
                        }

                        break;

                    case CConstants.SET_FRONT_END:
                        frontEndHost = parser.nextString();
                        frontEndPort = parser.nextInteger();
                        frontEndLang = parser.nextString();

                        ClaraComponent frontEnd = ClaraComponent.dpe(frontEndHost, frontEndPort, frontEndLang, 1, CConstants.UNDEFINED);
                        setFrontEnd(frontEnd);
                        for (Container cont : myContainers.values()) {
                            cont.setFrontEnd(frontEnd);
                            for (Service ser : cont.geServices().values()) {
                                ser.setFrontEnd(frontEnd);
                            }
                        }
                        break;

                    case CConstants.SET_FRONT_END_REMOTE:
                        dpeHost = parser.nextString();
                        dpePort = parser.nextInteger();
                        dpeLang = parser.nextString();
                        frontEndHost = parser.nextString();
                        frontEndPort = parser.nextInteger();
                        frontEndLang = parser.nextString();

                        dpe = ClaraComponent.dpe(dpeHost, dpePort, dpeLang, 1, CConstants.UNDEFINED);
                        topic = ClaraUtil.buildTopic(CConstants.DPE, dpe.getCanonicalName());
                        data = ClaraUtil.buildData(CConstants.SET_FRONT_END, frontEndHost, frontEndPort, frontEndLang);
                        send(dpe, new xMsgMessage(topic, data));
                        break;

                    case CConstants.PING_DPE:
                        report();
                        break;

                    case CConstants.PING_REMOTE_DPE:
                        try {
                            dpeHost = parser.nextString();
                            dpePort = parser.nextInteger();
                            dpeLang = parser.nextString();
                            dpe = ClaraComponent.dpe(dpeHost, dpePort, dpeLang, 1, CConstants.UNDEFINED);
                            topic = ClaraUtil.buildTopic(CConstants.DPE, dpe.getCanonicalName());
                            send(dpe, new xMsgMessage(topic, CConstants.PING_DPE));
                        } catch (NoSuchElementException e) {
                            System.out.println("Clara-Warning: malformed stopRemoteDpe request.");
                            break;
                        } catch (ClaraException e1) {
                            e1.printStackTrace();
                        }
                        break;

                    case CConstants.START_CONTAINER:
                        try {
                            containerName = parser.nextString();
                            poolSize = parser.nextInteger();
                            description = parser.nextString();
                            startContainer(containerName, poolSize, description);

                        } catch (NoSuchElementException e) {
                            System.out.println("Clara-Error: malformed startContainer request.");
                            break;
                        }
                        break;

                    case CConstants.START_REMOTE_CONTAINER:
                        try {
                            dpeHost = parser.nextString();
                            dpePort = parser.nextInteger();
                            dpeLang = parser.nextString();
                            containerName = parser.nextString();
                            poolSize = parser.nextInteger();
                            description = parser.nextString();
                            deploy(ClaraComponent.container(dpeHost, dpePort, dpeLang, containerName, poolSize, description));

                        } catch (NoSuchElementException e) {
                            System.out.println("Clara-Error: malformed startRemoteContainer request.");
                            break;
                        }
                        break;

                    case CConstants.STOP_CONTAINER:
                        try {
                            containerName = parser.nextString();
                        } catch (NoSuchElementException e) {
                            System.out.println("Clara-Error: malformed stopContainer request.");
                            break;
                        }
                        stopContainer(containerName);
                        break;

                    case CConstants.STOP_REMOTE_CONTAINER:
                        try {
                            dpeHost = parser.nextString();
                            dpePort = parser.nextInteger();
                            dpeLang = parser.nextString();
                            containerName = parser.nextString();
                            exit(ClaraComponent.container(dpeHost, dpePort, dpeLang, containerName, 1, CConstants.UNDEFINED));

                        } catch (NoSuchElementException e) {
                            System.out.println("Clara-Error: malformed stopRemoteContainer request.");
                            break;
                        }
                        break;

                    case CConstants.START_SERVICE:
                        try {
                            containerName = parser.nextString();
                            engineName = parser.nextString();
                            engineClass = parser.nextString();
                            poolSize = parser.nextInteger();
                            description = parser.nextString();
                            initialState = parser.nextString();
                        } catch (NoSuchElementException e) {
                            System.out.println("Clara-Warning: malformed startService request.");
                            break;
                        }
                        startService(containerName, engineName, engineClass, poolSize, description, initialState);
                        break;

                    case CConstants.START_REMOTE_SERVICE:
                        try {
                            dpeHost = parser.nextString();
                            dpePort = parser.nextInteger();
                            dpeLang = parser.nextString();
                            containerName = parser.nextString();
                            engineName = parser.nextString();
                            engineClass = parser.nextString();
                            poolSize = parser.nextInteger();
                            description = parser.nextString();
                            initialState = parser.nextString();
                        } catch (NoSuchElementException e) {
                            System.out.println("Clara-Warning: malformed startRemoteService request.");
                            break;
                        }
                        deploy(ClaraComponent.service(dpeHost, dpePort, dpeLang,
                                containerName, engineName, engineClass, poolSize, description, initialState));
                        break;

                    case CConstants.STOP_SERVICE:
                        try {
                            containerName = parser.nextString();
                            engineName = parser.nextString();
                        } catch (NoSuchElementException e) {
                            System.out.println("Clara-Warning: malformed stopService request.");
                            break;
                        }
                        stopService(containerName, engineName);
                        break;

                    case CConstants.STOP_REMOTE_SERVICE:
                        try {
                            dpeHost = parser.nextString();
                            dpePort = parser.nextInteger();
                            dpeLang = parser.nextString();
                            containerName = parser.nextString();
                            engineName = parser.nextString();
                        } catch (NoSuchElementException e) {
                            System.out.println("Clara-Warning: malformed stopRemoteService request.");
                            break;
                        }
                        exit(ClaraComponent.service(dpeHost, dpePort, dpeLang, containerName, engineName));
                        break;

                    default:
                        break;
                }
            } catch (ClaraException | IOException | xMsgException | TimeoutException e) {
                e.printStackTrace();
            }
            return msg;
        }
    }

}
