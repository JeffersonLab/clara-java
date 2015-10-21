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
import org.jlab.coda.xmsg.net.xMsgConnection;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;

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

    private String description = xMsgConstants.ANY;

    private xMsgSubscription subscriptionHandler;

    private xMsgProxyAddress cloudProxyAddress;
    private xMsgConnection cloudProxyCon = null;

    private DpeReport myReport;
    private JsonReportBuilder myReportBuilder = new JsonReportBuilder();

    private AtomicBoolean isReporting = new AtomicBoolean();

    /**
     * @param dpePort
     * @param subPoolSize
     * @param regHost
     * @param regPort
     * @param description
     * @param cloudHost
     * @param cloudPort
     * @throws xMsgException
     * @throws IOException
     * @throws ClaraException
     */
    public Dpe(int dpePort,
               int subPoolSize,
               String regHost,
               int regPort,
               String description,
               String cloudHost, int cloudPort)
            throws xMsgException, IOException, ClaraException {
        super(ClaraComponent.dpe(xMsgUtil.localhost(),
                        dpePort,
                        CConstants.JAVA_LANG,
                        subPoolSize),
                regHost, regPort);

        this.description = description;
        // Create a socket connections to the local dpe proxy
        connect();

        // Subscribe messages published to this dpe
        xMsgTopic topic = xMsgTopic.wrap(CConstants.DPE + ":" + getMe().getCanonicalName());

        // Register this subscriber
        registerAsSubscriber(topic, description);
        System.out.println(ClaraUtil.getCurrentTimeInH() + ": Registered DPE = " + getMe().getCanonicalName());

        // Subscribe by passing a callback to the subscription
        subscriptionHandler = listen(topic, new DpeCallBack());
        System.out.println(ClaraUtil.getCurrentTimeInH() + ": Started DPE = " + getMe().getCanonicalName());

        if (cloudHost != null && cloudPort > 0) {
            cloudProxyAddress = new xMsgProxyAddress(cloudHost, cloudPort);
            cloudProxyCon = connect(cloudProxyAddress);
        }

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
            String regHost = xMsgUtil.localhost();
            int regPort = xMsgConstants.REGISTRAR_PORT;
            String description = "Clara DPE";
            String cloudProxyHost = xMsgUtil.localhost();
            int cloudProxyPort = xMsgConstants.DEFAULT_PORT;

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

                    case "-RegHost":
                        if (i < args.length) {
                            regHost = args[i++];
                        } else {
                            usage();
                            System.exit(1);
                        }
                        break;
                    case "-RegPort":
                        if (i < args.length) {
                            regPort = Integer.parseInt(args[i++]);
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
                    case "-CloudProxyHost":
                        if (i < args.length) {
                            cloudProxyHost = args[i++];
                        } else {
                            usage();
                            System.exit(1);
                        }
                        break;
                    case "-CloudProxyPort":
                        if (i < args.length) {
                            cloudProxyPort = Integer.parseInt(args[i++]);
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
            new Dpe(dpePort, poolSize, regHost, regPort, description, cloudProxyHost, cloudProxyPort);
        } catch (IOException | ClaraException | xMsgException e) {
            System.err.println(e.getMessage());
        }
    }

    /**
     *
     */
    private void printLogo() {
        System.out.println("=========================================");
        System.out.println("                 CLARA DPE               ");
        System.out.println("=========================================");
        System.out.println(" Name             = " + getName());
        System.out.println(" Date             = " + ClaraUtil.getCurrentTimeInH());
        System.out.println(" Version          = 4.x");
        System.out.println(" Description      = " + description);
        if (cloudProxyCon != null) {
            System.out.println(" CloudProxy Host  = " + cloudProxyAddress.host());
            System.out.println(" CloudProxy Port  = " + cloudProxyAddress.port());
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
        }, 10, TimeUnit.SECONDS);
    }

    /**
     *
     */
    private void report() {
        try {

            xMsgTopic dpeReportTopic = xMsgTopic.wrap(CConstants.DPE_ALIVE + ":" + getDefaultProxyAddress().host());
            if (cloudProxyCon != null) {
                dpeReportTopic = xMsgTopic.wrap(CConstants.DPE_ALIVE + ":" + cloudProxyAddress.host());
            }

            while (isReporting.get()) {

                myReport.setMemoryUsage(ClaraUtil.getMemoryUsage());
                myReport.setCpuUsage(ClaraUtil.getCpuUsage());
                myReport.setSnapshotTime(ClaraUtil.getCurrentTime());

                String jsonData = myReportBuilder.generateReport(myReport);

                xMsgMessage msg = new xMsgMessage(dpeReportTopic, jsonData);
                if (cloudProxyCon != null) {
                    send(cloudProxyCon, msg);
                } else {
                    //@todo check to see if front end host is the same is this host
                    send(getFrontEnd(), msg);
                }
            }
        } catch (IOException | xMsgException | MalformedObjectNameException |
                ReflectionException | InstanceNotFoundException e) {
            e.printStackTrace();
        }

    }

    /**
     *
     * @throws IOException
     * @throws xMsgException
     * @throws TimeoutException
     * @throws ClaraException
     */
    private void stopDpe()
            throws IOException, xMsgException, TimeoutException, ClaraException {
        for (Container comp : myContainers.values()) {
            exit(comp.getMe());
        }
        stopListening(subscriptionHandler);
        isReporting.set(false);
        exit(getMe());
    }

    private void stopRemoteDpe(String dpeHost, int dpePort, String dpeLang) throws IOException, xMsgException {
        ClaraComponent dpe = ClaraComponent.dpe(dpeHost, dpePort, dpeLang, 1);
        xMsgTopic topic = xMsgTopic.wrap(CConstants.DPE + xMsgConstants.TOPIC_SEP + dpe.getCanonicalName());
        send(dpe, new xMsgMessage(topic, CConstants.STOP_DPE));
    }

    private void pingRemoteDpe(String dpeHost, int dpePort, String dpeLang) throws IOException, xMsgException {
        ClaraComponent dpe = ClaraComponent.dpe(dpeHost, dpePort, dpeLang, 1);
        xMsgTopic topic = xMsgTopic.wrap(CConstants.DPE + xMsgConstants.TOPIC_SEP + dpe.getCanonicalName());
        send(dpe, new xMsgMessage(topic, CConstants.PING_DPE));
    }

    private void startRemoteContainer(String dpeHost, int dpePort, String dpeLang,
                                      String containerName, int poolSize, String description)
            throws IOException, xMsgException {
        ClaraComponent dpe = ClaraComponent.dpe(dpeHost, dpePort, dpeLang, 1);
        xMsgTopic topic = xMsgTopic.wrap(CConstants.DPE + xMsgConstants.TOPIC_SEP + dpe.getCanonicalName());
        String data = CConstants.START_CONTAINER + xMsgConstants.DATA_SEP + containerName +
                xMsgConstants.DATA_SEP + poolSize + xMsgConstants.DATA_SEP + description;
        send(dpe, new xMsgMessage(topic, data));
    }

    private void stopRemoteContainer(String dpeHost, int dpePort, String dpeLang,
                                     String containerName)
            throws IOException, xMsgException {
        ClaraComponent dpe = ClaraComponent.dpe(dpeHost, dpePort, dpeLang, 1);
        xMsgTopic topic = xMsgTopic.wrap(CConstants.DPE + xMsgConstants.TOPIC_SEP + dpe.getCanonicalName());
        String data = CConstants.STOP_CONTAINER + xMsgConstants.DATA_SEP + containerName;
        send(dpe, new xMsgMessage(topic, data));
    }

    private void startRemoteService(String dpeHost, int dpePort, String dpeLang,
                                    String containerName, String engineName, String engineClass, int poolSize, String description)
            throws IOException, xMsgException {
        ClaraComponent dpe = ClaraComponent.dpe(dpeHost, dpePort, dpeLang, 1);
        xMsgTopic topic = xMsgTopic.wrap(CConstants.DPE + xMsgConstants.TOPIC_SEP + dpe.getCanonicalName());
        String data = CConstants.START_CONTAINER + xMsgConstants.DATA_SEP +
                containerName + xMsgConstants.DATA_SEP +
                engineName + xMsgConstants.DATA_SEP +
                engineClass + xMsgConstants.DATA_SEP +
                poolSize + xMsgConstants.DATA_SEP +
                description;
        send(dpe, new xMsgMessage(topic, data));
    }

    private void stopRemoteService(String dpeHost, int dpePort, String dpeLang,
                                   String containerName, String engineName)
            throws IOException, xMsgException {
        ClaraComponent dpe = ClaraComponent.dpe(dpeHost, dpePort, dpeLang, 1);
        xMsgTopic topic = xMsgTopic.wrap(CConstants.DPE + xMsgConstants.TOPIC_SEP + dpe.getCanonicalName());
        String data = CConstants.STOP_SERVICE + xMsgConstants.DATA_SEP + containerName + xMsgConstants.DATA_SEP + engineName;
        send(dpe, new xMsgMessage(topic, data));
    }

    /**
     * Container name is NOT a canonical name
     *
     * @param containerName
     * @param poolSize
     * @param description
     * @throws ClaraException
     * @throws xMsgException
     * @throws IOException
     */
    private void startContainer(String containerName, int poolSize, String description)
            throws ClaraException, xMsgException, IOException {

        if (myContainers.containsKey(containerName)) {
            String msg = "%s Clara-Warning: container %s already exists. No new container is started%n";
            System.err.printf(msg, ClaraUtil.getCurrentTimeInH(), containerName);
            return;

        }
        if (poolSize <= 0) {
            poolSize = getMe().getSubscriptionPoolSize();
        }

        ClaraComponent contComp = ClaraComponent.container(getMe().getDpeHost(),
                getMe().getDpePort(), CConstants.JAVA_LANG, containerName, poolSize);

        if (myContainers.containsKey(contComp.getCanonicalName())) {
            String msg = "%s Warning: container %s already exists. No new container is created%n";
            System.err.printf(msg, ClaraUtil.getCurrentTimeInH(), contComp.getCanonicalName());
        } else {
            // start a container
            System.out.println("Starting container " + contComp.getCanonicalName());
            Container container = new Container(contComp,
                    getDefaultRegistrarAddress().host(),
                    getDefaultRegistrarAddress().port(),
                    description);
            myContainers.put(containerName, container);
            myReport.addContainerReport(container.getReport());
        }
    }

    private void startService(String containerName, String engineName, String engineClass, int poolSize, String description)
            throws xMsgException, ClaraException, IOException {
        if (myContainers.containsKey(containerName)) {
            if (poolSize <= 0) {
                poolSize = getMe().getSubscriptionPoolSize();
            }
            ClaraComponent serComp = ClaraComponent.service(getMe().getDpeHost(),
                    getMe().getDpePort(), getMe().getDpeLang(), containerName, engineName, engineClass, poolSize);
            myContainers.get(containerName).addService(serComp,
                    getDefaultRegistrarAddress().host(),
                    getDefaultProxyAddress().port(),
                    description, "ready");
        } else {
            System.out.println("Clara-Error: container does not exists");
        }

    }

    private void stopService(String containerName, String engineName)
            throws ClaraException, IOException, xMsgException {
        if (myContainers.containsKey(containerName)) {
            ClaraComponent serComp = ClaraComponent.service(getMe().getDpeHost(),
                    getMe().getDpePort(), getMe().getDpeLang(), containerName, engineName);
            myContainers.get(containerName).removeService(serComp.getCanonicalName());
        } else {
            System.out.println("Clara-Error: container does not exists");
        }

    }

    private void stopContainer(String containerName)
            throws ClaraException, xMsgException, IOException, TimeoutException {
        if (myContainers.containsKey(containerName)) {
            System.out.println("Removing container " + containerName);
            myContainers.get(containerName).exit();
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
     *     data = CConstants.STOP_CONTAINER ? dpeHost ? dpePort ? dpeLang ? containerName
     * </li>
     * <li>
     *     Start service:
     *     <p>
     *     Local:
     *     data = CConstants.START_SERVICE ? containerName ? engineName ? engineClass ? poolSize ? description
     *     remote:
     *     data = CConstants.START_REMOTE_SERVICE ? dpeHost ? dpePort ? dpeLang ? containerName ? engineName ? engineClass ? poolSize ? description
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
                        String dpeHost, dpeLang, regHost, containerName, engineName, engineClass, description;
                        int dpePort, poolSize, regPort;
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
                        break;

                    case CConstants.STOP_DPE:
                        stopDpe();
                        break;

                    case CConstants.STOP_REMOTE_DPE:
                        try {
                            dpeHost = parser.nextString();
                            dpePort = parser.nextInteger();
                            dpeLang = parser.nextString();
                            stopRemoteDpe(dpeHost, dpePort, dpeLang);
                        } catch (NoSuchElementException e) {
                            System.out.println("Clara-Warning: malformed stopRemoteDpe request.");
                            break;
                        } catch (ClaraException e1) {
                            e1.printStackTrace();
                        }

                        break;

                    case CConstants.PING_DPE:
                        report();
                        break;

                    case CConstants.PING_REMOTE_DPE:
                        try {
                            dpeHost = parser.nextString();
                            dpePort = parser.nextInteger();
                            dpeLang = parser.nextString();
                            pingRemoteDpe(dpeHost, dpePort, dpeLang);
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
                            startRemoteContainer(dpeHost, dpePort, dpeLang, containerName, poolSize, description);

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
                            stopRemoteContainer(dpeHost, dpePort, dpeLang, containerName);

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
                        } catch (NoSuchElementException e) {
                            System.out.println("Clara-Warning: malformed startService request.");
                            break;
                        }
                        startService(containerName, engineName, engineClass, poolSize, description);
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
                        } catch (NoSuchElementException e) {
                            System.out.println("Clara-Warning: malformed startRemoteService request.");
                            break;
                        }
                        startRemoteService(dpeHost, dpePort, dpeLang, containerName, engineName, engineClass, poolSize, description);
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
                        stopRemoteService(dpeHost, dpePort, dpeLang, containerName, engineName);
                        break;

                    default:
                        break;
                }
                // sync request handling below
                if (!returnTopic.equals(xMsgConstants.UNDEFINED)) {
                    xMsgMessage returnMsg = new xMsgMessage(xMsgTopic.wrap(returnTopic), null);
                    msg.getMetaData().setReplyTo(xMsgConstants.UNDEFINED);

                    // sends back "Done" string
                    returnMsg.updateData("Done");
                    send(returnMsg);
                }
            } catch (ClaraException | IOException | xMsgException | TimeoutException e) {
                e.printStackTrace();
            }
            return msg;
        }
    }

}
