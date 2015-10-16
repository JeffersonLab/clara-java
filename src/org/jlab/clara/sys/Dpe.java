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
 * @version 1.x
 * @since 1/30/15
 */
public class Dpe extends ClaraBase {

    // The containers running on this DPE
    private Map<String, Container> myContainers = new HashMap<>();

    private String description = xMsgConstants.ANY.getStringValue();

    private xMsgSubscription subscriptionHandler;

    private xMsgProxyAddress cloudProxyAddress;
    private xMsgConnection cloudProxyCon = null;

    private DpeReport myReport = new DpeReport();
    private JsonReportBuilder myReportBuilder = new JsonReportBuilder();

    private AtomicBoolean isReporting;

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
        xMsgTopic topic = xMsgTopic.wrap(CConstants.DPE + ":" + getMe().getName());

        // Register this subscriber
        registerAsSubscriber(topic, description);
        System.out.println(ClaraUtil.getCurrentTimeInH() + ": Registered DPE = " + getMe().getName());

        // Subscribe by passing a callback to the subscription
        subscriptionHandler = listen(topic, new DpeCallBack());
        System.out.println(ClaraUtil.getCurrentTimeInH() + ": Started DPE = " + getMe().getName());

        if (cloudHost != null && cloudPort > 0) {
            cloudProxyAddress = new xMsgProxyAddress(cloudHost, cloudPort);
            cloudProxyCon = connect(cloudProxyAddress);
        } else {
            printLogo();
        }

        myReport.setName(getMe().getName());
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

    public static void main(String[] args) {
        try {
            int i = 0;
            int dpePort = xMsgConstants.DEFAULT_PORT.getIntValue();
            int poolSize = xMsgConstants.DEFAULT_POOL_SIZE.getIntValue();
            String regHost = xMsgUtil.localhost();
            int regPort = xMsgConstants.REGISTRAR_PORT.getIntValue();
            String description = "Clara DPE";
            String cloudProxyHost = xMsgUtil.localhost();
            int cloudProxyPort = xMsgConstants.DEFAULT_PORT.getIntValue();

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

    public static void usage() {
        System.err.println("Usage: j_dpe [ -cc | -fh <front_end> ]");
    }

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

    private void startHeartBeatReport() {
        ScheduledExecutorService scheduledPingService = Executors.newScheduledThreadPool(3);
        scheduledPingService.schedule(new Runnable() {
            @Override
            public void run() {
                try {


                    xMsgTopic dpeReportTopic = xMsgTopic.wrap(CConstants.DPE_ALIVE + ":" + getDefaultProxyAddress().host());
                    if (cloudProxyCon != null) {
                        dpeReportTopic = xMsgTopic.wrap(CConstants.DPE_ALIVE + ":" + cloudProxyAddress.host());
                    }

                    while (isReporting.get()) {

                        myReport.setMemoryUsage(ClaraUtil.getMemoryUsage());
                        myReport.setCpuUsage(ClaraUtil.getCpuUsage());

                        String jsonData = myReportBuilder.generateReport(myReport);

                        xMsgMessage msg = new xMsgMessage(dpeReportTopic, jsonData);
                        if (cloudProxyCon != null) {
                            send(cloudProxyCon, msg);
                        } else {
                            send(msg);
                        }
                    }
                } catch (IOException | xMsgException | MalformedObjectNameException |
                        ReflectionException | InstanceNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }, 10, TimeUnit.SECONDS);

    }


    private void stopDpe()
            throws IOException, xMsgException, TimeoutException, ClaraException {
        for (Container comp : myContainers.values()) {
            exit(comp.getMe());
        }
        isReporting.set(false);
        exit(getMe());
    }

    private void startContainer(String containerName, int poolSize, String description)
            throws ClaraException, xMsgException, IOException {

        if (poolSize <= 0) {
            poolSize = getMe().getSubscriptionPoolSize();
        }

        ClaraComponent contComp = ClaraComponent.container(getDefaultProxyAddress().host(),
                getDefaultProxyAddress().port(), CConstants.JAVA_LANG, containerName, poolSize);

        if (myContainers.containsKey(contComp.getName())) {
            String msg = "%s Warning: container %s already exists. No new container is created%n";
            System.err.printf(msg, ClaraUtil.getCurrentTimeInH(), contComp.getName());
        } else {
            // start a container
            System.out.println("Starting container " + contComp.getName());
            Container container = new Container(contComp,
                    getDefaultRegistrarAddress().host(),
                    getDefaultRegistrarAddress().port(),
                    description);
            myContainers.put(containerName, container);
            myReport.addContainerReport();
        }
    }

    private void stopContainer(String containerName)
            throws ClaraException, xMsgException, IOException, TimeoutException {
        if (myContainers.containsKey(containerName)) {
            System.out.println("Removing container " + containerName);
            exit(myContainers.get(containerName).getMe());
        }
    }


    /**
     * DPE callback.
     */
    private class DpeCallBack implements xMsgCallBack {

        @Override
        public xMsgMessage callback(xMsgMessage msg) {
//            xMsgMessage returnMsg = new xMsgMessage(null);
            xMsgMeta.Builder metadata = msg.getMetaData();
            try {
                String sender = metadata.getSender();
                String returnTopic = metadata.getReplyTo();

                RequestParser parser = RequestParser.build(msg);
                String cmd = parser.nextString();

                switch (cmd) {
                    // Sent from orchestrator.
                    case CConstants.STOP_DPE:
                        stopDpe();
                        break;

                    // @todo.......

                    // Sent from master DPE (FE). In this case the value
                    // is the canonical name of the master DPE (FE)
                    case CConstants.DPE_PING:
                        pingDpe(parser.nextString(), sender, returnTopic);
                        break;

                    // Sent from orchestrator. Value is the name (not canonical)of the container.
                    case CConstants.START_CONTAINER:
                        startContainer(parser.nextString(), 1, "");
                        break;

                    // Sent from orchestrator. Value is the name of the container.
                    // Note that the container name should be a canonical name
                    case CConstants.STOP_CONTAINER:
                        stopContainer(parser.nextString());
                        break;

                    default:
                        break;
                }
                if (!returnTopic.equals(xMsgConstants.UNDEFINED.getStringValue())) {
                    msg.getMetaData().setReplyTo(xMsgConstants.UNDEFINED.getStringValue());

                    // sends back "Done" string
                    msg.updateData("Done");
                    send(msg);
                }
            } catch (ClaraException | IOException | xMsgException | TimeoutException e) {
                e.printStackTrace();
            }
            return msg;
        }
    }

}
