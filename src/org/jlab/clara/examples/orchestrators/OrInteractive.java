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

package org.jlab.clara.examples.orchestrators;

import org.jlab.clara.base.CException;
import org.jlab.clara.util.CUtility;
import org.jlab.clara.util.XMLContainer;
import org.jlab.clara.util.XMLTagValue;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.net.SocketException;
import java.util.List;
import java.util.Scanner;

/**
 * <p>
 *     Interactive orchestrator.
 *     Runs with the local DPE.
 * </p>
 *
 * @author gurjyan
 * @version 1.x
 * @since 2/9/15
 */
public class OrInteractive {

    public OrInteractive(String dpeHost,
                         String feHost) throws xMsgException, SocketException {
//        super(dpeHost, feHost);
    }

    public OrInteractive() throws xMsgException, SocketException {
//        super();
    }

    public static void main(String[] args) {

        /**
        try {
            OrInteractive or = new OrInteractive();

            if (args.length > 0 && args[0].equalsIgnoreCase("-f")) {
                // read xml file and get deployment details
                try {
                    Document doc = CUtility.getXMLDocument(args[1]);

                    String[] serviceTags = {"dpe", "container", "engine", "pool"};
                    List<XMLContainer> services = CUtility.parseXML(doc, "service", serviceTags);


                    for (XMLContainer s : services) {
                        String dpe = null, container = null, engine = null;
                        int pool = 1;

                        for (XMLTagValue t : s.getContainer()) {
                            if (t.getTag().equals("dpe")) dpe = t.getValue();
                            if (t.getTag().equals("container")) container = t.getValue();
                            if (t.getTag().equals("engine")) engine = t.getValue();
                            if (t.getTag().equals("pool")) pool = Integer.parseInt(t.getValue());
                        }
                        if (dpe != null && container != null && engine != null) {

                            // ask if container exists
                            // start a container
                            or.start_container(dpe, container);
                            CUtility.sleep(1000);

                            // start a service
                            or.start_service(dpe + ":" + container, engine, pool);
                            CUtility.sleep(1000);
                        }
                    }

                    String[] applicationTags = {"composition", "data"};
                    List<XMLContainer> application = CUtility.parseXML(doc, "application", applicationTags);

                    for (XMLContainer a : application) {
                        String comp = null, inData = null;
                        int bytes;

                        for (XMLTagValue t : a.getContainer()) {

                            if (t.getTag().equals("composition")) comp = t.getValue();
                            if (t.getTag().equals("data")) {
                                bytes = Integer.parseInt(t.getValue());
                                StringBuilder sb = new StringBuilder();
                                for (int i = 0; i < bytes; i++) {
                                    sb.append('x');
                                }
                                inData = sb.toString();
                            }
                        }
                        if (comp != null && inData != null) {
                            // get canonical composition
//                            String canComposition = or.engineToCanonical(comp);

                            String canComposition = comp;

                            // find the first service in the composition
                            String firstService = or.getFirstServiceName(canComposition);

                            // create a transient data
                            xMsgD.Data.Builder data = xMsgD.Data.newBuilder();
                            data.setComposition(canComposition);
                            data.setDataType(xMsgD.Data.DType.T_STRING);
                            data.setSTRING(inData);
                            data.setAction(xMsgD.Data.ControlAction.EXECUTE);
                            data.setSender(or.getMyName());


                            // send the data to the service
                            or.run_service(firstService, data);

                            int rqc = 1;
                            // check to see if we need to perform bluster test
                            if (args.length == 3 && args[2].equals("-b")) {
                                while (true) {
                                    // send the data to the service
                                    or.run_service(firstService, data);
                                }
                            }
                        }
                    }

                } catch (ParserConfigurationException | IOException | SAXException e) {
                    e.printStackTrace();
                }

            } else {
                Scanner scanner = new Scanner(System.in);
                while (true) {
                    System.out.printf("command: ");
                    String cmd = scanner.nextLine().trim();
                    if (cmd.equalsIgnoreCase("help")) {
                        or.printHelp();
                    } else if (cmd.equalsIgnoreCase("exit")) {
                        System.exit(0);
                    } else {
                        switch (cmd) {
                            case "1":
                                System.out.println("DPE host ip = ");
                                String dpe = scanner.nextLine().trim();
                                System.out.println("Container name = ");
                                String container = scanner.nextLine().trim();
                                or.start_container(dpe, container);
                                break;
                            case "2":
                                System.out.println("Container canonical name = ");
                                String canCon = scanner.nextLine().trim();
                                System.out.println("Engine class name = ");
                                String engine = scanner.nextLine().trim();
                                System.out.println("Service object pool size = ");
                                int pSize = scanner.nextInt();
                                or.start_service(canCon, engine, pSize);
                                break;
                            case "3":
                                System.out.println("Composition = ");
                                String composition = scanner.nextLine().trim();
                                System.out.println("Input data = ");
                                String inData = scanner.nextLine().trim();

                                // get canonical composition
                                String canComposition = or.engineToCanonical(composition);

                                // find the first service in the composition
                                String firstService = or.getFirstServiceName(canComposition);

                                // create a transient data
                                xMsgD.Data.Builder data = xMsgD.Data.newBuilder();
                                data.setComposition(canComposition);
                                data.setDataType(xMsgD.Data.DType.T_STRING);
                                data.setSTRING(inData);
                                data.setAction(xMsgD.Data.ControlAction.EXECUTE);
                                data.setSender(or.getMyName());

                                // send the data to the service
                                or.run_service(firstService, data);
                                break;
                            case "4":
                                System.out.println("DPE name");
                                String dpe_name = scanner.nextLine().trim();
                                List<xMsgRegistrationData> containers = or.findContainers(dpe_name);
                                for (xMsgRegistrationData r : containers) {
                                    System.out.println(r.getMyName());
                                }
                        }
                    }
                }
            }
        }catch(xMsgException | CException | SocketException e){
            e.printStackTrace();
        }
        */
    }

    private void printHelp() {
        System.out.println("|----------|------------------------------|----------------------|");
        System.out.println("| ShortCut |          Parameters          |   Description        |");
        System.out.println("|----------|------------------------------|----------------------|");
        System.out.println("|    1     |  . DPE canonical name        |   Start a container  |");
        System.out.println("|          |  . Container given name      |   on a DPE           |");
        System.out.println("|----------|------------------------------|----------------------|");
        System.out.println("|    2     |  . Container canonical name  |   Start a service    |");
        System.out.println("|          |  . Engine class name         |   on a container     |");
        System.out.println("|          |  . Service object pool size  |                      |");
        System.out.println("|----------|------------------------------|----------------------|");
        System.out.println("|    3     |  . Application composition   | Start application    |");
        System.out.println("|          |  . Input data = String       | (with engine names)  |");
        System.out.println("|          |                              | on a local DPE       |");
        System.out.println("|----------|------------------------------|----------------------|");
        System.out.println("|    4     |  . DPE name                  | Prints the names     |");
        System.out.println("|          |                              | of all containers    |");
        System.out.println("|          |                              | in the DPE.          |");
        System.out.println("|----------|------------------------------|----------------------|");

    }
}
