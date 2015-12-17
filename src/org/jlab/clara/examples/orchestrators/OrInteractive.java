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

import static java.util.Arrays.asList;

import org.jlab.clara.base.BaseOrchestrator;
import org.jlab.clara.base.ContainerName;
import org.jlab.clara.base.ServiceName;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.util.ClaraUtil;
import org.jlab.clara.util.xml.XMLContainer;
import org.jlab.clara.util.xml.XMLTagValue;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.w3c.dom.Document;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.PrintStream;
import java.util.List;
import java.util.Scanner;

/**
 * Interactive orchestrator.
 * Runs with the local DPE.
 *
 * @author gurjyan
 * @version 4.x
 * @since 10/9/15
 */
public class OrInteractive extends BaseOrchestrator {

    public static void main(String[] args) {
        try {
            OptionParser parser = new OptionParser();
            OptionSpec<String> fileSpec = parser.accepts("f").withRequiredArg();
            parser.accepts("b");
            parser.acceptsAll(asList("h", "help")).forHelp();
            OptionSet options = parser.parse(args);

            if (options.has("help")) {
                usage(System.out);
                System.exit(0);
            }

            OrInteractive or = new OrInteractive();
            if (options.has(fileSpec)) {
                or.read(options.valueOf(fileSpec), options.has("b"));
            } else {
                or.interactive();
            }
        } catch (OptionException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public OrInteractive() throws Exception {
        super();
    }

    public void read(String appFile, boolean bluster) throws Exception {
        // read xml file and get deployment details
        Document doc = ClaraUtil.getXMLDocument(appFile);

        String[] serviceTags = { "dpe", "container", "engine", "pool" };
        List<XMLContainer> services = ClaraUtil.parseXML(doc, "service", serviceTags);

        for (XMLContainer s : services) {
            String dpe = null, container = null, engine = null;
            int pool = 1;

            for (XMLTagValue t : s.getContainer()) {
                if (t.getTag().equals("dpe"))
                    dpe = t.getValue();
                if (t.getTag().equals("container"))
                    container = t.getValue();
                if (t.getTag().equals("engine"))
                    engine = t.getValue();
                if (t.getTag().equals("pool"))
                    pool = Integer.parseInt(t.getValue());
            }
            if (dpe != null && container != null && engine != null) {

                // ask if container exists
                // start a container
                ContainerName cont = new ContainerName(container);
                deploy(cont).withPoolsize(pool).withDescription("test container").run();

                ClaraUtil.sleep(1000);

                // start a service
                ServiceName serv = new ServiceName(cont, engine);
                deploy(serv, engine).withPoolsize(pool).run();
                ClaraUtil.sleep(1000);
            }
        }

        String[] applicationTags = { "composition", "data" };
        List<XMLContainer> application = ClaraUtil.parseXML(doc, "application",
                applicationTags);

        for (XMLContainer a : application) {
            String comp = null, inData = null;
            int bytes;

            for (XMLTagValue t : a.getContainer()) {

                if (t.getTag().equals("composition"))
                    comp = t.getValue();
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

                // find the first service in the composition
                String firstService = ClaraUtil.getFirstService(comp);

                // create a transient data
                EngineData ed = new EngineData();
                ed.setData(inData, EngineDataType.STRING.mimeType());

                // send the data to the service
                ServiceName serv = new ServiceName(firstService);
                execute(serv).withData(ed).run();

                // check to see if we need to perform bluster test
                if (bluster) {
                    while (true) {
                        // send the data to the service
                        execute(serv).withData(ed).run();
                    }
                }
            }
        }
    }

    public void interactive() throws ClaraException, xMsgException {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.printf("command: ");
            String cmd = scanner.nextLine().trim();
            if (cmd.equalsIgnoreCase("help")) {
                printHelp();
            } else if (cmd.equalsIgnoreCase("exit")) {
                System.exit(0);
            } else {
                switch (cmd) {
                    case "1":
                        System.out.println("DPE host ip = ");
                        System.out.println("Container name = ");
                        String container = scanner.nextLine().trim();
                        ContainerName cont = new ContainerName(container);
                        deploy(cont).withPoolsize(3).withDescription("test container").run();
                        break;
                    case "2":
                        System.out.println("Container canonical name = ");
                        String canCon = scanner.nextLine().trim();
                        System.out.println("Engine class name = ");
                        String engine = scanner.nextLine().trim();
                        System.out.println("Service object pool size = ");
                        int pSize = scanner.nextInt();
                        ServiceName serv = new ServiceName(new ContainerName(canCon), engine);
                        deploy(serv, engine).withPoolsize(pSize).run();
                        break;
                    case "3":
                        System.out.println("Composition (canonical) = ");
                        String composition = scanner.nextLine().trim();
                        System.out.println("Input data = ");
                        String inData = scanner.nextLine().trim();

                        // get canonical composition

                        // find the first service in the composition
                        String firstService = ClaraUtil.getFirstService(composition);
                        // create a transient data
                        EngineData ed = new EngineData();
                        ed.setData(inData, EngineDataType.STRING.mimeType());

                        // send the data to the service
                        execute(new ServiceName(firstService)).withData(ed).run();

                        break;
                    case "4":
                        System.out.println("DPE name");
                        String dpe_name = scanner.nextLine().trim();
                        for (String name : getContainerNames(dpe_name)) {
                            System.out.println(name);
                        }
                }
            }
        }
    }

    private static void usage(PrintStream out) {
        out.printf("usage: jx_orchestrator [options]%n%n  Options:%n");
        out.printf("  %-22s  %s%n", "-f <file>", "the application description file");
        out.printf("  %-22s  %s%n", "-b", "run a bluster test");
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
