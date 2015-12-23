/*
 * Copyright (C) 2015. Jefferson Lab, CLARA framework (JLAB). All Rights Reserved.
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for educational, research, and not-for-profit purposes,
 * without fee and without a signed licensing agreement.
 *
 * Contact Vardan Gyurjyan
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
import org.jlab.clara.base.ClaraLang;
import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.base.Composition;
import org.jlab.clara.base.ContainerName;
import org.jlab.clara.base.ServiceName;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.util.xml.XMLContainer;
import org.jlab.clara.util.xml.XMLTagValue;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.w3c.dom.Document;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
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

    private final String defaultHost;
    private final String defaultContainer;

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
        } catch (OptionException | UserInputException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public OrInteractive() throws Exception {
        super();
        defaultHost = xMsgUtil.localhost();
        defaultContainer = System.getenv("USER");
    }

    public void read(String appFile, boolean bluster) throws Exception {
        // read xml file and get deployment details
        Document doc = ClaraUtil.getXMLDocument(appFile);

        for (ServiceInfo s : parseServices(doc)) {
            deployContainer(s.container);
            deployService(s);
        }

        for (AppInfo a : parseApplications(doc)) {
            runApp(a);
            if (bluster) {
                while (true) {
                    runApp(a);
                }
            }
        }
    }

    public void interactive() throws Exception {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            try {
                System.out.printf("> Command: ");
                String cmd = scanner.nextLine().trim().toLowerCase();

                switch (cmd) {
                    case "1":
                        deployContainer(askContainer(scanner));
                        break;

                    case "2":
                        deployService(askService(scanner));
                        break;

                    case "3":
                        runApp(askApp(scanner));
                        break;

                    case "4":
                        listContainers(scanner);
                        break;

                    case "h":
                    case "help":
                        printHelp();
                        break;

                    case "0":
                    case "e":
                    case "exit":
                        System.out.println("Exiting...");
                        return;

                    default:
                        System.out.println("Invalid command.");
                }
            } catch (UserInputException e) {
                System.out.println(e.getMessage());
            } catch (NoSuchElementException e) {
                System.out.println();
                System.out.println("Exiting...");
                return;
            }
        }
    }

    private List<ServiceInfo> parseServices(Document doc) {
        List<ServiceInfo> services = new ArrayList<>();
        String[] serviceTags = { "dpe", "container", "engine", "pool" };
        List<XMLContainer> nodes = ClaraUtil.parseXML(doc, "service", serviceTags);

        for (XMLContainer s : nodes) {

            String dpe = defaultHost;
            String container = defaultContainer;
            String engine = null;
            String pool = "1";

            for (XMLTagValue t : s.getContainer()) {
                if (t.getTag().equals("dpe")) {
                    dpe = t.getValue();
                } else if (t.getTag().equals("container")) {
                    container = t.getValue();
                } else if (t.getTag().equals("engine")) {
                    engine = t.getValue();
                } else if (t.getTag().equals("pool")) {
                    pool = t.getValue();
                }
            }

            if (dpe.isEmpty()) {
                throw new UserInputException("Empty service tag 'dpe'");
            }
            if (container.isEmpty()) {
                throw new UserInputException("Empty service tag 'container'");
            }
            if (engine == null) {
                throw new UserInputException("Missing service tag 'engine'");
            }
            if (pool.isEmpty()) {
                throw new UserInputException("Empty service tag 'pool'");
            }

            services.add(new ServiceInfo(dpe, container, engine, parsePoolSize(pool)));
        }

        return services;
    }

    private List<AppInfo> parseApplications(Document doc) {
        List<AppInfo> apps = new ArrayList<>();
        String[] applicationTags = { "composition", "data" };
        List<XMLContainer> application = ClaraUtil.parseXML(doc, "application", applicationTags);

        for (XMLContainer a : application) {
            String comp = null;
            String dataSize = null;

            for (XMLTagValue t : a.getContainer()) {
                if (t.getTag().equals("composition")) {
                    comp = t.getValue();
                } else if (t.getTag().equals("data")) {
                    dataSize = t.getValue();
                }
            }

            if (comp == null) {
                throw new UserInputException("Missing application tag 'composition'");
            }
            if (dataSize == null) {
                throw new UserInputException("Missing application tag 'data'");
            }
            if (dataSize.isEmpty()) {
                throw new UserInputException("Empty application tag 'data'");
            }

            apps.add(new AppInfo(comp, buildData(dataSize)));
        }

        return apps;
    }

    private ContainerName askContainer(Scanner scanner) {
        System.out.printf(">> DPE host = ");
        String dpeHost = scanner.nextLine().trim();
        if (dpeHost.isEmpty()) {
            dpeHost = defaultHost;
        }
        System.out.printf(">> Container name = ");
        String container = scanner.nextLine().trim();
        if (container.isEmpty()) {
            container = defaultContainer;
        }
        return buildContainerName(dpeHost, container);
    }

    private ServiceInfo askService(Scanner scanner) {
        System.out.printf(">> DPE host = ");
        String dpeHost = scanner.nextLine().trim();
        if (dpeHost.isEmpty()) {
            dpeHost = defaultHost;
        }
        System.out.printf(">> Container name = ");
        String container = scanner.nextLine().trim();
        if (container.isEmpty()) {
            container = defaultContainer;
        }
        System.out.printf(">> Engine class name = ");
        String engine = scanner.nextLine().trim();
        if (engine.isEmpty()) {
            throw new UserInputException("Empty engine class path");
        }
        System.out.printf(">> Service object pool size = ");
        String pSizeArg = scanner.nextLine().trim();
        int pSize = pSizeArg.isEmpty() ? 1 : parsePoolSize(pSizeArg);
        return new ServiceInfo(dpeHost, container, engine, pSize);
    }

    private AppInfo askApp(Scanner scanner) {
        System.out.println(">> Composition (canonical) = ");
        String composition = scanner.nextLine().trim();
        if (composition.isEmpty()) {
            throw new UserInputException("Empty composition");
        }
        System.out.println(">> Input data = ");
        String data = scanner.nextLine().trim();
        if (data.isEmpty()) {
            throw new UserInputException("Empty input data");
        }
        return new AppInfo(composition, data);
    }

    private void deployContainer(ContainerName container) throws ClaraException {
        System.out.println("Deploying " + container.canonicalName() + "...");
        deploy(container).run();
        ClaraUtil.sleep(1000);
    }

    private void deployService(ServiceInfo service) throws ClaraException {
        System.out.println("Deploying " + service.name.canonicalName() + "...");
        deploy(service.name, service.classPath).withPoolsize(service.poolSize).run();
        ClaraUtil.sleep(1000);
    }

    private void runApp(AppInfo app) throws ClaraException {
        execute(app.composition).withData(app.data).run();
    }

    private void listContainers(Scanner scanner) throws Exception {
        System.out.println(">> DPE host");
        String dpeHost = scanner.nextLine().trim();
        if (dpeHost.isEmpty()) {
            dpeHost = defaultHost;
        }
        for (String name : getContainerNames(dpeHost)) {
            System.out.println(name);
        }
    }

    private static void usage(PrintStream out) {
        out.printf("usage: jx_orchestrator [options]%n%n  Options:%n");
        out.printf("  %-22s  %s%n", "-f <file>", "the application description file");
        out.printf("  %-22s  %s%n", "-b", "run a bluster test");
    }

    private void printHelp() {
        System.out.println("|----------|------------------------------|----------------------|");
        System.out.println("| ShortCut |          Parameters          |     Description      |");
        System.out.println("|----------|------------------------------|----------------------|");
        System.out.println("|    1     |  * DPE canonical name        | Start a container    |");
        System.out.println("|          |  * Container given name      |                      |");
        System.out.println("|----------|------------------------------|----------------------|");
        System.out.println("|    2     |  * DPE canonical name        | Start a service      |");
        System.out.println("|          |  * Container name            |                      |");
        System.out.println("|          |  * Engine class name         |                      |");
        System.out.println("|          |  * Service object pool size  |                      |");
        System.out.println("|----------|------------------------------|----------------------|");
        System.out.println("|    3     |  * Application composition   | Start application    |");
        System.out.println("|          |  * Input data = String       |                      |");
        System.out.println("|          |                              |                      |");
        System.out.println("|----------|------------------------------|----------------------|");
        System.out.println("|    4     |  * DPE name                  | Prints the names     |");
        System.out.println("|          |                              | of all containers    |");
        System.out.println("|          |                              | in the DPE.          |");
        System.out.println("|----------|------------------------------|----------------------|");
    }

    private int parsePoolSize(String size) {
        try {
            return Integer.parseInt(size);
        } catch (NumberFormatException e) {
            throw new UserInputException("Cannot parse pool size from: " + size);
        }
    }

    private static ContainerName buildContainerName(String dpeHost, String container) {
        try {
            return new ContainerName(xMsgUtil.toHostAddress(dpeHost), ClaraLang.JAVA, container);
        } catch (IOException e) {
            throw new UserInputException(e);
        }
    }

    private static String buildData(String dataSize) {
        try {
            int bytes = Integer.parseInt(dataSize);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < bytes; i++) {
                sb.append('x');
            }
            return sb.toString();
        } catch (NumberFormatException e) {
            throw new UserInputException("Cannot parse data size from: " + dataSize);
        }
    }


    private static class UserInputException extends RuntimeException {

        public UserInputException(String msg) {
            super(msg);
        }

        public UserInputException(Throwable cause) {
            super(cause);
        }
    }


    private static final class ServiceInfo {

        private final ContainerName container;
        private final ServiceName name;
        private final String classPath;
        private final int poolSize;

        public ServiceInfo(String dpe, String container, String engine, int poolSize) {
            this.container = buildContainerName(dpe, container);

            int idx = engine.lastIndexOf(".");
            if (idx < 0) {
                throw new UserInputException("Invalid class path: " + engine);
            }
            String engineName = engine.substring(idx + 1);
            this.name = new ServiceName(this.container, engineName);
            this.classPath = engine;

            if (poolSize < 0) {
                throw new UserInputException("Invalid poolsize: " + poolSize);
            }
            this.poolSize = poolSize;
        }
    }


    private static final class AppInfo {

        private final Composition composition;
        private final EngineData data;

        public AppInfo(String composition, String data) {
            this.composition = new Composition(composition);
            this.data = new EngineData();
            this.data.setData(data, EngineDataType.STRING.mimeType());
        }
    }
}
