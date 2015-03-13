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
import org.jlab.clara.base.OrchestratorBase;
import org.jlab.coda.xmsg.data.xMsgD;
import org.jlab.coda.xmsg.excp.xMsgException;

import java.net.SocketException;
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
public class OrInteractive extends OrchestratorBase {

    public OrInteractive(String dpeHost,
                         String feHost) throws xMsgException, SocketException {
        super(dpeHost, feHost);
    }

    public OrInteractive() throws xMsgException, SocketException {
        super();
    }

    private void printHelp(){
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

    }

    public static void main(String[] args) {

        try {
            OrInteractive or = new OrInteractive();

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
                            data.setSender(or.getName());

                            // send the data to the service
                            or.run_service(firstService,data);
                            break;
                    }
                }
            }
        }catch(xMsgException | CException | SocketException e){
            e.printStackTrace();
        }
    }
}
