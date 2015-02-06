package org.jlab.clara.sys;

import org.jlab.clara.util.CUtility;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgD;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.xsys.xMsgNode;

import java.net.SocketException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 *     Creates a shared memory
 * </p>
 *
 * @author gurjyan
 * @version 1.x
 * @since 1/30/15
 */
public class Dpe {

    // Shared memory used by the services
    // deployed in the JVM where this DPE is running.
    public static ConcurrentHashMap<String, xMsgD.Data.Builder>
            sharedMemory = new ConcurrentHashMap<>();

    public static List<String> getCanonicalNames()
            throws SocketException {
        return xMsgUtil.getLocalHostIps();
    }

    public static void main(String[] args) {
        if(args.length == 2){
            if (args[0].equals("-fe_host")){
                try {
                    printLogo();
                    new xMsgNode(args[1]);
                } catch (xMsgException e) {
                    System.out.println(e.getMessage());
                    System.out.println("exiting...");
                }
            } else {
                System.out.println("wrong option. Accepts -fe_host option only.");
            }
        } else if(args.length == 0){
            try {
                printLogo();
                new xMsgNode();
            } catch (xMsgException e) {
                System.out.println(e.getMessage());
                System.out.println("exiting...");
            }
        }

    }

    private static void printLogo(){
        System.out.println("================================");
        System.out.println("             CLARA DPE        ");
        System.out.println("================================");
        System.out.println(" Binding = Java");
        System.out.println(" Date    = "+ CUtility.getCurrentTimeInH());
        try {
            for(String s: xMsgUtil.getLocalHostIps()) {
                System.out.println(" Host    = " + s);
            }
        } catch (SocketException e) {
            e.printStackTrace();
        }
        System.out.println("================================");
    }
}
