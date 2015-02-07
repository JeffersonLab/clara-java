package org.jlab.clara.sys;

import org.jlab.clara.base.CBase;
import org.jlab.clara.util.CConstants;
import org.jlab.clara.util.CUtility;
import org.jlab.coda.xmsg.core.xMsgCallBack;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgD;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.xsys.xMsgNode;

import java.net.SocketException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
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
public class Dpe extends CBase{

    // Shared memory used by the services
    // deployed in the JVM where this DPE is running.
    public static ConcurrentHashMap<String, xMsgD.Data.Builder>
            sharedMemory = new ConcurrentHashMap<>();

    private String
            dpeName = xMsgConstants.UNDEFINED.getStringValue();
    private String
            feHost = xMsgConstants.UNDEFINED.getStringValue();

    public Dpe() throws xMsgException, SocketException {
        super();
        dpeName = xMsgUtil.getLocalHostIps().get(0);
        setName(dpeName);

        printLogo();
        new xMsgNode(false);

        // Subscribe messages published to this container
        genericReceive(CConstants.DPE + ":" + getName(),
                new DpeCallBack(),
                true);

    }

    public Dpe(String feName) throws xMsgException, SocketException {
        super();
        dpeName = xMsgUtil.getLocalHostIps().get(0);
        setName(dpeName);
        feHost = feName;

        printLogo();
        new xMsgNode(feName, false);

        // Subscribe messages published to this container
        genericReceive(CConstants.DPE + ":" + getName(),
                new DpeCallBack(),
                true);
    }

    public static List<String> getCanonicalNames()
            throws SocketException {
        return xMsgUtil.getLocalHostIps();
    }

    private void printLogo(){
        System.out.println("================================");
        System.out.println("             CLARA DPE        ");
        System.out.println("================================");
        System.out.println(" Binding = Java");
        System.out.println(" Date    = "+ CUtility.getCurrentTimeInH());
        try {
            for(String s: xMsgUtil.getLocalHostIps()) {
                System.out.println(" Host    = " + getName());
            }
        } catch (SocketException e) {
            e.printStackTrace();
        }
        System.out.println("================================");
    }

    private class DpeCallBack implements xMsgCallBack {

        @Override
        // This method serves to start/deploy a service on
        // this container. In the future it will report
        // service specific statistics on a request
        public void callback(xMsgMessage msg) {

            final String dataType = msg.getDataType();
            final Object data = msg.getData();
            if(dataType.equals(xMsgConstants.ENVELOPE_DATA_TYPE_STRING.getStringValue())) {
//                System.out.println("DDD: dpe_request: " + msg);
                String cmdData = (String)data;
                String cmd = null, conName = null;
                try {
                    StringTokenizer st = new StringTokenizer(cmdData, "?");
                    cmd = st.nextToken();
                    conName = st.nextToken();
                } catch (NoSuchElementException e){
                    e.printStackTrace();
                }
                if(cmd!=null && conName!=null) {
                    // create canonical name for the container
                    conName = getName() + ":" + conName;
                    switch (cmd) {
                        case CConstants.START_CONTAINER:
                            try {
                                if(feHost.equals(xMsgConstants.UNDEFINED.getStringValue())) {
                                    new Container(conName);
                                } else {
                                    new Container(conName, feHost);
                                }
                            } catch (xMsgException | SocketException e) {
                                e.printStackTrace();
                            }
                            break;
                        case CConstants.REMOVE_CONTAINER:
                            break;
                    }
                }
            }
        }
    }

    public static void main(String[] args) {
        if(args.length == 2){
            if (args[0].equals("-fe_host")){
                try {
                    new Dpe(args[1]);
                } catch (xMsgException | SocketException e) {
                    System.out.println(e.getMessage());
                    System.out.println("exiting...");
                }
            } else {
                System.out.println("wrong option. Accepts -fe_host option only.");
            }
        } else if(args.length == 0){
            try {
                new Dpe();
            } catch (xMsgException | SocketException e) {
                System.out.println(e.getMessage());
                System.out.println("exiting...");
            }
        }
    }

}
