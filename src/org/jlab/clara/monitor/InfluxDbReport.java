package org.jlab.clara.monitor;

import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;

/**
 * Created by gurjyan on 4/25/17.
 */
public class InfluxDbReport extends DpeListenerAndReporter {

    int i;

    InfluxDbReport(String name, String proxyHost, int proxyPort){
        super(name,new xMsgProxyAddress(proxyHost, proxyPort));
    }

    @Override
    public void report(String jsonString) {
        System.out.printf("================== "+(++i)+" ===================");
        System.out.println(jsonString+"\n");
    }

    public static void main(String[] args) {
        System.out.println(args[0]);
        System.out.println(args[1]);
        try (InfluxDbReport rep = new InfluxDbReport(args[0], args[1], Integer.parseInt(args[2]))) {
            rep.start();
        } catch (xMsgException e) {
            e.printStackTrace();
        }
    }
}
