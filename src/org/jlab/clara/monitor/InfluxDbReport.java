package org.jlab.clara.monitor;

import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;

/**
 * Created by gurjyan on 4/25/17.
 */
public class InfluxDbReport extends DpeListenerAndReporter {

    int i;

    InfluxDbReport(String name, String proxyHost){
        super(name,new xMsgProxyAddress(proxyHost));
    }

    @Override
    public void report(String jsonString) {
        System.out.printf("================== "+(++i)+" ===================");
        System.out.println(jsonString+"\n");
    }

    public static void main(String[] args) {
        try (InfluxDbReport rep = new InfluxDbReport(args[1], args[2])) {
            rep.start();
        } catch (xMsgException e) {
            e.printStackTrace();
        }
    }
}
