package org.jlab.clara.monitor.dc.timeline;

import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.coda.xmsg.core.*;
import org.jlab.coda.xmsg.data.xMsgM;
import org.jlab.coda.xmsg.data.xMsgMimeType;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.jlab.coda.xmsg.net.xMsgRegAddress;

/**
 * Created by gurjyan on 10/3/17.
 */
public abstract class DcTListenerAndReporter extends xMsg {

    /**
     * Process the received DPE registration and runtime report.
     *
     * @param jsonString the DPE report
     */
    public abstract void report(String jsonString);

    // build the subscribing topic (hard-coded)
    private String domain = "reconstruction";
    private String subject = "dc";
    private String type = "timeline";


    DcTListenerAndReporter(String name, xMsgProxyAddress proxy) {
        super(name, proxy, new xMsgRegAddress(), 1);
    }

    /**
     * Subscribes to a hard-coded topic on the local proxy,
     * and registers with the local registrar.
     *
     * @throws xMsgException if subscription failed
     */
    public void start() throws xMsgException {
        xMsgTopic topic = xMsgTopic.build(domain, subject, type);

        // subscribe to default local proxy
        subscribe(topic, new DcTListenerAndReporter.MyCallBack());
        System.out.printf("Subscribed to = %s%n", topic.toString());
        xMsgUtil.keepAlive();
    }

    /**
     * Private callback class.
     */
    private class MyCallBack implements xMsgCallBack {

        @Override
        public void callback(xMsgMessage msg) {
            xMsgM.xMsgMeta.Builder metadata = msg.getMetaData();
            if (metadata.getDataType().equals(xMsgMimeType.STRING)) {
                String jsonString = new String(msg.getData());
                report(jsonString);
            }

        }
    }
}
