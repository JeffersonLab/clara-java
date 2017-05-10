package org.jlab.clara.monitor;

import com.google.protobuf.InvalidProtocolBufferException;
import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.coda.xmsg.core.*;
import org.jlab.coda.xmsg.data.xMsgD;
import org.jlab.coda.xmsg.data.xMsgM;
import org.jlab.coda.xmsg.data.xMsgMimeType;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.jlab.coda.xmsg.net.xMsgRegAddress;


/**
 * Created by gurjyan on 4/10/17.
 */
public abstract class DpeListenerAndReporter extends xMsg {

    public abstract void report(String jsonString);

    // build the subscribing topic (hard-coded)
    private String domain = ClaraConstants.DPE_REPORT;
    private String subject = "*";
    private String type = "*";

    DpeListenerAndReporter(String name, xMsgProxyAddress proxy) {
        super(name, proxy, new xMsgRegAddress(), 1);
    }

    /**
     * Subscribes to a hard-coded topic on the local proxy,
     * and registers with the local registrar.
     */
    public void start() throws xMsgException {
        xMsgTopic topic = xMsgTopic.build(domain, subject, type);

        // subscribe to default local proxy
        subscribe(topic, new MyCallBack());
        System.out.printf("Subscribed to = %s%n", topic);
        xMsgUtil.keepAlive();
    }


    /**
     * Private callback class.
     */
    private class MyCallBack implements xMsgCallBack {

        @Override
        public void callback(xMsgMessage msg) {
            try {
                xMsgM.xMsgMeta.Builder metadata = msg.getMetaData();
                if (metadata.getDataType().equals(xMsgMimeType.STRING)) {
                    xMsgD.xMsgData data = xMsgD.xMsgData.parseFrom(msg.getData());
                    String jsonString = data.getSTRING();
                    report(jsonString);
                }
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }

        }
    }

}


