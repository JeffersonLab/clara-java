package org.jlab.clara.engine;

import org.jlab.clara.base.ClaraAddress;
import org.jlab.clara.base.DpeName;
import org.jlab.clara.util.TimerFlag;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgContext;
import org.jlab.coda.xmsg.net.xMsgSocketFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

/**
 * Created by gurjyan on 10/13/17.
 */
public abstract class MonitorEngine {

    private ZMQ.Socket con;
    private final xMsgSocketFactory socketFactory;

    private TimerFlag timerFlag;
    private int reportingPeriod = 5;

    private String monClass;
    private String monAuthor;
    private String monType;

    public MonitorEngine(String monClass, String monAuthor, String monType, int reportingPeriod){
        if(reportingPeriod>0) this.reportingPeriod = reportingPeriod;
        this.monClass = monClass;
        this.monAuthor = monAuthor;
        this.monType = monType;

        socketFactory = new xMsgSocketFactory(xMsgContext.getInstance().getContext());
    }

    /**
     * Connects to Clara monitor front-end
     *
     * @return
     * @throws xMsgException
     */

    private ZMQ.Socket connect() throws xMsgException {
        ZMQ.Socket socket = socketFactory.createSocket(ZMQ.PUB);
        String monName = System.getenv("CLARA_MONITOR_FRONT_END");
        // we overwrite if there is a monitor DPE defined. Monitor DPE plays
        // the role of front-end DPE in this case.
        if (monName != null) {
            try {
                DpeName monDpe = new DpeName(monName);
                ClaraAddress monAddr = monDpe.address();
                socketFactory.connectSocket(socket, monAddr.host(), monAddr.pubPort());
//                System.out.println("Using monitoring front-end " + monName);
            } catch (IllegalArgumentException e) {
                System.err.println("Could not use monitor node: " + e.getMessage());
            }
        }
        return socket;
    }

    private void send(ZMQ.Socket con, xMsgMessage msg) throws xMsgException {
        msg.getMetaData().setSender(monAuthor);
        ZMsg zmsg = new ZMsg();
        zmsg.add(msg.getTopic().toString());
        zmsg.add(msg.getMetaData().build().toByteArray());
        zmsg.add(msg.getData());
        zmsg.send(con);
    }

}
