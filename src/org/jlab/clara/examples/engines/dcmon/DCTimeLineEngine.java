package org.jlab.clara.examples.engines.dcmon;

import org.jlab.clara.base.ClaraAddress;
import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.base.DpeName;
import org.jlab.clara.base.core.MessageUtil;
import org.jlab.clas.reco.ReconstructionEngine;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgContext;
import org.jlab.coda.xmsg.net.xMsgSocketFactory;
import org.jlab.io.base.DataEvent;
import org.jlab.io.hipo.HipoDataSource;
import org.json.JSONObject;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.io.FileNotFoundException;

/**
 * Created by gurjyan on 10/2/17.
 */
public class DCTimeLineEngine extends ReconstructionEngine {

    private final xMsgSocketFactory socketFactory;

    private final int reportingPeriod = 5;

    private final String monClass = "reconstruction";
    private final String monDetector = "dc";
    private final String type = "timeline";


    public DCTimeLineEngine() {
        super("DCTImeLineService", "ziegler-gurjyan", "1.0");
        socketFactory = new xMsgSocketFactory(xMsgContext.getInstance().getContext());
    }

    TrackingMon mon;

    @Override
    public boolean processDataEvent(DataEvent de) {
        mon.fetch_Trks(de);
        /* System.out.println(mon.getNbCTHits()+" "+mon.getNbCTHitsOnTrack()+" "+mon.getNbCTTracks()+" "
        +mon.getNbHBHits()+" "+mon.getNbHBHitsOnTrack()+" "+mon.getNbHBTracks()+" "
        +mon.getNbTBHits()+" "+mon.getNbTBHitsOnTrack()+" "+mon.getNbTBTracks()+" "
        +mon.getTimeResidual()[0]+" "+mon.getTimeResidual()[1]+" "+mon.getTimeResidual()[2]); */

            ZMQ.Socket con = null;
            try {
                con = connect();
                xMsgUtil.sleep(100);
            } catch (xMsgException e) {
                System.err.println("DCTimeLineService-Error: Could not start reporting thread:");
                e.printStackTrace();
            } catch (Exception e) {
                System.err.println("DCTimeLineService-Error: Error running reporting thread:");
                e.printStackTrace();
            }
            try {
                send(con, dcJsonMessage(mon));

            } catch (xMsgException e) {
                System.err.println("DCTimeLineService-Error: Could not publish report:" + e.getMessage());
            } finally {
                socketFactory.closeQuietly(con);
            }
        return true;
    }

    @Override
    public boolean init() {
        mon = new TrackingMon();
        return true;
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

    public String generateReport(TrackingMon data) {
        String snapshotTime = ClaraUtil.getCurrentTime();

        JSONObject dcRunTime = new JSONObject();
        dcRunTime.put("NbHBTracks", data.getNbHBTracks());           //number of hit-based tracks per event
        dcRunTime.put("NbTBTracks", data.getNbTBTracks());           //number of time-based tracks per event
        dcRunTime.put("NbCTTracks", data.getNbCTTracks());           //number of central tracks per event
        dcRunTime.put("NbHBHits", data.getNbHBHits());               //number of hit-based hits per event
        dcRunTime.put("NbTBHits", data.getNbTBHits());               //number of time-based hits per event
        dcRunTime.put("NbCTHits", data.getNbCTHits());               //number of central hits per event
        dcRunTime.put("NbHBHitsOnTrack", data.getNbHBHitsOnTrack()); //average number of hit-based hits on track per event
        dcRunTime.put("NbTBHitsOnTrack", data.getNbTBHitsOnTrack()); //average number of time-based hits on track per event
        dcRunTime.put("NbCTHitsOnTrack", data.getNbCTHitsOnTrack()); //average number of central hits on track per event
        dcRunTime.put("TimeResidual-1", data.getTimeResidual()[0]);  //average time residual-1 per region
        dcRunTime.put("TimeResidual-2", data.getTimeResidual()[1]);  //average time residual-2 per region
        dcRunTime.put("TimeResidual-3", data.getTimeResidual()[2]);  //average time residual-3 per region
        dcRunTime.put("snapshot_time", snapshotTime);
        dcRunTime.put("mon-class", monClass);
        dcRunTime.put("mon-detector", monDetector);

//        System.out.println(dcRunTime.toString(4));
        return dcRunTime.toString();
    }

    private xMsgMessage dcJsonMessage(TrackingMon data) {
        xMsgTopic topic = xMsgTopic.build(monClass, monDetector, type);
        return MessageUtil.buildRequest(topic, generateReport(data));
    }

    private void send(ZMQ.Socket con, xMsgMessage msg) throws xMsgException {
        msg.getMetaData().setSender(getName());
        ZMsg zmsg = new ZMsg();
        zmsg.add(msg.getTopic().toString());
        zmsg.add(msg.getMetaData().build().toByteArray());
        zmsg.add(msg.getData());
        zmsg.send(con);
    }


    public static void main(String[] args) throws FileNotFoundException {

        String inputFile = "/Users/ziegler/Workdir/Distribution/CLARA/CLARA_INSTALL/data/output/out_pion_smearz_gen_1.hipo";

        System.err.println(" \n[PROCESSING FILE] : " + inputFile);
        DCTimeLineEngine en = new DCTimeLineEngine();
        en.init();

        int counter = 0;

        HipoDataSource reader = new HipoDataSource();
        reader.open(inputFile);


        while (reader.hasEvent()) {
            counter++;

            DataEvent event = reader.getNextEvent();

            en.processDataEvent(event);

            System.out.println("  EVENT " + counter);

        }

    }

}
