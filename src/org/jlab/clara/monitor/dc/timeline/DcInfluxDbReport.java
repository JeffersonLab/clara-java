package org.jlab.clara.monitor.dc.timeline;

import org.influxdb.dto.Point;
import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.clara.monitor.IDROptionParser;
import org.jlab.coda.jinflux.JinFlux;
import org.jlab.coda.jinflux.JinFluxException;
import org.jlab.coda.jinflux.JinTime;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.json.JSONObject;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by gurjyan on 10/3/17.
 */
public class DcInfluxDbReport extends DcTListenerAndReporter {

    private final String dbName = "clara";
    private final String dbNode;

    private JinFlux jinFlux;
    private boolean jinFxConnected = true;

    private Map<String, String> tags = new HashMap<>();
    private Point.Builder p;

    private DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    DcInfluxDbReport(String name, String proxyHost, int proxyPort, String dbNode) {
        super(name, new xMsgProxyAddress(proxyHost, proxyPort));
        this.dbNode = dbNode;
        try {
            jinFlux = new JinFlux(dbNode);
            if (!jinFlux.existsDB(dbName)) {
                jinFlux.createDB(dbName, 1, JinTime.HOURE);
            }
        } catch (JinFluxException e) {
            jinFxConnected = false;
            e.printStackTrace();
        }
    }

    @Override
    public void report(String jsonString) {

        try {
            jinFlux = new JinFlux(dbNode);
            if (!jinFlux.existsDB(dbName)) {
                jinFlux.createDB(dbName, 1, JinTime.HOURE);
            }

        } catch (JinFluxException e) {
            jinFxConnected = false;
            e.printStackTrace();
        }

        if (jinFxConnected) {

            tags.clear();

            JSONObject base = new JSONObject(jsonString);

            String mon_class = base.getString("mon-class");
            String mon_detector = base.getString("mon-detector");

            int hb_tracks = base.getInt("NbHBTracks");
            int tb_tracks = base.getInt("NbTBTracks");
            int ct_tracks = base.getInt("NbCTTracks");
            int hb_hits = base.getInt("NbHBHits");
            int tb_hits = base.getInt("NbTBHits");
            int ct_hits = base.getInt("NbCTHits");
            int hb_hits_track = base.getInt("NbHBHitsOnTrack");
            int tb_hits_track = base.getInt("NbTBHitsOnTrack");
            int ct_hits_track = base.getInt("NbCTHitsOnTrack");
            double time_res_1 = ((Number)base.get("TimeResidual-1")).doubleValue();
            double time_res_2 = ((Number)base.get("TimeResidual-2")).doubleValue();
            double time_res_3 = ((Number)base.get("TimeResidual-3")).doubleValue();

            String sessionId = "mon-dc-timeline";
            System.out.println(dateFormat.format(new Date()) + ": reporting for " + sessionId);

            tags.put(ClaraConstants.SESSION, "mon-dc-timeline");
            tags.put("mon-class", mon_class);
            tags.put("mon-detector", mon_detector);

            p = jinFlux.openTB("dc_timeline", tags);
            jinFlux.addDP(p, "NbHBTracks", hb_tracks);
            jinFlux.addDP(p, "NbTBTracks", tb_tracks);
            jinFlux.addDP(p, "NbCTTracks", ct_tracks);
            jinFlux.addDP(p, "NbHBHits", hb_hits);
            jinFlux.addDP(p, "NbTBHits", tb_hits);
            jinFlux.addDP(p, "NbCTHits", ct_hits);
            jinFlux.addDP(p, "NbHBHitsOnTrack", hb_hits_track);
            jinFlux.addDP(p, "NbTBHitsOnTrack", tb_hits_track);
            jinFlux.addDP(p, "NbCTHitsOnTrack", ct_hits_track);
            jinFlux.addDP(p, "TimeResidual-1", time_res_1);
            jinFlux.addDP(p, "TimeResidual-2", time_res_2);
            jinFlux.addDP(p, "TimeResidual-3", time_res_3);

            try {
                jinFlux.write(dbName, p);
            } catch (JinFluxException e) {
                e.printStackTrace();
            }
        }
    }

    private static DcInfluxDbReport createInfluxReporter(IDROptionParser parser) {
        String name = UUID.randomUUID().toString();
        return new DcInfluxDbReport(
            name,
            parser.getProxyHost(),
            parser.getProxyPort(),
            parser.getDataBaseHost()
        );
    }

    public static void main(String[] args) {
        IDROptionParser options = new IDROptionParser();
        if (!options.parse(args)) {
            System.exit(1);
        }
        if (options.hasHelp()) {
            System.out.println(options.usage());
            System.exit(0);
        }
        try (DcInfluxDbReport rep = createInfluxReporter(options)) {
            rep.start();
        } catch (xMsgException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
