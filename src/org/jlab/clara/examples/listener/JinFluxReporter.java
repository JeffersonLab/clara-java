package org.jlab.clara.examples.listener;

import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.clara.util.report.JinFluxReportBuilder;
import org.jlab.coda.jinflux.JinFluxException;
import org.jlab.coda.xmsg.core.*;
import org.jlab.coda.xmsg.excp.xMsgException;

/**
 * Class description here....
 * <p>
 *
 * @author gurjyan
 *         Date 11/22/16
 * @version 3.x
 */
public class JinFluxReporter extends xMsg {
    private JinFluxReportBuilder myFluxReportBuilder = null;
    private String session;

    JinFluxReporter(String influxDb, String database, String session){
        super("JinFluxReporter", 1);
        this.session = session;

        // JinFluxReportBuilder initialization
        try {
            myFluxReportBuilder = new JinFluxReportBuilder(influxDb, database, session);
        } catch (JinFluxException e) {
            e.printStackTrace();
        }
        JinFluxReporter reporter = new JinFluxReporter("claraweb.jlab.org", "clara", session);

    }

    public void start() throws xMsgException {

        xMsgConnection connection = getConnection();
        xMsgTopic topic = xMsgTopic.build(ClaraConstants.DPE_REPORT, session, "*");

    }
}
