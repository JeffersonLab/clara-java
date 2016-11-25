/*
 *   Copyright (c) 2016.  Jefferson Lab (JLab). All rights reserved. Permission
 *   to use, copy, modify, and distribute  this software and its documentation for
 *   educational, research, and not-for-profit purposes, without fee and without a
 *   signed licensing agreement.
 *
 *   IN NO EVENT SHALL JLAB BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL
 *   INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING
 *   OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF JLAB HAS
 *   BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 *   JLAB SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 *   THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 *   PURPOSE. THE CLARA SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY,
 *   PROVIDED HEREUNDER IS PROVIDED "AS IS". JLAB HAS NO OBLIGATION TO PROVIDE
 *   MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 *   This software was developed under the United States Government license.
 *   For more information contact author at gurjyan@jlab.org
 *   Department of Experimental Nuclear Physics, Jefferson Lab.
 */

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
