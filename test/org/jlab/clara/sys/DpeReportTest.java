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

package org.jlab.clara.sys;

import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.coda.xmsg.core.xMsg;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.json.JSONObject;

public final class DpeReportTest {

    public static void main(String[] args) {
        xMsgProxyAddress dpeAddress = new xMsgProxyAddress("localhost");
        if (args.length > 0) {
            int port = Integer.parseInt(args[0]);
            dpeAddress = new xMsgProxyAddress("localhost", port);
        }
        xMsgTopic jsonTopic = xMsgTopic.build(ClaraConstants.DPE_REPORT);
        try (xMsg subscriber = new xMsg("report_subscriber")) {
            subscriber.subscribe(dpeAddress, jsonTopic, (msg) -> {
                try {
                    String data = new String(msg.getData());
                    String output = new JSONObject(data).toString(2);
                    System.out.println(output);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            xMsgUtil.keepAlive();
        } catch (xMsgException e) {
            e.printStackTrace();
        }
    }

    private DpeReportTest() { }
}
