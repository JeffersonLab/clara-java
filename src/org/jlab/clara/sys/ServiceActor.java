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

import org.jlab.clara.base.core.ClaraBase;
import org.jlab.clara.base.core.ClaraComponent;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.coda.xmsg.core.xMsgConnection;
import org.jlab.coda.xmsg.core.xMsgConnectionPool;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;

class ServiceActor {

    private final ClaraBase base;
    private final ConnectionPools connectionPools;

    ServiceActor(ClaraComponent me, ClaraComponent frontEnd, ConnectionPools connectionPools) {
        this.base = new ClaraBase(me, frontEnd);
        this.connectionPools = connectionPools;
    }

    public void close() {
        base.close();
    }

    public void start() throws ClaraException {
        base.cacheLocalConnection();
    }

    public void send(xMsgMessage msg) throws ClaraException {
        sendMsg(connectionPools.mainPool, getLocal(), msg);
    }

    public void send(xMsgProxyAddress address, xMsgMessage msg) throws ClaraException {
        sendMsg(connectionPools.mainPool, address, msg);
    }

    public void sendUncheck(xMsgMessage msg) throws ClaraException {
        sendMsg(connectionPools.uncheckedPool, getLocal(), msg);
    }

    public void sendUncheck(xMsgProxyAddress address, xMsgMessage msg) throws ClaraException {
        sendMsg(connectionPools.uncheckedPool, address, msg);
    }

    private void sendMsg(xMsgConnectionPool pool, xMsgProxyAddress address, xMsgMessage msg)
            throws ClaraException {
        try (xMsgConnection con = pool.getConnection(address)) {
            base.send(con, msg);
        } catch (xMsgException e) {
            throw new ClaraException("Could not send message", e);
        }
    }

    public String getName() {
        return base.getName();
    }

    public String getEngine() {
        return base.getMe().getEngineName();
    }

    public xMsgProxyAddress getLocal() {
        return base.getDefaultProxyAddress();
    }

    public xMsgProxyAddress getFrontEnd() {
        return base.getFrontEnd().getProxyAddress();
    }
}
