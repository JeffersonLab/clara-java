/*
 * Copyright (C) 2015. Jefferson Lab, CLARA framework (JLAB). All Rights Reserved.
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for educational, research, and not-for-profit purposes,
 * without fee and without a signed licensing agreement.
 *
 * Author Vardan Gyurjyan
 * Department of Experimental Nuclear Physics, Jefferson Lab.
 *
 * IN NO EVENT SHALL JLAB BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL,
 * INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF
 * THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF JLAB HAS BEEN ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * JLAB SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE. THE CLARA SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY, PROVIDED
 * HEREUNDER IS PROVIDED "AS IS". JLAB HAS NO OBLIGATION TO PROVIDE MAINTENANCE,
 * SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 */

package org.jlab.clara.base;

import org.jlab.clara.engine.EngineDataAccessor;
import org.jlab.coda.xmsg.core.*;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnection;
import org.jlab.coda.xmsg.net.xMsgConnectionOption;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 *  Clara base class providing methods build services,
 *  service container and orchestrator.
 *
 * @author gurjyan
 * @since 4.x
 */
public class ClaraBase extends xMsg {


    private EngineDataAccessor dataAccessor;


    private xMsgConnectionOption connectionOption = new xMsgConnectionOption() {
        @Override
        public void preConnection(ZMQ.Socket socket) {
            socket.setRcvHWM(0);
            socket.setSndHWM(0);
        }

        @Override
        public void postConnection() {
            xMsgUtil.sleep(100);
        }
    };

    public ClaraBase(ClaraAddress me,
                     String defaultRegistrarHost,
                     int defaultRegistrarPort,
                     int subCallbackPoolSize)
            throws IOException {
        super(me.getName(),
                me.getDpeHost(), me.getDpePort(),
                defaultRegistrarHost, defaultRegistrarPort,
                subCallbackPoolSize);
        setDefaultConnectionOption(connectionOption);
    }

    public ClaraBase(ClaraAddress me,
                     String defaultRegistrarHost,
                     int defaultRegistrarPort)
            throws IOException {
        this(me, defaultRegistrarHost, defaultRegistrarPort,
                xMsgConstants.DEFAULT_POOL_SIZE.getIntValue());
    }

    public ClaraBase(ClaraAddress me)
            throws IOException {
        this(me, xMsgUtil.localhost(),
                xMsgConstants.REGISTRAR_PORT.getIntValue(),
                xMsgConstants.DEFAULT_POOL_SIZE.getIntValue());
    }

    public void send(ClaraAddress address, xMsgMessage msg)
            throws xMsgException {
        xMsgConnection con = connect(address.getProxyAddress());
        publish(con,msg);
        release(con);
    }

    public void send(ClaraAddress address, String requestText)
            throws IOException, xMsgException {
        xMsgMessage msg = new xMsgMessage(address.getTopic(), requestText);
        xMsgConnection con = connect(address.getProxyAddress());
        publish(con,msg);
        release(con);
    }

    public xMsgMessage syncSend(ClaraAddress address, xMsgMessage msg, int timeout)
            throws xMsgException, TimeoutException {
        xMsgConnection con = connect(address.getProxyAddress());
        xMsgMessage m = syncPublish(con, msg, timeout);
        release(con);
        return m;
    }

    public xMsgMessage syncSend(ClaraAddress address, String requestText, int timeout)
            throws IOException, xMsgException, TimeoutException {
        xMsgMessage msg = new xMsgMessage(address.getTopic(), requestText);
        xMsgConnection con = connect(address.getProxyAddress());
        xMsgMessage m = syncPublish(con, msg, timeout);
        release(con);
        return m;
    }

    public xMsgSubscription listen(ClaraAddress address, xMsgCallBack callback)
            throws xMsgException {
        xMsgConnection con = connect(address.getProxyAddress());
        return subscribe(con, address.getTopic(), callback);
    }

    public xMsgSubscription listen(ClaraAddress address, xMsgTopic topic, xMsgCallBack callback)
            throws xMsgException {
        xMsgConnection con = connect(address.getProxyAddress());
        return subscribe(con, topic, callback);
    }

    public void stopListening(xMsgSubscription handle) throws xMsgException {
        unsubscribe(handle);
    }



}

