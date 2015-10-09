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

import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.EngineDataAccessor;
import org.jlab.coda.xmsg.core.*;
import org.jlab.coda.xmsg.data.xMsgR;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnection;
import org.jlab.coda.xmsg.net.xMsgConnectionOption;
import org.jlab.coda.xmsg.net.xMsgRegAddress;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.util.Set;
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
    private ClaraComponent myAddress;

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

    public ClaraBase(ClaraComponent me,
                     String defaultRegistrarHost,
                     int defaultRegistrarPort,
                     int subCallbackPoolSize)
            throws IOException {
        super(me.getName(),
                me.getDpeHost(), me.getDpePort(),
                defaultRegistrarHost, defaultRegistrarPort,
                subCallbackPoolSize);
        setDefaultConnectionOption(connectionOption);
        myAddress = me;
    }

    public ClaraBase(ClaraComponent me,
                     String defaultRegistrarHost,
                     int defaultRegistrarPort)
            throws IOException {
        this(me, defaultRegistrarHost, defaultRegistrarPort,
                xMsgConstants.DEFAULT_POOL_SIZE.getIntValue());
    }

    public ClaraBase(ClaraComponent me)
            throws IOException {
        this(me, xMsgUtil.localhost(),
                xMsgConstants.REGISTRAR_PORT.getIntValue(),
                xMsgConstants.DEFAULT_POOL_SIZE.getIntValue());
    }

    public void send(ClaraComponent component, xMsgMessage msg)
            throws xMsgException {
        xMsgConnection con = connect(component.getProxyAddress());
        publish(con,msg);
        release(con);
    }

    public void send(ClaraComponent component, String requestText)
            throws IOException, xMsgException {
        xMsgMessage msg = new xMsgMessage(component.getTopic(), requestText);
        xMsgConnection con = connect(component.getProxyAddress());
        publish(con,msg);
        release(con);
    }

    public xMsgMessage syncSend(ClaraComponent component, xMsgMessage msg, int timeout)
            throws xMsgException, TimeoutException {
        xMsgConnection con = connect(component.getProxyAddress());
        xMsgMessage m = syncPublish(con, msg, timeout);
        release(con);
        return m;
    }

    public xMsgMessage syncSend(ClaraComponent component, String requestText, int timeout)
            throws IOException, xMsgException, TimeoutException {
        xMsgMessage msg = new xMsgMessage(component.getTopic(), requestText);
        xMsgConnection con = connect(component.getProxyAddress());
        xMsgMessage m = syncPublish(con, msg, timeout);
        release(con);
        return m;
    }

    public xMsgSubscription listen(ClaraComponent component, xMsgCallBack callback)
            throws xMsgException {
        xMsgConnection con = connect(component.getProxyAddress());
        return subscribe(con, component.getTopic(), callback);
    }

    public xMsgSubscription listen(ClaraComponent component, xMsgTopic topic, xMsgCallBack callback)
            throws xMsgException {
        xMsgConnection con = connect(component.getProxyAddress());
        return subscribe(con, topic, callback);
    }

    public void stopListening(xMsgSubscription handle)
            throws xMsgException {
        unsubscribe(handle);
    }

    public void register(String regHost, int regPort, String description )
            throws IOException, xMsgException {
        xMsgRegAddress regAddress = new xMsgRegAddress(regHost, regPort);
        registerAsSubscriber(regAddress, myAddress.getTopic(), description);
    }

    public void register(String regHost, String description )
            throws IOException, xMsgException {
        xMsgRegAddress regAddress = new xMsgRegAddress(regHost);
        registerAsSubscriber(regAddress, myAddress.getTopic(), description);
    }

    public void register(String description )
            throws IOException, xMsgException {
        registerAsSubscriber(myAddress.getTopic(),description);
    }

    public void removeRegistration(String regHost, int regPort)
            throws IOException, xMsgException {
        xMsgRegAddress regAddress = new xMsgRegAddress(regHost, regPort);
        removeSubscriberRegistration(regAddress, myAddress.getTopic());
    }

    public void removeRegistration(String regHost)
            throws IOException, xMsgException {
        xMsgRegAddress regAddress = new xMsgRegAddress(regHost);
        removeSubscriberRegistration(regAddress, myAddress.getTopic());
    }

    public void removeRegistration()
            throws IOException, xMsgException {
        removeSubscriberRegistration(myAddress.getTopic());
    }

    public Set<xMsgR.xMsgRegistration> discover(String regHost, int regPort, xMsgTopic topic )
            throws IOException, xMsgException {
        xMsgRegAddress regAddress = new xMsgRegAddress(regHost, regPort);
        return findSubscribers(regAddress, topic);
    }

    public Set<xMsgR.xMsgRegistration> discover(String regHost, xMsgTopic topic)
            throws IOException, xMsgException {
        xMsgRegAddress regAddress = new xMsgRegAddress(regHost);
        return findSubscribers(regAddress, topic);
    }

    public Set<xMsgR.xMsgRegistration> discover(xMsgTopic topic )
            throws IOException, xMsgException {
        return findSubscribers(topic);
    }

    public void deploy(ClaraComponent component) throws ClaraException {
        if(component.isOrchestrator()) {
            throw new IllegalArgumentException("Clara-Error: can not deploy an orchestrator.");
        }
        if(component.isDpe()){

        } else if(component.isContainer()){

        } else if(component.isService()){

        } else {
            throw new ClaraException("Clara-Error: unknown or undefined component type. ");
        }

    }

}

