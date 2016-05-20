/*
 * Copyright (C) 2015. Jefferson Lab, CLARA framework (JLAB). All Rights Reserved.
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for educational, research, and not-for-profit purposes,
 * without fee and without a signed licensing agreement.
 *
 * Contact Vardan Gyurjyan
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

package org.jlab.clara.sys;

import org.jlab.clara.base.core.ClaraBase;
import org.jlab.clara.base.core.ClaraComponent;
import org.jlab.clara.base.core.MessageUtil;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.coda.xmsg.core.xMsgCallBack;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgSubscription;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;
import org.jlab.coda.xmsg.excp.xMsgException;

abstract class AbstractActor {

    protected final ClaraBase base;

    private boolean running = false;
    private final Object lock = new Object();

    AbstractActor(ClaraComponent component, ClaraComponent fe) {
        this.base = new ClaraBase(component, fe) {
            @Override
            public void start() throws ClaraException { }

            @Override
            protected void end() { }
        };
    }

    public void start() throws ClaraException {
        synchronized (lock) {
            initialize();
            startMsg();
            running = true;
        }
    }

    public void stop() {
        synchronized (lock) {
            end();
            base.close();
            if (running) {
                running = false;
                stopMsg();
            }
        }
    }

    /**
     * Initializes the CLARA actor.
     */
    protected abstract void initialize() throws ClaraException;

    /**
     * Runs before closing the actor.
     */
    protected abstract void end();

    protected abstract void startMsg();

    protected abstract void stopMsg();

    /**
     * Listens for messages of given topic published to the address of this component,
     * and registers as a subscriber with the front-end.
     *
     * @param topic topic of interest
     * @param callback the callback action
     * @param description a description for the registration
     * @return a handler to the subscription
     * @throws ClaraException if the subscription could not be started or
     *                        if the registration failed
     */
    xMsgSubscription startRegisteredSubscription(xMsgTopic topic,
                                                 xMsgCallBack callback,
                                                 String description) throws ClaraException {
        xMsgSubscription sub = base.listen(topic, callback);
        try {
            base.register(topic, description);
        } catch (Exception e) {
            base.unsubscribe(sub);
            throw e;
        }
        return sub;
    }

    void sendResponse(xMsgMessage msg, xMsgMeta.Status status, String data) {
        try {
            xMsgMessage repMsg = MessageUtil.buildRequest(msg.getReplyTopic(), data);
            repMsg.getMetaData().setStatus(status);
            base.send(repMsg);
        } catch (xMsgException e) {
            e.printStackTrace();
        }
    }
}
