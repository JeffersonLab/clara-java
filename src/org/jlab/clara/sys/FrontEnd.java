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
import org.jlab.clara.base.core.ClaraBase;
import org.jlab.clara.base.core.ClaraComponent;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.sys.RequestParser.RequestException;
import org.jlab.coda.xmsg.core.xMsgCallBack;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta.Builder;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.sys.xMsgRegistrar;
import org.zeromq.ZContext;

class FrontEnd {

    private final ClaraBase base;

    private final ZContext context = new ZContext();
    private final xMsgRegistrar registrar;

    FrontEnd(ClaraComponent frontEnd)
            throws ClaraException {
        try {
            // create the xMsg registrar
            registrar = new xMsgRegistrar(context, ClaraBase.getRegAddress(frontEnd));

            // create the xMsg actor
            base = new ClaraBase(frontEnd, frontEnd) {
                @Override
                public void start() { }

                @Override
                protected void end() { }
            };
        } catch (xMsgException e) {
            throw new ClaraException("Could not create front-end", e);
        }
    }


    public void start() throws ClaraException {
        // start registrar service
        registrar.start();

        // subscribe to forwarding requests
        xMsgTopic topic = xMsgTopic.build(ClaraConstants.DPE,
                                          base.getFrontEnd().getCanonicalName());
        base.listen(topic, new GatewayCallback());
        base.register(topic, base.getMe().getDescription());

        xMsgUtil.sleep(100);
    }


    public void stop() {
        context.destroy();
        registrar.shutdown();
        base.destroy();
    }


    private void startDpe(RequestParser parser, xMsgMeta.Builder meta)
            throws RequestException, ClaraException {
        // TODO implement this
    }


    private void stopDpe(RequestParser parser, Builder metadata)
            throws RequestException, ClaraException {
        // TODO implement this
    }


    private void setFrontEnd(RequestParser parser, Builder metadata)
            throws RequestException, ClaraException {
        // TODO implement this
    }


    private void pingDpe(RequestParser parser, Builder metadata)
            throws RequestException, ClaraException {
        // TODO implement this
    }


    private void startContainer(RequestParser parser, Builder metadata)
            throws RequestException, ClaraException {
        // TODO implement this
    }


    private void stopContainer(RequestParser parser, Builder metadata)
            throws RequestException, ClaraException {
        // TODO implement this
    }


    private void startService(RequestParser parser, Builder metadata)
            throws RequestException, ClaraException {
        // TODO implement this
    }


    private void stopService(RequestParser parser, Builder metadata)
            throws RequestException, ClaraException {
        // TODO implement this
    }


    /**
     * DPE callback.
     * <p>
     * The topic of this subscription is:
     * topic = CConstants.DPE + ":" + dpeCanonicalName
     * <p>
     * The following are accepted message data:
     * <li>
     *     CConstants.START_DPE ?
     *     dpeHost ? dpePort ? dpeLang ? poolSize ? regHost ? regPort ? description
     * </li>
     * <li>
     *     CConstants.STOP_REMOTE_DPE ?
     *     dpeHost ? dpePort ? dpeLang
     * </li>
     * <li>
     *     CConstants.SET_FRONT_END_REMOTE ?
     *     dpeHost ? dpePort ? dpeLang ? frontEndHost ? frontEndPort ? frontEndLang
     * </li>
     * <li>
     *     CConstants.PING_REMOTE_DPE ?
     *     dpeHost ? dpePort ? dpeLang
     * </li>
     * <li>
     *     CConstants.START_REMOTE_CONTAINER ?
     *     dpeHost ? dpePort ? dpeLang ? containerName ? poolSize ? description
     * </li>
     * <li>
     *     CConstants.STOP_REMOTE_CONTAINER ?
     *     dpeHost ? dpePort ? dpeLang ? containerName
     * </li>
     * <li>
     *     CConstants.START_REMOTE_SERVICE ?
     *     dpeHost ? dpePort ? dpeLang ? containerName ? engineName ? engineClass ?
     *     poolSize ? description ? initialState
     * </li>
     * <li>
     *     CConstants.STOP_REMOTE_SERVICE ?
     *     dpeHost ? dpePort ? dpeLang ? containerName ? engineName
     * </li>
     */
    private class GatewayCallback implements xMsgCallBack {

        @Override
        public void callback(xMsgMessage msg) {
            xMsgMeta.Builder metadata = msg.getMetaData();
            try {
                RequestParser parser = RequestParser.build(msg);
                String cmd = parser.nextString();

                switch (cmd) {
                    case ClaraConstants.START_DPE:
                        startDpe(parser, metadata);
                        break;

                    case ClaraConstants.STOP_REMOTE_DPE:
                        stopDpe(parser, metadata);
                        break;

                    case ClaraConstants.SET_FRONT_END_REMOTE:
                        setFrontEnd(parser, metadata);
                        break;

                    case ClaraConstants.PING_REMOTE_DPE:
                        pingDpe(parser, metadata);
                        break;

                    case ClaraConstants.START_REMOTE_CONTAINER:
                        startContainer(parser, metadata);
                        break;

                    case ClaraConstants.STOP_REMOTE_CONTAINER:
                        stopContainer(parser, metadata);
                        break;

                    case ClaraConstants.START_REMOTE_SERVICE:
                        startService(parser, metadata);
                        break;

                    case ClaraConstants.STOP_REMOTE_SERVICE:
                        stopService(parser, metadata);
                        break;

                    default:
                        break;
                }
            } catch (RequestException e) {
                e.printStackTrace();
            } catch (ClaraException e) {
                e.printStackTrace();
            }
        }
    }
}
