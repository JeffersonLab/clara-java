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

package org.jlab.clara.sys;

import static org.jlab.clara.base.ClaraComponent.container;
import static org.jlab.clara.base.ClaraComponent.dpe;
import static org.jlab.clara.base.ClaraComponent.service;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.jlab.clara.base.ClaraBase;
import org.jlab.clara.base.ClaraComponent;
import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.sys.RequestParser.RequestException;
import org.jlab.clara.util.CConstants;
import org.jlab.clara.util.shell.ClaraFork;
import org.jlab.coda.xmsg.core.xMsgCallBack;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgSubscription;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta.Builder;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.jlab.coda.xmsg.xsys.xMsgRegistrar;
import org.zeromq.ZContext;

class FrontEnd {

    private ClaraBase base;

    private xMsgRegistrar registrar;
    private xMsgSubscription fwdSubscription;


    public FrontEnd(xMsgProxyAddress frontEndAddress, int poolSize, String description)
            throws ClaraException {
        try {
            // create the xMsg registrar
            registrar = new xMsgRegistrar(new ZContext());
            registrar.start();

            // create the xMsg actor
            ClaraComponent frontEnd = dpe(frontEndAddress.host(),
                                          frontEndAddress.port(),
                                          CConstants.JAVA_LANG,
                                          poolSize,
                                          description);
            base = new ClaraBase(frontEnd, frontEnd) {
                @Override
                public void start(ClaraComponent component) { }

                @Override
                public void end() { }
            };
            base.setFrontEnd(frontEnd);

            // subscribe to forwarding requests
            xMsgTopic topic = xMsgTopic.wrap(CConstants.DPE + ":" + frontEnd.getCanonicalName());
            fwdSubscription = base.listen(topic, new GatewayCallback());
            base.register(topic, description);

        } catch (xMsgException e) {
            throw new ClaraException("Cannot create front-end", e);
        }
    }


    private void startDpe(RequestParser parser, xMsgMeta.Builder meta)
            throws RequestException, ClaraException {
        String dpeHost = parser.nextString();
        int dpePort = parser.nextInteger();
        String dpeLang = parser.nextString();
        int poolSize = parser.nextInteger();
        String regHost = parser.nextString();
        int regPort = parser.nextInteger();
        String description = parser.nextString();

        if (dpeLang.equals(CConstants.JAVA_LANG)) {
            StringBuilder cmd = new StringBuilder();
            cmd.append("ssh").append(" ").append(dpeHost).append(" ");
            cmd.append("-DpePort").append(" ").append(dpePort).append(" ");
            cmd.append("-PoolSize").append(" ").append(poolSize).append(" ");
            cmd.append("-RegHost").append(" ").append(regHost).append(" ");
            cmd.append("-RegPort").append(" ").append(regPort).append(" ");
            cmd.append("-Description").append(" ").append(description).append(" ");
            ClaraFork.fork(cmd.toString(), false);
        } else {
            throw new RequestException("Unsupported DPE language: " + dpeLang);
        }
    }


    private void stopDpe(RequestParser parser, Builder metadata)
            throws RequestException, ClaraException {
        String dpeHost = parser.nextString();
        int dpePort = parser.nextInteger();
        String dpeLang = parser.nextString();
        ClaraComponent dpe = dpe(dpeHost, dpePort, dpeLang, 1, "");
        try {
            base.exit(dpe);
        } catch (xMsgException | IOException | TimeoutException e) {
            throw new ClaraException("Could not stop DPE " + dpe, e);
        }
    }


    private void setFrontEnd(RequestParser parser, Builder metadata)
            throws RequestException, ClaraException {
        String dpeHost = parser.nextString();
        int dpePort = parser.nextInteger();
        String dpeLang = parser.nextString();
        String frontEndHost = parser.nextString();
        int frontEndPort = parser.nextInteger();
        String frontEndLang = parser.nextString();

        ClaraComponent dpe = dpe(dpeHost, dpePort, dpeLang, 1, "");
        try {
            xMsgTopic topic = ClaraUtil.buildTopic(CConstants.DPE, dpe.getCanonicalName());
            String data = ClaraUtil.buildData(CConstants.SET_FRONT_END,
                                              frontEndHost, frontEndPort, frontEndLang);
            base.send(dpe, new xMsgMessage(topic, data));
        } catch (xMsgException | IOException e) {
            throw new ClaraException("Could not set front-end of " + dpe, e);
        }
    }


    private void pingDpe(RequestParser parser, Builder metadata)
            throws RequestException, ClaraException {
        String dpeHost = parser.nextString();
        int dpePort = parser.nextInteger();
        String dpeLang = parser.nextString();
        ClaraComponent dpe = dpe(dpeHost, dpePort, dpeLang, 1, "");
        try {
            xMsgTopic topic = ClaraUtil.buildTopic(CConstants.DPE, dpe.getCanonicalName());
            base.send(dpe, new xMsgMessage(topic, CConstants.PING_DPE));
        } catch (xMsgException | IOException e) {
            throw new ClaraException("Could not ping DPE " + dpe, e);
        }
    }


    private void startContainer(RequestParser parser, Builder metadata)
            throws RequestException, ClaraException {
        String dpeHost = parser.nextString();
        int dpePort = parser.nextInteger();
        String dpeLang = parser.nextString();
        String containerName = parser.nextString();
        int poolSize = parser.nextInteger();
        String description = parser.nextString();

        ClaraComponent cont = container(dpeHost, dpePort, dpeLang, containerName,
                                        poolSize, description);
        try {
            base.deploy(cont);
        } catch (TimeoutException | xMsgException | IOException e) {
            throw new ClaraException("Could not start container " + cont, e);
        }
    }


    private void stopContainer(RequestParser parser, Builder metadata)
            throws RequestException, ClaraException {
        String dpeHost = parser.nextString();
        int dpePort = parser.nextInteger();
        String dpeLang = parser.nextString();
        String containerName = parser.nextString();
        ClaraComponent cont = container(dpeHost, dpePort, dpeLang, containerName, 1, "");
        try {
            base.exit(cont);
        } catch (TimeoutException | xMsgException | IOException e) {
            throw new ClaraException("Could not stop container " + cont, e);
        }
    }


    private void startService(RequestParser parser, Builder metadata)
            throws RequestException, ClaraException {
        String dpeHost = parser.nextString();
        int dpePort = parser.nextInteger();
        String dpeLang = parser.nextString();
        String containerName = parser.nextString();
        String engineName = parser.nextString();
        String engineClass = parser.nextString();
        int poolSize = parser.nextInteger();
        String description = parser.nextString();
        String initialState = parser.nextString();

        ClaraComponent service = service(dpeHost, dpePort, dpeLang,
                                         containerName, engineName, engineClass,
                                         poolSize, description, initialState);
        try {
            base.deploy(service);
        } catch (TimeoutException | xMsgException | IOException e) {
            throw new ClaraException("Could not start service " + service, e);
        }
    }


    private void stopService(RequestParser parser, Builder metadata)
            throws RequestException, ClaraException {
        String dpeHost = parser.nextString();
        int dpePort = parser.nextInteger();
        String dpeLang = parser.nextString();
        String containerName = parser.nextString();
        String engineName = parser.nextString();
        ClaraComponent service = service(dpeHost, dpePort, dpeLang, containerName, engineName);
        try {
            base.exit(service);
        } catch (TimeoutException | xMsgException | IOException e) {
            throw new ClaraException("Could not stop service " + service, e);
        }
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
        public xMsgMessage callback(xMsgMessage msg) {
            xMsgMeta.Builder metadata = msg.getMetaData();
            try {
                RequestParser parser = RequestParser.build(msg);
                String cmd = parser.nextString();

                switch (cmd) {
                    case CConstants.START_DPE:
                        startDpe(parser, metadata);
                        break;

                    case CConstants.STOP_REMOTE_DPE:
                        stopDpe(parser, metadata);
                        break;

                    case CConstants.SET_FRONT_END_REMOTE:
                        setFrontEnd(parser, metadata);
                        break;

                    case CConstants.PING_REMOTE_DPE:
                        pingDpe(parser, metadata);
                        break;

                    case CConstants.START_REMOTE_CONTAINER:
                        startContainer(parser, metadata);
                        break;

                    case CConstants.STOP_REMOTE_CONTAINER:
                        stopContainer(parser, metadata);
                        break;

                    case CConstants.START_REMOTE_SERVICE:
                        startService(parser, metadata);
                        break;

                    case CConstants.STOP_REMOTE_SERVICE:
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
            return msg;
        }
    }
}
