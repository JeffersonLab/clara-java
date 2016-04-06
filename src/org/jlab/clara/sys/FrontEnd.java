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

import static org.jlab.clara.base.core.ClaraComponent.container;
import static org.jlab.clara.base.core.ClaraComponent.dpe;
import static org.jlab.clara.base.core.ClaraComponent.service;

import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.clara.base.core.ClaraBase;
import org.jlab.clara.base.core.ClaraComponent;
import org.jlab.clara.base.core.MessageUtils;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.sys.RequestParser.RequestException;
import org.jlab.clara.util.shell.ClaraFork;
import org.jlab.coda.xmsg.core.xMsgCallBack;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta.Builder;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.jlab.coda.xmsg.net.xMsgRegAddress;
import org.jlab.coda.xmsg.xsys.xMsgRegistrar;
import org.zeromq.ZContext;

import java.io.IOException;
import java.util.concurrent.TimeoutException;


class FrontEnd {

    private final ClaraBase base;

    private final ZContext context = new ZContext();
    private final xMsgRegistrar registrar;

    FrontEnd(xMsgProxyAddress frontEndAddress, int poolSize, String description)
            throws ClaraException {
        try {
            // create the xMsg registrar
            xMsgRegAddress regAddress = new xMsgRegAddress(frontEndAddress.host());
            registrar = new xMsgRegistrar(context, regAddress);

            // create the xMsg actor
            ClaraComponent frontEnd = dpe(frontEndAddress.host(),
                                          frontEndAddress.pubPort(),
                                          ClaraConstants.JAVA_LANG,
                                          poolSize,
                                          description);
            base = new ClaraBase(frontEnd, frontEnd) {
                @Override
                public void start() { }

                @Override
                protected void end() { }
            };
            base.setFrontEnd(frontEnd);
        } catch (xMsgException e) {
            throw new ClaraException("Cannot create front-end", e);
        }
    }


    public void start() throws ClaraException {
        try {
            // start registrar service
            registrar.start();

            // subscribe to forwarding requests
            xMsgTopic topic = xMsgTopic.build(ClaraConstants.DPE,
                                              base.getFrontEnd().getCanonicalName());
            base.listen(topic, new GatewayCallback());
            base.register(topic, base.getMe().getDescription());

            xMsgUtil.sleep(100);

        } catch (ClaraException e) {
            throw new ClaraException("Cannot start front-end", e);
        }
    }


    public void stop() {
        context.destroy();
        registrar.shutdown();
        base.destroy();
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

        if (dpeLang.equals(ClaraConstants.JAVA_LANG)) {
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
            xMsgTopic topic = MessageUtils.buildTopic(ClaraConstants.DPE, dpe.getCanonicalName());
            String data = MessageUtils.buildData(ClaraConstants.SET_FRONT_END,
                                              frontEndHost, frontEndPort, frontEndLang);
            base.send(dpe, MessageUtils.buildRequest(topic, data));
        } catch (xMsgException e) {
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
            xMsgTopic topic = MessageUtils.buildTopic(ClaraConstants.DPE, dpe.getCanonicalName());
            base.send(dpe, MessageUtils.buildRequest(topic, ClaraConstants.PING_DPE));
        } catch (xMsgException e) {
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
