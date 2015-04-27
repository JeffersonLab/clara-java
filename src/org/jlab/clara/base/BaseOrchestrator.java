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

import java.net.SocketException;
import java.util.Random;

import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.sys.CBase;
import org.jlab.coda.xmsg.excp.xMsgException;


/**
 * Base class for orchestration of applications.
 */
public class BaseOrchestrator {

    private final CBase base;

    /**
     * Creates a new orchestrator. Uses localhost as front-end node.
     *
     * @throws ClaraException in case of connection errors
     */
    public BaseOrchestrator() throws ClaraException {
        try {
            base = getClaraBase("localhost");
            base.setName(generateName());
        } catch (SocketException | xMsgException e) {
            throw new ClaraException("Could not start orchestrator", e);
        }
    }


    /**
     * Creates a new orchestrator. Receives the location of the front-end node.
     *
     * @param frontEndHost the IP of the front-end node
     * @throws ClaraException in case of connection errors
     */
    public BaseOrchestrator(String frontEndHost) throws ClaraException {
        try {
            base = getClaraBase(frontEndHost);
            base.setName(generateName());
        } catch (SocketException | xMsgException e) {
            throw new ClaraException("Could not start orchestrator", e);
        }
    }


    /**
     * Creates the internal Clara object.
     * It can be overridden to return a mock for testing purposes.
     */
    CBase getClaraBase(String frontEndHost) throws SocketException, xMsgException {
        return new CBase(frontEndHost);
    }


    private String generateName() {
        Random rand = new Random();
        return "orchestrator" + rand.nextInt(1000) + ":" + "localhost";
    }
}
