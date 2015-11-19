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

import org.jlab.clara.util.ClaraUtil;
import org.jlab.coda.xmsg.core.xMsgConstants;

/**
 * The name of a Clara DPE.
 */
public class DpeName implements ClaraName {

    private final String name;

    /**
     * Identify a DPE with host and language.
     * The default port will be used.
     *
     * @param host the host address where the DPE is running
     * @param lang the language of the DPE
     */
    public DpeName(String host, ClaraLang lang) {
        name = new StringBuilder().append(host).append(xMsgConstants.LANG_SEP)
                                  .append(lang).toString();
    }

    /**
     * Identify a DPE with host, port and language.
     *
     * @param host the host address where the DPE is running
     * @param lang the port used by the DPE
     * @param lang the language of the DPE
     */
    public DpeName(String host, int port, ClaraLang lang) {
        name = new StringBuilder().append(host).append(xMsgConstants.PRXHOSTPORT_SEP)
                                  .append(port).append(xMsgConstants.LANG_SEP)
                                  .append(lang).toString();
    }

    /**
     * Identify a DPE with a canonical name.
     *
     * @param canonicalName the canonical name of the DPE
     */
    public DpeName(String canonicalName) {
        if (!ClaraUtil.isDpeName(canonicalName)) {
            throw new IllegalArgumentException("Invalid DPE name: " + canonicalName);
        }
        name = canonicalName;
    }

    @Override
    public String canonicalName() {
        return name;
    }

    @Override
    public String name() {
        return name;
    }
}
