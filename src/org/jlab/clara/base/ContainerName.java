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
import org.jlab.clara.util.ClaraUtil;
import org.jlab.coda.xmsg.core.xMsgConstants;

public class ContainerName implements ClaraName {

    private final String canonicalName;
    private final String name;

    /**
     * Identify a container with host and language of its DPE and name.
     *
     * @param host the host address of the DPE
     * @param lang the language of the DPE
     * @param name the name of the container
     */
    public ContainerName(String host, ClaraLang lang, String name) {
        this(new DpeName(host, lang), name);
    }

    /**
     * Identify a container with its DPE and name.
     *
     * @param dpe the DPE of the container
     * @param name the name of the container
     */
    public ContainerName(DpeName dpe, String name) {
        this.canonicalName = new StringBuilder().append(dpe.canonicalName())
                                                .append(xMsgConstants.TOPIC_SEP)
                                                .append(name).toString();
        this.name = name;
    }

    /**
     * Identify a container with its canonical name.
     *
     * @param canonicalName the canonical name of the container
     */
    public ContainerName(String canonicalName) {
        if (!ClaraUtil.isContainerName(canonicalName)) {
            throw new IllegalArgumentException("Invalid container name: " + canonicalName);
        }
        try {
            this.name = ClaraUtil.getContainerName(canonicalName);
            this.canonicalName = canonicalName;
        } catch (ClaraException e) {
            // TODO Auto-generated catch block
            throw new IllegalArgumentException("Invalid container name: " + canonicalName);
        }
    }

    @Override
    public String canonicalName() {
        return canonicalName;
    }

    @Override
    public String name() {
        return name;
    }
}
