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

package org.jlab.clara.base;

import org.jlab.clara.base.error.ClaraException;
import org.jlab.coda.xmsg.core.xMsgConstants;

public class ContainerName implements ClaraName {

    private final DpeName dpe;
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
        this.dpe = dpe;
        this.canonicalName = dpe.canonicalName() + xMsgConstants.TOPIC_SEP + name;
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
            this.dpe = new DpeName(ClaraUtil.getDpeName(canonicalName));
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

    @Override
    public ClaraAddress address() {
        return dpe.address();
    }

    @Override
    public ClaraLang language() {
        return dpe.language();
    }

    public DpeName dpe() {
        return dpe;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + canonicalName.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ContainerName other = (ContainerName) obj;
        return canonicalName.equals(other.canonicalName);
    }

    @Override
    public String toString() {
        return canonicalName;
    }
}
