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

import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.clara.base.error.ClaraException;

/**
 * The name of a Clara DPE.
 */
public class DpeName implements ClaraName {

    private final ClaraAddress address;
    private final ClaraLang language;
    private final String name;

    /**
     * Identify a DPE with host and language.
     * The default port will be used.
     *
     * @param host the host address where the DPE is running
     * @param lang the language of the DPE
     */
    public DpeName(String host, ClaraLang lang) {
        address = new ClaraAddress(host);
        language = lang;
        name = host + ClaraConstants.LANG_SEP + lang;
    }

    /**
     * Identify a DPE with host, port and language.
     *
     * @param host the host address where the DPE is running
     * @param port the port used by the DPE
     * @param lang the language of the DPE
     */
    public DpeName(String host, int port, ClaraLang lang) {
        address = new ClaraAddress(host, port);
        language = lang;
        name = host + ClaraConstants.PORT_SEP +
               port + ClaraConstants.LANG_SEP +
               lang;
    }

    /**
     * Identify a DPE with a canonical name.
     *
     * @param canonicalName the canonical name of the DPE
     */
    public DpeName(String canonicalName) {
        if (!ClaraUtil.isCanonicalName(canonicalName)) {
            throw new IllegalArgumentException("Invalid canonical name: " + canonicalName);
        }
        try {
            String host = ClaraUtil.getDpeHost(canonicalName);
            int port = ClaraUtil.getDpePort(canonicalName);
            address = new ClaraAddress(host, port);
            language = ClaraLang.fromString(ClaraUtil.getDpeLang(canonicalName));
            name = canonicalName;
        } catch (ClaraException e) {
            throw new IllegalArgumentException("Invalid canonical name: " + canonicalName, e);
        }
    }

    @Override
    public String canonicalName() {
        return name;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public ClaraLang language() {
        return language;
    }

    @Override
    public ClaraAddress address() {
        return address;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + name.hashCode();
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
        DpeName other = (DpeName) obj;
        return name.equals(other.name);
    }

    @Override
    public String toString() {
        return name;
    }
}
