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

package org.jlab.clara.base;

import org.jlab.clara.base.error.ClaraException;
import org.jlab.coda.xmsg.core.xMsgConstants;

public class ServiceName implements ClaraName {

    private final ContainerName container;
    private final String engine;
    private final String canonicalName;

    /**
     * Identify a service with its container and engine name.
     *
     * @param container the name of the service container
     * @param engine the name of the service engine
     */
    public ServiceName(ContainerName container, String engine) {
        this.container = container;
        this.engine = engine;
        this.canonicalName = container.canonicalName() + xMsgConstants.TOPIC_SEP + engine;
    }

    /**
     * Identify a service with its DPE, container name and engine name.
     *
     * @param dpe the name of the DPE
     * @param container the name of the service container
     * @param engine the name of the service engine
     */
    public ServiceName(DpeName dpe, String container, String engine) {
        this(new ContainerName(dpe, container), engine);
    }

    /**
     * Identify a service with its host, language, container and engine name.
     *
     * @param host the host address of the DPE
     * @param lang the language of the DPE
     * @param container the name of the service container
     * @param engine the name of the service engine
     */
    public ServiceName(String host, ClaraLang lang, String container, String engine) {
        this(new ContainerName(new DpeName(host, lang), container), engine);
    }

    /**
     * Identify a service with its canonical name.
     *
     * @param canonicalName the canonical name of the service
     */
    public ServiceName(String canonicalName) {
        if (!ClaraUtil.isServiceName(canonicalName)) {
            throw new IllegalArgumentException("Invalid service name: " + canonicalName);
        }
        try {
            this.container = new ContainerName(ClaraUtil.getContainerCanonicalName(canonicalName));
            this.canonicalName = canonicalName;
            this.engine = ClaraUtil.getEngineName(canonicalName);
        } catch (ClaraException e) {
            throw new IllegalArgumentException("Invalid service name: " + canonicalName);
        }
    }

    @Override
    public String canonicalName() {
        return canonicalName;
    }

    @Override
    public String name() {
        return engine;
    }

    @Override
    public ClaraAddress address() {
        return container.address();
    }

    @Override
    public ClaraLang language() {
        return container.language();
    }

    public DpeName dpe() {
        return container.dpe();
    }

    public ContainerName container() {
        return container;
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
        ServiceName other = (ServiceName) obj;
        if (!canonicalName.equals(other.canonicalName)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return canonicalName;
    }
}
