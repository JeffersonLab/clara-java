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

package org.jlab.clara.util.report;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author gurjyan
 * @version 4.x
 */
public class ContainerReport extends BaseReport {

    private Map<String, ServiceReport> services = new ConcurrentHashMap<>();

    public ContainerReport(String name) {
        super(name);
    }

    public int getServiceCount() {
        return services.size();
    }

    public Collection<ServiceReport> getServices() {
        return services.values();
    }

    public void setServices(Map<String, ServiceReport> services) {
        this.services = services;
    }

    public void addService(ServiceReport sr) {
        if (!services.containsKey(sr.getName())) {
            services.put(sr.getName(), sr);
        }
    }

    public void removeService(ServiceReport sr) {
        if (services.containsKey(sr.getName())) {
            services.remove(sr.getName());
        }
    }

    public void removeAllServices() {
        services.clear();
    }
}
