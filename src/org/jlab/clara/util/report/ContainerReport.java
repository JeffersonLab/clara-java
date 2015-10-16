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
package org.jlab.clara.util.report;

import java.util.HashMap;
import java.util.Map;

/**
 * @author gurjyan
 * @version 4.x
 */
public class ContainerReport extends CReportBase {
    private int serviceCount;

    private Map<String, ServiceReport> services = new HashMap<>();

    public int getServiceCount() {
        return serviceCount;
    }

    public void setServiceCount(int serviceCount) {
        this.serviceCount = serviceCount;
    }

    public Map<String, ServiceReport> getServices() {
        return services;
    }

    public void setServices(Map<String, ServiceReport> services) {
        this.services = services;
    }

    public void addServiceReport(ServiceReport sr) {
        if (!services.containsKey(sr.getName())) {
            services.put(sr.getName(), sr);
        }
    }

    public void removeServiceReport(String serviceName) {
        if (services.containsKey(serviceName)) {
            services.remove(serviceName);
        }
    }

    public void removeAllServiceReportings() {
        services.clear();
    }
}
