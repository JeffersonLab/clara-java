/*
 *   Copyright (c) 2017.  Jefferson Lab (JLab). All rights reserved. Permission
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

package org.jlab.clara.std.orchestrators;

import org.jlab.clara.base.ServiceName;
import org.json.JSONObject;

class ServiceConfig {

    static final String READER = "reader";
    static final String WRITER = "writer";

    private static final String IO_CONFIG = "io-services";
    private static final String GLOBAL_CONFIG = "global";
    private static final String SERVICE_CONFIG = "services";

    private final JSONObject configData;

    ServiceConfig(JSONObject configData) {
        this.configData = configData;
    }

    JSONObject reader() {
        return getIO(READER);
    }

    JSONObject writer() {
        return getIO(WRITER);
    }

    private JSONObject getIO(String key) {
        JSONObject conf = new JSONObject();
        if (configData.has(IO_CONFIG)) {
            JSONObject ioConf = configData.getJSONObject(IO_CONFIG);
            if (ioConf.has(key)) {
                addServiceConfig(conf, ioConf, key);
            }
        }
        return conf;
    }

    JSONObject get(ServiceName service) {
        JSONObject conf = new JSONObject();
        if (configData.has(GLOBAL_CONFIG)) {
            addServiceConfig(conf, configData, GLOBAL_CONFIG);
        }
        if (configData.has(SERVICE_CONFIG)) {
            JSONObject services = configData.getJSONObject(SERVICE_CONFIG);
            if (services.has(service.name())) {
                addServiceConfig(conf, services, service.name());
            }
        }
        return conf;
    }

    private void addServiceConfig(JSONObject target, JSONObject parent, String serviceKey) {
        JSONObject config = parent.getJSONObject(serviceKey);
        for (String key : config.keySet()) {
            target.put(key, config.get(key));
        }
    }
}
