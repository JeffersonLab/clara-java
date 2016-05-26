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

import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.clara.base.core.ClaraComponent;
import org.jlab.clara.base.core.MessageUtil;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.util.report.ContainerReport;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.excp.xMsgException;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Service container.
 *
 * @author gurjyan
 * @version 4.x
 */
class Container extends AbstractActor {

    private final ConcurrentHashMap<String, Service> myServices = new ConcurrentHashMap<>();
    private final ContainerReport myReport;

    private boolean isRegistered = false;

    Container(ClaraComponent comp, ClaraComponent frontEnd) {
        super(comp, frontEnd);

        myReport = new ContainerReport(base, System.getenv("USER"));
    }

    @Override
    protected void initialize() throws ClaraException {
        register();
    }

    @Override
    protected void end() {
        removeAllServices();
        removeRegistration();
    }

    @Override
    protected void startMsg() {
        Logging.info("started container = %s", base.getName());
    }

    @Override
    protected void stopMsg() {
        Logging.info("removed container = %s", base.getName());
    }

    public void addService(ClaraComponent comp, ClaraComponent frontEnd)
                           throws ClaraException {
        String serviceName = comp.getCanonicalName();
        Service service = myServices.get(serviceName);
        if (service == null) {
            service = new Service(comp, frontEnd);
            Service result = myServices.putIfAbsent(serviceName, service);
            if (result == null) {
                try {
                    service.start();
                } catch (ClaraException e) {
                    service.stop();
                    myServices.remove(serviceName, service);
                    throw e;
                }
            } else {
                service.stop();    // destroy the extra engine object
            }
        } else {
            Logging.error("service = %s already exists. No new service is deployed", serviceName);
        }
    }

    public boolean removeService(String serviceName) {
        Service service = myServices.remove(serviceName);
        if (service != null) {
            service.stop();
            return true;
        }
        return false;
    }

    private void removeAllServices() {
        myServices.values().forEach(Service::stop);
        myServices.clear();
    }

    private void register() throws ClaraException {
        xMsgTopic topic = xMsgTopic.build(ClaraConstants.CONTAINER, base.getName());
        base.register(topic, base.getDescription());
        isRegistered = true;
    }

    private void removeRegistration() {
        if (isRegistered) {
            try {
                reportDown();
                base.removeRegistration(base.getMe().getTopic());
            } catch (ClaraException e) {
                Logging.error("container = %s: %s", base.getName(), e.getMessage());
            } finally {
                isRegistered = false;
            }
        }
    }

    private void reportDown() {
        try {
            // broadcast to the local proxy
            String data = MessageUtil.buildData(ClaraConstants.CONTAINER_DOWN, base.getName());
            base.send(base.getFrontEnd(), data);
        } catch (xMsgException e) {
            Logging.error("container = %s: could not send down report: %s",
                          base.getName(), e.getMessage());
        }
    }

    public void setFrontEnd(ClaraComponent frontEnd) {
        base.setFrontEnd(frontEnd);
    }

    public ConcurrentHashMap<String, Service> geServices() {
        return myServices;
    }

    public ContainerReport getReport() {
        return myReport;
    }
}
