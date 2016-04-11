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

import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.clara.base.core.ClaraBase;
import org.jlab.clara.base.core.ClaraComponent;
import org.jlab.clara.base.core.MessageUtils;
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
class Container extends ClaraBase {

    private final ConcurrentHashMap<String, Service> myServices = new ConcurrentHashMap<>();
    private final ContainerReport myReport;

    private boolean isRegistered = false;

    Container(ClaraComponent comp, ClaraComponent frontEnd) {
        super(comp, frontEnd);

        myReport = new ContainerReport(comp.getCanonicalName());
        myReport.setLang(getMe().getDpeLang());
        myReport.setDescription(comp.getDescription());
        myReport.setAuthor(System.getenv("USER"));
    }

    @Override
    public void start() throws ClaraException {
        register();
        myReport.setStartTime(ClaraUtil.getCurrentTime());
        System.out.printf("%s: started container = %s%n",
                          ClaraUtil.getCurrentTimeInH(), getMe().getCanonicalName());
    }

    @Override
    protected void end() {
        removeAllServices();
        removeRegistration();
        System.out.printf("%s: removed container = %s%n",
                          ClaraUtil.getCurrentTimeInH(), getMe().getCanonicalName());
    }

    public void addService(ClaraComponent comp, ClaraComponent frontEnd)
                           throws ClaraException {
        // in this case serviceName is a canonical nam
        String serviceName = comp.getCanonicalName();

        if (myServices.containsKey(serviceName)) {
            String msg = "%s: service = %s already exists. No new service is deployed%n";
            System.err.printf(msg, ClaraUtil.getCurrentTimeInH(), serviceName);
            return;
        }

        Service service = new Service(comp, frontEnd);
        service.start();

        myServices.put(serviceName, service);
    }

    public void removeService(String serviceName) throws ClaraException {
        if (myServices.containsKey(serviceName)) {
            Service service = myServices.remove(serviceName);
            service.close();
        }
    }

    private void removeAllServices() {
        for (Service s : myServices.values()) {
            s.close();
        }
        myServices.clear();
    }

    private void register() throws ClaraException {
        xMsgTopic topic = xMsgTopic.build(ClaraConstants.CONTAINER, getMe().getCanonicalName());
        register(topic, getMe().getDescription());
        isRegistered = true;
    }

    private void removeRegistration() {
        if (isRegistered) {
            try {
                reportDown();
                removeRegistration(getMe().getTopic());
            } catch (ClaraException e) {
                System.err.printf("%s: container = %s: %s%n", ClaraUtil.getCurrentTimeInH(),
                                  getMe().getCanonicalName(), e.getMessage());
            }
        }
    }

    private void reportDown() {
        try {
            // broadcast to the local proxy
            String data = MessageUtils.buildData(ClaraConstants.CONTAINER_DOWN,
                                                 getMe().getContainerName());
            send(getFrontEnd(), data);
        } catch (xMsgException e) {
            System.out.printf("%s: container = %s: could not send down report: %s%n",
                              ClaraUtil.getCurrentTimeInH(),
                              getMe().getCanonicalName(),
                              e.getMessage());
        }
    }

    public ConcurrentHashMap<String, Service> geServices() {
        return myServices;
    }

    public ContainerReport getReport() {
        return myReport;
    }
}
