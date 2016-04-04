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

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service container.
 *
 * @author gurjyan
 * @version 4.x
 */
class Container extends ClaraBase {


    private ConcurrentHashMap<String, Service> myServices = new ConcurrentHashMap<>();
    private ContainerReport myReport;

    Container(ClaraComponent comp, ClaraComponent frontEnd) throws ClaraException, xMsgException {
        super(comp, frontEnd);

        if (!comp.isContainer()) {
            throw new ClaraException("Clara-Error: incompatible component.");
        }

        // Create a socket connections to the dpe proxy
        releaseConnection(getConnection());

        // Subscribe messages published to this container
        xMsgTopic topic = xMsgTopic.build(ClaraConstants.CONTAINER, comp.getCanonicalName());

        // Register this subscriber
        register(topic, comp.getDescription());
        System.out.println(ClaraUtil.getCurrentTimeInH() + ": Registered container = " + comp.getCanonicalName());

        System.out.println(ClaraUtil.getCurrentTimeInH() + ": Started container = " + comp.getCanonicalName());

        myReport = new ContainerReport(comp.getCanonicalName());
        myReport.setLang(getMe().getDpeLang());
        myReport.setDescription(comp.getDescription());
        myReport.setAuthor(System.getenv("USER"));
        myReport.setStartTime(ClaraUtil.getCurrentTime());
    }

    @Override
    public void start() throws ClaraException {
        // nothing
    }

    @Override
    protected void end() {
        try {
            // broadcast to the local proxy
            String data = MessageUtils.buildData(ClaraConstants.CONTAINER_DOWN,
                                                 getMe().getContainerName());
            send(getFrontEnd(), data);

            removeRegistration(getMe().getTopic());
            removeAllServices();
        } catch (ClaraException | xMsgException | IOException e) {
            e.printStackTrace();
        }
    }

    public void addService(ClaraComponent comp, ClaraComponent frontEnd)
                           throws xMsgException, ClaraException, IOException {

        // in this case serviceName is a canonical nam
        String serviceName = comp.getCanonicalName();


        if (myServices.containsKey(serviceName)) {
            String msg = "%s Clara-Warning: service %s already exists. No new service is deployed%n";
            System.err.printf(msg, ClaraUtil.getCurrentTimeInH(), serviceName);
            return;
        }

        Service service = new Service(comp, frontEnd);
        myServices.put(serviceName, service);
    }

    /**
     *
     * @param serviceName service canonical name
     * @throws ClaraException
     */
    public void removeService(String serviceName)
            throws ClaraException, IOException, xMsgException {
        if (myServices.containsKey(serviceName)) {
            Service service = myServices.remove(serviceName);
            service.end();
        }
    }

    /**
     *
     * @throws ClaraException
     */
    public void removeAllServices() throws ClaraException, IOException, xMsgException {
        for (Service s : myServices.values()) {
            s.end();
        }
        myServices.clear();
    }

    public ConcurrentHashMap<String, Service> geServices() {
        return myServices;
    }

    public ContainerReport getReport() {
        return myReport;
    }
}
