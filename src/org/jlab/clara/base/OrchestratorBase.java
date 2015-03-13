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

import org.jlab.clara.util.CConstants;
import org.jlab.clara.util.CUtility;
import org.jlab.coda.xmsg.core.SubscriptionHandler;
import org.jlab.coda.xmsg.core.xMsgCallBack;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgD;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistrationData;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgAddress;
import org.jlab.coda.xmsg.net.xMsgConnection;

import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.TimeoutException;


/**
 * <p>
 *    Base class for Clara application orchestrator classes.
 * </p>
 *
 * @author gurjyan
 * @version 1.x
 * @since 2/6/15
 */
public class OrchestratorBase extends CBase {

    public OrchestratorBase()
            throws xMsgException, SocketException {
        super();
        setName("orchestrator" + (int) (Math.random() * 100.0) + ":" + "localhost");
    }

    public OrchestratorBase(String dpeHost,
                            String feHost)
            throws xMsgException, SocketException {
        super(dpeHost, feHost);
        setName("orchestrator" + (int) (Math.random() * 100.0) + ":" + "localhost");
    }

    /**
     * <p>
     * Asks the Registrar service of a specified DPE
     * (host) to return the registration information of
     * service/services based on dpe_name
     * </p>
     *
     * @param dpe_name name of the required DPE
     * @return List of xMsgRegistrationData objects
     */
    public List<xMsgRegistrationData> get_service_by_host(String dpe_name)
            throws xMsgException, CException, SocketException {

        String s_name = CUtility.form_service_name(dpe_name,
                xMsgConstants.ANY.getStringValue(),
                xMsgConstants.ANY.getStringValue());
        return find_service(s_name);
    }

    /**
     * <p>
     * Asks the Registrar service of a specified DPE
     * (host) to return the registration information of
     * service/services based on container name
     * Note:  takes the first network card IP address
     * (if node has multiple network cards)
     * </p>
     *
     * @param container_name name of the required container
     * @return List of xMsgRegistrationData objects
     */
    public List<xMsgRegistrationData> get_service_by_container(String container_name)
            throws xMsgException, CException, SocketException {

        String s_name = CUtility.form_service_name(xMsgUtil.getLocalHostIps().get(0),
                container_name,
                xMsgConstants.ANY.getStringValue());
        return find_service(s_name);
    }

    /**
     * <p>
     * Asks the Registrar service of a specified DPE
     * (host) to return the registration information of
     * service/services based on container name
     * </p>
     *
     * @param dpe_name       name of the required DPE
     * @param container_name name of the required container
     * @return List of xMsgRegistrationData objects
     */
    public List<xMsgRegistrationData> get_service_by_container(String dpe_name,
                                                               String container_name)
            throws xMsgException, CException, SocketException {

        String s_name = CUtility.form_service_name(dpe_name,
                container_name,
                xMsgConstants.ANY.getStringValue());
        return find_service(s_name);
    }

    /**
     * <p>
     * Asks the Registrar service of a specified DPE
     * (host) to return the registration information of
     * service/services based on engine name
     * Note:  takes the first network card IP address
     * (if node has multiple network cards)
     * </p>
     *
     * @param engine_name name of the required container
     * @return List of xMsgRegistrationData objects
     */
    public List<xMsgRegistrationData> get_service_by_engine(String engine_name)
            throws xMsgException, CException, SocketException {

        String s_name = CUtility.form_service_name(xMsgUtil.getLocalHostIps().get(0),
                xMsgConstants.ANY.getStringValue(),
                engine_name);
        return find_service(s_name);
    }

    /**
     * <p>
     * Asks the Registrar service of a specified DPE
     * (host) to return the registration information of
     * service/services based on engine name
     * </p>
     *
     * @param dpe_name    name of the required DPE
     * @param engine_name name of the required container
     * @return List of xMsgRegistrationData objects
     */
    public List<xMsgRegistrationData> get_service_by_engine(String dpe_name,
                                                            String engine_name)
            throws xMsgException, CException, SocketException {

        String s_name = CUtility.form_service_name(dpe_name,
                xMsgConstants.ANY.getStringValue(),
                engine_name);
        return find_service(s_name);
    }


    /**
     * <p>
     *    Asks the Registrar service of a specified DPE
     *    to return the description of a service
     *    based on the name of the service container and
     *    the name of the service engine
     *
     * </p>
     * @param dpe name of the required DPE
     * @param container name of the required container
     * @param engine name of the engine
     * @return service engine description
     */
    public String get_service_description(String dpe,
                                          String container,
                                          String engine)
            throws xMsgException, CException, SocketException {

        String s_name = CUtility.form_service_name(dpe, container, engine);
        List<xMsgRegistrationData> s = find_service(s_name);
        return s.get(0).getDescription();
    }

    /**
     * <p>
     *    This method accepts composition that has engine names in it,
     *    and asks platform Discovery service to see if composition
     *    engines are deployed as services and returns composition
     *    with service canonical names. If at least one service is not
     *    deployed this method throws CException.
     *
     * </p>
     * @return canonical name of the application composition
     */
    public String engineToCanonical(String composition)
            throws xMsgException, CException, SocketException {

        // find branching compositions in supplied composition string
        StringTokenizer st = new StringTokenizer(composition, ";");

        // List of sub compositions, i.e. branched compositions
        List<String> sub_comps = new ArrayList<>();
        while(st.hasMoreTokens()){
            sub_comps.add(st.nextToken());
        }

        // final canonical composition string
        StringBuilder can_comp = new StringBuilder();

        // Go over the sub compositions
        for (String sb:sub_comps){

            // Find participating services engine names in the composition
            st = new StringTokenizer(sb, "+");

            // List of engine names within the sub composition
            List<String> se_list = new ArrayList<>();
            while(st.hasMoreTokens()){
                se_list.add(st.nextToken());
            }

            // Canonical sub composition string
            StringBuilder sub_can_comp = new StringBuilder();

            // Go over engine names with the sub composition
            for(String se:se_list){

                // Check to see if we have multiple
                // service outputs as an input to a
                // service, i.e. logical OR (a+c,b)
                if(se.contains(",")){
                    st = new StringTokenizer(se, ",");
                    StringBuilder or_can = new StringBuilder();
                    while(st.hasMoreTokens()) {
                        List<xMsgRegistrationData> tsn = get_service_by_engine(st.nextToken());
                        if(tsn.size()<=0){
                            throw new CException("no registration record fond");
                        }
                        String can = tsn.get(0).getName();
                        or_can.append(can).append(",");
                    }
                    // remove the last character and add
                    // to the sub canonical composition
                    sub_can_comp.append(or_can.substring(0, or_can.length()- 1)).append("+");

                    // logical AND case. (a,b+&c)
                } else if (se.startsWith("&")){
                    if(get_service_by_engine(CUtility.remove_first(se)).size()<=0){
                        throw new CException("no registration record fond");
                    }
                    String can = get_service_by_engine(CUtility.remove_first(se)).get(0).getName();
                    sub_can_comp.append("&").append(can).append("+");

                    // single engine case (a+b)
                } else {

                    if(get_service_by_engine(se).size()<=0){
                        throw new CException("no registration record fond");
                    }
                    String can = get_service_by_engine(se).get(0).getName();
                    sub_can_comp.append(can).append("+");
                }
            }
            String str_sub_can_comp = CUtility.remove_last( sub_can_comp.toString());
                    can_comp.append(str_sub_can_comp).append(";");
        }
        return CUtility.remove_last(can_comp.toString());
    }

    /**
     * <p>
     *     Parses composition string and returns the names of
     *     all services participating in the application.
     *     Note. services in the composition must have canonical names.
     * </p>
     * @param composition application canonical composition
     * @return List of service canonical names
     * @throws xMsgException
     * @throws CException
     * @throws SocketException
     */
    public List<String> getAllServiceNames(String composition)
            throws xMsgException, CException, SocketException {

         List<String> result = new ArrayList<>();

        // find branching compositions in supplied composition string
        StringTokenizer st = new StringTokenizer(composition, ";");

        // List of sub compositions, i.e. branched compositions
        List<String> sub_comps = new ArrayList<>();
        while(st.hasMoreTokens()){
            sub_comps.add(st.nextToken());
        }

        // Go over the sub compositions
        for (String sb:sub_comps){

            // Find participating services engine names in the composition
            st = new StringTokenizer(sb, "+");

            // List of service names within the sub composition
            List<String> se_list = new ArrayList<>();
            while(st.hasMoreTokens()){
                se_list.add(st.nextToken());
            }

            // Go over engine names with the sub composition
            for(String se:se_list){

                // Check to see if we have multiple
                // service outputs as an input to a
                // service, i.e. logical OR (a+c,b)
                if(se.contains(",")){
                    st = new StringTokenizer(se, ",");
                    while(st.hasMoreTokens()) {
                        result.add(st.nextToken());
                     }

                    // logical AND case. (a,b+&c)
                } else if (se.startsWith("&")){
                    String can = CUtility.remove_first(se);
                    result.add(can);

                    // single engine case (a+b)
                } else {
                    result.add(se);
                }
            }
        }
        return result;
    }

    /**
     * <p>
     *     Gets first service name (Clara canonical name)
     *     of the application composition.
     * </p>
     * @param composition application composition
     * @return Canonical name of the service
     * @throws xMsgException
     * @throws CException
     * @throws SocketException
     */
    public String getFirstServiceName(String composition)
            throws xMsgException, CException, SocketException {
        return getAllServiceNames(composition).get(0);
    }

    /**
     * Subscribes all error messages generated from services
     * of entire Clara cloud.
     *
     * @param call_back user call back function
     * @param severity_id if 0 > id < 4 will report only the
     *                    required severity otherwise all
     * @throws xMsgException
     */
    public SubscriptionHandler  listen_errors(xMsgCallBack call_back,
                              int severity_id) throws xMsgException, SocketException {

        // Create a socket connections to the xMsg node.
        xMsgAddress address;
        if(getFeHostName().equals(xMsgConstants.UNDEFINED.getStringValue())) {
            address = new xMsgAddress("localhost");
        } else {
            address = new xMsgAddress(getFeHostName());
        }
        xMsgConnection socket = getNewConnection(address);

        if(severity_id>0 && severity_id<4) {
            return genericReceive(socket, xMsgConstants.ERROR.getStringValue() + ":" + severity_id,
                    call_back);
        } else {
            return genericReceive(socket, xMsgConstants.ERROR.getStringValue(),
                    call_back);
        }
    }

    /**
     * Subscribes all warning messages generated from services
     * of entire Clara cloud.
     *
     * @param call_back user call back function
     * @param severity_id if 0 > id < 4 will report only the
     *                    required severity otherwise all
     * @throws xMsgException
     */
    public SubscriptionHandler  listen_warnings(xMsgCallBack call_back,
                                int severity_id) throws xMsgException, SocketException {

        // Create a socket connections to the xMsg node.
        xMsgAddress address;
        if(getFeHostName().equals(xMsgConstants.UNDEFINED.getStringValue())) {
            address = new xMsgAddress("localhost");
        } else {
            address = new xMsgAddress(getFeHostName());
        }
        xMsgConnection socket = getNewConnection(address);

        if(severity_id>0 && severity_id<4) {
            return genericReceive(socket, xMsgConstants.WARNING.getStringValue() + ":" + severity_id,
                    call_back);
        } else {
            return genericReceive(socket, xMsgConstants.WARNING.getStringValue(),
                    call_back);
        }
    }

    /**
     * Subscribes all info messages generated from services
     * of entire Clara cloud.
     *
     * @param call_back user call back function
     * @throws xMsgException
     */
    public SubscriptionHandler  listen_infos(xMsgCallBack call_back)
            throws xMsgException, SocketException {

        // Create a socket connections to the xMsg node.
        xMsgAddress address;
        if(getFeHostName().equals(xMsgConstants.UNDEFINED.getStringValue())) {
            address = new xMsgAddress("localhost");
        } else {
            address = new xMsgAddress(getFeHostName());
        }
        xMsgConnection socket = getNewConnection(address);

        return genericReceive(socket, xMsgConstants.INFO.getStringValue(),
                call_back);
    }

    /**
     * Subscribes info messages from  a specified service.
     *
     * @param call_back user call back function
     * @throws xMsgException
     */
    public SubscriptionHandler  listen_info_from(String sName,
                                                 xMsgCallBack call_back)
            throws xMsgException, SocketException {

        // Create a socket connections to the xMsg node.
        xMsgAddress address;
        if(getFeHostName().equals(xMsgConstants.UNDEFINED.getStringValue())) {
            address = new xMsgAddress("localhost");
        } else {
            address = new xMsgAddress(getFeHostName());
        }
        xMsgConnection socket = getNewConnection(address);

        return genericReceive(socket, xMsgConstants.INFO.getStringValue() + ":" +
                        sName,
                call_back);
    }

    /**
     * Subscribes error messages of a certain severity
     * generated from  a specified service.
     * Note: accepted severities are 1 - 3.
     * If required severity is outside of this range
     * CException will be thrown
     *
     * @param call_back user call back function
     * @param severity_id if 0 > id < 4 will report only the
     *                    required severity otherwise all
     * @throws xMsgException
     */
    public SubscriptionHandler  listen_error_from(String sName,
                                  xMsgCallBack call_back,
                                  int severity_id)
            throws xMsgException, CException, SocketException {

        // Create a socket connections to the xMsg node.
        xMsgAddress address;
        if(getFeHostName().equals(xMsgConstants.UNDEFINED.getStringValue())) {
            address = new xMsgAddress("localhost");
        } else {
            address = new xMsgAddress(getFeHostName());
        }
        xMsgConnection socket = getNewConnection(address);

        if(severity_id>0 && severity_id<4) {
            return genericReceive(socket, xMsgConstants.ERROR.getStringValue() + ":" +
                            sName  + ":" +
                            severity_id,
                    call_back);
        } else {
            throw new CException("unsupported severity ID");
        }
    }

    /**
     * Subscribes all error messages
     * generated from  a specified service.
     * Note: accepted severities are 1 - 3.
     * If required severity is outside of this range
     * CException will be thrown
     *
     * @param call_back user call back function
     * @throws xMsgException
     */
    public SubscriptionHandler listen_error_from(String sName,
                                  xMsgCallBack call_back)
            throws xMsgException, CException, SocketException {

        // Create a socket connections to the xMsg node.
        xMsgAddress address;
        if(getFeHostName().equals(xMsgConstants.UNDEFINED.getStringValue())) {
            address = new xMsgAddress("localhost");
        } else {
            address = new xMsgAddress(getFeHostName());
        }
        xMsgConnection socket = getNewConnection(address);

            return genericReceive(socket, xMsgConstants.ERROR.getStringValue() + ":" +
                            sName,
                    call_back);
    }

    /**
     * Subscribes warning messages of a certain severity
     * generated from  a specified service.
     * Note: accepted severities are 1 - 3.
     * If required severity is outside of this range
     * CException will be thrown
     *
     * @param call_back user call back function
     * @param severity_id if 0 > id < 4 will report only the
     *                    required severity otherwise all
     * @throws xMsgException
     */
    public SubscriptionHandler  listen_warning_from(String sName,
                                    xMsgCallBack call_back,
                                    int severity_id)
            throws xMsgException, CException, SocketException {

        // Create a socket connections to the xMsg node.
        xMsgAddress address;
        if(getFeHostName().equals(xMsgConstants.UNDEFINED.getStringValue())) {
            address = new xMsgAddress("localhost");
        } else {
            address = new xMsgAddress(getFeHostName());
        }
        xMsgConnection socket = getNewConnection(address);

        if(severity_id>0 && severity_id<4) {
            return genericReceive(socket, xMsgConstants.WARNING.getStringValue() + ":" +
                            sName + ":" +
                            severity_id,
                    call_back);
        } else {
            throw new CException("unsupported severity ID");
        }
    }

    /**
     * Subscribes all warning messages
     * generated from  a specified service.
     * Note: accepted severities are 1 - 3.
     * If required severity is outside of this range
     * CException will be thrown
     *
     * @param call_back user call back function
     * @throws xMsgException
     */
    public SubscriptionHandler  listen_warning_from(String sName,
                                    xMsgCallBack call_back)
            throws xMsgException, CException, SocketException {

        // Create a socket connections to the xMsg node.
        xMsgAddress address;
        if(getFeHostName().equals(xMsgConstants.UNDEFINED.getStringValue())) {
            address = new xMsgAddress("localhost");
        } else {
            address = new xMsgAddress(getFeHostName());
        }
        xMsgConnection socket = getNewConnection(address);

            return genericReceive(socket, xMsgConstants.WARNING.getStringValue() + ":" +
                            sName,
                    call_back);
    }

    /**
     * Subscribes done messages from all services
     *
     * @param call_back user call back function
     * @throws xMsgException
     */
    public SubscriptionHandler  listen_done(xMsgCallBack call_back)
            throws xMsgException, SocketException {

        // Create a socket connections to the xMsg node.
        xMsgAddress address;
        if(getFeHostName().equals(xMsgConstants.UNDEFINED.getStringValue())) {
            address = new xMsgAddress("localhost");
        } else {
            address = new xMsgAddress(getFeHostName());
        }
        xMsgConnection socket = getNewConnection(address);

        return genericReceive(socket, xMsgConstants.DONE.getStringValue(),
                call_back);
    }

    /**
     * Subscribes done messages from all services
     *
     * @param call_back user call back function
     * @throws xMsgException
     */
    public SubscriptionHandler  listen_done_from(String sName,
                                 xMsgCallBack call_back)
            throws xMsgException, SocketException {

        // Create a socket connections to the xMsg node.
        xMsgAddress address;
        if(getFeHostName().equals(xMsgConstants.UNDEFINED.getStringValue())) {
            address = new xMsgAddress("localhost");
        } else {
            address = new xMsgAddress(getFeHostName());
        }
        xMsgConnection socket = getNewConnection(address);

        return genericReceive(socket, xMsgConstants.DONE.getStringValue() + ":" +
                sName,
                call_back);
    }

    /**
     * Subscribes data messages from all services
     *
     * @param call_back user call back function
     * @throws xMsgException
     */
    public SubscriptionHandler  listen_data(xMsgCallBack call_back)
            throws xMsgException, SocketException {

        // Create a socket connections to the xMsg node.
        xMsgAddress address;
        if(getFeHostName().equals(xMsgConstants.UNDEFINED.getStringValue())) {
            address = new xMsgAddress("localhost");
        } else {
            address = new xMsgAddress(getFeHostName());
        }
        xMsgConnection socket = getNewConnection(address);

        return genericReceive(socket, xMsgConstants.DONE.getStringValue(),
                call_back);
    }

    /**
     * Subscribes data messages from all services
     *
     * @param call_back user call back function
     * @throws xMsgException
     */
    public SubscriptionHandler  listen_data_from(String sName,
                                 xMsgCallBack call_back)
            throws xMsgException, SocketException {

        // Create a socket connections to the xMsg node.
        xMsgAddress address;
        if(getFeHostName().equals(xMsgConstants.UNDEFINED.getStringValue())) {
            address = new xMsgAddress("localhost");
        } else {
            address = new xMsgAddress(getFeHostName());
        }
        xMsgConnection socket = getNewConnection(address);

        return genericReceive(socket, xMsgConstants.DONE.getStringValue() + ":" +
                sName,
                call_back);
    }

    /**
     * <p>
     *     Stops listening to a specific topic
     * </p>
     * @param handler SubscriptionHandler for a specific subscription (listening)
     * @throws xMsgException
     */
    public void stop_listening(SubscriptionHandler handler)
            throws xMsgException {
        cancelReceive(handler);
    }

    /**
     * <p>
     *     Asks DPE to start a container
     * </p>
     *
     * @param dpeName DPE canonical name, i.e. IP
     *                of the host where DPE is running
     * @param containerName user given name of the service container
     * @throws xMsgException
     */
    public void start_container(String dpeName,
                                String containerName)
            throws xMsgException, SocketException {

        genericSend(dpeName,
                CConstants.DPE + ":" + dpeName,
                CConstants.START_CONTAINER + "?" + containerName);

    }

    /**
     * <p>
     *     Sync asks DPE to start a container
     * </p>
     *
     * @param dpeName DPE canonical name, i.e. IP
     *                of the host where DPE is running
     * @param containerName user given name of the service container
     * @param timeOut in seconds.
     * @throws xMsgException
     */
    public void sync_start_container(String dpeName,
                                     String containerName,
                                     int timeOut)
            throws xMsgException, SocketException, TimeoutException {

        genericSyncSend(dpeName,
                CConstants.DPE + ":" + dpeName,
                CConstants.START_CONTAINER + "?" + containerName,
                timeOut);

    }

    /**
     * <p>
     *     Asks DPE to stop a container
     * </p>
     *
     * @param dpeName DPE canonical name, i.e. IP
     *                of the host where DPE is running
     * @param containerName canonical name of the service container
     * @throws xMsgException
     */
    public void stop_container(String dpeName,
                               String containerName)
            throws xMsgException, SocketException {

        genericSend(dpeName,
                CConstants.DPE + ":" + dpeName,
                CConstants.REMOVE_CONTAINER + "?" + containerName);

    }

    /**
     * <p>
     *     Sync asks DPE to stop a container
     * </p>
     *
     * @param dpeName DPE canonical name, i.e. IP
     *                of the host where DPE is running
     * @param containerName canonical name of the service container
     * @param timeOut in seconds
     * @throws xMsgException
     */
    public void sync_stop_container(String dpeName,
                                    String containerName,
                                    int timeOut)
            throws xMsgException, SocketException, TimeoutException {

        genericSyncSend(dpeName,
                CConstants.DPE + ":" + dpeName,
                CConstants.REMOVE_CONTAINER + "?" + containerName,
                timeOut);

    }

    /**
     * Asks container to deploy a service
     * @param containerName canonical name of the container
     * @param engineName user specified engine name, i.e. engine class name
     * @param objectPoolSize object pool size to hold required
     *                       service objects for multi-threading
     * @throws xMsgException
     */
    public void start_service(String containerName,
                              String engineName,
                              int objectPoolSize)
            throws xMsgException, SocketException, CException {

        String dpeName = CUtility.getDpeName(containerName);

        genericSend(dpeName,
                CConstants.CONTAINER + ":" + containerName,
                CConstants.DEPLOY_SERVICE + "?" + engineName + "?" + objectPoolSize);

    }

    /**
     * sync asks container to deploy a service
     *
     * @param containerName canonical name of the container
     * @param engineName user specified engine name, i.e. engine class name
     * @param objectPoolSize object pool size to hold required
     *                       service objects for multi-threading
     * @throws xMsgException
     */
    public void sync_start_service(String containerName,
                                   String engineName,
                                   int objectPoolSize,
                                   int timeOut)
            throws xMsgException, SocketException, CException, TimeoutException {

        String dpeName = CUtility.getDpeName(containerName);

        genericSyncSend(dpeName,
                CConstants.CONTAINER + ":" + containerName,
                CConstants.DEPLOY_SERVICE + "?" + engineName + "?" + objectPoolSize,
                timeOut);

    }

    /**
     * Asks container to deploy a service
     * @param containerName canonical name of the container
     * @param engineName user specified engine name, i.e. engine class name
     * @throws xMsgException
     */
    public void start_service(String containerName,
                              String engineName)
            throws xMsgException, CException, SocketException {

        String dpeName = CUtility.getDpeName(containerName);

        genericSend(dpeName,
                CConstants.CONTAINER + ":" + containerName,
                CConstants.DEPLOY_SERVICE + "?" + engineName);

    }

    /**
     * Sync asks container to deploy a service
     * @param containerName canonical name of the container
     * @param engineName user specified engine name, i.e. engine class name
     * @throws xMsgException
     */
    public void sync_start_service(String containerName,
                                   String engineName,
                                   int timeOut)
            throws xMsgException, CException, SocketException, TimeoutException {

        String dpeName = CUtility.getDpeName(containerName);

        genericSyncSend(dpeName,
                CConstants.CONTAINER + ":" + containerName,
                CConstants.DEPLOY_SERVICE + "?" + engineName,
                timeOut);

    }

    /**
     * <p>
     *     Sends transient data to a service
     * </p>
     *
     * @param name canonical name or engine
     *             name of a service
     * @param data xMsgD.Data.Builder object
     *
     * @throws xMsgException
     */
    public void run_service(String name,
                            xMsgD.Data.Builder data)
            throws xMsgException, CException, SocketException {

        // Check the passed service name
        if(!CUtility.isCanonical(name)) {
             throw new CException("not a canonical name");
        }
        serviceSend(name, data);

    }

    /**
     * Asks container to stop the service
     *
     * @param containerName canonical name of the container
     * @param serviceName canonical name of the service
     * @throws xMsgException
     */
    public void stop_service(String containerName,
                              String serviceName)
            throws xMsgException, CException, SocketException {

        String dpeName = CUtility.getDpeName(containerName);

        genericSend(dpeName,
                CConstants.CONTAINER + ":" + containerName,
                CConstants.REMOVE_SERVICE + "?" + serviceName);

    }

    /**
     * Sync asks container to stop the service
     *
     * @param containerName canonical name of the container
     * @param serviceName   canonical name of the service
     * @throws xMsgException
     */
    public void sync_stop_service(String containerName,
                                  String serviceName,
                                  int timeOut)
            throws xMsgException, CException, SocketException, TimeoutException {

        String dpeName = CUtility.getDpeName(containerName);

        genericSyncSend(dpeName,
                CConstants.CONTAINER + ":" + containerName,
                CConstants.REMOVE_SERVICE + "?" + serviceName,
                timeOut);

    }

    /**
     * Asks service to report done
     *
     * @param  serviceName canonical name of the service
     * @param eventCount report every event count
     * @throws xMsgException
     */
    public void report_done(String serviceName,
                              int eventCount)
            throws xMsgException, CException, SocketException {

        // Check the passed service name
        if(!CUtility.isCanonical(serviceName)) {
            throw new CException("not a canonical name");
        }
        serviceSend(serviceName,
                CConstants.SERVICE_REPORT_DONE + "?" + eventCount);

    }

    public void stop_done_reporting(String serviceName) throws CException, xMsgException, SocketException {
        // Check the passed service name
        if(!CUtility.isCanonical(serviceName)) {
            throw new CException("not a canonical name");
        }
        serviceSend(serviceName,
                CConstants.SERVICE_REPORT_DONE + "?" + 0);

    }

    /**
     * Asks service to report data
     *
     * @param  serviceName canonical name of the service
     * @param eventCount report every event count
     * @throws xMsgException
     */
    public void report_data(String serviceName,
                              int eventCount)
            throws xMsgException, CException, SocketException {

        // Check the passed service name
        if(!CUtility.isCanonical(serviceName)) {
            throw new CException("not a canonical name");
        }
        serviceSend(serviceName,
                CConstants.SERVICE_REPORT_DATA + "?" + eventCount);

    }

    public void stop_data_reporting(String serviceName) throws CException, xMsgException, SocketException {
        // Check the passed service name
        if(!CUtility.isCanonical(serviceName)) {
            throw new CException("not a canonical name");
        }
        serviceSend(serviceName,
                CConstants.SERVICE_REPORT_DATA + "?" + 0);

    }

}
