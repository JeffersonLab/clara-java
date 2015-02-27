package org.jlab.clara.base;

import org.jlab.clara.util.CConstants;
import org.jlab.clara.util.CUtility;
import org.jlab.coda.xmsg.core.xMsgCallBack;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgD;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistrationData;
import org.jlab.coda.xmsg.excp.xMsgException;

import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;


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
        setName("orchestrator" + (int) (Math.random() * 100.0));
    }

    public OrchestratorBase(String dpeHost,
                            String feHost)
            throws xMsgException, SocketException {
        super(dpeHost, feHost);
        setName("orchestrator" + (int) (Math.random() * 100.0));
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
    public void listen_errors(xMsgCallBack call_back,
                              int severity_id) throws xMsgException {
        if(severity_id>0 && severity_id<4) {
            genericReceive(xMsgConstants.ERROR.getStringValue() + ":" + severity_id,
                    call_back);
        } else {
            genericReceive(xMsgConstants.ERROR.getStringValue(),
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
    public void listen_warnings(xMsgCallBack call_back,
                                int severity_id) throws xMsgException {
        if(severity_id>0 && severity_id<4) {
            genericReceive(xMsgConstants.WARNING.getStringValue() + ":" + severity_id,
                    call_back);
        } else {
            genericReceive(xMsgConstants.WARNING.getStringValue(),
                    call_back);
        }
    }

    /**
     * Subscribes all info messages generated from services
     * of entire Clara cloud.
     * Note: DONE messages from services are reported
     * using INFO messages envelope. User must check received
     * xMsgD.Data object STRING filed to see if this is
     * DONE message and react accordingly.
     *
     * @param call_back user call back function
     * @throws xMsgException
     */
    public void listen_infos(xMsgCallBack call_back)
            throws xMsgException {
        genericReceive(xMsgConstants.INFO.getStringValue(),
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
    public void listen_error_from(String sName,
                                  xMsgCallBack call_back,
                                  int severity_id)
            throws xMsgException, CException {
        if(severity_id>0 && severity_id<4) {
            genericReceive(xMsgConstants.ERROR.getStringValue() + ":" +
                            severity_id + ":" +
                            sName,
                    call_back);
        } else {
            throw new CException("unsupported severity ID");
        }
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
    public void listen_warning_from(String sName,
                                    xMsgCallBack call_back,
                                    int severity_id)
            throws xMsgException, CException {
        if(severity_id>0 && severity_id<4) {
            genericReceive(xMsgConstants.WARNING.getStringValue() + ":" +
                            severity_id + ":" +
                            sName,
                    call_back);
        } else {
            throw new CException("unsupported severity ID");
        }
    }

    /**
     * Subscribes info messages from  a specified service.
     * Note: DONE messages from services are reported
     * using INFO messages envelope. User must check received
     * xMsgD.Data object STRING filed to see if this is
     * DONE message and react accordingly.
     *
     * @param call_back user call back function
     * @throws xMsgException
     */
    public void listen_info_from(String sName,
                                 xMsgCallBack call_back)
            throws xMsgException{
        genericReceive(xMsgConstants.INFO.getStringValue() + ":" +
                        sName,
                call_back);
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
            throws xMsgException {

        genericSend(CConstants.DPE + ":" + dpeName,
                CConstants.START_CONTAINER + "?" + containerName);

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
    public void stop_container(String dpeName, String containerName)
            throws xMsgException {

        genericSend(CConstants.DPE + ":" + dpeName,
                CConstants.REMOVE_CONTAINER + "?" + containerName);

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
            throws xMsgException {

        genericSend(CConstants.CONTAINER + ":" + containerName,
                CConstants.DEPLOY_SERVICE + "?" + engineName + "?" + objectPoolSize);

    }

    /**
     * Asks container to deploy a service
     * @param containerName canonical name of the container
     * @param engineName user specified engine name, i.e. engine class name
     * @throws xMsgException
     */
    public void start_service(String containerName,
                              String engineName)
            throws xMsgException {

        genericSend(CConstants.CONTAINER + ":" + containerName,
                CConstants.DEPLOY_SERVICE + "?" + engineName);

    }

    /**
     * <p>
     *     Sends the receiving service
     *     string = CConstants.RUN_SERVICE.
     *     The name can be a canonical name of
     *     a service or service engine name.
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
            throws xMsgException, CException {

        // Check the passed service name
        if(!CUtility.isCanonical(name)) {
             throw new CException("not a canonical name");
        }
        serviceSend(name, data);

    }

    /**
     * Asks container to deploy a service
     *
     * @param containerName canonical name of the container
     * @param serviceName canonical name of the service
     * @throws xMsgException
     */
    public void stop_service(String containerName,
                              String serviceName)
            throws xMsgException {

        genericSend(CConstants.CONTAINER + ":" +containerName,
                CConstants.REMOVE_SERVICE + "?" +serviceName);

    }

}
