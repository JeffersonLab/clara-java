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
package org.jlab.clara.sys.ccc;

import org.jlab.clara.base.CException;
import org.jlab.clara.engine.EngineData;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgMessage;

import java.util.*;

/**
 * <p>
 *     This class presents routing schema for a service, result of the Clara composition
 *     compiler, parsing routing statements of a composition.
 *
 *     Contains Map that has keys = input service names, data from which are required
 *     logically to be ANDed. I.e. data from all services in the AND must be present
 *     in order for the receiving service to execute its service engine.
 *     Also contains a Set of names of all services that are linked to the service
 *     of interest, i.e. names of all services that this services will send it's output data.
 * </p>
 *
 * @author gurjyan
 * @version 1.x
 * @since 5/21/15
 */
public class Statement {

    // The Map that has keys = input service names, data from which are required
    // logically to be ANDed. I.e. data from all services in the AND must be present
    // in order for the receiving service to execute its service engine.
    private Map<String, EngineData> logAndInputs = new HashMap<>();

    // Names of all services that are linked to the service of interest, i.e. names
    // of all services that send data to this service
    private Set<String> inputLinks = new HashSet<>();

    // Names of all services that are linked to the service of interest, i.e. names
    // of all services that this services will send it's output data.
    private Set<String> outputLinks = new HashSet<>();

    // statement string
    private String statementString = xMsgConstants.UNDEFINED.toString();

    // The name of the service that this statement is relevant to.
    private String serviceName = xMsgConstants.UNDEFINED.toString();


    public Statement(String statementString, String serviceName) throws CException {
        if(statementString.contains(serviceName)) {
            this.statementString = statementString;
            this.serviceName = serviceName;
            process(statementString);
        } else {
            throw new CException("irrelevant statement");
        }
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getStatementString() {
        return statementString;
    }

    public Set<String> getInputLinks() {
        return inputLinks;
    }

    public Set<String> getOutputLinks() {
        return outputLinks;
    }

    public Map<String, EngineData> getLogAndInputs() {
        return logAndInputs;
    }


    /**
     * <p>
     * Analyses of the composition string
     * </p>
     *
     * @param statement string
     * @throws org.jlab.clara.base.CException
     */
    private void process(String statement) throws CException {
        // This is new routing statement
        // clear local containers
        inputLinks.clear();
        outputLinks.clear();
        logAndInputs.clear();

        // parse the new statement to find input and output
        // linked service names
             if (statement.contains(serviceName)) {
                 inputLinks = parse_linked(serviceName, statement, 0);

                 outputLinks = parse_linked(serviceName, statement, 1);

                if (is_log_and(serviceName, statement)) {
                    for(String sn: inputLinks){
                        logAndInputs.put(sn, null);
                    }
                }
            }
    }

    /**
     * <p>
     * Parses composition field of the transient data
     * and returns the list of services output linked
     * to this service, i.e. that are getting output
     * data of this service.
     * Attention: service name CAN NOT appear twice
     * in the composition.
     * </p>
     *
     * @param service_name   the name of the service
     *                       for which we find input/output links
     * @param statement    the string of the composition
     * @param link_direction 0 = input-inked, >0 = output-linked
     * @return the list containing names of linked services
     */
    private Set<String> parse_linked(String service_name,
                                     String statement,
                                     int link_direction) throws CException {

        // List of output service names
        Set<String> out_list = new HashSet<>();

        // List that contains composition elements
        Set<String> elm_list = new HashSet<>();

        StringTokenizer st = new StringTokenizer(statement, "+");
        while (st.hasMoreTokens()) {
            elm_list.add(st.nextToken());
        }

        // List that contains service names
        List<String> elm2_list = new ArrayList<>();
        for (String s : elm_list) {
            // remove  '&' from the service name
            // (e.g. 129.57.81.247:cont1:&Engine3 to 129.57.81.247:cont1:Engine3
            if (s.startsWith("&")) {
                s = s.replace("&", "");
            }
            elm2_list.add(s);
        }

        // See if the string contains this service name, and record the index,
        // and analyze index+1 element.
        // Note: multiple services can send to a single service, like: s1,s2+s3.
        // (this is the reason we use in:contains)
        int index = -1;
        for (String s : elm2_list) {
            if (s.contains(service_name)) {
                index = elm2_list.indexOf(s);
            }
        }
        if (index == -1) {
            throw new CException("Composition parsing exception. " +
                    "Service name can not be found in the composition.");
        } else {
            if (link_direction == 0 && index > 0) {
                // index of the next component in the composition
                index -= 1;
                String element = elm2_list.get(index);
                // the case to fan out the output of this service
                if (element.contains(",")) {
                    StringTokenizer st1 = new StringTokenizer(element, ",");
                    while (st1.hasMoreTokens()) {
                        out_list.add(st1.nextToken());
                    }
                } else {
                    out_list.add(element);
                }
                return out_list;
            } else if (link_direction > 0) {
                index += 1;
                if (elm2_list.size() > index) {
                    String element = elm2_list.get(index);
                    // the case to fan out the output of this service
                    if (element.contains(",")) {
                        StringTokenizer st1 = new StringTokenizer(element, ",");
                        while (st1.hasMoreTokens()) {
                            out_list.add(st1.nextToken());
                        }
                    } else {
                        out_list.add(element);
                    }
                }

                return out_list;
            }
        }
        // returns empty list. Most likely this service
        // is the first service in the composition
        return out_list;
    }

    /**
     * <p>
     * Check to see in the composition this service
     * is required to logically AND inputs before
     * executing its service
     * </p>
     *
     * @param service_name in the composition
     * @param composition  the string of the composition
     * @return true if component name is programmed
     * as "&<service_name>"
     */
    private boolean is_log_and(String service_name,
                              String composition) {
        String ac = "&" + service_name;

        // List that contains composition elements
        List<String> elm_list = new ArrayList<>();

        StringTokenizer st = new StringTokenizer(composition, "+");
        while (st.hasMoreTokens()) {
            elm_list.add(st.nextToken());
        }

        for (String s : elm_list) {
            if (s.equals(ac)) {
                return true;
            }
        }
        return false;
    }

}
