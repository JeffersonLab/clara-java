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

import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.util.ClaraUtil;
import org.jlab.coda.xmsg.core.xMsgConstants;

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
    private Set<String> inputLinks = new LinkedHashSet<>();

    // Names of all services that are linked to the service of interest, i.e. names
    // of all services that this services will send it's output data.
    private Set<String> outputLinks = new LinkedHashSet<>();

    // statement string
    private String statementString = xMsgConstants.UNDEFINED;

    // The name of the service that this statement is relevant to.
    private String serviceName = xMsgConstants.UNDEFINED;


    public Statement(String statementString, String serviceName) throws ClaraException {
        if(statementString.contains(serviceName)) {
            this.statementString = statementString;
            this.serviceName = serviceName;
            process(statementString);
        } else {
            throw new ClaraException("irrelevant statement");
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
     * @throws ClaraException
     */
    private void process(String statement) throws ClaraException {
        // This is new routing statement
        // clear local containers
        inputLinks.clear();
        outputLinks.clear();
        logAndInputs.clear();

        // parse the new statement to find input and output
        // linked service names
        if (statement.contains(serviceName)) {
            parse_linked(serviceName, statement);
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
     * @return the list containing names of linked services
     */
    private void parse_linked(String service_name,
                              String statement) throws ClaraException {

        // List that contains composition elements
        Set<String> elm_set = new LinkedHashSet<>();

        StringTokenizer st = new StringTokenizer(statement, "+");
        while (st.hasMoreTokens()) {
            String el = st.nextToken();
            el =  ClaraUtil.removeFirst(el, "&");
            el =  ClaraUtil.removeFirst(el, "{");
            elm_set.add(el);
        }

        // See if the string contains this service name, and record the index,
        // and analyze index+1 element.
        // Note: multiple services can send to a single service, like: s1,s2+s3.
        // (this is the reason we use in:contains)
        int index = -1;
        for (String s : elm_set) {
            index++;
            if (s.contains(service_name)) {
                break;
            }
        }
        if (index == -1) {
            throw new ClaraException("Routing statement parsing exception. " +
                    "Service name can not be found in the statement.");
        } else {
            int pIndex = index -1;
            if(pIndex >= 0) {
                String element = ClaraUtil.getJSetElementAt(elm_set, pIndex);
                // the case to fan out the output of this service
                if (element.contains(",")) {
                    StringTokenizer st1 = new StringTokenizer(element, ",");
                    while (st1.hasMoreTokens()) {
                        inputLinks.add(st1.nextToken());
                    }
                } else {
                    inputLinks.add(element);
                }
            }

            // define output links
            int nIndex = index +1;
            if (elm_set.size() > nIndex) {
                String element = ClaraUtil.getJSetElementAt(elm_set, nIndex);
                // the case to fan out the output of this service
                if (element.contains(",")) {
                    StringTokenizer st1 = new StringTokenizer(element, ",");
                    while (st1.hasMoreTokens()) {
                        outputLinks.add(st1.nextToken());
                    }
                } else {
                    outputLinks.add(element);
                }
            }
        }
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
        Set<String> elm_set = new LinkedHashSet<>();

        StringTokenizer st = new StringTokenizer(composition, "+");
        while (st.hasMoreTokens()) {
            elm_set.add(st.nextToken());
        }

        for (String s : elm_set) {
            if (s.equals(ac)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "Statement{" +
                "logAndInputs=" + logAndInputs +
                ", inputLinks=" + inputLinks +
                ", outputLinks=" + outputLinks +
                ", statementString='" + statementString + '\'' +
                ", serviceName='" + serviceName + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Statement)) return false;

        Statement statement = (Statement) o;

        if (inputLinks != null ? !inputLinks.equals(statement.inputLinks) : statement.inputLinks != null) return false;
        if (logAndInputs != null ? !logAndInputs.equals(statement.logAndInputs) : statement.logAndInputs != null)
            return false;
        if (outputLinks != null ? !outputLinks.equals(statement.outputLinks) : statement.outputLinks != null)
            return false;
        if (!serviceName.equals(statement.serviceName)) return false;
        if (statementString != null ? !statementString.equals(statement.statementString) : statement.statementString != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = logAndInputs != null ? logAndInputs.hashCode() : 0;
        result = 31 * result + (inputLinks != null ? inputLinks.hashCode() : 0);
        result = 31 * result + (outputLinks != null ? outputLinks.hashCode() : 0);
        result = 31 * result + (statementString != null ? statementString.hashCode() : 0);
        result = 31 * result + serviceName.hashCode();
        return result;
    }
}
