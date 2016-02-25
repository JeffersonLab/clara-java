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

package org.jlab.clara.sys.ccc;

import org.jlab.clara.base.error.ClaraException;
import org.jlab.coda.xmsg.core.xMsgConstants;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *     Defines it's own as well as all input service states that if
 *     exists in run-time will make this CLARA composition condition
 *     a true condition.
 * <p>
 *
 * @author gurjyan
 * @version 4.x
 * @since 5/21/15
 */
public class Condition {

    // States of services that are required to be present in order this condition to be true
    private Set<ServiceState> andStates = new LinkedHashSet<>();

    // NOT states of services that are required to be present in order this condition to be true
    private Set<ServiceState> andNotStates = new LinkedHashSet<>();

    // Required states of services that will make this statement true
    private Set<ServiceState> orStates = new LinkedHashSet<>();

    // Required states of services that will make this statement true
    private Set<ServiceState> orNotStates = new LinkedHashSet<>();

    // The name of the service that this condition is relevant to.
    private String serviceName = xMsgConstants.UNDEFINED;

    public Condition(String conditionString, String serviceName) throws ClaraException {
        this.serviceName = serviceName;
        process(conditionString);
    }

    public String getServiceName() {
        return serviceName;
    }


    public Set<ServiceState> getAndStates() {
        return andStates;
    }

    private void addAndState(ServiceState andState) {
        this.andStates.add(andState);
    }

    public Set<ServiceState> getAndNotStates() {
        return andNotStates;
    }

    private void addAndNotState(ServiceState andNotState) {
        this.andNotStates.add(andNotState);
    }

    public Set<ServiceState> getOrStates() {
        return orStates;
    }

    private void addOrState(ServiceState orState) {
        this.orStates.add(orState);
    }

    public Set<ServiceState> getOrNotStates() {
        return orNotStates;
    }

    private void addOrNotState(ServiceState orState) {
        this.orNotStates.add(orState);
    }


    private void process(String cs) throws ClaraException {
        if (cs.contains("(")) {
            cs = cs.replaceAll("\\(", "");
        }
        if (cs.contains(")")) {
            cs = cs.replaceAll("\\)", "");
        }

        if (cs.contains("&&")) {
            parseCondition(cs, "&&");
        } else if (cs.contains("!!")) {
            parseCondition(cs, "!!");
        } else {
            parseCondition(cs, null);
        }
    }

    private void parseCondition(String cs, String logicOperator) throws ClaraException {


        StringTokenizer t0, t1;
        if (logicOperator == null) {
            Pattern p = Pattern.compile(CCompiler.sCond);
            Matcher m = p.matcher(cs);
            if (m.matches()) {
                if (cs.contains("!=")) {
                    t1 = new StringTokenizer(cs, "!=\"");
                    if (t1.countTokens() != 2) {
                        throw new ClaraException("syntax error: malformed conditional statement");
                    }
                    ServiceState sst = new ServiceState(t1.nextToken(), t1.nextToken());
                    addOrNotState(sst);

                } else if (cs.contains("==")) {
                    t1 = new StringTokenizer(cs, "==\"");
                    if (t1.countTokens() != 2) {
                        throw new ClaraException("syntax error: malformed conditional statement");
                    }
                    ServiceState sst = new ServiceState(t1.nextToken(), t1.nextToken());
                    addOrState(sst);

                } else {
                    throw new ClaraException("syntax error: malformed conditional statement");
                }
            } else {
                throw new ClaraException("syntax error: malformed conditional statement");
            }

        } else {

            if (cs.contains("&&") && !cs.contains("!!")) {
                t0 = new StringTokenizer(cs, logicOperator);
                while (t0.hasMoreTokens()) {
                    String ac = t0.nextToken();

                    Pattern p = Pattern.compile(CCompiler.sCond);
                    Matcher m = p.matcher(ac);
                    if (m.matches()) {

                        if (ac.contains("!=")) {
                            t1 = new StringTokenizer(t0.nextToken(), "!=\"");
                            if (t1.countTokens() != 2) {
                                throw new ClaraException("syntax error: malformed conditional statement");
                            }
                            ServiceState sst = new ServiceState(t1.nextToken(), t1.nextToken());
                            addAndNotState(sst);

                        } else if (ac.contains("==")) {
                            t1 = new StringTokenizer(t0.nextToken(), "==\"");
                            if (t1.countTokens() != 2) {
                                throw new ClaraException("syntax error: malformed conditional statement");
                            }
                            ServiceState sst = new ServiceState(t1.nextToken(), t1.nextToken());
                            addAndState(sst);

                        } else {
                            throw new ClaraException("syntax error: malformed conditional statement");
                        }
                    } else {
                        throw new ClaraException("syntax error: malformed conditional statement");
                    }
                }
            } else if (cs.contains("!!") && !cs.contains("&&")) {
                t0 = new StringTokenizer(cs, logicOperator);
                while (t0.hasMoreTokens()) {
                    String ac = t0.nextToken();

                    Pattern p = Pattern.compile(CCompiler.sCond);
                    Matcher m = p.matcher(ac);
                    if (m.matches()) {

                        if (ac.contains("!=")) {
                            t1 = new StringTokenizer(t0.nextToken(), "!=\"");
                            if (t1.countTokens() != 2) {
                                throw new ClaraException("syntax error: malformed conditional statement");
                            }
                            ServiceState sst = new ServiceState(t1.nextToken(), t1.nextToken());
                            addOrNotState(sst);

                        } else if (ac.contains("==")) {
                            t1 = new StringTokenizer(t0.nextToken(), "==\"");
                            if (t1.countTokens() != 2) {
                                throw new ClaraException("syntax error: malformed conditional statement");
                            }
                            ServiceState sst = new ServiceState(t1.nextToken(), t1.nextToken());
                            addOrState(sst);

                        } else {
                            throw new ClaraException("syntax error: malformed conditional statement");
                        }
                    } else {
                        throw new ClaraException("syntax error: malformed conditional statement");
                    }
                }
            } else {
                throw new ClaraException("syntax error: malformed or unsupported conditional statement");
            }
        }
    }

    /**
     * Returns true if passed states make this condition true.
     *
     * @return true/false
     */
    public boolean isTrue(ServiceState ownerSS, ServiceState inputSS){

        boolean checkAnd = getAndStates().isEmpty() || checkANDCondition(getAndStates(), ownerSS, inputSS);
        boolean checkAndNot = getAndNotStates().isEmpty() || !checkANDCondition(getAndNotStates(), ownerSS, inputSS);
        boolean checkOr = getOrStates().isEmpty() || checkORCondition(getOrStates(), ownerSS, inputSS);
        boolean checkOrNot = getOrNotStates().isEmpty() || !checkORCondition(getOrNotStates(), ownerSS, inputSS);

        return checkAnd && checkAndNot && checkOr && checkOrNot;
    }

    private boolean checkANDCondition(Set<ServiceState> sc, ServiceState s1, ServiceState s2){
        if (sc.contains(s1) && sc.contains(s2)) {
            return true;
        }
        return false;
    }

    private boolean checkORCondition(Set<ServiceState> sc, ServiceState s1, ServiceState s2){
        if (sc.contains(s1) || sc.contains(s2)) {
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return "Condition{" +
                "andStates=" + andStates +
                ", andNotStates=" + andNotStates +
                ", orStates=" + orStates +
                ", orNotStates=" + orNotStates +
                ", serviceName='" + serviceName + '\'' +
                '}';
    }
}
